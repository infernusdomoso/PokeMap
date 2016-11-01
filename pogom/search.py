#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Search Architecture:
 - Have a list of accounts
 - Create an "overseer" thread
 - Search Overseer:
   - Tracks incoming new location values
   - Tracks "paused state"
   - During pause or new location will clears current search queue
   - Starts search_worker threads
 - Search Worker Threads each:
   - Have a unique API login
   - Listens to the same Queue for areas to scan
   - Can re-login as needed
   - Pushes finds to db queue and webhook queue
'''

import pprint
import logging
import math
import json
import os
import random
import time
import geopy
import geopy.distance
import re
import smtplib
import ssl
from email.mime.text import MIMEText

from operator import itemgetter
from threading import Thread
from queue import Queue, Empty

from pgoapi import PGoApi
from pgoapi.utilities import f2i
from pgoapi import utilities as util
from pgoapi.exceptions import AuthException
from datetime import datetime, timedelta
from base64 import b64encode, b64decode

from .models import parse_map, Pokemon, hex_bounds, GymDetails, parse_gyms, ScannedLocation, CurrentLocation, calc_distance, Vars, PokeAlert, AlertLog, Locations, LocLog, captcha
from .transform import generate_location_steps,mults_for_loc
from .fakePogoApi import FakePogoApi
from .utils import now, get_pokemon_name, get_pokemon_rarity, get_pokemon_types, get_args

import terminalsize

log = logging.getLogger(__name__)

bots = 1

TIMESTAMP = '\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000'


# Apply a location jitter
def jitterLocation(location=None, maxMeters=10):
    origin = geopy.Point(location[0], location[1])
    b = random.randint(0, 360)
    d = math.sqrt(random.random()) * (float(maxMeters) / 1000)
    destination = geopy.distance.distance(kilometers=d).destination(origin, b)
    return (destination.latitude, destination.longitude, location[2])


# gets the current time past the hour
def cur_sec():
    return (60 * time.gmtime().tm_min) + time.gmtime().tm_sec


# Thread to handle user input
def switch_status_printer(display_enabled, current_page):
    # Get a reference to the root logger
    mainlog = logging.getLogger()
    # Disable logging of the first handler - the stream handler, and disable it's output
    mainlog.handlers[0].setLevel(logging.CRITICAL)

    while True:
        # Wait for the user to press a key
        command = raw_input()

        if command == '':
            # Switch between logging and display.
            if display_enabled[0]:
                # Disable display, enable on screen logging
                mainlog.handlers[0].setLevel(logging.DEBUG)
                display_enabled[0] = False
            else:
                # Enable display, disable on screen logging (except for critical messages)
                mainlog.handlers[0].setLevel(logging.CRITICAL)
                display_enabled[0] = True
        elif command.isdigit():
                current_page[0] = int(command)


# Thread to print out the status of each worker
def status_printer(threadStatus, search_items_queue, db_updates_queue, wh_queue):
    display_enabled = [True]
    current_page = [1]

    # Start another thread to get user input
    t = Thread(target=switch_status_printer,
               name='switch_status_printer',
               args=(display_enabled, current_page))
    t.daemon = True
    t.start()

    while True:
        if display_enabled[0]:

            # Get the terminal size
            width, height = terminalsize.get_terminal_size()
            # Queue and overseer take 2 lines.  Switch message takes up 2 lines.  Remove an extra 2 for things like screen status lines.
            usable_height = height - 6
            # Prevent people running terminals only 6 lines high from getting a divide by zero
            if usable_height < 1:
                usable_height = 1

            # Create a list to hold all the status lines, so they can be printed all at once to reduce flicker
            status_text = []

            # Calculate total skipped items
            skip_total = 0
            for item in threadStatus:
                if 'skip' in threadStatus[item]:
                    skip_total += threadStatus[item]['skip']

            # Print the queue length
            status_text.append('Queues: {} search items, {} db updates, {} webhook.  Total skipped items: {}'.format(search_items_queue.qsize(), db_updates_queue.qsize(), wh_queue.qsize(), skip_total))

            # Print status of overseer
            status_text.append('{} Overseer: {}'.format(threadStatus['Overseer']['method'], threadStatus['Overseer']['message']))

            # Calculate the total number of pages.  Subtracting 1 for the overseer.
            total_pages = math.ceil((len(threadStatus) - 1) / float(usable_height))

            # Prevent moving outside the valid range of pages
            if current_page[0] > total_pages:
                current_page[0] = total_pages
            if current_page[0] < 1:
                current_page[0] = 1

            # Calculate which lines to print
            start_line = usable_height * (current_page[0] - 1)
            end_line = start_line + usable_height
            current_line = 1

            # longest username
            userlen = 4
            for item in threadStatus:
                if threadStatus[item]['type'] == 'Worker':
                    userlen = max(userlen, len(threadStatus[item]['user']))

            # How pretty
            status = '{:10} | {:' + str(userlen) + '} | {:7} | {:6} | {:5} | {:7} | {:10}'

            # Print the worker status
            status_text.append(status.format('Worker ID', 'User', 'Success', 'Failed', 'Empty', 'Skipped', 'Message'))
            for item in sorted(threadStatus):
                if(threadStatus[item]['type'] == 'Worker'):
                    current_line += 1

                    # Skip over items that don't belong on this page
                    if current_line < start_line:
                        continue
                    if current_line > end_line:
                        break

                    status_text.append(status.format(item, threadStatus[item]['user'], threadStatus[item]['success'], threadStatus[item]['fail'], threadStatus[item]['noitems'], threadStatus[item]['skip'], threadStatus[item]['message']))

            status_text.append('Page {}/{}.  Type page number and <ENTER> to switch pages.  Press <ENTER> alone to switch between status and log view'.format(current_page[0], total_pages))
            # Clear the screen
            os.system('cls' if os.name == 'nt' else 'clear')
            # Print status
            print "\n".join(status_text)
        time.sleep(1)


# The main search loop that keeps an eye on the over all process
def search_overseer_thread(args, method, new_location_queue, pause_bit, encryption_lib_path, db_updates_queue, wh_queue):

    log.info('Search overseer starting')
    Vars.set("scanradius", args.step_limit)
    Vars.set("autoloc", 0)
    Vars.clear("TempLat")
    Vars.clear("TempLon")
    captcha.clear()

    search_items_queue = Queue()
    threadStatus = {}
    search_id = 0
    
    threadStatus['Overseer'] = {
        'message': 'Initializing',
        'type': 'Overseer',
        'method': 'Hex Grid' if method == 'hex' else 'Spawn Point'
    }

    if(args.print_status):
        log.info('Starting status printer thread')
        t = Thread(target=status_printer,
                   name='status_printer',
                   args=(threadStatus, search_items_queue, db_updates_queue, wh_queue))
        t.daemon = True
        t.start()

    # Create a search_worker_thread per account
    log.info('Starting search worker threads')

    current_location = False

    num_accounts = len(args.accounts)
    for i, account in enumerate(args.accounts):
        bot = 0
        type = 'Worker'
        if i == num_accounts-bots:
            bot = 1
            log.info('Starting bot thread %d for user %s', i, account['username'])
            type = 'Bot'
        else:
            log.debug('Starting search worker thread %d for user %s', i, account['username'])
        workerId = 'Worker {:03}'.format(i)
        threadStatus[workerId] = {
            'type': type,
            'message': 'Creating thread...',
            'success': 0,
            'fail': 0,
            'noitems': 0,
            'skip': 0,
            'user': account['username']
        }

        t = Thread(target=search_worker_thread,
                   name='search-w{}'.format(i),
                   args=(args, account, search_items_queue, pause_bit,
                         encryption_lib_path, threadStatus[workerId],
                         db_updates_queue, wh_queue, i, new_location_queue, bot))
        t.daemon = True
        t.start()

    '''
    For hex scanning, we can generate the full list of scan points well
    in advance. When then can queue them all up to be searched as fast
    as the threads will allow.

    With spawn point scanning (sps) we can come up with the order early
    on, and we can populate the entire queue, but the individual threads
    will need to wait until the point is available (and ensure it is not
    to late as well).
    '''

    # A place to track the current location

    # Used to tell SPS to scan for all CURRENT pokemon instead
    # of, like during a normal loop, just finding the next one
    # which will appear (since you've already scanned existing
    # locations in the prior loop)
    # Needed in a first loop and pausing/changing location.
    sps_scan_current = True

    # The real work starts here but will halt on pause_bit.set()
    while True:

        # paused; clear queue if needed, otherwise sleep and loop
        while pause_bit.is_set():
            if not search_items_queue.empty():
                try:
                    while True:
                        search_items_queue.get_nowait()
                except Empty:
                    pass
            threadStatus['Overseer']['message'] = 'Scanning is paused'
            sps_scan_current = True
            time.sleep(1)


        # If a new location has been passed to us, get the most recent one
        newlocs = Vars.get("newlocs")
        if newlocs == 1:
            Vars.set("newlocs", "0")
            if not search_items_queue.empty():
                try:
                    while True:
                        search_items_queue.get_nowait()
                except Empty:
                    pass

        if not new_location_queue.empty():
            log.info('New location caught, moving search grid')
            Vars.clear("TempLat")
            Vars.clear("TempLon")
            sps_scan_current = True
            try:
                while True:
                    current_location = new_location_queue.get_nowait()
            except Empty:
                pass
                
            CurrentLocation.set_loc(current_location[0],  current_location[1])

            # We (may) need to clear the search_items_queue
            if not search_items_queue.empty():
                try:
                    while True:
                        search_items_queue.get_nowait()
                except Empty:
                    pass

        # If there are no search_items_queue either the loop has finished (or been
        # cleared above) -- either way, time to fill it back up
        if search_items_queue.empty():
            log.debug('Search queue empty, restarting loop')
            search_id = search_id + 1

            # locations = [((lat, lng, alt), ts_appears, ts_leaves),...]
            if method == 'hex':
                locations = get_hex_location_list(args, current_location, search_id)
            else:
                locations = get_sps_location_list(args, current_location, sps_scan_current, search_id)
                sps_scan_current = False

            if len(locations) == 0:
                log.warning('Nothing to scan!')

            threadStatus['Overseer']['message'] = 'Queuing steps'
            for step, step_location in enumerate(locations, 1):
                log.debug('Queueing step %d @ %f/%f/%f', step, step_location[0][0], step_location[0][1], step_location[0][2])
                search_args = (step, step_location[0], step_location[1], step_location[2], step_location[3])
                search_items_queue.put(search_args)
        else:
            nextitem = search_items_queue.queue[0]
            threadStatus['Overseer']['message'] = 'Processing search queue, next item is {:6f},{:6f}'.format(nextitem[1][0], nextitem[1][1])
            # If times are specified, print the time of the next queue item, and how many seconds ahead/behind realtime
            if nextitem[2]:
                threadStatus['Overseer']['message'] += ' @ {}'.format(time.strftime('%H:%M:%S', time.localtime(nextitem[2])))
                if nextitem[2] > now():
                    threadStatus['Overseer']['message'] += ' ({}s ahead)'.format(nextitem[2] - now())
                else:
                    threadStatus['Overseer']['message'] += ' ({}s behind)'.format(now() - nextitem[2])

        # Now we just give a little pause here
        time.sleep(1)


def get_hex_location_list(args, current_location, search_id):
    # if we are only scanning for pokestops/gyms, then increase step radius to visibility range
    if args.no_pokemon:
        step_distance = 0.900
    else:
        step_distance = 0.070

    # update our list of coords
    dohex = 1
    aloc = Vars.get("autoloc")
    
    if aloc == 1:
        driving = Vars.get("driving")
        if driving:
            nl = LocLog.predict_loc()
            locations = generate_location_steps((nl[0], nl[1], 0), len(args.username)-bots, step_distance, "line", nl[2])
            Vars.set("driving", "1")
            dohex = 0
        
    if dohex:
        locations = generate_location_steps(current_location, Vars.get("scanradius"), step_distance, "hex")
        Vars.set("driving", "0")

    # In hex "spawns only" mode, filter out scan locations with no history of pokemons
    if args.spawnpoints_only and not args.no_pokemon:
        n, e, s, w = hex_bounds(current_location, Vars.get("scanradius"))
        spawnpoints = set((d['latitude'], d['longitude']) for d in Pokemon.get_spawnpoints(s, w, n, e))

        if len(spawnpoints) == 0:
            log.warning('No spawnpoints found in the specified area! (Did you forget to run a normal scan in this area first?)')

        def any_spawnpoints_in_range(coords):
            return any(geopy.distance.distance(coords, x).meters <= 70 for x in spawnpoints)

        locations = [coords for coords in locations if any_spawnpoints_in_range(coords)]

    # put into the right struture with zero'ed before/after values
    # locations = [(lat, lng, alt, ts_appears, ts_leaves),...]
    locationsZeroed = []
    for location in locations:
        locationsZeroed.append(((location[0], location[1], 0), 0, 0, search_id))

    return locationsZeroed


def get_sps_location_list(args, current_location, sps_scan_current, location_id):
    locations = []

    # Attempt to load spawns from file
    if args.spawnpoint_scanning != 'nofile':
        log.debug('Loading spawn points from json file @ %s', args.spawnpoint_scanning)
        try:
            with open(args.spawnpoint_scanning) as file:
                locations = json.load(file)
        except ValueError as e:
            log.exception(e)
            log.error('JSON error: %s; will fallback to database', e)
        except IOError as e:
            log.error('Error opening json file: %s; will fallback to database', e)

    # No locations yet? Try the database!
    if not len(locations):
        log.debug('Loading spawn points from database')
        locations = Pokemon.get_spawnpoints_in_hex(current_location, Vars.get("scanradius"))

    # Well shit...
    if not len(locations):
        raise Exception('No availabe spawn points!')

    # locations[]:
    # {"lat": 37.53079079414139, "lng": -122.28811690874117, "spawnpoint_id": "808f9f1601d", "time": 511

    log.info('Total of %d spawns to track', len(locations))

    locations.sort(key=itemgetter('time'))

    if args.verbose or args.very_verbose:
        for i in locations:
            sec = i['time'] % 60
            minute = (i['time'] / 60) % 60
            m = 'Scan [{:02}:{:02}] ({}) @ {},{}'.format(minute, sec, i['time'], i['lat'], i['lng'])
            log.debug(m)

    # 'time' from json and db alike has been munged to appearance time as seconds after the hour
    # Here we'll convert that to a real timestamp
    for location in locations:
        # For a scan which should cover all CURRENT pokemon, we can offset
        # the comparison time by 15 minutes so that the "appears" time
        # won't be rolled over to the next hour.

        # TODO: Make it work. The original logic (commented out) was producing
        #       bogus results if your first scan was in the last 15 minute of
        #       the hour. Wrapping my head around this isn't work right now,
        #       so I'll just drop the feature for the time being. It does need
        #       to come back so that repositioning/pausing works more nicely,
        #       but we can live without it too.

        # if sps_scan_current:
        #     cursec = (location['time'] + 900) % 3600
        # else:
        cursec = location['time']

        if cursec > cur_sec():
            # hasn't spawn in the current hour
            from_now = location['time'] - cur_sec()
            appears = now() + from_now
        else:
            # won't spawn till next hour
            late_by = cur_sec() - location['time']
            appears = now() + 3600 - late_by

        location['appears'] = appears
        location['leaves'] = appears + 900

    # Put the spawn points in order of next appearance time
    locations.sort(key=itemgetter('appears'))

    # Match expected structure:
    # locations = [((lat, lng, alt), ts_appears, ts_leaves),...]
    retset = []
    for location in locations:
        retset.append(((location['lat'], location['lng'], 40.32), location['appears'], location['leaves']))

    return retset

def random_latlon(zones):
    minx = 1000
    maxx = -1000
    miny = 1000
    maxy = -1000
    for z in zones:
        ul = z[0]
        lr = z[1]
        if ul[1] < minx:
            minx = ul[1]
        if lr[1] > maxx:
            maxx = lr[1]
        if lr[0] < miny:
            miny = lr[0]
        if ul[0] > maxy:
            maxy = ul[0]
    
    xrange = (maxx - minx) * 1000000
    yrange = (maxy - miny) * 1000000
    
    xloc = 0
    yloc = 0
    
    while 1:
        xrand = (random.random() * xrange) / 1000000
        yrand = (random.random() * yrange) / 1000000
    
        yloc = miny + yrand
        xloc = minx + xrand
        for z in zones:
            if xloc >= z[0][1] and xloc <= z[1][1] and yloc <= z[0][0] and yloc >= z[1][0]:
              return (yloc, xloc)
              

def search_worker_thread(args, account, search_items_queue, pause_bit, encryption_lib_path, status, dbq, whq, worker_id, lq, bot):

    zones = list()
    # 83 COrridor to York
    zones.append(((40.022943, -76.831876), (39.486616, -76.587430)))
    # Baltimore Area
    zones.append(((39.464355, -76.855222), (39.253054, -76.451474)))
    # 70 / Frederick / Silver Spring
    zones.append(((39.362592, -77.115225), (39.028655, -76.724420)))

    stagger_thread(args, account)

    log.debug('Search worker thread starting')
    
    poke_stat_list = list()    
    bot = 0
    botting = 0
    if bot and not botting:
        botting = 1
    tryagain = 0
    last_bot = datetime.now()

    # The forever loop for the thread
    while True:
        try:
            # New lease of life right here
            status['fail'] = 0
            status['success'] = 0
            status['noitems'] = 0
            status['skip'] = 0

            # Create the API instance this will use
            if args.mock != '':
                api = FakePogoApi(args.mock)
            else:
                api = PGoApi()

            if args.proxy:
                api.set_proxy({'http': args.proxy, 'https': args.proxy})

            api.activate_signature(encryption_lib_path)

            # The forever loop for the searches
            no_searches = 1
            last_search_id = 0
            while True:

                # If this account has been messing up too hard, let it rest
                if status['fail'] >= args.max_failures:
                    end_sleep = now() + (3600 * 2)
                    long_sleep_started = time.strftime('%H:%M:%S')
                    while now() < end_sleep:
                        status['message'] = 'Worker {} failed more than {} scans; possibly banned account. Sleeping for 2 hour sleep as of {}'.format(account['username'], args.max_failures, long_sleep_started)
                        log.error(status['message'])
                        time.sleep(300)
                    break  # exit this loop to have the API recreated

                while pause_bit.is_set():
                    status['message'] = 'Scanning paused'
                    time.sleep(2)
                
                capt = captcha.get(account['username'])
                if capt['hold']:
                    time.sleep(5)
                    log.info("Waiting on challenge")
                    continue
                elif capt['answer'] is not None:
                    r = api.verify_challenge(token=capt['answer'])
                    if r['responses']['VERIFY_CHALLENGE'].get('success'):
                        log.info("Challenge cleared")
                        captcha.clear(account['username'])
                    else:
                        log.info("Answer submit error? %s", r)
                        time.sleep(5)
                        continue

                statsearch = 0
                if bot and not botting and last_bot < datetime.now()-timedelta(minutes=5):
                    botting = 1
                    last_bot = datetime.now()
                if len(poke_stat_list):
                    poke = poke_stat_list.pop()
                    if poke['disappear_time'] >= (datetime.utcnow() + timedelta(minutes=2)):
                        log.info("Pulling stats for pokemon ID %d at %.6f,%.6f", poke['pokemon_id'], poke['latitude'], poke['longitude'])
                        stats = Pokemon.get_stats(poke, api)
                        statsearch = 1
                        p_alert = PokeAlert.alert(poke['pokemon_id'])
                        highstat = 0
                        maxdist = .5
                        if p_alert == 3:
                            maxdist = 2

                        if (stats['att'] + stats['deff'] + stats['hp']) >= 43:
                            highstat = 1
                            if p_alert == 2:
                                maxdist = 1
                            elif p_alert == 1:
                                p_alert = 2
                        if p_alert >= 2 and not Pokemon.alert_sent(b64encode(str(poke['encounter_id']))):
                            cloc = CurrentLocation.get_temploc()
                            clocc = (cloc['latitude'], cloc['longitude'])
                            dist = calc_distance(clocc, (poke['latitude'], poke['longitude']))
                            dt = (stats['disappear_time'] - datetime(1970, 1, 1)).total_seconds() - time.time()
                            dtcalc = (dt - 120) / dist
                            dtmin = math.floor(dt / 60)
                            dtsec = dt % 60
                            if dtcalc >= 300 and dist <= maxdist:
                                pname = get_pokemon_name(poke['pokemon_id'])
                                if highstat:
                                    pname = "High Stat %s" % pname
                                ddt = datetime.fromtimestamp((time.time() + dt))
                                log.info('Sending alert for a %s found at %f, %f', pname, poke['latitude'], poke['longitude'])
                                msg = "%s found %.2fkm away.\nStats: %d/%d/%d\nExpires at %s.\ngooglechrome://unbridledgames.com:18222/?zoom=16&loc=%f,%f" % (get_pokemon_name(poke['pokemon_id']), dist, stats['att'], stats['deff'], stats['hp'], ddt.strftime('%I:%M%p'), stats['latitude'], stats['longitude'])
                                  
                                smtpObj = smtplib.SMTP('smtp.gmail.com:587')
                                smtpObj.ehlo()
                                smtpObj.starttls()
                                smtpObj.login("pokealert88@gmail.com","Ferret67")
                                try:
                                    efrom = 'pokealert88@gmail.com'
                                    eto = 'pokealert@icloud.com'
                                    if p_alert == 3:
                                        eto = 'pokealerthp@icloud.com'
                                    
                                    msg = 'From: "PokeAlert" <pokealert88@gmail.com>\nTo: "PokeAlert" <%s>\nSubject: %s found %.2fkm away\n\n%s' % (eto, pname, dist, msg)
                                    Pokemon.sent_alert(b64encode(str(poke['encounter_id'])))
                                    smtpObj.sendmail(efrom, eto, msg)
                                except Exception as e:
                                    log.info('Failed sending email %s', e)
                                AlertLog.log(b64encode(str(poke['encounter_id'])))     
                            else:
                                log.info('Found a %s, but it is %.2fkm away and expires in %d:%02d', get_pokemon_name(poke['pokemon_id']), dist, dtmin, dtsec)
                elif bot and botting:
#bot code here
                    if botting == 1:
                        pokeballs = 0
                        cl = CurrentLocation.get_loc(1)
                        if check_login(args, account, api, cl):
                            botting = 0
                            continue
                        api.set_position(cl[0], cl[1], 0)
                        inv = api.get_inventory()
                        r = inv['responses']['GET_INVENTORY']['inventory_delta']['inventory_items'];
                        for itm in r:
                            itm = itm['inventory_item_data']
                            if 'item' in itm:
                                if itm['item']['item_id'] == 1:
                                    pokeballs = itm['item']['count']
                                    log.info("I have %d pokeballs", pokeballs)
                        if pokeballs < 30:
                            botting = 2
                        else:
                            botting = 4
                    elif botting == 2:
                        pokestop_to_hit = []
                        log.info("Moving to pokestop")
                        botting = 3
                        imat = (39.497547, -76.654729, 0)
                        api.set_position(*imat)
                        response_dict = map_request(api, (39.497547, -76.654729, 0), args.jitter)
                        cells = response_dict['responses']['GET_MAP_OBJECTS']['map_cells']
                        for cell in cells:
                            for f in cell.get('forts', []):
                                if f.get('type') == 1 and f.get('enabled'):
                                    if f.get('cooldown_complete_timestamp_ms') and f.get('cooldown_complete_timestamp_ms') > long(int(round(time.time() * 1000))):
                                        continue
                                    dist = calc_distance(imat, (f['latitude'], f['longitude'], 0))
                                    if dist <= .035:
                                        log.info("Found pokestop: %.6f, %.6f", f['latitude'], f['longitude'])
                                        pokestop_to_hit.append(f);
                        botting = 3
                    elif botting == 3:
                        if len(pokestop_to_hit):
                            log.info("Hitting pokestops...")
                            pokestop = pokestop_to_hit.pop()
                            newballs = 0
                            
                            dict = api.fort_search(fort_id = pokestop['id'], player_latitude=imat[0], player_longitude=imat[1], fort_latitude=pokestop['latitude'], fort_longitude=pokestop['longitude'])
                            x = pprint.pformat(dict, width=1)
                            items = dict['responses']['FORT_SEARCH']['items_awarded']
                            for itm in items:
                                if itm['item_id'] == 1:
                                    newballs += 1
                            if newballs:
                                pokeballs += newballs
                                log.info("I got %d new pokeballs. %d total now.", newballs, pokeballs)
                        else:
                            log.info("No pokestops left, checking for pokemon")
                            botting = 4
                    elif botting == 4:
                        if pokeballs < 5:
                            log.info("Less than 5 pokeballs left, going for more")
                            botting = 2
                        else:
                            if not tryagain:
                                poke = Pokemon.catchable()
                            if poke is not None:
                                if not tryagain:
                                    pname = get_pokemon_name(poke['pokemon_id'])
                                    log.info("Attempting to catch a %s (%s), getting into position", pname, poke['encounter_id'])
                                    myloc = jitterLocation((poke['latitude'], poke['longitude'], 0), 20)
                                    api.set_position(*myloc)
                                    mapreq = map_request(api, myloc)
                                    time.sleep(2 + (2* random.random()));
                                    log.info("Encountering the %s", pname)
                                rett = api.encounter(encounter_id = long(b64decode(poke['encounter_id'])), spawn_point_id = poke['spawnpoint_id'], player_latitude = myloc[0], player_longitude = myloc[0])
                                if rett['responses']['ENCOUNTER']['status'] == 6:
                                    log.info("Uncatchable. Encounter status 6. Marking caught")
                                    Pokemon.caught(poke['encounter_id'])
                                    time.sleep(2)
                                    tryagain = 0
                                    continue
                                elif rett['responses']['ENCOUNTER']['status'] != 1:
                                    log.info("Weird encounter status: %s", rett)
                                    time.sleep(2)
                                    continue
                                else: 
                                    rrett = rett['responses']['ENCOUNTER']['wild_pokemon']['pokemon_data']
                                    log.info("%s: %d/%d/%d, catch chance: %.2f%%", pname, rrett.get('individual_attack', 0), rrett.get('individual_defense', 0), rrett.get('individual_stamina', 0), rett['responses']['ENCOUNTER']['capture_probability']['capture_probability'][0] * 100)
                                    time.sleep(1.5 + (2 * random.random()));
                                tryagain = 0
                                log.info("Chucking a ball...")
                                cres = api.catch_pokemon(encounter_id = long(b64decode(poke['encounter_id'])), spawn_point_id = poke['spawnpoint_id'], hit_pokemon=True, normalized_reticle_size=1.10 + (.85 * random.random()), normalized_hit_position=1, spin_modifier = 0.0, pokeball=1)
                                pokeballs -= 1
                                if cres['responses']['CATCH_POKEMON']['status'] == 1:
                                    Pokemon.caught(poke['encounter_id'])
                                    log.info("Caught a %s!",  pname)
                                elif cres['responses']['CATCH_POKEMON']['status'] == 3:
                                    log.info("Missed - trying again.")
                                    tryagain = 1
                                else:
                                    log.info("Missed? %s", cres)
                            else:
                                log.info("No catchable pokemon currently. Searching.")
                                last_bot = datetime.now()
                                botting = 0
                            
                else:
                    # Check if we're driving or not
                    aloc = Vars.get("autoloc")
                    
                    if aloc == 1:
                        mph = LocLog.lastspeed()
                        driving = Vars.get("driving")
                        if mph >= 30 and mph <= 90 and not driving:
                            log.info("Moving %d MPH, driving, searching forward path", mph)
                            Vars.set("driving", "1")
                            Vars.set("newlocs", "1")
                            Vars.clear("TempLat")
                            Vars.clear("TempLon")
                            ll = LocLog.lastloc()
                            CurrentLocation.set_loc(ll[0], ll[1])
                            if not search_items_queue.empty():
                                try:
                                    while True:
                                        search_items_queue.get_nowait()
                                except Empty:
                                    pass
                            continue
                        elif mph < 30 and driving:
                            log.info("Slowed to %d MPH, setting search loc to new area", mph)
                            if not search_items_queue.empty():
                                try:
                                    while True:
                                        search_items_queue.get_nowait()
                                except Empty:
                                    pass
                            Vars.set("driving", "0")
                            Vars.set("newlocs", 1)
                            ll = LocLog.lastloc()
                            lat = ll[0]
                            lon = ll[1]
                            lq.put((lat, lon, 0))
                            CurrentLocation.set_loc(lat, lon)
                            continue
                        elif not driving and (mph < 30 or mph > 90):
                            cl = CurrentLocation.get_loc(1)
                            ll = LocLog.lastloc()
                            if ll[3] < (datetime.now()-timedelta(minutes=10)):
                                ll = cl
                            if cl != ll:
                                dist = calc_distance(cl, ll)
                                if dist > .07:
                                    lq.put(ll)
                                    CurrentLocation.set_loc(ll[0], ll[1])
                                    Vars.clear("TempLat")
                                    Vars.clear("TempLon")
                                    log.info('Autoloc set, distance changed by %.2fm, setting new scan location: %s,%s', dist * 1000, ll[0], ll[1])
                                    Vars.set("newlocs", 1)
                                    if not search_items_queue.empty():
                                        try:
                                            while True:
                                                search_items_queue.get_nowait()
                                        except Empty:
                                            pass
                                    
                    
                    # Grab the next thing to search (when available)
                    status['message'] = 'Waiting for item from queue'
                    step, step_location, appears, leaves, search_id = search_items_queue.get()
                    
                    dz = Locations.is_deadzone(step_location)
                    
                    skipit = 0
                    cloc = CurrentLocation.get_temploc()
                    if dz:
                        mults = mults_for_loc(step_location)
                        args.dbp.execute_sql("INSERT INTO scannedlocation (latitude, longitude, last_modified, clat, clon, search_id, step, latmult, lonmult) VALUES (%f, %f, '%s', %f, %f, %d, %d, %d, %d) ON DUPLICATE KEY UPDATE last_modified = '%s'" % (step_location[0], step_location[1], datetime.utcnow(), cloc['latitude'], cloc['longitude'], search_id, step, mults[0], mults[1], datetime.utcnow()))
                        continue
                    
                    
                    if last_search_id == 0:
                        last_search_id = search_id
                    elif last_search_id != search_id:
                        # New search loop
                        last_search_id = search_id
                        if no_searches == 1:
                            # Do a random search here
                            
                            rll = random_latlon(zones)
                            step_location = (rll[0], rll[1], 0)
                            log.info('Random area searching %s', step_location)
                        no_searches = 1

                    query = (ScannedLocation.select().where(
                        (ScannedLocation.last_modified >= (datetime.utcnow() - timedelta(minutes=3.5))) 
                        & (((ScannedLocation.clat <> cloc['latitude']) 
                        & (ScannedLocation.clon <> cloc['longitude']))
                        | (ScannedLocation.search_id <> search_id)))
                        .dicts())
                    inside_old = 0;
                    closeto = 0
                    for p in query:
                        dist = calc_distance(step_location, (p['latitude'], p['longitude']))
                        if dist <= 0.035:
    #                        log.info('Step %d inside old ring', step)
                            inside_old = 1
                            continue
                        if inside_old:
                            break
                    if inside_old == 1:
                        for p in query:
                            dist = calc_distance(step_location, (p['latitude'], p['longitude']))
                            if dist >= 0.1217:
                                continue
                            closeto = closeto + 1
    #                        log.info('Step %d close to ring %2.5f %2.5f, count %d', step, p['latitude'], p['longitude'], closeto)
                    
                    if closeto >= 3:
    #                    log.info('Step %d is within a prior search radius, skipping. %d %d', step, search_id, no_searches)
                        search_items_queue.task_done()
                        continue
                    
                    no_searches = 0

                    # too soon?
                    if appears and now() < appears + 10:  # adding a 10 second grace period
                        first_loop = True
                        paused = False
                        while now() < appears + 10:
                            if pause_bit.is_set():
                                paused = True
                                break  # why can't python just have `break 2`...
                            remain = appears - now() + 10
                            status['message'] = 'Early for {:6f},{:6f}; waiting {}s...'.format(step_location[0], step_location[1], remain)
                            if first_loop:
                                log.info(status['message'])
                                first_loop = False
                            time.sleep(1)
                        if paused:
                            search_items_queue.task_done()
                            continue

                    # too late?
                    if leaves and now() > (leaves - args.min_seconds_left):
                        search_items_queue.task_done()
                        status['skip'] += 1
                        # it is slightly silly to put this in status['message'] since it'll be overwritten very shortly after. Oh well.
                        status['message'] = 'Too late for location {:6f},{:6f}; skipping'.format(step_location[0], step_location[1])
                        log.info(status['message'])
                        # No sleep here; we've not done anything worth sleeping for. Plus we clearly need to catch up!
                        continue

                    status['message'] = 'Searching step {:d} id {:d} at {:6f},{:6f}'.format(step, search_id, step_location[0], step_location[1])
                    log.info(status['message'])

                    # Let the api know where we intend to be for this loop
                    api.set_position(*step_location)

                    # Ok, let's get started -- check our login status
                    if check_login(args, account, api, step_location):
                        continue

                    # Make the actual request (finally!)
                    response_dict = map_request(api, step_location, args.jitter)
                    cc = response_dict['responses']['CHECK_CHALLENGE']
                    if cc.get('show_challenge'):
                        log_challenge(args, cc, account)
                        continue

                    # G'damnit, nothing back. Mark it up, sleep, carry on
                    if not response_dict:
                        status['fail'] += 1
                        status['message'] = 'Invalid response at {:6f},{:6f}, abandoning location'.format(step_location[0], step_location[1])
                        log.error(status['message'])
                        time.sleep(args.scan_delay)
                        continue

                    # Got the response, parse it out, send todo's to db/wh queues
                    try:
                        db_loc = (CurrentLocation.get_loc())
                        cl = (db_loc['latitude'], db_loc['longitude'], 0)
                        parsed = parse_map(args, response_dict, step_location, dbq, whq, cl, search_id, step, api, worker_id)
                        mults = mults_for_loc(step_location)
                        spawnpoint = 0
                        for poke in parsed['pokemons']:
                            spawnpoint = 1
                            if not Pokemon.has_db_stats(poke):
                                pp = parsed['pokemons'][poke]
                                log.info("Pokemon ID %s at %.6f, %.6f has no saved stats, queueing for pull", pp['pokemon_id'], pp['latitude'], pp['longitude'])
                                poke_stat_list.append(parsed['pokemons'][poke])
                        args.dbp.execute_sql("INSERT INTO locations (latmult, lonmult, latitude, longitude, scancount, lastscan, spawnpoint) VALUES (%d, %d, %f, %f, 1, NOW(), %d) ON DUPLICATE KEY UPDATE scancount = scancount + 1, spawnpoint = IF(spawnpoint < %d, %d, spawnpoint), lastscan='%s'" % (mults[0], mults[1], step_location[0], step_location[1], spawnpoint, spawnpoint, spawnpoint, datetime.utcnow()))
                            
                        search_items_queue.task_done()
                        status[('success' if parsed['count'] > 0 else 'noitems')] += 1
                        status['message'] = 'Search at {:6f},{:6f} completed with {} finds'.format(step_location[0], step_location[1], parsed['count'])
                        log.debug(status['message'])
                    except KeyError:
                        parsed = False
                        status['fail'] += 1
                        status['message'] = 'Map parse failed at {:6f},{:6f}, abandoning location. {} may be banned.'.format(step_location[0], step_location[1], account['username'])
                        log.exception(status['message'])

                    # Get detailed information about gyms
                    if args.gym_info and parsed:
                        # build up a list of gyms to update
                        gyms_to_update = {}
                        for gym in parsed['gyms'].values():
                            # Can only get gym details within 1km of our position
                            distance = calc_distance(step_location, [gym['latitude'], gym['longitude']])
                            if distance < 1:
                                # check if we already have details on this gym (if not, get them)
                                try:
                                    record = GymDetails.get(gym_id=gym['gym_id'])
                                except GymDetails.DoesNotExist as e:
                                    gyms_to_update[gym['gym_id']] = gym
                                    continue

                                # if we have a record of this gym already, check if the gym has been updated since our last update
                                if record.last_scanned < gym['last_modified']:
                                    gyms_to_update[gym['gym_id']] = gym
                                    continue
                                else:
                                    log.debug('Skipping update of gym @ %f/%f, up to date', gym['latitude'], gym['longitude'])
                                    continue
                            else:
                                log.debug('Skipping update of gym @ %f/%f, too far away from our location at %f/%f (%fkm)', gym['latitude'], gym['longitude'], step_location[0], step_location[1], distance)

                        if len(gyms_to_update):
                            gym_responses = {}
                            current_gym = 1
                            status['message'] = 'Updating {} gyms for location {},{}...'.format(len(gyms_to_update), step_location[0], step_location[1])
                            log.debug(status['message'])

                            for gym in gyms_to_update.values():
                                status['message'] = 'Getting details for gym {} of {} for location {},{}...'.format(current_gym, len(gyms_to_update), step_location[0], step_location[1])
                                time.sleep(random.random() + 2)
                                response = gym_request(api, step_location, gym)

                                # make sure the gym was in range. (sometimes the API gets cranky about gyms that are ALMOST 1km away)
                                if response['responses']['GET_GYM_DETAILS']['result'] == 2:
                                    log.warning('Gym @ %f/%f is out of range (%dkm), skipping', gym['latitude'], gym['longitude'], distance)
                                else:
                                    gym_responses[gym['gym_id']] = response['responses']['GET_GYM_DETAILS']

                                # increment which gym we're on (for status messages)
                                current_gym += 1

                            status['message'] = 'Processing details of {} gyms for location {},{}...'.format(len(gyms_to_update), step_location[0], step_location[1])
                            log.debug(status['message'])

                            if gym_responses:
                                parse_gyms(args, gym_responses, whq)

                # Always delay the desired amount after "scan" completion
                status['message'] += ', sleeping {}s until {}'.format(args.scan_delay, time.strftime('%H:%M:%S', time.localtime(time.time() + args.scan_delay)))
                if statsearch:
                    time.sleep(2.5 + (random.randrange(1, 100) / 100))
                else:
                    time.sleep(args.scan_delay)

        # catch any process exceptions, log them, and continue the thread
        except Exception as e:
            status['message'] = 'Exception in search_worker: {}'.format(e)
            log.exception(status['message'])
            time.sleep(args.scan_delay)

def log_challenge(args, cc, account):
    args.dbp.execute_sql("INSERT INTO captcha (user, url, tstamp, comp, loaded, answer) VALUES ('%s', '%s', NOW(), 0, 0, '') ON DUPLICATE KEY UPDATE url = '%s', tstamp = NOW(), comp=0, loaded=0, answer=''" % (account['username'], cc['challenge_url'], cc['challenge_url']))
    smtpObj = smtplib.SMTP('smtp.gmail.com:587')
    smtpObj.ehlo()
    smtpObj.starttls()
    smtpObj.login("pokealert88@gmail.com","Ferret67")
    try:
        efrom = 'pokealert88@gmail.com'
        eto = 'ed@ebonmists.com'

        msg = 'From: "PokeAlert" <pokealert88@gmail.com>\nTo: "Ed Benckert" <%s>\nSubject: Account locked, needs captcha'
        Pokemon.sent_alert(b64encode(str(poke['encounter_id'])))
        smtpObj.sendmail(efrom, eto, "<a href=googlechrome:https://pgorelease.nianticlabs.com>Fix it</a>")
    except Exception as e:
        log.info('Failed sending email %s', e)
    log.info("Challenge requested. Grr.")
    return 0


def check_login(args, account, api, position):

    # Logged in? Enough time left? Cool!
    if api._auth_provider and api._auth_provider._ticket_expire:
        remaining_time = api._auth_provider._ticket_expire / 1000 - time.time()
        if remaining_time > 60:
            log.debug('Credentials remain valid for another %f seconds', remaining_time)
            return

    # Try to login (a few times, but don't get stuck here)
    i = 0
    api.set_position(position[0], position[1], position[2])
    while i < args.login_retries:
        try:
            if args.proxy:
                ret = api.set_authentication(provider=account['auth_service'], username=account['username'], password=account['password'], proxy_config={'http': args.proxy, 'https': args.proxy})
            else:
                ret = api.set_authentication(provider=account['auth_service'], username=account['username'], password=account['password'])
            time.sleep(1)
            cl = CurrentLocation.get_loc(1)
            api.set_position(cl[0], cl[1], 0)
            inv = api.get_player()
            cc = inv['responses']['CHECK_CHALLENGE']
            if cc.get('show_challenge'):
                log_challenge(args, cc, account)
                return 1
            time.sleep(args.scan_delay)
            break
        except AuthException:
            if i >= args.login_retries:
                raise TooManyLoginAttempts('Exceeded login attempts')
            else:
                i += 1
                log.error('Failed to login to Pokemon Go with account %s. Trying again in %g seconds', account['username'], args.login_delay)
                time.sleep(args.login_delay)
    log.debug('Login for account %s successful', account['username'])
    
    return 0


def map_request(api, position, jitter=False):
    # create scan_location to send to the api based off of position, because tuples aren't mutable
    if jitter:
        # jitter it, just a little bit.
        scan_location = jitterLocation(position)
        log.debug('Jittered to: %f/%f/%f', scan_location[0], scan_location[1], scan_location[2])
    else:
        # Just use the original coordinates
        scan_location = position

    try:
        cell_ids = util.get_cell_ids(scan_location[0], scan_location[1])
        timestamps = [0, ] * len(cell_ids)
        return  api.get_map_objects(latitude=f2i(scan_location[0]),
                                   longitude=f2i(scan_location[1]),
                                   since_timestamp_ms=timestamps,
                                   cell_id=cell_ids)
            
    except Exception as e:
        log.warning('Exception while downloading map: %s', e)
        return False


def gym_request(api, position, gym):
    try:
        log.debug('Getting details for gym @ %f/%f (%fkm away)', gym['latitude'], gym['longitude'], calc_distance(position, [gym['latitude'], gym['longitude']]))
        x = api.get_gym_details(gym_id=gym['gym_id'],
                                player_latitude=f2i(position[0]),
                                player_longitude=f2i(position[1]),
                                gym_latitude=gym['latitude'],
                                gym_longitude=gym['longitude'])

        # print pretty(x)
        return x

    except Exception as e:
        log.warning('Exception while downloading gym details: %s', e)
        return False




# Delay each thread start time so that logins only occur ~1s
def stagger_thread(args, account):
    if args.accounts.index(account) == 0:
        return  # No need to delay the first one
    delay = args.accounts.index(account) + ((random.random() - .5) / 2)
    log.debug('Delaying thread startup for %.2f seconds', delay)
    time.sleep(delay)


class TooManyLoginAttempts(Exception):
    pass
