#!/usr/bin/python -*- coding: utf-8 -*-
import logging
import math
import calendar
import sys
import gc
import time
import geopy
import json
import re
import smtplib
import ssl
from email.mime.text import MIMEText
from peewee import SqliteDatabase, InsertQuery, \
    IntegerField, CharField, DoubleField, BooleanField, \
    DateTimeField, fn, DeleteQuery, CompositeKey, FloatField, SQL, TextField
from playhouse.flask_utils import FlaskDB
from playhouse.pool import PooledMySQLDatabase
from playhouse.shortcuts import RetryOperationalError
from playhouse.migrate import migrate, MySQLMigrator, SqliteMigrator
from datetime import datetime, timedelta
from base64 import b64encode, b64decode

from . import config
from .utils import get_pokemon_name, get_pokemon_rarity, get_pokemon_types, get_args
from .transform import transform_from_wgs_to_gcj, get_new_coords, mults_for_loc
from .customLog import printPokemon
from pgoapi import PGoApi

log = logging.getLogger(__name__)

args = get_args()

flaskDb = FlaskDB()

poke_alerts = dict()

db_schema_version = 7
lockpos = (0, 0, 0)

class MyRetryDB(RetryOperationalError, PooledMySQLDatabase):
    pass


def init_database(app):
    global pokejson
    global args
    with open('static/dist/data/pokemon.min.json', 'r') as myfile:
        pokejson=myfile.read().replace('\n', '')
    
    pokejson = json.loads(pokejson)
    if args.db_type == 'mysql':
        log.info('Connecting to MySQL database on %s:%i', args.db_host, args.db_port)
        connections = args.db_max_connections
        if hasattr(args, 'accounts'):
            connections *= len(args.accounts)
        db = MyRetryDB(
            args.db_name,
            user=args.db_user,
            password=args.db_pass,
            host=args.db_host,
            port=args.db_port,
            max_connections=connections,
            stale_timeout=300)
    else:
        log.info('Connecting to local SQLite database')
        db = SqliteDatabase(args.db)

    app.config['DATABASE'] = db
    args.dbp = db
    flaskDb.init_app(app)

    return db


class BaseModel(flaskDb.Model):

    @classmethod
    def get_all(cls):
        results = [m for m in cls.select().dicts()]
        if args.china:
            for result in results:
                result['latitude'], result['longitude'] = \
                    transform_from_wgs_to_gcj(
                        result['latitude'], result['longitude'])
        return results

class captcha(BaseModel):
    user = CharField(max_length=60,primary_key=True)
    url = CharField(max_length=200)
    tstamp = DateTimeField()
    comp = IntegerField(index=True)
    loaded = IntegerField()
    answer = CharField(max_length=2048)

    @staticmethod
    def clear(user = None):
        if user is None:
            query = (captcha.delete().execute())
        else:
            query = (captcha.delete().where((captcha.user == user)).execute())
        
        return 1

    @staticmethod
    def get(userx = None):
        ret = {}
        if userx is None:
            query = (captcha
                 .select().where((captcha.comp == 0) & (captcha.loaded == 0)).dicts())
            ql = list(query)
            if not len(ql):
                ret['url'] = "";
                ret['user'] = "";
                ret['ok'] = 0;
                return ret;
        
            query = captcha.update(loaded = 1).where((captcha.url == ql[0]['url'])).execute()
        
            ql[0]['ok'] = 1
            return ql[0]

        query = (captcha
             .select().where((captcha.user == userx)).dicts())
        ql = list(query)
        
        ret['hold'] = 1
        if not len(ql):
            ret['hold'] = 0
            ret['answer'] = None
            return ret
        
        if len(ql[0]['answer']) > 10:
            ret['hold'] = 0
            ret['answer'] = ql[0]['answer']
        else:
            ret['hold'] = 1
            ret['answer'] = None
            
        return ret
            
        
        
        
    def complete(user):
        query = (captcha
                 .update(complete = 1).where((captcha.user == user))
                 .execute())
        
        return 1
        

class Vars(BaseModel):
    varname = CharField(max_length=35,primary_key=True)
    value = CharField(max_length=50)

    @staticmethod
    def get(vname):
        query = (Vars
                 .select().where(Vars.varname == vname)
                 .dicts())
        ql = list(query)
        if not len(ql):
            log.warning("Looking for variable '%s' but not set yet", vname)
            return None
        val = u"" + str(ql[0]['value'])
        if val.isnumeric():
            return int(val)
        
        return val
        

    @staticmethod
    def set(vname, vvalue):
        Vars.delete().where(Vars.varname == vname).execute()
        vvalue = "%s" % (vvalue)
        InsertQuery(Vars, {'varname': vname, 'value': vvalue}).upsert().execute()

    @staticmethod
    def clear(vname):
        Vars.delete().where(Vars.varname == vname).execute()

class CurrentLocation(BaseModel):
    @staticmethod
    def get_loc(list = 0):
        if list:
            clat = Vars.get("CurrentLatitude")
            clon = Vars.get("CurrentLongitude")
            if clat and clon:
                ret = (float(Vars.get("CurrentLatitude")), float(Vars.get("CurrentLongitude")), 0)
            else:
                ret = (0, 0, 0)
        else:
            ret = dict()
            ret['latitude'] = float(Vars.get("CurrentLatitude"))
            ret['longitude'] =float(Vars.get("CurrentLongitude"))
                
        return ret

    @staticmethod
    def get_temploc():
        ret = dict()
        query = (Vars
                 .select().where((Vars.varname == 'TempLat') | (Vars.varname == 'TempLon'))
                 .dicts())
        if len(query):
            for p in query:
                if p['varname'] == 'TempLat':
                    ret['latitude'] = float(p['value'])
                else:
                    ret['longitude'] =float(p['value'])
        else:
            return CurrentLocation.get_loc()
                
        return ret

    @staticmethod
    def set_loc(lat, lon):
        Vars.delete().where((Vars.varname == 'CurrentLatitude') | (Vars.varname == 'CurrentLongitude')).execute()
        ival=([{'varname': 'CurrentLatitude', 'value': lat}, {'varname': 'CurrentLongitude', 'value': lon}])
        InsertQuery(Vars, rows=ival).upsert().execute()

class AlertLog(BaseModel):
    encounter_id = CharField(primary_key=True, max_length=50)
    tstamp = DateTimeField()
    
    @staticmethod
    def log(id):
        args.dbp.execute_sql("INSERT IGNORE INTO alertlog (encounter_id, tstamp) VALUES ('%s', '%s')" % (id, datetime.utcnow()))

class Locations(BaseModel):
    latmult = IntegerField()
    lonmult = IntegerField()
    latitude = DoubleField()
    longitude = DoubleField()
    scancount = IntegerField()
    lastscan = DateTimeField(index=True)
    spawnpoint = IntegerField()

    class Meta:
        primary_key = CompositeKey('latmult', 'lonmult')
    
    @staticmethod
    def is_spawnpoint(loc):
        lat = loc[0]
        lon = loc[1]
        
        mults = mults_for_loc(loc)
        
        lonmult = mults[0]
        latmult = mults[1]
        
        q = (Locations.select(Locations.spawnpoint).where((Locations.lonmult == lonmult) & (Locations.latmult == latmult)).execute())
        return q

    @staticmethod
    def is_deadzone(loc):
        lat = loc[1]
        lon = loc[0]
        
        mults = mults_for_loc(loc)
        
        lonmult = mults[1]
        latmult = mults[0]
        
        q = (Locations.select().where((Locations.lonmult == lonmult) & (Locations.latmult == latmult)).dicts())
        ret = list(q)
        if not len(ret):
            return 0
        
        r = ret[0]
        
        if r['spawnpoint']:
            return 0
        
        return 1

class LocLog(BaseModel):
    seq = IntegerField(primary_key=True)
    latitude = DoubleField()
    longitude = DoubleField()
    tstamp = DateTimeField()

    @staticmethod
    def lastloc():
        query = (LocLog.select().order_by(LocLog.tstamp.desc(), LocLog.seq.asc()).limit(1).dicts())
        ret = list(query)
        s1 = ret[0]
        pos1 = (s1['latitude'], s1['longitude'], 0, s1['tstamp'])
        return pos1

    @staticmethod
    def lastspeed():
        query = (LocLog.select().order_by(LocLog.tstamp.desc(), LocLog.seq.asc()).limit(2).dicts())
        ret = list(query)
        s1 = ret[0]
        s2 = ret[1]
        pos1 = (s1['latitude'], s1['longitude'], 0)
        pos2 = (s2['latitude'], s2['longitude'], 0)
        dd = geopy.distance.distance(pos1, pos2).meters
        td = max(1, (s1['tstamp'] - s2['tstamp']).total_seconds())
        mph = dd / td * (25/11)
        return mph

    @staticmethod
    def predict_loc(pos1=None, pos2=None, timedelay=10):
        if pos1 is None:
            query = (LocLog.select().order_by(LocLog.tstamp.desc(), LocLog.seq.asc()).limit(5).dicts())
            ret = list(query)
            s1 = ret[0]
            s2 = ret[1]
            pos1 = (s1['latitude'], s1['longitude'], 0)
            pos2 = (s2['latitude'], s2['longitude'], 0)
        
        latd = pos1[0] - pos2[0]
        lond = pos1[1] - pos2[1]
        
        rads = math.atan2(lond, latd)
        degs = math.degrees(rads)
        if degs < 0:
            degs = 360 + degs        
        dd = geopy.distance.distance(pos1, pos2).meters
        td = max(1, (s1['tstamp'] - s2['tstamp']).total_seconds())
        mps = dd / td * timedelay

        nc = get_new_coords(pos1, mps / 1000, degs)
        
        ret = (nc[0], nc[1], degs, mps / timedelay)
        
        return ret

class PokeAlert(BaseModel):
    pokemon_id = IntegerField(primary_key=True)
    pokemon = CharField(max_length=30)
    priority = IntegerField()
    catch = IntegerField()

    @staticmethod
    def alert(id):
        query = (PokeAlert.select()
                        .where((PokeAlert.pokemon_id == id))
                        .dicts())
        ret = list(query)
        if len(ret) == 0:
            return 0
        
        return ret[0]['priority']

    @staticmethod
    def enable_alert(id, pri):
        PokeAlert.delete().where(PokeAlert.pokemon_id == id).execute()
        if pri > 0:
            na = PokeAlert.create(pokemon_id=id, priority=pri)
            na.save()


class Pokemon(BaseModel):
    # We are base64 encoding the ids delivered by the api
    # because they are too big for sqlite to handle
    encounter_id = CharField(primary_key=True, max_length=50)
    spawnpoint_id = CharField(index=True)
    pokemon_id = IntegerField(index=True)
    latitude = DoubleField()
    longitude = DoubleField()
    disappear_time = DateTimeField(index=True)
    alerted = IntegerField(index=True)
    att = IntegerField()
    deff = IntegerField()
    hp = IntegerField()
    latmult = IntegerField()
    lonmult = IntegerField()
    pcaught = IntegerField(index=True)
    chance1 = DoubleField()
    chance2 = DoubleField()
    chance3 = DoubleField()

    class Meta:
        indexes = ((('latitude', 'longitude'), False),)


    @staticmethod
    def catchable():
        query = (Pokemon.select().join(PokeAlert, on=((PokeAlert.pokemon_id == Pokemon.pokemon_id) & (PokeAlert.catch == 1))).where((Pokemon.disappear_time >= datetime.utcnow()+timedelta(minutes=2)) & (Pokemon.pcaught == 0)).dicts())
        if len(query):
            return query[0]
        
        return None

    @staticmethod
    def caught(eid):
        query = (Pokemon.update(pcaught=1).where(Pokemon.encounter_id == eid))
        query.execute()
        
        return None

    @staticmethod
    def has_db_stats(eid):
        eid = b64encode(str(eid)) 
        query = (Pokemon.select().where((Pokemon.encounter_id == eid) & (Pokemon.hp > 0) & (Pokemon.deff > 0) & (Pokemon.att > 0)).dicts())

        if len(query):
            return 1
        
        return 0

    @staticmethod
    def get_stats(p, api):
        eid = u"" + str(p['encounter_id'])
        if eid.isnumeric():
            neid = p['encounter_id']
            eid = b64encode(str(p['encounter_id'])) 
        else:
            p['encounter_id'] = long(b64decode(p['encounter_id']))
            neid = p['encounter_id']
        query = (Pokemon.select().where((Pokemon.encounter_id == eid)).dicts())
        
        ret = dict()
        ret['fail'] = 0
        if len(query):
            pp = query[0]
            ret['hp'] = pp['hp']
            ret['att'] = pp['att']
            ret['deff'] = pp['deff']
            ret['chance1'] = pp['chance1']
            ret['chance2'] = pp['chance2']
            ret['chance3'] = pp['chance3']
            ret['pokemon_rarity'] = get_pokemon_rarity(pp['pokemon_id'])
            ret['alert_priority'] = PokeAlert.alert(pp['pokemon_id'])
            ret['disappear_time'] = pp['disappear_time']
            ret['latitude'] = pp['latitude']
            ret['longitude'] = pp['longitude']
        elif api is None:
            ret['hp'] = -1
            ret['deff'] = -1
            ret['att'] = -1
            ret['chance1'] = 0
            ret['chance2'] = 0
            ret['chance3'] = 0
            
        if api and (ret['hp'] < 0 or ret['att'] < 0 or ret['deff'] < 0):
            try:
                rett = api.encounter(encounter_id = neid, spawn_point_id = pp['spawnpoint_id'], player_latitude = p['latitude'], player_longitude = p['longitude'])
            except Exception as e:
                log.info("API error: %s %s %s", e, neid, pp)
                ret['fail'] = 1
                return ret
                
            try:
                rb = rett['responses']['ENCOUNTER']['wild_pokemon']['pokemon_data']
                ret['hp'] = rb.get('individual_stamina', 0)
                ret['att'] = rb.get('individual_attack', 0)
                ret['deff'] = rb.get('individual_defense', 0)
                ret['chance1'] = rett['responses']['ENCOUNTER']['capture_probability']['capture_probability'][0] * 100
                ret['chance2'] = rett['responses']['ENCOUNTER']['capture_probability']['capture_probability'][1]* 100
                ret['chance3'] = rett['responses']['ENCOUNTER']['capture_probability']['capture_probability'][2] * 100
                query = Pokemon.update(hp = ret['hp'], att = ret['att'], deff = ret['deff'], chance1 = ret['chance1'], chance2 = ret['chance2'], chance3 = ret['chance3'],).where(Pokemon.encounter_id == eid).execute()
            except Exception as e:
                ret['fail'] = 1
                log.info("Exception: %s %s", e, rett)
        
        return ret
            
    
    @staticmethod
    def sent_alert(encounter_id):
        global poke_alerts
        poke_alerts[encounter_id] = 1
        query = (Pokemon.update(alerted=1).where(Pokemon.encounter_id == encounter_id))
        query.execute()
    
    
    @staticmethod
    def alert_sent(eid):
        eid = u"" + str(eid)
        if eid.isnumeric():
            eid = b64encode(str(eid)) 

        global poke_alerts
        if eid in poke_alerts:
            return 1

        query = (Pokemon.select()
                        .where((Pokemon.encounter_id == eid))
                        .dicts().iterator())
        ret = list(query)
        if len(ret) == 0:
            log.info("Checking alert sent for %s, not finding an existing row?", eid)
            return 0
        
        return ret[0]['alerted']


    @staticmethod
    def get_active(swLat, swLng, neLat, neLng):
        if swLat is None or swLng is None or neLat is None or neLng is None:
            query = (Pokemon
                     .select()
                     .where(Pokemon.disappear_time > datetime.utcnow())
                     .dicts())
        else:
            query = (Pokemon
                     .select()
                     .where((Pokemon.disappear_time > datetime.utcnow()) &
                            (((Pokemon.latitude >= swLat) &
                              (Pokemon.longitude >= swLng) &
                              (Pokemon.latitude <= neLat) &
                              (Pokemon.longitude <= neLng))))
                     .dicts())

        # Performance: Disable the garbage collector prior to creating a (potentially) large dict with append()
        gc.disable()

        pokemons = []
        for p in query:
            rarity = get_pokemon_rarity(p['pokemon_id'])
            alert = PokeAlert.alert(p['pokemon_id'])
            p['junk'] = 0
            skipme = 0
            if rarity == 'Common' or rarity == 'Uncommon':
                skipme = 1
            
            if alert:
                skipme = 0
            
            stats = p['att'] + p['deff'] + p['hp']
            
            if stats == -3 or stats >= 43:
                skipme = 0
            
            if skipme:
                p['junk'] = 1
        
            p['pokemon_name'] = get_pokemon_name(p['pokemon_id'])
            p['pokemon_rarity'] = rarity
            p['pokemon_types'] = get_pokemon_types(p['pokemon_id'])
            p['alert_priority'] = alert
            if p['alerted'] == 1:
                poke_alerts[p['encounter_id']] = 1
            if args.china:
                p['latitude'], p['longitude'] = \
                    transform_from_wgs_to_gcj(p['latitude'], p['longitude'])
            pokemons.append(p)

        # Re-enable the GC.
        gc.enable()

        return pokemons

    @staticmethod
    def get_active_by_id(ids, swLat, swLng, neLat, neLng):
        if swLat is None or swLng is None or neLat is None or neLng is None:
            query = (Pokemon
                     .select()
                     .where((Pokemon.pokemon_id << ids) &
                            (Pokemon.disappear_time > datetime.utcnow()))
                     .dicts())
        else:
            query = (Pokemon
                     .select()
                     .where((Pokemon.pokemon_id << ids) &
                            (Pokemon.disappear_time > datetime.utcnow()) &
                            (Pokemon.latitude >= swLat) &
                            (Pokemon.longitude >= swLng) &
                            (Pokemon.latitude <= neLat) &
                            (Pokemon.longitude <= neLng))
                     .dicts())

        # Performance: Disable the garbage collector prior to creating a (potentially) large dict with append()
        gc.disable()

        pokemons = []
        for p in query:
            p['pokemon_name'] = get_pokemon_name(p['pokemon_id'])
            p['pokemon_rarity'] = get_pokemon_rarity(p['pokemon_id'])
            p['pokemon_types'] = get_pokemon_types(p['pokemon_id'])
            p['alert_priority'] = PokeAlert.alert(p['pokemon_id'])
            if args.china:
                p['latitude'], p['longitude'] = \
                    transform_from_wgs_to_gcj(p['latitude'], p['longitude'])
            pokemons.append(p)

        # Re-enable the GC.
        gc.enable()

        return pokemons

    @classmethod
    def get_seen(cls, timediff):
        if timediff:
            timediff = datetime.utcnow() - timediff
        pokemon_count_query = (Pokemon
                               .select(Pokemon.pokemon_id,
                                       fn.COUNT(Pokemon.pokemon_id).alias('count'),
                                       fn.MAX(Pokemon.disappear_time).alias('lastappeared')
                                       )
                               .where(Pokemon.disappear_time > timediff)
                               .group_by(Pokemon.pokemon_id)
                               .alias('counttable')
                               )
        query = (Pokemon
                 .select(Pokemon.pokemon_id,
                         Pokemon.disappear_time,
                         Pokemon.latitude,
                         Pokemon.longitude,
                         pokemon_count_query.c.count)
                 .join(pokemon_count_query, on=(Pokemon.pokemon_id == pokemon_count_query.c.pokemon_id))
                 .distinct()
                 .where(Pokemon.disappear_time == pokemon_count_query.c.lastappeared)
                 .dicts()
                 )

        # Performance: Disable the garbage collector prior to creating a (potentially) large dict with append()
        gc.disable()

        pokemons = []
        total = 0
        for p in query :
            p['pokemon_name'] = get_pokemon_name(p['pokemon_id'])
            pokemons.append(p)
            total += p['count']

        # Re-enable the GC.
        gc.enable()

        return {'pokemon': pokemons, 'total': total}

    @classmethod
    def get_appearances(cls, pokemon_id, last_appearance, timediff):
        '''
        :param pokemon_id: id of pokemon that we need appearances for
        :param last_appearance: time of last appearance of pokemon after which we are getting appearances
        :param timediff: limiting period of the selection
        :return: list of  pokemon  appearances over a selected period
        '''
        if timediff:
            timediff = datetime.utcnow() - timediff
        query = (Pokemon
                 .select()
                 .where((Pokemon.pokemon_id == pokemon_id) &
                        (Pokemon.disappear_time > datetime.utcfromtimestamp(last_appearance / 1000.0)) &
                        (Pokemon.disappear_time > timediff)
                        )
                 .order_by(Pokemon.disappear_time.asc())
                 .dicts()
                 )

        return list(query)

    @classmethod
    def get_spawn_time(cls, disappear_time):
        return (disappear_time + 2700) % 3600

    @classmethod
    def get_spawnpoints(cls, southBoundary, westBoundary, northBoundary, eastBoundary):
        query = Pokemon.select(Pokemon.latitude, Pokemon.longitude, Pokemon.spawnpoint_id, ((Pokemon.disappear_time.minute * 60) + Pokemon.disappear_time.second).alias('time'), fn.Count(Pokemon.spawnpoint_id).alias('count'))

        if None not in (northBoundary, southBoundary, westBoundary, eastBoundary):
            query = (query
                     .where((Pokemon.latitude <= northBoundary) &
                            (Pokemon.latitude >= southBoundary) &
                            (Pokemon.longitude >= westBoundary) &
                            (Pokemon.longitude <= eastBoundary)
                            ))

        query = query.group_by(Pokemon.latitude, Pokemon.longitude, Pokemon.spawnpoint_id, SQL('time'))

        queryDict = query.dicts()
        spawnpoints = {}

        for sp in queryDict:
            key = sp['spawnpoint_id']
            disappear_time = cls.get_spawn_time(sp.pop('time'))
            count = int(sp['count'])

            if key not in spawnpoints:
                spawnpoints[key] = sp
            else:
                spawnpoints[key]['special'] = True

            if 'time' not in spawnpoints[key] or count >= spawnpoints[key]['count']:
                spawnpoints[key]['time'] = disappear_time
                spawnpoints[key]['count'] = count

        for sp in spawnpoints.values():
            del sp['count']

        return list(spawnpoints.values())

    @classmethod
    def get_spawnpoints_in_hex(cls, center, steps):
        log.info('Finding spawn points {} steps away'.format(steps))

        n, e, s, w = hex_bounds(center, steps)

        query = (Pokemon
                 .select(Pokemon.latitude.alias('lat'),
                         Pokemon.longitude.alias('lng'),
                         ((Pokemon.disappear_time.minute * 60) + Pokemon.disappear_time.second).alias('time'),
                         Pokemon.spawnpoint_id
                         ))
        query = (query.where((Pokemon.latitude <= n) &
                             (Pokemon.latitude >= s) &
                             (Pokemon.longitude >= w) &
                             (Pokemon.longitude <= e)
                             ))
        # Sqlite doesn't support distinct on columns
        if args.db_type == 'mysql':
            query = query.distinct(Pokemon.spawnpoint_id)
        else:
            query = query.group_by(Pokemon.spawnpoint_id)

        s = list(query.dicts())

        # The distance between scan circles of radius 70 in a hex is 121.2436
        # steps - 1 to account for the center circle then add 70 for the edge
        step_distance = ((steps - 1) * 121.2436) + 70
        # Compare spawnpoint list to a circle with radius steps * 120
        # Uses the direct geopy distance between the center and the spawnpoint.
        filtered = []

        for idx, sp in enumerate(s):
            if geopy.distance.distance(center, (sp['lat'], sp['lng'])).meters <= step_distance:
                filtered.append(s[idx])

        # at this point, 'time' is DISAPPEARANCE time, we're going to morph it to APPEARANCE time
        for location in filtered:
            # examples: time    shifted
            #           0       (   0 + 2700) = 2700 % 3600 = 2700 (0th minute to 45th minute, 15 minutes prior to appearance as time wraps around the hour)
            #           1800    (1800 + 2700) = 4500 % 3600 =  900 (30th minute, moved to arrive at 15th minute)
            # todo: this DOES NOT ACCOUNT for pokemons that appear sooner and live longer, but you'll _always_ have at least 15 minutes, so it works well enough
            location['time'] = cls.get_spawn_time(location['time'])

        return filtered


class Pokestop(BaseModel):
    idkey = IntegerField(primary_key=True)
    pokestop_id = CharField(index=True, max_length=50)
    enabled = BooleanField()
    latitude = DoubleField()
    longitude = DoubleField()
    last_modified = DateTimeField(index=True)
    lure_expiration = DateTimeField(null=True, index=True)
    active_fort_modifier = CharField(max_length=50, null=True)

    class Meta:
        indexes = ((('latitude', 'longitude'), False),)

    @staticmethod
    def get_stops(swLat, swLng, neLat, neLng):
        if swLat is None or swLng is None or neLat is None or neLng is None:
            query = (Pokestop
                     .select()
                     .dicts())
        else:
            query = (Pokestop
                     .select()
                     .where((Pokestop.latitude >= swLat) &
                            (Pokestop.longitude >= swLng) &
                            (Pokestop.latitude <= neLat) &
                            (Pokestop.longitude <= neLng))
                     .dicts())

        # Performance: Disable the garbage collector prior to creating a (potentially) large dict with append()
        gc.disable()

        pokestops = []
        for p in query:
            if args.china:
                p['latitude'], p['longitude'] = \
                    transform_from_wgs_to_gcj(p['latitude'], p['longitude'])
            pokestops.append(p)

        # Re-enable the GC.
        gc.enable()

        return pokestops

    @staticmethod
    def get_stop(p_id):
        query = (Pokestop.select(Pokestop.latitude, Pokestop.longitude)
                     .where((Pokestop.pokestop_id == p_id))
                     .dicts())

        q = list(query)
        log.info("zz: %s", q)
        return q[0]

class Gym(BaseModel):
    UNCONTESTED = 0
    TEAM_MYSTIC = 1
    TEAM_VALOR = 2
    TEAM_INSTINCT = 3

    gym_id = CharField(primary_key=True, max_length=50)
    team_id = IntegerField()
    guard_pokemon_id = IntegerField()
    gym_points = IntegerField()
    enabled = BooleanField()
    latitude = DoubleField()
    longitude = DoubleField()
    last_modified = DateTimeField(index=True)
    last_scanned = DateTimeField(default=datetime.utcnow)

    class Meta:
        indexes = ((('latitude', 'longitude'), False),)

    @staticmethod
    def get_gyms(swLat, swLng, neLat, neLng):
        if swLat is None or swLng is None or neLat is None or neLng is None:
            results = (Gym
                       .select()
                       .dicts())
        else:
            results = (Gym
                       .select()
                       .where((Gym.latitude >= swLat) &
                              (Gym.longitude >= swLng) &
                              (Gym.latitude <= neLat) &
                              (Gym.longitude <= neLng))
                       .dicts())

        # Performance: Disable the garbage collector prior to creating a (potentially) large dict with append()
        gc.disable()

        gyms = {}
        gym_ids = []
        for g in results:
            g['name'] = None
            g['pokemon'] = []
            gyms[g['gym_id']] = g
            gym_ids.append(g['gym_id'])

        if len(gym_ids) > 0:
            pokemon = (GymMember
                       .select(
                           GymMember.gym_id,
                           GymPokemon.cp.alias('pokemon_cp'),
                           GymPokemon.pokemon_id,
                           Trainer.name.alias('trainer_name'),
                           Trainer.level.alias('trainer_level'))
                       .join(Gym, on=(GymMember.gym_id == Gym.gym_id))
                       .join(GymPokemon, on=(GymMember.pokemon_uid == GymPokemon.pokemon_uid))
                       .join(Trainer, on=(GymPokemon.trainer_name == Trainer.name))
                       .where(GymMember.gym_id << gym_ids)
                       .where(GymMember.last_scanned > Gym.last_modified)
                       .order_by(GymMember.gym_id, GymPokemon.cp)
                       .dicts())

            for p in pokemon:
                p['pokemon_name'] = get_pokemon_name(p['pokemon_id'])
                gyms[p['gym_id']]['pokemon'].append(p)

            details = (GymDetails
                       .select(
                           GymDetails.gym_id,
                           GymDetails.name)
                       .where(GymDetails.gym_id << gym_ids)
                       .dicts())

            for d in details:
                gyms[d['gym_id']]['name'] = d['name']

        # Re-enable the GC.
        gc.enable()

        return gyms


class ScannedLocation(BaseModel):
    latitude = DoubleField()
    longitude = DoubleField()
    last_modified = DateTimeField(index=True)
    clat = CharField(max_length=20)
    clon = CharField(max_length=20)
    search_id = IntegerField()
    step = IntegerField()
    latmult = IntegerField()
    lonmult = IntegerField()

    class Meta:
        primary_key = CompositeKey('latitude', 'longitude')

    @staticmethod
    def get_recent(swLat, swLng, neLat, neLng):
        query = (ScannedLocation
                 .select(ScannedLocation.latitude, 
                         ScannedLocation.longitude,
                         ScannedLocation.last_modified, 
                         Locations.spawnpoint
                         )
                 .join(Locations, on=((Locations.latmult == ScannedLocation.latmult) & (Locations.lonmult == ScannedLocation.lonmult)))
                 .where((ScannedLocation.last_modified >=
                        (datetime.utcnow() - timedelta(minutes=5))) &
                        (ScannedLocation.latitude >= swLat) &
                        (ScannedLocation.longitude >= swLng) &
                        (ScannedLocation.latitude <= neLat) &
                        (ScannedLocation.longitude <= neLng))
                        
                 .order_by(ScannedLocation.last_modified.asc())
                 .dicts())
        hold = list(query)
        
        return hold


class Versions(flaskDb.Model):
    key = CharField()
    val = IntegerField()

    class Meta:
        primary_key = False


class GymMember(BaseModel):
    gym_id = CharField(index=True)
    pokemon_uid = CharField()
    last_scanned = DateTimeField(default=datetime.utcnow)

    class Meta:
        primary_key = False


class GymPokemon(BaseModel):
    pokemon_uid = CharField(primary_key=True, max_length=50)
    pokemon_id = IntegerField()
    cp = IntegerField()
    trainer_name = CharField()
    num_upgrades = IntegerField(null=True)
    move_1 = IntegerField(null=True)
    move_2 = IntegerField(null=True)
    height = FloatField(null=True)
    weight = FloatField(null=True)
    stamina = IntegerField(null=True)
    stamina_max = IntegerField(null=True)
    cp_multiplier = FloatField(null=True)
    additional_cp_multiplier = FloatField(null=True)
    iv_defense = IntegerField(null=True)
    iv_stamina = IntegerField(null=True)
    iv_attack = IntegerField(null=True)
    last_seen = DateTimeField(default=datetime.utcnow)


class Trainer(BaseModel):
    name = CharField(primary_key=True, max_length=50)
    team = IntegerField()
    level = IntegerField()
    last_seen = DateTimeField(default=datetime.utcnow)


class GymDetails(BaseModel):
    gym_id = CharField(primary_key=True, max_length=50)
    name = CharField()
    description = TextField(null=True, default="")
    url = CharField()
    last_scanned = DateTimeField(default=datetime.utcnow)


def hex_bounds(center, steps):
    # Make a box that is (70m * step_limit * 2) + 70m away from the center point
    # Rationale is that you need to travel
    sp_dist = 0.07 * 2 * steps
    n = get_new_coords(center, sp_dist, 0)[0]
    e = get_new_coords(center, sp_dist, 90)[1]
    s = get_new_coords(center, sp_dist, 180)[0]
    w = get_new_coords(center, sp_dist, 270)[1]
    return (n, e, s, w)


# todo: this probably shouldn't _really_ be in "models" anymore, but w/e
def parse_map(args, map_dict, step_location, db_update_queue, wh_update_queue, current_location, search_id, step, api, worker_id):
    global lockpos
    pokemons = {}
    pokestops = {}
    gyms = {}
    spawnpoints = set()
    
    cells = map_dict['responses']['GET_MAP_OBJECTS']['map_cells']
    smults = mults_for_loc(step_location)
    for cell in cells:
        if cell.get('spawn_points'):
            sps = cell.get('spawn_points')
            for sp in sps:
                ll = mults_for_loc((sp['latitude'], sp['longitude'], 0))
                spawnpoints.add(ll)
                
        if config['parse_pokemon']:
            for p in cell.get('wild_pokemons', []):
                # time_till_hidden_ms was overflowing causing a negative integer.
                # It was also returning a value above 3.6M ms.
                if 0 < p['time_till_hidden_ms'] < 3600000:
                    d_t = datetime.utcfromtimestamp(
                        (p['last_modified_timestamp_ms'] +
                         p['time_till_hidden_ms']) / 1000.0)
                else:
                    # Set a value of 15 minutes because currently its unknown but larger than 15.
                    d_t = datetime.utcfromtimestamp((p['last_modified_timestamp_ms'] + 900000) / 1000.0)
                
                printPokemon(p['pokemon_data']['pokemon_id'], p['latitude'],
                             p['longitude'], d_t)
                stats = Pokemon.get_stats(p, None)
                mults = mults_for_loc((p['latitude'],  p['longitude']))
                pokemons[p['encounter_id']] = {
                    'encounter_id': b64encode(str(p['encounter_id'])),
                    'spawnpoint_id': p['spawn_point_id'],
                    'pokemon_id': p['pokemon_data']['pokemon_id'],
                    'latitude': p['latitude'],
                    'longitude': p['longitude'],
                    'disappear_time': d_t,
                    'alerted': 0,
                    'att': stats['att'],
                    'deff': stats['deff'],
                    'hp': stats['hp'],
                    'latmult': mults[0],
                    'lonmult': mults[1],
                    'chance1': stats['chance1'],
                    'chance2': stats['chance2'],
                    'chance3': stats['chance3']
                }

                if args.webhooks:
                    wh_update_queue.put(('pokemon', {
                        'encounter_id': b64encode(str(p['encounter_id'])),
                        'spawnpoint_id': p['spawn_point_id'],
                        'pokemon_id': p['pokemon_data']['pokemon_id'],
                        'latitude': p['latitude'],
                        'longitude': p['longitude'],
                        'alert_priority': PokeAlert.alert(p['pokemon_data']['pokemon_id']),
                        'disappear_time': calendar.timegm(d_t.timetuple()),
                        'last_modified_time': p['last_modified_timestamp_ms'],
                        'time_until_hidden_ms': p['time_till_hidden_ms'],
                        'att': stats['att'],
                        'deff': stats['deff'],
                        'hp': stats['hp'] 
                    }))

        for f in cell.get('forts', []):
            if config['parse_pokestops'] and f.get('type') == 1:  # Pokestops
                if 'active_fort_modifier' in f:
                    lure_expiration = datetime.utcfromtimestamp(
                        f['last_modified_timestamp_ms'] / 1000.0) + timedelta(minutes=30)
                    active_fort_modifier = f['active_fort_modifier']
                    if args.webhooks and args.webhook_updates_only:
                        wh_update_queue.put(('pokestop', {
                            'pokestop_id': b64encode(str(f['id'])),
                            'enabled': f['enabled'],
                            'latitude': f['latitude'],
                            'longitude': f['longitude'],
                            'last_modified_time': f['last_modified_timestamp_ms'],
                            'lure_expiration': calendar.timegm(lure_expiration.timetuple()),
                            'active_fort_modifier': active_fort_modifier
                        }))
                else:
                    lure_expiration, active_fort_modifier = None, None

                pokestops[f['id']] = {
                    'pokestop_id': f['id'],
                    'enabled': f['enabled'],
                    'latitude': f['latitude'],
                    'longitude': f['longitude'],
                    'last_modified': datetime.utcfromtimestamp(
                        f['last_modified_timestamp_ms'] / 1000.0),
                    'lure_expiration': lure_expiration,
                    'active_fort_modifier': active_fort_modifier
                }

                # Send all pokéstops to webhooks
                if args.webhooks and not args.webhook_updates_only:
                    # Explicitly set 'webhook_data', in case we want to change the information pushed to webhooks,
                    # similar to above and previous commits.
                    l_e = None

                    if lure_expiration is not None:
                        l_e = calendar.timegm(lure_expiration.timetuple())

                    wh_update_queue.put(('pokestop', {
                        'pokestop_id': b64encode(str(f['id'])),
                        'enabled': f['enabled'],
                        'latitude': f['latitude'],
                        'longitude': f['longitude'],
                        'last_modified': calendar.timegm(pokestops[f['id']]['last_modified'].timetuple()),
                        'lure_expiration': l_e,
                        'active_fort_modifier': active_fort_modifier
                    }))

            elif config['parse_gyms'] and f.get('type') is None:  # Currently, there are only stops and gyms
                gyms[f['id']] = {
                    'gym_id': f['id'],
                    'team_id': f.get('owned_by_team', 0),
                    'guard_pokemon_id': f.get('guard_pokemon_id', 0),
                    'gym_points': f.get('gym_points', 0),
                    'enabled': f['enabled'],
                    'latitude': f['latitude'],
                    'longitude': f['longitude'],
                    'last_modified': datetime.utcfromtimestamp(
                        f['last_modified_timestamp_ms'] / 1000.0),
                }

                # Send gyms to webhooks
                if args.webhooks and not args.webhook_updates_only:
                    # Explicitly set 'webhook_data', in case we want to change the information pushed to webhooks,
                    # similar to above and previous commits.
                    wh_update_queue.put(('gym', {
                        'gym_id': b64encode(str(f['id'])),
                        'team_id': f.get('owned_by_team', 0),
                        'guard_pokemon_id': f.get('guard_pokemon_id', 0),
                        'gym_points': f.get('gym_points', 0),
                        'enabled': f['enabled'],
                        'latitude': f['latitude'],
                        'longitude': f['longitude'],
                        'last_modified': calendar.timegm(gyms[f['id']]['last_modified'].timetuple())
                    }))

    if len(pokemons):
        db_update_queue.put((Pokemon, pokemons))
    if len(pokestops):
        db_update_queue.put((Pokestop, pokestops))
    if len(gyms):
        db_update_queue.put((Gym, gyms))

    log.debug('Parsing found %d pokemons, %d pokestops, and %d gyms',
             len(pokemons),
             len(pokestops),
             len(gyms))

    db_update_queue.put((ScannedLocation, {0: {
        'latitude': step_location[0],
        'longitude': step_location[1],
        'last_modified': datetime.utcnow(),
        'clat': current_location[0],
        'clon': current_location[1],
        'search_id': search_id,
        'step': step,
        'latmult': smults[0],
        'lonmult': smults[1]
    }}))

    for s in spawnpoints:
        Locations.update(spawnpoint=1, lastscan=datetime.utcnow()).where((Locations.lonmult == s[0]) & (Locations.latmult == s[1])).execute()
    return {
        'count': len(pokemons) + len(pokestops) + len(gyms),
        'gyms': gyms,
        'pokemons': pokemons
    }


def parse_gyms(args, gym_responses, wh_update_queue):
    gym_details = {}
    gym_members = {}
    gym_pokemon = {}
    trainers = {}

    i = 0
    for g in gym_responses.values():
        gym_state = g['gym_state']
        gym_id = gym_state['fort_data']['id']

        gym_details[gym_id] = {
            'gym_id': gym_id,
            'name': g['name'],
            'description': g.get('description'),
            'url': g['urls'][0],
        }

        if args.webhooks:
            webhook_data = {
                'id': gym_id,
                'latitude': gym_state['fort_data']['latitude'],
                'longitude': gym_state['fort_data']['longitude'],
                'team': gym_state['fort_data'].get('owned_by_team', 0),
                'name': g['name'],
                'description': g.get('description'),
                'url': g['urls'][0],
                'pokemon': [],
            }

        for member in gym_state.get('memberships', []):
            gym_members[i] = {
                'gym_id': gym_id,
                'pokemon_uid': member['pokemon_data']['id'],
            }

            gym_pokemon[i] = {
                'pokemon_uid': member['pokemon_data']['id'],
                'pokemon_id': member['pokemon_data']['pokemon_id'],
                'cp': member['pokemon_data']['cp'],
                'trainer_name': member['trainer_public_profile']['name'],
                'num_upgrades': member['pokemon_data'].get('num_upgrades', 0),
                'move_1': member['pokemon_data'].get('move_1'),
                'move_2': member['pokemon_data'].get('move_2'),
                'height': member['pokemon_data'].get('height_m'),
                'weight': member['pokemon_data'].get('weight_kg'),
                'stamina': member['pokemon_data'].get('stamina'),
                'stamina_max': member['pokemon_data'].get('stamina_max'),
                'cp_multiplier': member['pokemon_data'].get('cp_multiplier'),
                'additional_cp_multiplier': member['pokemon_data'].get('additional_cp_multiplier', 0),
                'iv_defense': member['pokemon_data'].get('individual_defense', 0),
                'iv_stamina': member['pokemon_data'].get('individual_stamina', 0),
                'iv_attack': member['pokemon_data'].get('individual_attack', 0),
                'last_seen': datetime.utcnow(),
            }

            trainers[i] = {
                'name': member['trainer_public_profile']['name'],
                'team': gym_state['fort_data']['owned_by_team'],
                'level': member['trainer_public_profile']['level'],
                'last_seen': datetime.utcnow(),
            }

            if args.webhooks:
                webhook_data['pokemon'].append({
                    'pokemon_uid': member['pokemon_data']['id'],
                    'pokemon_id': member['pokemon_data']['pokemon_id'],
                    'cp': member['pokemon_data']['cp'],
                    'num_upgrades': member['pokemon_data'].get('num_upgrades', 0),
                    'move_1': member['pokemon_data'].get('move_1'),
                    'move_2': member['pokemon_data'].get('move_2'),
                    'height': member['pokemon_data'].get('height_m'),
                    'weight': member['pokemon_data'].get('weight_kg'),
                    'stamina': member['pokemon_data'].get('stamina'),
                    'stamina_max': member['pokemon_data'].get('stamina_max'),
                    'cp_multiplier': member['pokemon_data'].get('cp_multiplier'),
                    'additional_cp_multiplier': member['pokemon_data'].get('additional_cp_multiplier', 0),
                    'iv_defense': member['pokemon_data'].get('individual_defense', 0),
                    'iv_stamina': member['pokemon_data'].get('individual_stamina', 0),
                    'iv_attack': member['pokemon_data'].get('individual_attack', 0),
                    'trainer_name': member['trainer_public_profile']['name'],
                    'trainer_level': member['trainer_public_profile']['level'],
                })

            i += 1
        if args.webhooks:
            wh_update_queue.put(('gym_details', webhook_data))

    # All this database stuff is synchronous (not using the upsert queue) on purpose.
    # Since the search workers load the GymDetails model from the database to determine if a gym
    # needs rescanned, we need to be sure the GymDetails get fully committed to the database before moving on.
    #
    # We _could_ synchronously upsert GymDetails, then queue the other tables for
    # upsert, but that would put that Gym's overall information in a weird non-atomic state.

    # upsert all the models
    if len(gym_details):
        bulk_upsert(GymDetails, gym_details)
    if len(gym_pokemon):
        bulk_upsert(GymPokemon, gym_pokemon)
    if len(trainers):
        bulk_upsert(Trainer, trainers)

    # This needs to be completed in a transaction, because we don't wany any other thread or process
    # to mess with the GymMembers for the gyms we're updating while we're updating the bridge table.
    with flaskDb.database.transaction():
        # get rid of all the gym members, we're going to insert new records
        if len(gym_details):
            DeleteQuery(GymMember).where(GymMember.gym_id << gym_details.keys()).execute()

        # insert new gym members
        if len(gym_members):
            bulk_upsert(GymMember, gym_members)

    log.info('Upserted %d gyms and %d gym members',
             len(gym_details),
             len(gym_members))


def db_updater(args, q):
    # The forever loop
    while True:
        try:

            while True:
                try:
                    flaskDb.connect_db()
                    break
                except Exception as e:
                    log.warning('%s... Retrying', e)

            # Loop the queue
            while True:
                model, data = q.get()
                bulk_upsert(model, data)
                q.task_done()
                log.debug('Upserted to %s, %d records (upsert queue remaining: %d)',
                          model.__name__,
                          len(data),
                          q.qsize())
                if q.qsize() > 50:
                    log.warning("DB queue is > 50 (@%d); try increasing --db-threads", q.qsize())

        except Exception as e:
            log.exception('Exception in db_updater: %s', e)


def clean_db_loop(args):
    while True:
        try:

            # Clean out old scanned locations
            query = (ScannedLocation
                     .delete()
                     .where((ScannedLocation.last_modified <
                            (datetime.utcnow() - timedelta(minutes=5)))))
            query.execute()

            # If desired, clear old pokemon spawns
            if args.purge_data > 0:
                query = (Pokemon
                         .delete()
                         .where((Pokemon.disappear_time <
                                (datetime.utcnow() - timedelta(hours=args.purge_data)))))

            log.info('Regular database cleaning complete')
            time.sleep(60)
        except Exception as e:
            log.exception('Exception in clean_db_loop: %s', e)


def bulk_upsert(cls, data):
    num_rows = len(data.values())
    i = 0
    step = 120

    while i < num_rows:
        log.debug('Inserting items %d to %d', i, min(i + step, num_rows))
        try:
            InsertQuery(cls, rows=data.values()[i:min(i + step, num_rows)]).upsert().execute()
        except Exception as e:
            log.warning('%s... Retrying', e)
            continue

        i += step


def create_tables(db):
    db.connect()
    verify_database_schema(db)
    db.create_tables([Pokemon, Pokestop, Gym, ScannedLocation, GymDetails, GymMember, GymPokemon, Trainer, Vars, PokeAlert, Locations], safe=True)
    db.close()


def drop_tables(db):
    db.connect()
    db.drop_tables([Pokemon, Pokestop, Gym, ScannedLocation, Versions, GymDetails, GymMember, GymPokemon, Trainer, Vars], safe=True)
    db.close()


def verify_database_schema(db):
    if not Versions.table_exists():
        db.create_tables([Versions])

        if ScannedLocation.table_exists():
            # Versions table didn't exist, but there were tables. This must mean the user
            # is coming from a database that existed before we started tracking the schema
            # version. Perform a full upgrade.
            InsertQuery(Versions, {Versions.key: 'schema_version', Versions.val: 0}).execute()
            database_migrate(db, 0)
        else:
            InsertQuery(Versions, {Versions.key: 'schema_version', Versions.val: db_schema_version}).execute()

    else:
        db_ver = Versions.get(Versions.key == 'schema_version').val

        if db_ver < db_schema_version:
            database_migrate(db, db_ver)

        elif db_ver > db_schema_version:
            log.error("Your database version (%i) appears to be newer than the code supports (%i).",
                      db_ver, db_schema_version)
            log.error("Please upgrade your code base or drop all tables in your database.")
            sys.exit(1)


def database_migrate(db, old_ver):
    # Update database schema version
    Versions.update(val=db_schema_version).where(Versions.key == 'schema_version').execute()

    log.info("Detected database version %i, updating to %i", old_ver, db_schema_version)

    # Perform migrations here
    migrator = None
    if args.db_type == 'mysql':
        migrator = MySQLMigrator(db)
    else:
        migrator = SqliteMigrator(db)

#   No longer necessary, we're doing this at schema 4 as well
#    if old_ver < 1:
#        db.drop_tables([ScannedLocation])

    if old_ver < 2:
        migrate(migrator.add_column('pokestop', 'encounter_id', CharField(max_length=50, null=True)))

    if old_ver < 3:
        migrate(
            migrator.add_column('pokestop', 'active_fort_modifier', CharField(max_length=50, null=True)),
            migrator.drop_column('pokestop', 'encounter_id'),
            migrator.drop_column('pokestop', 'active_pokemon_id')
        )

    if old_ver < 4:
        db.drop_tables([ScannedLocation])

    if old_ver < 5:
        # Some pokemon were added before the 595 bug was "fixed"
        # Clean those up for a better UX
        query = (Pokemon
                 .delete()
                 .where(Pokemon.disappear_time >
                        (datetime.utcnow() - timedelta(hours=24))))
        query.execute()

    if old_ver < 6:
        migrate(
            migrator.add_column('gym', 'last_scanned', DateTimeField(null=True)),
        )

    if old_ver < 7:
        migrate(
            migrator.drop_column('gymdetails', 'description'),
            migrator.add_column('gymdetails', 'description', TextField(null=True, default=""))
        )

def calc_distance(pos1, pos2):
    R = 6378.1  # km radius of the earth

    dLat = math.radians(pos1[0] - pos2[0])
    dLon = math.radians(pos1[1] - pos2[1])

    a = math.sin(dLat / 2) * math.sin(dLat / 2) + \
        math.cos(math.radians(pos1[0])) * math.cos(math.radians(pos2[0])) * \
        math.sin(dLon / 2) * math.sin(dLon / 2)

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = R * c

    return d

def setLockPos(pos):
    global lockpos
    
    lockpos = pos
    