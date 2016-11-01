#!/usr/bin/python

from warnings import filterwarnings
import pymysql
filterwarnings('ignore', category = pymysql.Warning)
import math
import geopy
import geopy.distance
import sys
import os, time
def get_new_coords(init_loc, distance, bearing):
    """
    Given an initial lat/lng, a distance(in kms), and a bearing (degrees),
    this will calculate the resulting lat/lng coordinates.
    """
    origin = geopy.Point(init_loc[0], init_loc[1])
    destination = geopy.distance.distance(kilometers=distance).destination(origin, bearing)
    return (destination.latitude, destination.longitude)


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

    
dbc = pymysql.connect(host="localhost",    # your host, usually localhost
                     user="ub",         # your username
                     passwd="kusa0213",  # your password
                     db="pokemon")        # name of the data base
db = dbc.cursor(pymysql.cursors.DictCursor)
db2 = dbc.cursor(pymysql.cursors.DictCursor)

#pulse_radius = .034 * 2         # km - radius of players heartbeat is 70m
pulse_radius = .06 * 2         # km - radius of players heartbeat is 70m
mingroupsize = 6

db.execute("DELETE FROM pslink");
dbc.commit()
    
db.execute("SET sql_notes = 0;")
db.execute("SELECT * FROM pokestop limit 1")
row = list(db)[0]

holder = get_new_coords((row['latitude'], row['longitude']), pulse_radius, 0)
londist = abs(row['latitude'] - holder[0])
holder = get_new_coords((row['latitude'], row['longitude']), pulse_radius, 270)
latdist = abs(row['longitude'] - holder[1])

#fetch initial list of all stops

db.execute("SELECT pokestop_id,latitude,longitude from pokestop")
rows = db.fetchall()

print "Grabbing all Pokestops"

#main loop
ctr = 0
hh = 0
hhh = 0
for row in rows:
    rlon = row['longitude']
    rlat = row['latitude']
    rxmin = rlon - londist
    rxmax = rlon + londist
    rymin = rlat - latdist
    rymax = rlat + latdist
    q = "SELECT pokestop_id,latitude,longitude from pokestop where latitude >= %f AND latitude <= %f AND longitude >= %f AND longitude <= %f AND pokestop_id != '%s'" % (rymin, rymax, rxmin, rxmax, row['pokestop_id'])
#    print "%f, %f: %s" % (row['latitude'], row['longitude'], q)
    db.execute(q)
    crows = db.fetchall()
    ct = len(crows)
    
    lead = row['pokestop_id']
    

    for crow in crows:
        dist = calc_distance((crow['latitude'], crow['longitude']), (rlat, rlon))
        hid = crow['pokestop_id']
        if dist > pulse_radius:
#            print "Pokestop %s too far (%.4f) from %s (%f, %f)" % (hid, dist, lead, londist, latdist)
            ct = ct - 1
            continue
        if ct <= 0:
            continue
        
        rr = db2.execute("INSERT INTO pslink (head, tail, distance) VALUES ('%s', '%s', %d)" % (lead, hid, dist * 10000))
        dbc.commit()

    if ct > 0:
        print "Pokestop %s has %d nearby stops" % (row['pokestop_id'], ct)
    if ct > hh:
        hh = ct
        hhh = lead
    
print "Total: %d\nHighest is %d with %d" % (ctr, hhh, hh)

dbc.close()



