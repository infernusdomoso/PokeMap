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
pulse_radius = .2         # km - radius of players heartbeat is 70m
mingroupsize = 6

db.execute("DELETE FROM groups");
dbc.commit()
    
db.execute("SET sql_notes = 0;")
db.execute("SELECT * FROM pokestop limit 1")
row = list(db)[0]

holder = get_new_coords((row['latitude'], row['longitude']), pulse_radius, 0)
londist = abs(row['latitude'] - holder[0])
holder = get_new_coords((row['latitude'], row['longitude']), pulse_radius, 270)
latdist = abs(row['longitude'] - holder[1])

db.execute("SELECT max(groupid) FROM groups")
row = list(db)
if not len(row) or row[0]['max(groupid)'] is None:
    nextgroup = 1
else:
    nextgroup = row[0]['max(groupid)']


#fetch initial list of all stops

db.execute("SELECT idkey,latitude,longitude from pokestop")
rows = db.fetchall()

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
    q = "SELECT idkey,latitude,longitude from pokestop where latitude >= %f AND latitude <= %f AND longitude >= %f AND longitude <= %f AND idkey != %d" % (rymin, rymax, rxmin, rxmax, row['idkey'])
#    print "%f, %f: %s" % (row['latitude'], row['longitude'], q)
    db.execute(q)
    crows = db.fetchall()
    ct = len(crows)
    if ct <= mingroupsize:
        continue
    ctr = ctr + 1
    
    lead = row['idkey']
    
    db.execute("INSERT INTO groups (groupid, lead, member) VALUES (%d, %d, %d)" % (nextgroup, lead, lead));
    dbc.commit()
    
    if ct > hh:
        hh = ct
        hhh = row['idkey']
    for crow in crows:
        dist = calc_distance((crow['latitude'], crow['longitude']), (rlat, rlon))
        if dist > pulse_radius:
            ct = ct - 1
            continue
        if ct <= mingroupsize:
            db.execute("DELETE FROM groups WHERE groupid=%d" % nextgroup)
            dbc.commit()
            nextgroup = nextgroup - 1
            break
        hid = crow['idkey']
        
        rr = db2.execute("select * from groups as g1 inner join groups as g2 on g1.groupid=g2.groupid where g1.member=%d AND g2.member=%d" % (lead, hid))
        rrows = db2.fetchall()
        if not len(rrows):
            db.execute("INSERT INTO groups (groupid, lead, member) VALUES (%d, %d, %d)" % (nextgroup, lead, hid))
            dbc.commit()

    nextgroup = nextgroup + 1
    
    
    
    
#        else:
#            grows = grows[0]
#            if grows['m1'] == crow['idkey']:
##                sf = grows['m2']
#            else:
#                sf = grows['m1']
#            db.execute("SELECT pokestop.idkey,pokestop.latitude,pokestop.longitude,groups.groupid,groups.m1,groups.m2 FROM groups INNER JOIN pokestop ON pokestop.idkey = groups.m1 OR pokestop.idkey = groups.m2 WHERE groups.m1 = %d OR groups.m2 = %d" % (sf, sf))
#            mgrows = db.fetchall()
#            for mgrow in mgrows:
#                if mgrow['m1'] == crow['idkey'] or mgrow['m2'] == crow['idkey']:
#                    continue
#                dist = calc_distance((crow['latitude'], crow['longitude']), (mgrow['latitude'], mgrow['longitude']))
##                if dist > .069:
#                    continue
#                lid = mgrow['idkey']
##                hid = crow['idkey']
#                if lid > hid:
#                    hid = mgrow['idkey']
#                    lid = crow['idkey']
#                print "xxNo len, %d %d (%d %d) %s" % (lid, hid, mgrow['groupid'], crow['idkey'], mgrow)
#                db.execute("INSERT IGNORE INTO groups (groupid, m1, m2) VALUES (%d, %d, %d)" % (mgrow['groupid'], lid, hid))
#                dbc.commit()

print "Total: %d\nHighest is %d with %d" % (ctr, hhh, hh)

dbc.close()



