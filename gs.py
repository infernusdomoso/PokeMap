#!/usr/bin/python

import pymysql
import math
import geopy
import geopy.distance

def get_new_coords(init_loc, distance, bearing):
    """
    Given an initial lat/lng, a distance(in kms), and a bearing (degrees),
    this will calculate the resulting lat/lng coordinates.
    """
    origin = geopy.Point(init_loc[0], init_loc[1])
    destination = geopy.distance.distance(kilometers=distance).destination(origin, bearing)
    return (destination.latitude, destination.longitude)
    
dbc = pymysql.connect(host="localhost",    # your host, usually localhost
                     user="ub",         # your username
                     passwd="kusa0213",  # your password
                     db="pokemon")        # name of the data base
db = dbc.cursor(pymysql.cursors.DictCursor)

pulse_radius = .034         # km - radius of players heartbeat is 70m

db.execute("SELECT * FROM pokestop limit 1")
row = list(db)[0]

holder = get_new_coords((row['latitude'], row['longitude']), pulse_radius, 0)
londist = abs(row['latitude'] - holder[0])
holder = get_new_coords((row['latitude'], row['longitude']), pulse_radius, 270)
latdist = abs(row['longitude'] - holder[1])

#fetch initial list of all stops

db.execute("SELECT idkey,latitude,longitude from pokestop")
rows = db.fetchall()

#main loop
ctr = 0
for row in rows:
    rxmin = row['longitude'] - londist
    rxmax = row['longitude'] + londist
    rymin = row['latitude'] - latdist
    rymax = row['latitude'] + latdist
    db.execute("SELECT idkey,latitude,longitude from pokestop where latitude >= %f AND latitude <= %f AND longitude >= %f AND longitude <= %f AND idkey != %d" % (rymin, rymax, rxmin, rxmax, row['idkey']))
    crows = db.fetchall()
    if len(crows) <= 2:
        continue
    ctr = ctr + 1
    
print "%d: %d" % (row['idkey'], len(crows))

dbc.close()



