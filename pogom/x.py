from math import atan2, degrees, pi
import urllib2
import json
import geopy
import math

def get_new_coords(init_loc, distance, bearing):
    R = 6378.1 #Radius of the Earth
    brng = math.radians(bearing) #Bearing is 90 degrees converted to radians.
    d = distance #Distance in km

    lat1 = math.radians(init_loc[0]) #Current lat point converted to radians
    lon1 = math.radians(init_loc[1]) #Current long point converted to radians

    lat2 = math.asin( math.sin(lat1)*math.cos(d/R) +
         math.cos(lat1)*math.sin(d/R)*math.cos(brng))

    lon2 = lon1 + math.atan2(math.sin(brng)*math.sin(d/R)*math.cos(lat1),
                 math.cos(d/R)-math.sin(lat1)*math.sin(lat2))

    lat2 = degrees(lat2)
    lon2 = degrees(lon2)
    return (lat2, lon2)

p = list()

p.append((39.519252, -76.636943))
p.append((39.518872, -76.636407))
p.append((39.518425, -76.636085))
p.append((39.518160, -76.635699))

latd = p[3][0] - p[2][0]
lond = p[3][1] - p[2][1]

ctr = 0
rads = atan2(lond, latd)
degs = degrees(rads)
if degs < 0:
    degs = 360 + degs        

while ctr < 4:
    nc = get_new_coords(p[3+ctr], .064, degs)
    print "%f %f" % (nc[0], nc[1])
    p.append((nc[0], nc[1]))
    ctr += 1

path = "https://roads.googleapis.com/v1/snapToRoads?key=AIzaSyB3W6_-0lVvikQObH4TbBjy7l0PJxhQO7k&path="
for pp in p:
    path = "%s%f,%f|" % (path, pp[0], pp[1])

path = path.strip("|")

content = json.loads(urllib2.urlopen(path).read())
c = content['snappedPoints']
for x in c:
    lat = x['location']['latitude']
    lon = x['location']['longitude']
    print "%d: %.8f,%.8f" % (x['originalIndex'], lat, lon)
    
