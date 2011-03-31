# -*- coding: utf-8 -*-
# Downloads the SRTM data for Australia

# Import some libraries:
import httplib
import urllib
import re
import os
import shutil
import sys
import tempfile

from data import util

def handleDownload(block):
    file.write(block)
    print ".",

def usage():
    print "First argument should be Africa, Australia, Eurasia, Islands, North_America or South_America.\n",\
        "Argument 2-5 optionally specify a bounding box: north, south, west, east"

def main():
    # First we make a list of all files that need to be download. This depends
    # on the arguments given to the program.
    # The first argument should be the continent:
    # * Africa  
    # * Australia  
    # * Eurasia  
    # * Islands  
    # * North_America  
    # * South_America    

    try:
        continent = '_'.join(map(lambda s: s.capitalize(), re.split('[ _]', sys.argv[1])))
    except:
        continent = ""
    if not continent in ["Africa", "Australia", "Eurasia",  "Islands", "North_America", "South_America"]:
        usage()
        sys.exit(1)
        
    # First we get the list of files through an HTTP connection.
    http = httplib.HTTPConnection('dds.cr.usgs.gov')

    http.request("GET", "/srtm/version2_1/SRTM3/" + continent + "/");

    # Now list all tiles of that continent.
    # See http://dds.cr.usgs.gov/srtm/version2_1/SRTM3/[continent]/
    
    response = http.getresponse()
    html = response.read()

    files = re.findall(r'<a href="([^"]+)"', html)
    files.pop(0) #remove "Parent Directory" link
    
    # And close connection.
    
    http.close()

    # Now download all files using urllib.urlretrieve
    
    # Do we have a bounding box?
    [north, south, west, east] = util.getBoundingBox(sys.argv, 2)
    for i in range(len(files)):
      if not (os.path.exists("data/" + continent + "/" + files[i])):
          [lat,lon] = util.getLatLonFromFileName(files[i])
          if util.inBoundingBox(lat, lon, north, south, west, east):
            print "Downloading " + files[i] + " (lat = " + str(lat)  + " , lon = " + str(lon) + " )... (" + str(i + 1) + " of " + str(len(files)) +")"
            (f, tmp) = tempfile.mkstemp()
            try:
                urllib.urlretrieve("http://dds.cr.usgs.gov/srtm/version2_1/SRTM3/" + continent + "/"  + files[i], tmp)
                os.close(f)
                shutil.move(tmp, "data/" + continent + "/" + files[i])
            except Exception, msg:
                sys.stderr.write(str(msg) + "\n")
                os.remove(tmp)
            
if __name__ == '__main__':            
    main()
