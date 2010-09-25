# -*- coding: utf-8 -*-
# Downloads the SRTM data for Australia

# Import some libraries:
import httplib
import urllib
import re
import sys

from data import util

def handleDownload(block):
    file.write(block)
    print ".",

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

    if len(sys.argv) > 1:
        continent = sys.argv[1]
        util.verifyIsContinent(continent)
    else:
        print "Please provide arguments \n",\
        "First argument should be Africa, Australia, Eurasia, Islands, North_America or South_America.\n",\
        "Second argument (optional) specifies from which tile to resume. Use full file name e.g. \n",\
        "'N36W004.hgt.zip'. Set to 0 start at the first file. \n",\
        "Argument 3-6 optionally specify a bounding box: north, south, west, east"
        exit()
        
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
    
    # Determine if we need to resume at a certain point
    if(len(sys.argv) > 2):
        resume = sys.argv[2]
        if not(resume == "0"):
            skip = True     
            print "Resume from " + resume + "..."
        else:
            skip = False
    else: 
        skip = False

    # Do we have a bounding box?
    [north, south, west, east] = util.getBoundingBox(sys.argv, 3)
    for i in range(len(files)):
      if skip:
          if files[i] == resume:
              skip = False
          
      if not(skip):
          [lat,lon] = util.getLatLonFromFileName(files[i])
          if util.inBoundingBox(lat, lon, north, south, west, east):
            print "Downloading " + files[i] + " (lat = " + str(lat)  + " , lon = " + str(lon) + " )... (" + str(i + 1) + " of " + str(len(files)) +")"
            urllib.urlretrieve("http://dds.cr.usgs.gov/srtm/version2_1/SRTM3/" + continent + "/"  + files[i],"data/" + continent + "/" + files[i])
            
if __name__ == '__main__':            
    main()
