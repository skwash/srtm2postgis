# Read srtm data files and put them in a Cassandra ColumnFamily.
import pycassa
from pycassa.pool import ConnectionPool

import database_cas

import re
import sys

from math import sqrt
from read_data import verify, posFromLatLon, tileFromLatLon, loadTile

from data import files
from data import util

"""
To Create your keyspace and CF:
create keyspace SRTM with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = [{replication_factor:1}];
use SRTM;
create column family AltitudeSRTM3 WITH comparator=LongType AND key_validation_class=UTF8Type and default_validation_class = LongType;
"""

class ColumnFamilyCass:
  def __init__(self, keyspace, cf_name, nodelist):

    pool = ConnectionPool(keyspace, nodelist)
    self.cf = pycassa.ColumnFamily(pool, cf_name)

  def __del__(self):
    pass
    #self.cursor.connection.commit()

  def createColumnFamily(self):
    pass

  def dropColumnFamily(self):
    pass

  def purge(self):
    self.cf.truncate()

  def fetchTopLeftAltitude(self, lat, lon):
    tileName = tileFromLatLon(lat,lon)
    pos = posFromLatLon(lat,lon)

    try:
      res = self.cf.get(tileName, columns=[pos])
    except Exception:
      res = []

    return int(res[pos])

  def readTile(self, lat0, lon0):
    # We need the tile name to know which row key to get.
    tileName = tileFromLatLon(lat0,lon0)

    # Calculate begin and end position
    begin = posFromLatLon(lat0,lon0)
    end = posFromLatLon(lat0 + 1, lon0 + 1)

    try:
      data = self.cf.get(tileName, column_start=int(begin), column_finish=int(end))
      res = data.values()
    except Exception:
      res = []

    # Now turn the result into a 2D array

    tile = []

    # Calculate tile width (should be 1200, or 10 for test tiles)
    tile_width = int(sqrt(len(res)))
    i = 0
    for x in range(tile_width):
      row = []
      for y in range(tile_width):
        row.append(int(res[i]))
        i = i + 1

      tile.append(row)

    print tile
    return tile

  def insertTile(self, tile, lat0, lon0):

    # We need the tile name to know which row key to get.
    tileName = tileFromLatLon(lat0, lon0)
    #print "tileName:", tileName

    # Calculate begin position
    begin = posFromLatLon(lat0,lon0)

    # We want to accumulate a bunch of columns before we insert into Cassandra.
    columns = {}
    i=0
    insert_max=1000

    # We drop the top row and right column.
    for row in range(1, len(tile)):
      for col in range(0, len(tile) - 1):
        alt = long(tile[row][col])
        key = long(begin + (row-1) * 1200 + col)
        #print key, alt
        columns[key] = int(alt)

        # We've reached our ceiling, insert. 
        # We could do a bulk insert but since we're hitting one key, a single mutation should be more efficient.
        if i == insert_max:
          self.cf.insert(tileName, columns)
          columns = {}

    # Send insert any thing left after the loop exits.
    self.cf.insert(tileName, columns);

def main():
  db_cas = ColumnFamilyCass(database_cas.keyspace, database_cas.cf_name, database_cas.nodelist) 

  # Does the user want to empty the column family?
  if 'empty' in sys.argv:
    print "Purging data." 
    db_cas.purge()
    print "Done..."
    exit()

  try:
      continent = '_'.join(map(lambda s: s.capitalize(), re.split('[ _]', sys.argv[1])))
  except: 
      print "Please specify the continent. Africa, Australia, Eurasia, Islands, North_America or South_America."
      sys.exit(1)

  [north, south, west, east] = util.getBoundingBox(sys.argv, 3)

  files_hashes = util.getFilesHashes(continent)

  number_of_tiles = util.numberOfFiles(files_hashes, north, south, west, east)

  # Verify result?
  if 'verify' in sys.argv:
    verify(db_cas, number_of_tiles, files_hashes, continent,  north, south, west, east)
    #@todo how does this works?

  # If a tile name is given as the second argument it will resume from there.
  p = re.compile('[NSEW]\d*')
  resume_from = ""
  try:
    if(p.find(sys.argv[2])):
      resume_from = sys.argv[2]

  except: 
    None

  i = 0

  for file in files_hashes:
    # Strip .hgt.zip extension:
    file = file[1][0:-8] 
    # Get latitude and longitude from file name 
    [lat,lon] = util.getLatLonFromFileName(file)

    if util.inBoundingBox(lat, lon, north, south, west, east):
      i = i + 1

      # Are we resuming?
      if resume_from == file:
        resume_from = "" 

      if resume_from == "":

        # First check if the tile is not already in the database:
        try:
          db_cas.fetchTopLeftAltitude(lat, lon)

          print("Skipping tile " + file + " (" + str(i) + " / " + str(number_of_tiles) + ") ...")

        except IndexError:
          print("Insert data for tile " + file + " (" + str(i) + " / " + str(number_of_tiles) + ") ...")

          # Load tile from file
          tile = loadTile(continent, file)

          db_cas.insertTile(tile, lat, lon)

  print("All tiles inserted. You will want to run a nodetool repair.")

  print("Import is done.  Pleasy verify the result with python read_data_cas.py verify")


if __name__ == '__main__':
  main()
