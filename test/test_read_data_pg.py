import sys
import os

import unittest
sys.path += [os.path.abspath('.')]

import database_pg_test 

import read_data_pg
from read_data import loadTile, posFromLatLon

from data.util import getLatLonFromFileName

class TestImportScript(unittest.TestCase):
    def testPointsPerTile(self):
      data = loadTile('Australia', 'S11E119')
      self.assertEqual(len(data) * len(data[0]),1201*1201)
    
    def testFirstNumber(self):
      data = loadTile('Australia', 'S11E119')
      self.assertEqual(data[0][0],0)

    def testTileIntegrity(self):
      # The sum of altitudes of all 1201*1201 points in two tiles
     
       # Melbourne (West):
      west = loadTile('Australia', "S37E144")
      self.assertEqual(west.sum(),223277664)

      ## Melbourne (East):
      east = loadTile('Australia', "S37E145")
      self.assertEqual(east.sum(), 271136969)
    
    def testTileOverlap(self):
      # Uses two adjecant tiles around Melbourne to test
      # * Eastern edge of West Melbourne tile should be the same as 
      #   western edge of East Melbourne tile.
      
      # Melbourne (West):
      west = loadTile('Australia', "S37E144")

      ## Melbourne (East):
      east = loadTile('Australia', "S37E145")

      for i in range(1201):
        self.assertEqual(west[i][1200] - east[i][0],0) 

      # Also try for north - south boundary:
      north = loadTile('Australia', "S36E144")
      south = loadTile('Australia', "S37E144")
      
      for i in range(1201):
        self.assertEqual(north[1200][i] - south[0][i],0) 

      # Some other area north - south boundary:
      south = loadTile('Australia', "S33E147")
      north = loadTile('Australia', "S32E147")

      for i in range(1201):
        self.assertEqual(north[1200][i] - south[0][i],0) 

      # Some other area west - east boundary:
      west = loadTile('Australia', "S33E147")

      ## Melbourne (East):
      east = loadTile('Australia', "S33E148")

      for i in range(1201):
        self.assertEqual(west[i][1200] - east[i][0],0) 


class TestDatabase(unittest.TestCase):
  def setUp(self):
    # Drop all tables
    self.db_psycopg2 = read_data_pg.DatabasePsycopg2(database_pg_test.db, database_pg_test.db_user, database_pg_test.db_pass) 
    self.db_psycopg2.dropAllTables()

  def testDatabasePresent(self):
    # Already tested in setUp()
    self.assert_(True)

  def testDatabaseEmpty(self):
    self.assert_(self.db_psycopg2.checkDatabaseEmpty())
  
  def testTableAltitudeExists(self):
    # Call function to create table
    self.db_psycopg2.createTableAltitude()

    tables = self.db_psycopg2.getTables()
    
    self.assert_('altitude' in tables) 

  def testGetLatLonFromFileName(self):
    self.assertEqual([-11,119], getLatLonFromFileName("S11E119"))
    self.assertEqual([11,119], getLatLonFromFileName("N11E119"))
    self.assertEqual([11,-119], getLatLonFromFileName("N11W119"))
    self.assertEqual([-11,-119], getLatLonFromFileName("S11W119"))

  def testPosFromLatLon(self):
    self.assertEqual(posFromLatLon(0,0),0) 
    self.assertEqual(posFromLatLon(0,1),1200*1200) 
    self.assertEqual(posFromLatLon(0,2),1200*1200*2) 
    self.assertEqual(posFromLatLon(1,0),1200*1200*360) 
    self.assertEqual(posFromLatLon(0,-1),-1200*1200) 
    self.assertEqual(posFromLatLon(-37,145),(-37 * 360 + 145) * 1200 * 1200) 
  
  def testInsertTileIntoDatabase(self):
    # Create table
    self.assert_(self.db_psycopg2.createTableAltitude())

    # Load example tile
    fulltile = loadTile('Australia', 'S37E145')
    tile = []
    for row in fulltile[0:11]:
      tile.append(row[0:11])

    # Get lat and lon from filename
    [lat,lon] = getLatLonFromFileName("S37E145")

    # Make the tile smaller, so this will be faster:
    # 11x11 tile: because the top row and right column are dropped,
    # only the bottom-left 10x10 tile will be stored in the
    # database.

    # Insert tile into database
    # We use psycopg2 for the connection in this case.
    self.db_psycopg2.insertTile(tile, lat, lon)

    # Check if the tile is indeed in the database
    tile_back = self.db_psycopg2.readTile(lat, lon)
    for i in range(len(tile) - 1):
      for j in range(len(tile) - 1):
        self.assert_(tile_back[i][j] == tile[i+1][j])
  
  def testInsertTileWithNull(self):
    # Create table
    self.assert_(self.db_psycopg2.createTableAltitude())

    # Some tiles contain the value -32768, which means NULL (not implemented yet)
    # Tile S27E123 has several -32768 values, for example tile[1086][462]
    fulltile = loadTile('Australia', 'S27E123')
    self.assertEqual(fulltile[1086][462], -32768)

    # Take part of the tile around that area
    tile = []
    for row in fulltile[1080:1091]:
      tile.append(row[460:471])

    # Get lat and lon from filename
    [lat,lon] = getLatLonFromFileName("S27E123")

    # Insert tile into database
    self.db_psycopg2.insertTile(tile, lat, lon)

    # Check if the tile is indeed in the database
    tile_back = self.db_psycopg2.readTile(lat, lon)
    for i in range(len(tile) - 1):
      for j in range(len(tile) - 1):
        self.assert_(tile_back[i][j] == tile[i+1][j])

  def tearDown(self):
    # Drop all tables that might have been created:
    self.db_psycopg2.dropAllTables()
    self.db_psycopg2 = None

if __name__ == '__main__':
    unittest.main()
