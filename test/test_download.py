# -*- coding: utf-8 -*-
# Runs tests for download.py
import unittest

# Import same libraries as download.py:
import httplib
import urllib

class TestDownloadScript(unittest.TestCase):
    
    def testFtp(self):
        # Make sure:
        
        print "Does the FTP server still exists and is it online?"
        self.http = httplib.HTTPConnection('dds.cr.usgs.gov')

        print "Is the data in the right folder?"
        self.http.request("GET", "/srtm/version2_1/SRTM3/Australia/");
        r1 = self.http.getresponse();
        self.assertEqual(r1.status, 200)

        print "Are there 1060 files in that folder?"
        self.assertEqual(r1.read().count('<a href='), 1060+1) #+1 for "Parent Directory" Link

if __name__ == '__main__':
    unittest.main()

