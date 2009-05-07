"""
Basic timetric tests. These aren't easy to run since you need an authorized
OAuth client, so to make this work you'll need a timetric.conf file in this
directory with the appropriate authorized info. It should look like::

    [timetric_tests]
    consumer_secret = XXX
    consumer_key = XXX
    oauth_token = XXX
    oauth_secret = XXX
    
You'll realize that this means the tests doen't test the OAuth authenorization
flow. Patches welcome!
"""

import datetime
import ConfigParser
import os
import time
import timetric
import unittest
from cStringIO import StringIO

class TimetricTests(unittest.TestCase):
    
    def setUp(self):
        conf = ConfigParser.ConfigParser()
        conf.read(os.path.join(os.path.dirname(__file__), 'timetric.conf'))
        self.client = timetric.TimetricClient(dict(conf.items('timetric_tests')))
        
    def tearDown(self):
        self.client = None
        
    def make_series(self, data=None):
        return self.client.create_series(
            caption = 'Timetric-python test series',
            title = 'Timetric-python test series',
            data = data,
        )
    
    def test_create_series(self):
        series = self.make_series()
        self.assertEqual(list(series), [])
        series.delete()
        
    def test_create_series_with_data(self):
        data = [
            (1236735000, 1.0),
            (1236735500, 2.5),
            (1236736000, 5.0),
        ]
        series = self.make_series(data)

        # Give Timetric a bit to catch up before checking the data
        time.sleep(5)
        self.assertEqual(list(series), data)
        series.delete()
        
    def test_update_single_value(self):
        series = self.make_series()
        series.update(10.0)
        time.sleep(5)        
        self.assertEqual(float(series), 10.0)
        series.delete()
        
    def test_update_from_iterable(self):
        series = self.make_series()
        data = [
            (1236735000, 1.0),
            (1236735500, 2.5),
            (1236736000, 5.0),
        ]
        series.update(data)
        time.sleep(5)
        self.assertEqual(len(list(series)), 3)
        series.delete()
        
    def test_update_from_file(self):
        io = StringIO('1236735000,1.0\n1236735500,2.5\n1236736000,5.0')
        series = self.make_series()
        series.update(io)
        time.sleep(5)
        self.assertEqual(len(list(series)), 3)
        series.delete()
        
    def test_increment_decrement(self):
        series = self.make_series()
        series.update(10.0)
        series.increment(4.5)
        series.increment(-2.0)
        time.sleep(5)
        self.assertEqual(float(series), 12.5)
        series.delete()
        
    def test_increment_syntactic_sugar(self):
        series = self.make_series()
        series.update(10.0)
        series += 4.5
        series -= 2.0
        time.sleep(5)
        self.assertEqual(float(series), 12.5)
        series.delete()

    def test_rewrite(self):
        series = self.make_series()
        data = [
            (1236735000, 1.0),
            (1236735500, 2.5),
            (1236736000, 5.0),
        ]
        series.update(data)
        time.sleep(5)
        self.assertEqual(len(list(series)), 3)

        data2 = [
            (1236735000, 9),
            (1236735500, 10),
            (1236736000, 11),
        ]
        series.rewrite(data2)
        time.sleep(5)
        self.assertEqual(list(series), data2)

                
if __name__ == '__main__':
    import httplib2
    #httplib2.debuglevel = 1
    unittest.main()