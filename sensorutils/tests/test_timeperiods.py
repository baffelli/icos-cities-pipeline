import unittest
import tempfile as tf
from sensorutils import files as fu
from sensorutils import secrets as se
from sensorutils import calc as calc
from sensorutils import utils as utils
import json
import yaml
import pandas as pd
import datetime as dt


def relative_error(res:float, target:float) -> float:
    return abs(res - target)/target

class TestPeriods(unittest.TestCase):

    def setUp(self) -> None:
        self.start = dt.datetime.now()
        self.end = self.start + dt.timedelta(days=10)
        self.start1 = self.start + dt.timedelta(days=1)
        self.end1 = self.end + dt.timedelta(days=-1)
        self.end2 = self.end + dt.timedelta(days=2)
    def test_check(self):
        with self.assertRaises(ValueError):
            utils.TimeInterval(self.end, self.start)

    def test_contains(self):
        p1 = utils.TimeInterval(self.start, self.end, mult=1)
        p2 = utils.TimeInterval(self.start1, self.end1, mult=1)
        print(p1.draw(p2))
        self.assertTrue(p1.contains(p2))
    
    def test_overlaps(self):
        p1 = utils.TimeInterval(self.start, self.end, mult=1)
        p2 = utils.TimeInterval(self.start1, self.end2, mult=1)
        print(p1.draw(p2))
        self.assertTrue(p1.overlaps(p2))

if __name__ == '__main__':
    unittest.main()