import imp
import unittest
import tempfile as tf
from sensorutils import files as fu
from sensorutils import secrets as se
from sensorutils import calc as calc
import json
import yaml
import pandas as pd


def relative_error(res:float, target:float) -> float:
    return abs(res - target)/target

class TestCalcs(unittest.TestCase):

    def test_pw(self):
        """
        Test if the computation of saturation vapour pressure is correct
        having a relative error smaller than 0.3 %
        Data for comparison is taken from
        https://en.wikipedia.org/wiki/Vapour_pressure_of_water
        """
        tb = [(0, 0.6113e3), (20, 2.3388e3), (35, 5.6267e3), (100, 101.32e3)]
        for t, res in tb:
            re = relative_error(calc.saturation_vapor_pressure(calc.T_0 + t), res)
            print(re)
            self.assertTrue(re < 0.3/100)
    
    def test_ah(self):
        """
        Test if the computation of absolute humidity is correct
        """
        self.assertAlmostEquals(calc.rh_to_ah(80,  calc.T_0 + 20), 13.83, 2)

if __name__ == '__main__':
    unittest.main()