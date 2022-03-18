import unittest
import tempfile as tf
from sensorutils import files as fu
from sensorutils import data as icos_data
import json
import yaml
import pandas as pd
import pathlib as pl

class TestParameters(unittest.TestCase):

    def setUp(self) -> None:
        self.param_path = pl.Path(fu.get_g_drive(), '503_Themen', 'CarboSense', 'Kalibrationen Picarros_clean.xlsx')

    def test_read_xls(self):
        data = icos_data.read_picarro_calibration_parameters(self.param_path)
        cal_obj = icos_data.make_calibration_parameters_table(data)
        import pdb; pdb.set_trace()


if __name__ == '__main__':
    unittest.main()