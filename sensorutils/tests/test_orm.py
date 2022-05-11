import unittest
from sensorutils import files as fu
from sensorutils import db as du
from sensorutils import models as mods
from sensorutils import calibration as cal
from sensorutils import data as sda

import pandas as pd
import datetime as dt

import sqlalchemy.orm as orm

class TestOrm(unittest.TestCase):

    def setUp(self) -> None:
        eng = du.connect_to_metadata_db()
        self.session = orm.sessionmaker(eng)
        

    def testGetCalibration(self):
        with self.session() as ses:
            ci = cal.get_calibration_info(ses, 1151, 'LP8', dt.datetime.now())
        print(ci)

    def testGetLP8Data(self):
        fd = dt.datetime.now() - dt.timedelta(days=14)
        with self.session() as ses:
            ci = cal.get_sensor_data(ses, 1151, sda.AvailableSensors.LP8, fd, dt.datetime.now())
        print(ci)
    
    def testGrouper(self):
        fd = dt.datetime.now() - dt.timedelta(days=14)
        aggs = {'CO2': 'AVG(senseair_lp8_co2)'}
        with self.session() as ses:
            cal.group_data(ses, 1151, sda.AvailableSensors.LP8, fd, dt.datetime.now(), 600, aggs)
            # grouped_table = cal.grouper_factory(mods.HPPData)
            # res = ses.query(grouped_table).first()
            # print(res)
    
    def testGetHPPData(self):
        fd = dt.datetime.now() - dt.timedelta(days=14)
        with self.session() as ses:
            ci = cal.get_sensor_data(ses, 445, sda.AvailableSensors.HPP, fd, dt.datetime.now())
        print(ci)

if __name__ == '__main__':
    unittest.main()