import unittest
from sensorutils import files as fu
from sensorutils import db as du
from sensorutils import models as mods
from sensorutils import calibration as cal
from sensorutils import data as sda

import pandas as pd
import datetime as dt
import sqlalchemy

import sqlalchemy.orm as orm

class TestOrm(unittest.TestCase):

    def setUp(self) -> None:
        eng = du.connect_to_metadata_db()
        self.session = orm.sessionmaker(eng)
        self.eng = eng
        md = sqlalchemy.MetaData(bind=eng)
        md.reflect()
        self.md = md
    
    def testGetSerialnumber(self):
        sn = du.get_serialnumber(self.eng, self.md, 1051, 'LP8')
        print(sn)

    def testListSensors(self):
        als = du.list_all_sensor_ids("LP8", self.eng,)
        print(als)

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
        fd = dt.datetime.now() - dt.timedelta(days=1214)
        aggs = {'CO2': 'AVG(senseair_lp8_co2)', 'CO2_MIN': 'MIN(senseair_lp8_co2)', 'CO2_MAX': 'MAX(senseair_lp8_co2)'}
        with self.session() as ses:
            grouped = cal.get_sensor_data_agg(ses, 1151, sda.AvailableSensors.LP8, fd, dt.datetime.now(), 3600*24, aggs)
            self.assertEqual(grouped.diff()['time'].iloc[1], dt.timedelta(days=1).total_seconds())
            print(grouped)
    
    def testGetHPPData(self):
        fd = dt.datetime.now() - dt.timedelta(days=14)
        with self.session() as ses:
            ci = cal.get_sensor_data(ses, 445, sda.AvailableSensors.HPP, fd, dt.datetime.now())
        print(ci)

if __name__ == '__main__':
    unittest.main()