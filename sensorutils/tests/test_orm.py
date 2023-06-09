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
        sn = du.get_serialnumber(self.eng, 1051, 'LP8')
        print(sn)

    def testListSensors(self):
        als = du.list_all_sensor_ids("LP8", self.eng,)
        print(als)

    def testGetCalibration(self):
        with self.session() as ses:
            stm = sqlalchemy.select(mods.Deployment)
            dt = ses.execute(stm).first()
            breakpoint()

    def testGetCalibrationInfo(self):
        with self.session() as ses:
            ci = cal.get_calibration_info(ses, 1151, sda.AvailableSensors.LP8)
        print(ci)

    def testGetLP8Data(self):
        fd = dt.datetime.now() - dt.timedelta(days=3)
        with self.session() as ses:
            ci = cal.get_sensor_data(ses, 1016, sda.AvailableSensors.LP8, fd, dt.datetime.now())
        breakpoint()
        print(ci)
    
    def testGrouper(self):
        fd = dt.datetime.now() - dt.timedelta(days=1214)
        aggs = {'CO2': 'AVG(senseair_lp8_co2)', 'CO2_MIN': 'MIN(senseair_lp8_co2)', 'CO2_MAX': 'MAX(senseair_lp8_co2)'}
        with self.session() as ses:
            grouped = cal.get_sensor_data_agg(ses, 1151, sda.AvailableSensors.LP8, fd, dt.datetime.now(), 3600*24, aggs)
            self.assertEqual(grouped.reset_index().diff()['time'].iloc[1], dt.timedelta(days=1).total_seconds())
            print(grouped)
    
    def testGetHPPData(self):
        fd = dt.datetime.now() - dt.timedelta(days=14)
        with self.session() as ses:
            ci = cal.get_sensor_data(ses, 445, sda.AvailableSensors.HPP, fd, dt.datetime.now())
        print(ci)
    
    def testGetCalData(self):
        fd = dt.datetime.now() - dt.timedelta(days=60)
        with self.session() as ses:
            cd = cal.get_cal_ts(ses, 1151, sda.AvailableSensors.LP8, fd, dt.datetime.now(), 600)
            print(cd)
    
    def testGetCalDataDep(self):
        fd = dt.datetime.now() - dt.timedelta(days=60)
        with self.session() as ses:
            
            cd = cal.get_cal_ts(ses, 1151, sda.AvailableSensors.LP8, fd, dt.datetime.now(), 600, dep=True)
            breakpoint()
            print(cd)

    def testGetCylinder(self):
        with self.session() as ses:
            sel = sqlalchemy.select(mods.Cylinder)
            res = ses.execute(sel)
            cyls = res.all()
            breakpoint()

    def testGetCylinderAnalysis(self):
        with self.session() as ses:
            sel = sqlalchemy.select(mods.CylinderAnalysis)
            res = ses.execute(sel)
            cyls = res.all()
            breakpoint()
    
    def testGetCylinderDeployment(self):
        with self.session() as ses:
            sel = sqlalchemy.select(mods.CylinderDeployment)
            res = ses.execute(sel)
            cyls = res.all()
            breakpoint()

    def testGetDeployment(self):
        with self.session() as ses:
            sel = sqlalchemy.select(mods.Deployment)
            res = ses.execute(sel)
            cyls = res.first()
            breakpoint()


    def testGetLocationsWithDeployment(self):
        with self.session() as ses:
            locs = du.list_locations_with_deployment(ses, start= dt.datetime(2022,1,1,0,0))
            breakpoint()

if __name__ == '__main__':
    unittest.main()