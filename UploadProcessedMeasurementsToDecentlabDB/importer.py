from __future__ import print_function

import os
from threading import Thread
import time
from datetime import datetime
import gzip
import json

import pymysql
import influxdb
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import sqlalchemy as sqa

import sensorutils.db as dbu
import sensorutils.secrets as sec

from typing import Dict

import itertools

import argparse as ap
parser = ap.ArgumentParser(description="Export ICOS-Cities metadata to influxdb")




loc_query = '''
             SELECT *
             FROM
               (SELECT SensorUnit_ID,
                       d.Date_UTC_from as ts,
                       d.LocationName as LocationName,
                       Remark,
                       HeightAboveGround,
                       h,
                       LAT_WGS84,
                       LON_WGS84,
                       H_WGS84,
                       Network,
                       SiteType,
                       1 as deployed
                FROM Deployment AS d
                LEFT JOIN `Location` AS l ON d.LocationName = l.LocationName AND d.Date_UTC_from > l.Date_UTC_from
                WHERE SensorUnit_ID = :s1

                UNION

                SELECT SensorUnit_ID,
                       LEAST(current_timestamp(), d.Date_UTC_to) as ts,
                       d.LocationName as LocationName,
                       Remark,
                       HeightAboveGround,
                       h,
                       LAT_WGS84,
                       LON_WGS84,
                       H_WGS84,
                       Network,
                       SiteType,
                       0 as deployed
                FROM Deployment AS d
                LEFT JOIN `Location` AS l ON d.LocationName = l.LocationName AND d.Date_UTC_from > l.Date_UTC_from
                WHERE SensorUnit_ID = :s1 AND d.Date_UTC_to < current_timestamp()

                UNION

                SELECT SensorUnit_ID,
                             Date_UTC_from,
                             NULL AS LocationName,
                             Reason AS Remark,
                             NULL,
                             NULL,
                             NULL,
                             NULL,
                             NULL,
                             NULL,
                             NULL,
                             0 as deployed
                FROM SensorExclusionPeriods
                WHERE SensorUnit_ID = :s1

                UNION

                SELECT SensorUnit_ID,
                             Date_UTC_to,
                             NULL AS LocationName,
                             Reason AS Remark,
                             NULL,
                             NULL,
                             NULL,
                             NULL,
                             NULL,
                             NULL,
                             NULL,
                             1 as deployed
                FROM SensorExclusionPeriods
                WHERE SensorUnit_ID = :s1) r
             ORDER BY ts'''


def unix_time_millis(dt, epoch=datetime.utcfromtimestamp(0)):
    return int((dt - epoch).total_seconds() * 1000)


def ornull(v, nullvalue=-999):
    return None if v == nullvalue else v


def get_entry(r):
    return {'measurement': 'metadata',
            'tags': {'latitude': ornull(r.get('LAT_WGS84')),
                     'longitude': ornull(r.get('LON_WGS84')),
                     'node': r['SensorUnit_ID'],
                     'altitude': ornull(r.get('h')),
                     'height': ornull(r.get('HeightAboveGround')),
                     'name': 'deployment',
                     'place': r['LocationName'],
                     'site': r.get('Network'),
                     'holder': r.get('SiteType'),
                     'remark': r.get('Remark')},
            'time': unix_time_millis(r['ts']),
            'fields': {'value': float(r['deployed'])}}

def get_info(con: sqa.engine.Connection, id: int) :
    dts = con.execute(sqa.text(loc_query),  s1=id)
    return dts


def importsql(eng: sqa.engine.Engine):

    #Get all deployed sensors
    with eng.connect() as con:
        all_sens = con.execute(sqa.text('SELECT DISTINCT(SensorUnit_ID) AS SensorUnit_ID FROM Deployment ORDER BY SensorUnit_ID'))

    
    with eng.connect() as con:
        dts = [get_info(con, sid) for sid, in all_sens]
        dps = [[get_entry(entry._asdict()) for entry in row] for r in dts if (row := r.fetchall())]

    passw = sec.get_key('decentlab')
    client = influxdb.InfluxDBClient("swiss.co2.live",
                                     443,
                                     None,
                                     None,
                                     "geo",
                                     path='/api/datasources/proxy/6',
                                     ssl=True,
                                     verify_ssl=True,
                                     headers={'Authorization': 'Bearer ' + passw})
    breakpoint()
    points = [dp for dp
              in itertools.chain(*dps)
              if dp['time'] != 4102444800 * 1000]

    print("writing %d points ..." % len(points))
    client.write_points(points, time_precision='ms')
    print("done")


args = parser.parse_args()
eng = dbu.connect_to_metadata_db()

importsql(eng)
  
