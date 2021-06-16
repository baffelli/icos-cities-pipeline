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
import mysql.connector as mariadb

PATH = os.environ.get('FTP_DIR', './')

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
                WHERE SensorUnit_ID = {dev}

                UNION

                SELECT SensorUnit_ID,
                       d.Date_UTC_to as ts,
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
                WHERE SensorUnit_ID = {dev}

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
                WHERE SensorUnit_ID = {dev}

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
                WHERE SensorUnit_ID = {dev}) r
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


def importsql():
    # print("importing from %s ..." % path)
    #time.sleep(10)
    con = pymysql.connect(read_default_file="~/.my.cnf", read_default_group="CarboSense_MySQL",
                          charset='utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)
    try:
        # if path.endswith('.gz'):
        #     openf = gzip.open
        # elif path.endswith('.sql'):
        #     openf = open
        # else:
        #     print("not supported file: " + path)
        #     return

        # with openf(path, 'rb') as f:
        #     sql = f.read()

        # with con.cursor() as cur:
        #     cur.execute(sql)
        #     con.commit()

        with con.cursor() as cur:
            cur.execute('SELECT DISTINCT(SensorUnit_ID) '
                        'FROM Deployment '
                        'ORDER BY SensorUnit_ID')
            devs = cur.fetchall()

        dps = []
        for dev in devs:
            with con.cursor() as cur:
                cur.execute(loc_query.format(dev=dev['SensorUnit_ID']))
                while True:
                    r = cur.fetchone()
                    if r is None:
                        break
                    dps.append(get_entry(r))
    finally:
        con.close()
    passw = "eyJrIjoiN2lSN1VJQjg1OUkwOWJyeTZUUFBiSVNDRjh5WGxGZTMiLCJuIjoic2ltb25lLmJhZmZlbGxpQGVtcGEuY2giLCJpZCI6MX0="
    client = influxdb.InfluxDBClient("swiss.co2.live",
                                     443,
                                     None,
                                     None,
                                     "geo",
                                     path='/api/datasources/proxy/6',
                                     ssl=True,
                                     verify_ssl=True,
                                     headers={'Authorization': 'Bearer ' + passw})
    points = [dp for dp
              in dps
              if dp['time'] != 4102444800 * 1000]
    print("writing %d points ..." % len(points))
    client.write_points(points, time_precision='ms')
    print("done")


if __name__ == "__main__":
    import sys
    con = pymysql.connect(read_default_file="~/.my.cnf", read_default_group="CarboSense_MySQL")
    importsql()
    sys.exit()

    running = []
    event_handler = PatternMatchingEventHandler(patterns=['*.sql.gz',
                                                          '*.sql'],
                                                ignore_directories=True)
    event_handler.on_created = lambda e: (print(e),
                                          Thread(target=importsql,
                                                 args=(e.src_path,)).start())
    observer = Observer()
    observer.schedule(event_handler, path=PATH, recursive=False)
    print("starting observer ...")
    observer.start()
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    for r in running:
        r.join()
