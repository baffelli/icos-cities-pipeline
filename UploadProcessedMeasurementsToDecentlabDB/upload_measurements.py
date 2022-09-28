"""
Exports the CO2 measurements from the mysql db to decentlab influxdb
- incremental export
- configurable using :obj:`sensorutils.files.DataSourceMapping`
"""


import os
from threading import Thread

from datetime import datetime

import influxdb
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import sqlalchemy as sqa

import sensorutils.db as dbu
import sensorutils.files as fu
import sensorutils.secrets as sec
import sensorutils.data as du
import sensorutils.decentlab as dec
import sensorutils.log as log
import argparse as ap

import pathlib as pl 

#Setup command line parser
parser = ap.ArgumentParser(description="Export ICOS-Cities measurements to influxdb")
parser.add_argument('config', type=pl.Path, help="Path of the data source mapping configuration file")
parser.add_argument('type', type=du.AvailableSensors, help="Sensor type")
parser.add_argument('id', type=str, help="Sensor id to copy")
parser.add_argument('--full', action='store_true', help="If set, dump full timeseries instead of last n days (use --backfill to set the value)")
parser.add_argument('--backfill', type=int, default=10 , help="If set, dump full timeseries instead of last n days (use --backfill to set the value)")
parser.add_argument('--date', type=datetime.fromisoformat)
args = parser.parse_args()

#Read datasource mapping
mapping = fu.DataMappingFactory.read_config(args.config)[args.type.value]
#Connect databases
eng = dbu.connect_to_metadata_db()
influx_client = dec.decentlab_client(token=sec.get_key('decentlab'), database=mapping.dest.db)
mapping.source.attach_db(eng)
mapping.dest.attach_db(influx_client)
#Check id
sn = dbu.get_serialnumber(eng, args.id, args.type.value, start=du.ICOS_START, end=du.ICOS_MISSING)
if sn is not None:
    #FIXME: the group key in the configuration file should have a key called `node` for this line to work
    groups = dict(node=args.id)
    missing_dates = mapping.list_files_missing_in_dest(group=groups, all=args.full, backfill=args.backfill) if not args.date else [args.date]
    for date in missing_dates:
        log.logger.info(f"Copying {date}")
        source_file = mapping.source.read_file(date, group=groups)
        dest_file = mapping.map_file(source_file)
        mapping.dest.write_file(dest_file, group=groups)
else:
    raise ValueError(f"Sensor {args.type} with {args.id} does not exist")
