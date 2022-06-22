"""
This script dumps the raw data from the decentlab DB into a database table
"""
import pandas as pd
import sqlalchemy.engine as eng
import sqlalchemy as db 
import itertools as ito

import sensorutils.decentlab as dl
import sensorutils.db as db_utils
import sensorutils.secrets as sec
import sensorutils.files as fu
import sensorutils.data as du
import sensorutils.command_line as cu
from datetime import datetime as dt
from datetime import timedelta 
import argparse as ap

import pathlib as pl

from sensorutils.log import logger 

from typing import List, Match, Optional
import re

def parse_range(input_range:str) -> Optional[List[int]]:
	"""
	Parses a range in the format a-b
	and return a list of numbers
	"""
	parser = re.compile(r'(\d+)(-)?(\d+)?')
	matches = parser.match(input_range)
	e = ValueError(f"Invalid range specification: {input_range}")
	match matches:
		case None:
			raise e
		case m:
			match m.groups():
				case x, None, None:
					rg = [int(x)]
				case x, _, y:
					rg = list(range(int(x), int(y)))
				case _:
					raise e
	return rg

#Read command line arguments

parser = ap.ArgumentParser(description='Import raw data from decentlab into database')
parser.add_argument('config', type=cu.path_or_config, help='Path to the datasource mapping configuration file or a dict of config')
parser.add_argument('sensor_type', type=du.AvailableSensors, help='Sensor type to import')
parser.add_argument('id', type=parse_range, nargs='?', help='Sensor id to import: either number or numeric range start-end')
parser.add_argument('start', type=dt.fromisoformat, help='First date to import')
parser.add_argument('--import-all', default=False, action='store_true', help='Import all data or only incremental import?')
parser.add_argument('--backfill', default=0, type=int, help='Backfill time for incremental import')

args = parser.parse_args()
logger.info('Started')
#Mapping table <> sensor type
breakpoint()

#Get API key from secrets
passw = sec.get_key('decentlab')
#Create influxdb client
client = dl.decentlab_client(token=passw)
#Connect to  target database
logger.info('Connecting to the DB')
engine = db_utils.connect_to_metadata_db()
#Set the start date of the loading
cfg = args.config[args.sensor_type.value]
cfg['dest']['date_from'] = args.start
cfg['source']['date_from'] = args.start
mapping = fu.DataMappingFactory.create_mapping(**cfg)
#Attach the client to the source and destination
mapping.source.attach_db(client)
mapping.dest.attach_db(engine)

#List all ids
if not args.id:
	logger.info(f"Getting all ids for sensor type {args.sensor_type}")
	sensor_ids = db_utils.list_all_sensor_ids(args.sensor_type.value, engine)
else:
	logger.info('Processing only sensor with id {args.id}')
	sensor_ids = pd.DataFrame({'SensorUnit_ID':args.id})
#Iterate over all sensors
for row in sensor_ids.itertuples():
	logger.info(f"Listing missing files for sensor  {row.SensorUnit_ID}")
	all_missing = mapping.list_files_missing_in_dest(group=dict(node=row.SensorUnit_ID), backfill=args.backfill, all=args.import_all)
	missing = [m for m in all_missing if m >= args.start]
	logger.info(f"The missing files for sensor {row.SensorUnit_ID} are {missing}")
	for m in missing:
		logger.info(f"Loading file {m} for sensor {row.SensorUnit_ID}")
		source_file = mapping.source.read_file(m, group=dict(node=row.SensorUnit_ID))
		dest_file = mapping.map_file(source_file)
		affected = mapping.dest.write_file(dest_file)
		logger.info(f"Done transfering")

