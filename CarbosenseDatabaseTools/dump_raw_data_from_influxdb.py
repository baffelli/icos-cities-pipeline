"""
This script dumps the raw data from the decentlab DB into a database table
"""
from importlib_resources import path
import influxdb
import pandas as pd
import pymysql
import sqlalchemy.engine as eng
import sqlalchemy as db 
import itertools as ito

import sensorutils.decentlab as dl
import sensorutils.db as db_utils
import sensorutils.secrets as sec
import sensorutils.files as fu
import sensorutils.data as du

from datetime import datetime as dt
from datetime import timedelta 
import argparse as ap

import pathlib as pl


#Renam

parser = ap.ArgumentParser(description='Import raw data from decentlab into database')
parser.add_argument('config', type=pl.Path, help='Path to the datasource mapping configuration file')
parser.add_argument('--import-all', default=False, action='store_true', help='Import all data or only incremental import?')

args = parser.parse_args()

#Default date
default_date = dt.strptime('2017-01-01 00:00:00', '%Y-%m-%d %H:%M:%S') 
#Mapping table <> sensor type
table_mapping = {'HPP':"hpp_data", "LP8":'lp8_data'}

mapping = fu.DataMappingFactory.read_config(args.config)
import pdb; pdb.set_trace()
#Get API key from secrets
passw = sec.get_key('decentlab')
#Create influxdb client
client = dl.decentlab_client(token=passw)
#Connect to  target database
engine = db_utils.connect_to_metadata_db()
db_metadata = db.MetaData(bind=engine, reflect=True)

#Load the mapping file

#List all ids
sensor_ids = db_utils.list_all_sensor_ids(sensor_type, engine)
#Iterate over all sensors
for row in sensor_ids.itertuples():
	mapping['']
	# src = fu.InfluxdbSource(path='a', date_from=default_date, client=client, group=row.SensorUnit_ID, grouping_key='node', na=None)
	# dest = fu.DBSource(path=table_mapping[sensor_type], date_from=default_date, db_prefix='CarboSense_MySQL', date_column='time', na=-999, group=row.SensorUnit_ID, grouping_key='SensorUnit_ID')
	# ad = dest.list_files()
	# mapping = fu.SourceMapping(source=src, dest=dest, columns=None)
	# missing = mapping.list_files_missing_in_dest()
	# if len(missing) > 0:
	# 	for missing_date in missing:
	# 		data = src.read_file(missing_date)
	# 		import pdb; pdb.set_trace()
	# 		data.rename(lambda x: x.replace('-',"_"))
	# 		import pdb; pdb.set_trace()
	print('a')





def check_sensor_type(sensor_type):
	st =  ['LP8', 'HPP']
	if sensor_type not in st:
		raise ValueError(f"'sensor_type' must be in {st}")

def get_sensors(con, sensor_type='LP8'):
	check_sensor_type(sensor_type)
	table_mapping = {'LP8':{'table':'lp8_data', 'first_id':1010, 'sensor_type':sensor_type}, 'HPP':{'table':'hpp_data', 'first_id':426, 'sensor_type':sensor_type}}
	sens_query = f"""
	WITH max_dt AS
	(
		SELECT
			SensorUnit_ID,
			MAX(time) AS lt
		FROM {table_mapping[sensor_type]['table']}
		GROUP BY SensorUnit_ID
	)
	SELECT
		SensorUnit_ID,
		first_timestamp,
		COALESCE(last_timestamp, first_timestamp) AS  last_timestamp
	FROM
	(
		SELECT DISTINCT 
			su.SensorUnit_ID, UNIX_TIMESTAMP(su.Date_UTC_from) AS first_timestamp, max_dt.lt AS last_timestamp
		FROM SensorUnits AS su
		JOIN Sensors AS sens ON sens.SensorUnit_ID = su.SensorUnit_ID
		LEFT JOIN max_dt ON max_dt.SensorUnit_ID = su.SensorUnit_ID
		WHERE sens.Type = :sens_type AND EXISTS (
			SELECT DISTINCT SensorUnit_ID
			FROM Deployment AS dep
			WHERE dep.SensorUnit_ID = sens.SensorUnit_ID
		) AND sens.SensorUnit_ID >= :min
	) AS a
	ORDER BY SensorUnit_ID
	"""
	qp = db.text(sens_query)
	sens = con.execute(qp, {'sens_type':sensor_type, 'min':table_mapping[sensor_type]['first_id']})
	return sens, table_mapping[sensor_type]['table']


def get_lp8_data(client, current_sens, start_ts, limit=200):
	averaging_time = "10m" 
	start_ts_s = dl.format_influxdb_timestamp(start_ts)
	data_query = f"""SELECT MEAN("value") AS value FROM "measurements" WHERE node=~ /{current_sens}/ AND sensor =~ /senseair*|sensirion*|battery|calibration/ AND "time" > $start_ts GROUP BY "node","sensor", time({averaging_time}) LIMIT {limit}"""
	return client.query(data_query, epoch='s', bind_params={"current_sens":current_sens, "start_ts":start_ts_s})

def get_hpp_data(client, current_sens, start_ts, limit=200):
	start_ts_s = dl.format_influxdb_timestamp(start_ts)
	data_query = f"""SELECT value FROM "measurements" WHERE node =~ /{current_sens}/ AND sensor =~ /senseair|sensirion|battery|calibration/ AND "time" >= $start_ts GROUP BY sensor,node LIMIT {limit}"""
	return client.query(data_query, epoch='s', bind_params={"current_sens":current_sens, "start_ts":start_ts_s})

def get_raw_series(client, current_sens, start_ts, limit=200, sensor_type='HPP'):
	max_ts = start_ts
	query_function = get_lp8_data if sensor_type == 'LP8' else get_hpp_data
	for off_mult in ito.count():
		res = query_function(client, current_sens, max_ts, limit=limit)
		if len(res)==0:
			raise StopIteration()
		else:
			max_ts =  pd.DataFrame(res.get_points())['time'].max()
			yield res

def parse_result_set(result_set):
	import pdb; pdb
	df = pd.concat([pd.DataFrame(ms).assign(**tg).set_index('time') for (nm, tg),ms in result_set.items()], sort=True, axis=0, join='inner').pivot_table(index=['node', 'time'],values='value', columns='sensor')
	return df.rename(columns=lambda x: x.replace("-","_"))

def get_last_influxdb_datapoint(client, nodes):
	query = f"""SELECT * FROM (SELECT "sensor", LAST("value") FROM "measurements" WHERE "sensor" =~ /battery/ AND "node" =~ /{nodes}/ GROUP BY "node" )"""
	return [pt for pt in client.query(query, epoch='s').get_points()]

#Connect to mariadb
with engine.connect() as con:
	sens, out_table = get_sensors(con, sensor_type=sensor_type)
	for current_sens, start_ts, end_ts, in sens:
		#Get last time in influxdb
		last_entry, *others = get_last_influxdb_datapoint(client, current_sens)
		start_ts_part = start_ts if import_all else int(end_ts)
		res = get_raw_series(client, current_sens, start_ts_part, limit=5000, sensor_type=sensor_type)
		print(f"Getting data for sensor {current_sens}")
		for rs in res: 
			df = parse_result_set(rs).reset_index().rename(columns={"node":'SensorUnit_ID'})
			print(f"Writing data for sensor {current_sens} between {df['time'].min()} and {df['time'].max()}")
			with con.begin() as cb:
				meta = db.MetaData(con)
				method = db_utils.create_upsert_metod(meta)
				df.to_sql(out_table, con, schema="CarboSense", index=False, if_exists='append', method=method)

