import influxdb
import pandas as pd
import pymysql
import sqlalchemy.engine as eng
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sqa
import sqlalchemy as db 
import itertools as ito

import sensorutils.decentlab as dl
import sensorutils.db as db_utils
import sensorutils.files as fu
import pathlib as pl


from datetime import datetime as dt
from datetime import timedelta 

import re


import pathlib as pl

import glob

from typing import Optional, List, Union


import argparse as ap


def get_all_picarro_files(station: str, rexp='.*\.(csv|CSV)') -> List[pl.Path]:
    rg = re.compile(rexp)
    return [f for f in fu.get_nabel_station_data_path(station).iterdir() if re.match(rg, f.name)]

def get_date_from_filename(filename: pl.Path) -> Optional[dt]:
    m = re.search('(\d){6}', filename.name)
    res = fu.parse_nabel_date(m.group()) if m else None
    return res

def make_available_files_table(station, rexp='*.csv') -> pd.DataFrame:
    return pd.DataFrame([(get_date_from_filename(f), f, station) for f in get_all_picarro_files(station, rexp=rexp)], columns=['date', 'path', 'station'])

def get_available_measurements_on_db(station: str, db_group='CarboSense_MySQL') -> pd.DataFrame:
    eng = db_utils.connect_to_metadata_db()
    metadata = db.MetaData(bind=eng)
    table = db.Table(station, metadata, autoload=True)
    Session = sessionmaker(eng)
    session = Session()
    dq = session.query(sqa.cast(sqa.func.from_unixtime(table.columns.timestamp), sqa.DATE).label('date')).distinct()
    return pd.read_sql_query(str(dq), eng, parse_dates =['date'])




def transform_to_db_format(data: pd.DataFrame) -> pd.DataFrame:
    out_tb = pd.DataFrame()
    #Localise timezone
    out_tb['timestamp'] = data['date']






#Map names in file to names in destination DB table
name_mapping = {
    'TEMP': 'T',
    'TEMP_F': 'T',
    'FEUCHT': 'RH',
    'FEUCHT_F': 'RH_F',
    'DRUCK': 'pressure',
    'DRUCK_F': 'pressure_F',
}





def load_and_map(source_dest_mapping: List[fu.SourceMapping]):
    #Iterate over the mapping
    for mapping in source_dest_mapping:
        #Get Missing files
        missing_files = mapping.list_files_missing_in_dest()
        #Iterate over files
    for group_name, group_files in missing_files.groupby('date'):
    #Read all data
    #     station_data = [read_nabel_csv(f) for f in group_files['path']]
    #     #Merge them
    #     all_data = pd.concat([d.set_index('date') for d in station_data], axis=1)

#Load the file

if __name__ == "__main__":


    parser = ap.ArgumentParser(description='Load Picarro Data into DB.')
    parser.add_argument('config', help='Location of configuration file')
    args = parser.parse_args()
    #Generate the mapping
    mapping = fu.DataMappingFactory.read_json(args.config)
    load_and_map(mapping)