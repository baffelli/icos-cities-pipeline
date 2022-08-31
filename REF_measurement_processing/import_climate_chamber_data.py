"""
Imports all picarro and climate chamber data
from a given path into the database table `ClimateChamber_00_DUE`. If you want to import
to another table, use the `--dest` command line argument.
The script takes three arguments, one for each data type. 
As an example, to import both picarro and climate chamber data for experiments in 2021, use:
```
import_climate_chamber_data.py --picarro /mnt/basi/CarboSense/Data/Klimakammer_Versuche_27022017_XXXXXXXX/ **/**/*2021*.dat 
--climate  /mnt/basi/CarboSense/Data/Klimakammer_Versuche_27022017_XXXXXXXX/ Klimakammerdaten_2021*.csv
```
"""
from asyncore import read
from multiprocessing.sharedctypes import Value
from typing import Dict
from xml.dom import ValidationErr
import sensorutils.db as db_utils
import sensorutils.files as fu
import sensorutils.data as du
import sensorutils.log as log
import pathlib as pl
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sqa
import numpy as np

import datetime as dt
import argparse as ap
parser = ap.ArgumentParser(description='Import data from the Climate chamber experiment')
parser.add_argument('type', type=str, help='Device type')
parser.add_argument('path', type=pl.Path, help='Base path to locate data')
parser.add_argument('re', type=str, help='Regex to find the files to import')
parser.add_argument('dest', type=str, help='Destination table')
parser.add_argument('dest_loc', type=str, help='Destination location name for the table')
args = parser.parse_args()

#Get engine
eng = db_utils.connect_to_metadata_db()



def rename_and_subset(df: pd.DataFrame, subset:Dict[str, str]) -> pd.DataFrame:
    """
    Rename a pandas Dataframe with the `subset` dictionary only keeping the
    columns mentioned there.
    """
    return df.rename(columns=subset)[list(subset.values())]

#Select type of file to read
match args.type:
    case 'picarro':
        cols = {
        'timestamp':'timestamp', 
        'CO2_mean':'CO2', 
        'CO2_F_max': 'CO2_F',
        'CO2_DRY_F_max':'CO2_DRY_F',
        'H2O_mean':'H2O',
        'H2O_F_max': 'H2O_F',
        'CO2_DRY_mean':'CO2_DRY'}
        reader = fu.read_picarro_data
        grouping = []
    case 'climate':
        cols = {
        'timestamp':'timestamp', 
        'temperature_mean':'T', 
        'target_temperature_mean':'T_Soll',
        'target_RH_mean':'RH_Soll',
        'RH_mean':'RH'}
        reader = lambda x: fu.read_pressure_data(x).set_index('date').resample('1 min').pad().reset_index()
        grouping = []
    case 'pressure':
        cols = {
        'timestamp':'timestamp',
        'pressure_mean':'pressure'}
        reader = fu.read_pressure_data
        grouping = []
    case 'climate_new':
        cols = {
        'timestamp': 'timestamp',
        'T_mean':'T',
        'RH_mean':'RH',
        'T_F_max': 'T_F',
        'RH_F_max': 'RH_F',
        'CO2_mean': 'CO2',
        'CO2_DRY_mean': 'CO2_DRY',
        'H2O_mean': 'H2O',
        'chamber_status': 'chamber_status',
        'calibration_mode': 'calibration_mode'
        }
        reader = fu.read_new_climate_chamber_data
        grouping = ['chamber_status', 'calibration_mode']
    case _:
        raise ValueError(f"The sensor type {args.type} is not supported")

pf = pl.Path(args.path)
cn_paths = [f for f in pf.glob(args.re)]

for p in cn_paths:
    orig_data = reader(p)
    data = rename_and_subset(du.date_to_timestamp(du.average_df(orig_data, groups=grouping).reset_index(), 'date'), cols).dropna(subset=cols.values())
    #Set the destination location to write in the table
    data["LocationName"] = args.dest_loc
    log.logger.info(f"Importing {p}")
    with eng.connect() as con:
        md = sqa.MetaData(bind=con)
        md.reflect()
        with con.begin() as tr:
            mt = db_utils.create_upsert_metod(md)

            data.to_sql(args.dest, con, index=False, if_exists='append', method=mt)