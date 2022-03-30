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
from typing import Dict
import sensorutils.db as db_utils
import sensorutils.files as fu
import sensorutils.data as du
import pathlib as pl
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sqa
import numpy as np

import datetime as dt
import argparse as ap
parser = ap.ArgumentParser(description='Import data from the Climate chamber experiment')
parser.add_argument('--picarro', type=str, nargs=2, help='Base path and regex for Picarro CRDS data')
parser.add_argument('--climate', type=str, nargs=2, help='Base path and regex for climate chamber data (temperature program)')
parser.add_argument('--pressure', type=str, nargs=2, help='Base path and regex for climate chamber data (pressure sensor)')
parser.add_argument('--dest', type=str, default='ClimateChamber_00_DUE', help='Destination table')

args = parser.parse_args()

#Get engine
eng = db_utils.connect_to_metadata_db()



def rename_and_subset(df: pd.DataFrame, subset:Dict[str, str]) -> pd.DataFrame:
    """
    Rename a pandas Dataframe with the `subset` dictionary only keeping the
    columns mentioned there.
    """
    return df.rename(columns=subset)[[v for v in subset.values()]]

#Load picarro data
if args.picarro:
    pf = pl.Path(args.picarro[0])
    pic_paths = [f for f in pf.glob(args.picarro[1])]
    #New names of picarro columns
    pic_cols = {
        'timestamp':'timestamp', 
        'CO2_mean':'CO2', 
        'H2O_mean':'H2O',
        'CO2_DRY_mean':'CO2_DRY'}
#Load climate chamber data
if args.climate:
    clim_cols = {
    'timestamp':'timestamp', 
    'temperature_mean':'T', 
    'target_temperature_mean':'T_Soll',
    'target_RH_mean':'RH_Soll',
    'RH_mean':'RH'}

    pf = pl.Path(args.climate[0])
    clim_paths = [f for f in pf.glob(args.climate[1])]
if args.pressure:
    pf = pl.Path(args.pressure[0])
    press_paths = [f for f in pf.glob(args.pressure[1])]
    pressure_cols = {
        'timestamp':'timestamp',
        'pressure_mean':'pressure'
    }



pd = {'mapping':pic_cols, 'paths':pic_paths, 'reader':fu.read_picarro_data} if args.picarro else None
cd = {'mapping':clim_cols, 'paths':clim_paths, 'reader':fu.read_climate_chamber_data} if args.climate else None
pressd = {'mapping':pressure_cols, 'paths':press_paths, 'reader': lambda x: fu.read_pressure_data(x).set_index('date').resample('1 min').pad().reset_index()} if args.pressure else None


cfgs = [el for el in [pd, cd, pressd] if el is not None]

for cfg in cfgs:
    for p in cfg['paths']:
        orig_data = cfg['reader'](p)
        import pdb; pdb.set_trace()
        data = rename_and_subset(du.date_to_timestamp(du.average_df(orig_data).reset_index(), 'date'), cfg['mapping']).dropna(subset=cfg['mapping'].values())
        with eng.connect() as con:
            with con.begin() as tr:
                mt = db_utils.create_upsert_metod(sqa.MetaData(bind=con))
                data.to_sql(args.dest, con, index=False, if_exists='append', method=mt)