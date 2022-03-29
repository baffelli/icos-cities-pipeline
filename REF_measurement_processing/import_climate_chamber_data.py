"""
Imports all picarro and climate chamber data
from a given path into the database table `ClimateChamber_00_DUE`
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
parser = ap.ArgumentParser()
parser.add_argument('--picarro', type=str, nargs=2)
parser.add_argument('--climate', type=str, nargs=2)


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

    for p in pic_paths:
        dt = fu.read_picarro_data(p)
        dt_agg = rename_and_subset(du.date_to_timestamp(du.average_df(dt).reset_index(), 'date').reset_index(), pic_cols)\
            .reset_index().drop('index',axis=1).dropna(subset=pic_cols.values())
        import pdb; pdb.set_trace()
        with eng.connect() as con:
            with con.begin() as tr:
                mt = db_utils.create_upsert_metod(sqa.MetaData(bind=con))
                dt_agg.to_sql('ClimateChamber_00_DUE', con, index=False, if_exists='append', method=mt)
                #tr.rollback()
        print(dt_agg)
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
    for p in clim_paths:
        climate_data = rename_and_subset(du.date_to_timestamp(du.average_df(fu.read_climate_chamber_data(p)).reset_index(), 'date'), clim_cols).dropna(subset=clim_cols.values())
        import pdb; pdb.set_trace()
        with eng.connect() as con:
            with con.begin() as tr:
                mt = db_utils.create_upsert_metod(sqa.MetaData(bind=con))
                climate_data.to_sql('ClimateChamber_00_DUE', con, index=False, if_exists='append', method=mt)