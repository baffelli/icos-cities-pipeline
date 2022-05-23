"""
Interpolate pressure from a reference location
to the sensor location
"""
import argparse as ap
import sensorutils.files as fu
import sensorutils.db as du
import sensorutils.log as log
import pathlib as pl
import re
import pdb
import sensorutils.calc as calc
import sensorutils.models as mods
import sensorutils.files as fu
import sensorutils.data as icos_dat
import sensorutils.calc as calc
from sensorutils.log import logger
from sqlalchemy.orm import sessionmaker, Session
import sqlalchemy as sqa
import pandas as pd
from typing import List
import numpy as np

parser = ap.ArgumentParser(description='Interpolate pressure to station height')

#parser.add_argument('config', type=pl.Path, help='Datasource mapping configuration file')
parser.add_argument('id', type=str, help='Location ID to process')
parser.add_argument('reference', type=str, help='Reference station')
parser.add_argument('--import_all', default=False, action='store_true', help='If set, perform bulk import, otherwise incremental import')

#Parse arguments
args = parser.parse_args()

#Connect to DB
eng = du.connect_to_metadata_db()
md = sqa.MetaData(bind=eng)
md.reflect()
session = sessionmaker(eng)


def interp_pres(dt_in: pd.DataFrame, source_loc: mods.Location, loc: mods.Location) -> List[mods.PressureInterpolation]:
    pressure_interp = pd.DataFrame({
    'timestamp': dt_in['timestamp'], 
    'pressure': calc.pressure_interpolation(dt_in['pressure'] * 100, calc.absolute_temperature(dt_in['T']), source_loc.h, loc.h) / 1e2
    }).replace(np.nan, None)
    return [mods.PressureInterpolation(time = tp.timestamp, id = loc.id, pressure = tp.pressure) for tp in pressure_interp.itertuples()]


with session() as ses:
    sel = sqa.select(mods.Location).filter(mods.Location.id == args.id)
    res = ses.execute(sel)
    #Iterate all locations (there could be more than one)
    for loc, *rest in res.all():
        first_date, *_ = du.get_first_deployment_at_location(ses, loc.id)
        source_loc_query = sqa.select(mods.Location).filter(
            (mods.Location.id == args.reference) &
            (mods.Location.start < first_date.start)
        )
        source_loc, *_ = ses.execute(source_loc_query).first()
        #Create a dataset mapping
        source = fu.DBSource(
            date_from = icos_dat.CS_START,
            table= mods.PicarroData.__tablename__, 
            date_column='CAST(FROM_UNIXTIME(timestamp) AS DATE)', 
            column_filter='pressure IS NOT NULL',
            grouping_key='LocationName',
            group=args.reference,
            metadata=md,
            na=-999,
            eng = eng)
        dest = fu.DBSource(
            date_from = loc.start,
            table=mods.PressureInterpolation.__tablename__, 
            date_column='CAST(FROM_UNIXTIME(timestamp) AS DATE)', 
            column_filter='pressure IS NOT NULL',
            grouping_key='location',
            group=args.id,
            metadata=md,
            na = -999,
            eng = eng)
        map = fu.SourceMapping(source, dest)
        logger.info(f"Listing all pressure entries in {loc.id}")
        missing = map.list_files_missing_in_dest()
        for m in missing:
            sf = source.read_file(m)
            sf_out = sf.copy()
            #Average data
            sf_out['date'] = pd.to_datetime(sf_out['timestamp'], unit='s')
            sf_out.set_index('date').resample('10min').agg(np.mean)
            #Compute interpolation
            logger.info(f"Interpolating pressure for  {loc.id} on {m}")
            pi = interp_pres(sf_out, source_loc, loc)
            [ses.add(p) for p in pi]
            ses.commit()

