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
import sensorutils.calc as calc
import sensorutils.models as mods
import sensorutils.files as fu
import sensorutils.data as icos_dat
import sensorutils.calc as calc
from sensorutils.log import logger
from sqlalchemy.orm import sessionmaker, Session, session
import sqlalchemy as sqa
import pandas as pd
from typing import List, Tuple
import numpy as np
import datetime as dt
import sqlalchemy.sql.functions as func

parser = ap.ArgumentParser(description='Interpolate pressure to station and sensor height')

#parser.add_argument('config', type=pl.Path, help='Datasource mapping configuration file')
parser.add_argument('id', type=int, help='Sensor ID to process')
parser.add_argument('reference', type=str, help='Reference station')
parser.add_argument('start', type=dt.datetime.fromisoformat, help='Starting date')
parser.add_argument('--import_all', default=False, action='store_true', help='If set, perform bulk import, otherwise incremental import')

#Parse arguments
args = parser.parse_args()

#Connect to DB
eng = du.connect_to_metadata_db()
md = sqa.MetaData(bind=eng)
md.reflect()
session = sessionmaker(eng)


def get_deployment_info(session: session, id: int, start: dt.datetime) -> List[Tuple[mods.Deployment, mods.Location]]:
    """
    For a given sensor id, get the location
    and deployment information
    """
    sel = sqa.select(mods.Deployment, mods.Location).join(
        mods.Location,
        (mods.Location.id == mods.Deployment.location) &
        (mods.Location.start < mods.Deployment.start)
    ).filter(
        (mods.Deployment.id == id) &
        (mods.Deployment.end >= start)
    )
    return [(dep, loc) for dep, loc in ses.execute(sel)]

def get_loc_info(session: session, location: str, start: dt.datetime) -> mods.Location:
    """
    For a given location and start date, return the location information
    """
    qr = sqa.select(mods.Location).filter(
        (mods.Location.id == location) &
        (mods.Location.end >= start)
    )
    return session.execute(qr).first()

def interp_pres(dt_in: pd.DataFrame, id:int, source_loc: mods.Location, loc: mods.Location, dep: mods.Deployment) -> List[mods.PressureInterpolation]:
    pressure_interp = pd.DataFrame({
    'timestamp': dt_in['timestamp'], 
    'pressure': calc.pressure_interpolation(dt_in['pressure'] * 100, calc.absolute_temperature(dt_in['T']), source_loc.h, loc.h + (dep.height or 0)) / 1e2
    }).replace(np.nan, None).dropna(subset='pressure')
    return [mods.PressureInterpolation(time = tp.timestamp, location = loc.id, sensor_id=id, pressure = tp.pressure) for tp in pressure_interp.itertuples()]


with session() as ses:
    res = get_deployment_info(ses, args.id, args.start)
    source_loc, *rest = get_loc_info(ses, args.reference, args.start)
    #Iterate all locations (there could be more than one)
    for dep, dest_loc in res:
        #Create a dataset mapping
        first_date = max([args.start, dep.start])
        print(dep)
        print(dest_loc)
        source = fu.DBSource(
            date_from = first_date,
            table= mods.PicarroData.__tablename__, 
            date_column='CAST(FROM_UNIXTIME(timestamp) AS DATE)', 
            column_filter='pressure IS NOT NULL',
            grouping_key='LocationName',
            group=args.reference,
            metadata=md,
            na=-999,
            eng = eng)
        #The key is composite
        dest = fu.DBSource(
            date_from = first_date,
            table=mods.PressureInterpolation.__tablename__, 
            date_column='CAST(FROM_UNIXTIME(timestamp) AS DATE)', 
            column_filter='pressure IS NOT NULL',
            grouping_key='CONCAT(sensor_id, location)',
            group=f"{args.id}{dest_loc.id}",
            metadata=md,
            na = -999,
            eng = eng)
        map = fu.SourceMapping(source, dest)
        logger.info(f"Listing all pressure entries in {dest_loc.id}")
        #Filter missing
        missing = [m for m in map.list_files_missing_in_dest(all=args.import_all) if m<= dep.end]
        for m in missing:
            logger.info(f"Reading reference pressure for {args.id} from {args.reference} on {m}")
            sf = source.read_file(m)
            sf_out = sf.copy()
            #Average data
            sf_out['date'] = pd.to_datetime(sf_out['timestamp'], unit='s')
            sf_out.set_index('date').resample('10min').agg(np.mean)
            #Compute interpolation
            logger.info(f"Interpolating pressure for {args.id} in {dest_loc.id} on {m}")
            pi = interp_pres(sf_out, args.id, source_loc, dest_loc, dep)
            logger.info(f"Storing data")
            [ses.merge(p) for p in pi]
            ses.commit()

