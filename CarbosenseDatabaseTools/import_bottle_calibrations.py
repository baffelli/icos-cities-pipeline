"""
This script imports the calibration values from the bottle measurements
performed by
"""
from curses import beep
import sensorutils.data as du
import sensorutils.db as db_utils
import sensorutils.files as fu
import sensorutils.models as mo

from sensorutils.log import logger
import pathlib as pl
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sqa

import argparse as ap


import itertools

parser = ap.ArgumentParser()
parser.add_argument('base_path', type=pl.Path, help='Input file (csv or xls) with bottle values')
parser.add_argument('glob', type=str, help='Glob pattern')
parser.add_argument('--dest', type=str, default="ref_gas_cylinder_analysis", help="Destination DB table")
args = parser.parse_args()

eng = db_utils.connect_to_metadata_db()

logger.info(f"Reading bottle calibrations from {args.base_path}")
pts = args.base_path.glob(args.glob)

cals = list(itertools.chain(*[du.read_bottle_calibrations(pt) for pt in pts]))
session = sessionmaker(bind=eng)

for ce in cals:
    with session() as ses:
        c_old = sqa.select(mo.CylinderAnalysis).filter(
            (mo.CylinderAnalysis.cylinder_id == ce.cylinder_id),
            (mo.CylinderAnalysis.analysed == ce.analysed),
            (mo.CylinderAnalysis.fill_from == ce.fill_from),
            (mo.CylinderAnalysis.fill_to == ce.fill_to) 
            )
        logger.info(f"Adding calibration {ce} to the session")
        c1 = ses.merge(ce)
        if c1 == ce:
            ses.add(c1)
        else:
            session.add(c1)
       
        logger.info(f"Commiting ORM session ")
        ses.commit()
