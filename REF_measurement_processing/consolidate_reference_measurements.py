"""
Consolidates the reference measurements from several tables 
into a single table. The source tables and destination tables
are expressed as configuration, as is the column mapping
For an example of the mapping file, look at '/config/reference_table_mapping.yaml' in
this repository.
"""
from unicodedata import name
import influxdb
from matplotlib.pyplot import table
import pandas as pd
import pymysql
import sqlalchemy.engine as eng
import sqlalchemy as db
from sqlalchemy.orm.session import Session, sessionmaker
import sqlalchemy.sql as sql
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

from typing import Dict, List

from sensorutils.log import logger
from sqlalchemy.dialects.mysql import insert

parser = ap.ArgumentParser(
    description='Consolidate data from multiple reference table into one')
parser.add_argument('config', type=pl.Path,
                    help="Path to configuration file for table mapping")
parser.add_argument('--temporary', default=False,
                    action='store_true', help='Execute temporary transaction?')

parser.add_argument('--import-all', default=False, action='store_true',
                    help='Import all data or only incremental import?')
parser.add_argument('--name', nargs='*', type=str)
args = parser.parse_args()

logger.info('Connecting to the DB')
engine = db_utils.connect_to_metadata_db()
db_metadata = db.MetaData(bind=engine)
db_metadata.reflect()

# Create a ORM session
Session = sessionmaker(engine)
session = Session()


# Load Colum mapping
table_mapping = fu.DataMappingFactory.read_config(args.config)

if args.name:
    table_mapping = {k:v for k,v in table_mapping.items() if k in args.name}
else:
    pass

# Iterate over mapping
for alias, mapping in table_mapping.items():
    source_table_name = mapping.source.path
    dest_table_name = mapping.dest.path
    dest_loc_column = mapping.dest.grouping_key
    source_time_column = mapping.source.date_column
    dest_time_column = mapping.dest.date_column

    logger.info(
        f"Processing input table '{source_table_name}', copying to '{dest_table_name}' with location id '{alias}'")
    source_tb = db_metadata.tables[source_table_name]
    dest_tb = db_metadata.tables[dest_table_name]
    # Select from source table
    cm_sel = [c.make_query() for c in mapping.columns]
    cm_names = [c.name for c in mapping.columns] + [dest_loc_column]
    source_sel = db.select(cm_sel).select_from(source_tb).cte().alias('src')

    # Find subset with same name

    # Create existence query
    source_query = session.query(source_sel, db.sql.expression.literal(alias).label(dest_loc_column))
    if not args.import_all:
        insert_quert = source_query.filter(
            ~session.query(dest_tb).filter(
                (source_sel.c[source_time_column] == dest_tb.c[dest_time_column]) &
                (dest_tb.c[dest_loc_column] == alias)).exists()
        )
    else:
        insert_quert = source_query

    insert_cmd = dest_tb.insert().from_select(cm_names, insert_quert)
 

    with engine.connect() as cur:
        with cur.begin() as tr:
            stmt = insert_cmd.compile(compile_kwargs={"literal_binds": True})
            logger.debug(f"The insert statement is {str(stmt)}")
            res = cur.execute(stmt)
            logger.debug(f"Done inserting for table {source_table_name}")
            if args.temporary:
                logger.debug(
                    "Rolling back as `temporary` command-line argument was set")
                tr.rollback()
