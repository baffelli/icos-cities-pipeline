"""
Consolidates the reference measurements from several tables 
into a single table. The source tables and destination tables
are expressed as configuration 
"""
from unicodedata import name
from importlib_resources import path
import influxdb
import pandas as pd
import pymysql
import sqlalchemy.engine as eng
import sqlalchemy as db 
from sqlalchemy.orm.session import Session, sessionmaker
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

from sensorutils.log import logger 

parser = ap.ArgumentParser(description='Consolidate data from multiple reference table into one')
parser.add_argument('--import-all', default=False, action='store_true', help='Import all data or only incremental import?')
args = parser.parse_args()

logger.info('Connecting to the DB')
engine = db_utils.connect_to_metadata_db()
db_metadata = db.MetaData(bind=engine, reflect=True)

#Create a ORM session
Session = sessionmaker(engine)
session = Session()

cols = ['timestamp', 'CO2','CO2_F', 'CO2_DRY','CO2_DRY_CAL','CO2_DRY_F', 'T', 'pressure', 'RH', 'H2O', 'H2O_F']

tables = {
    'DUE1':'NABEL_DUE', 
    'HAE':'NABEL_HAE', 
    'RIG:':'NABEL_RIG', 
    'GIMM':'UNIBE_GIMM', 
    'BRM':'UNIBE_BRM', 
    'LAEG':'EMPA_LAEG', 
    'DUE2':'ClimateChamber_00_DUE'}

target_table = 'picarro_data'
dest_tb = db_metadata.tables[target_table]
name_column = 'LocationName'

for alias, table_name in tables.items():
    source_tb = db_metadata.tables[table_name]
    #Select from source table
    source_sel = session.query(*[source_tb.c[id] for id in cols]).cte().alias('src')
    #Find subset with same name

    #Create existence query
    #ext = session.query(source_tb).filter((source_tb.c['timestamp']==dest_tb.c['timestamp']) & (dest_tb.c[name_column] == alias))
    #anti_query = session.query(db.select([source_tb.c[id] for id in cols])).filter(~ext)

    #qr = anti_query.statement.compile(compile_kwargs={"literal_binds": True})


    eq = session.query(source_sel, db.sql.expression.literal(alias).label(name_column)).filter(
        ~session.query(dest_tb).filter((source_sel.c['timestamp']==dest_tb.c['timestamp']) & (dest_tb.c[name_column] == alias)).exists()
    )
    
    #Insert into
    insert_cmd = dest_tb.insert().from_select(cols + [name_column],eq)

    with engine.connect() as cur:
        with cur.begin() as tr:
            stmt = insert_cmd.compile(compile_kwargs={"literal_binds": True})
            stmt.execute(tr)
            #missing = pd.read_sql(eq.statement.compile(), cur)
            tr.rollback()