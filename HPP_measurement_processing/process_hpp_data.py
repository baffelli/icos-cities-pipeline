"""
This script processes the LP8/ Picarro data to produce calibration data
respectively calibrated data. The type of sensors respectively the processing mode
are choosen with a command line arguments, while the calibration model and the tables
are specified using a configuration file
"""
import imp
import sensorutils.db as db_utils
import sensorutils.files as fu
import sensorutils.data as du
from sensorutils.log import logger
import pathlib as pl
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
import sqlalchemy as sqa
from sqlalchemy import func
import datetime as dt
import argparse as ap

# Utility functions


def get_reference_data(id: int,
                       eng: sqa.engine.Engine,
                       sensor_table: str,
                       join_time:int,
                       cal_table: str = "Calibration",
                       id_col:str = "SensorUnit_ID",
                       ref_table:str = "picarro_data",
                       loc_column:str = "LocationName"):
    """
    Gets the reference data for a given sensor
    and calibration period
    Parameters
    ----------
    id: int
        The sensor id to process
    eng: sqalchemy.engine.Engine
        A sqlalchemy engine for the ORM session
    sensor_table: str
        The table with sensor data
    join_time: int
        The time in second on which to join the timestamps
    cal_table: str
        The name of the table containing the calibration information
    ref_table: str
        The name of the table containing the reference instrument data
    """
    db_metadata = sqa.MetaData(bind=engine, reflect=True)
    sm = sessionmaker(bind=eng)
    session: Session = sm()
    cal_tab = db_metadata.tables[cal_table]
    #Query calibration table
    ct = session.query(cal_tab).filter(cal_tab.c[id_col] == id).cte().alias("cal")
    #Query reference table
    ref_tab = db_metadata.tables[ref_table]
    #Sensor table
    sens_tab = db_metadata.tables[sensor_table]
    jt = session.query(ct, ref_tab, sens_tab).join(ref_tab,
        (ct.c[loc_column] == ref_tab.c[loc_column]) &
        ref_tab.c['timestamp'].between(func.unix_timestamp(ct.c['Date_UTC_from']),func.unix_timestamp(ct.c['Date_UTC_to']))
        ).join(sens_tab,
        func.floor(sens_tab.c['time'] / join_time) * join_time == ref_tab.c['timestamp']
        )
    jt_comp = jt.statement.compile()
    print(jt_comp)
    with eng.connect() as con:
        pd.read_sql(jt_comp, con)
    import pdb; pdb.set_trace()
    #


# Setup parser
parser = ap.ArgumentParser(description="Calibrate or process HPP/LP8 data")
#parser.add_argument('config', type=pl.Path, help="Path to configuration file")
parser.add_argument('mode', type=str, choices=[
                    'calibrate', 'process'], help="Processing mode")
parser.add_argument('sensor_type', type=str, choices=[
                    'HPP', 'LP8'], help="Sensor type")
parser.add_argument('id', type=int, nargs='?',
                    help="If passed, only process one sensor")
parser.add_argument('--full', default=True, action='store_false',
                    help="Partial processing or full data")
args = parser.parse_args()

# Connect to metadata/measurement database
logger.info('Connecting to the DB')
engine = db_utils.connect_to_metadata_db()
db_metadata = sqa.MetaData(bind=engine, reflect=True)


# List all ids
ids = db_utils.list_all_sensor_ids(args.sensor_type, engine)
# Find the ids to process
ids_to_process = list(
    (ids if not args.id else ids[ids['SensorUnit_ID'] == args.id])['SensorUnit_ID'])
logger.debug(f"Processing mode: '{args.mode}'")
logger.debug(
    f"Processing sensor type '{args.sensor_type}' with ids:\n {ids_to_process}")

tm = {'HPP': 'hpp_data', 'LP8':'lp8_data'}
for id in ids_to_process:
    logger.debug(f"Processing sensor id {id}")
    logger.debug(f"Getting reference data")
    ref_dt = get_reference_data(id, engine, tm[args.sensor_type], 600)