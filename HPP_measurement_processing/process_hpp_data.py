"""
This script processes the LP8/ Picarro data to produce calibration data
respectively calibrated data. The type of sensors respectively the processing mode
are choosen with a command line arguments, while the calibration model and the tables
are specified using a configuration file
"""
import imp
import sqlite3
import sensorutils.db as db_utils
import sensorutils.files as fu
import sensorutils.data as du
import sensorutils.calc as calc
from sensorutils.log import logger
import pathlib as pl
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
import sqlalchemy as sqa
from sqlalchemy import func
import datetime as dt
import argparse as ap
from typing import Dict, List, NamedTuple
import numpy as np

import matplotlib.pyplot as plt


from sklearn.model_selection import train_test_split, TimeSeriesSplit


# Utility functions

def make_aggregation_column(table: sqa.Table, column: str, avg_time: int) -> sqa.sql.elements.Label:
    return (func.floor(table.columns[column] / avg_time) * avg_time).label(column)


def make_aggregate_columns(table: sqa.table, aggregations: Dict[str, str]) -> List[sqa.sql.expression.ColumnClause]:
    return [sqa.sql.literal_column(query).label(name) for name, query in aggregations.items()]


def get_sensor_data(
        eng: sqa.engine.Engine,
        md: sqa.MetaData,
        id: int,
        sensor_table: str,
        ts_from: int,
        ts_to: int,
        aggregations: Dict[str, str],
        avg_time: int,
        time_col: str = 'time',
        id_col: str = "SensorUnit_ID") -> pd.DataFrame:
    # Get the table
    st: sqa.Table = md.tables[sensor_table]
    # Make an aggegation column
    ac = make_aggregation_column(st, time_col, avg_time)
    id_c = st.c[id_col]
    # Make a list of selections
    cols = [ac, id_c] + make_aggregate_columns(st, aggregations)
    # Ask for the data
    qr = sqa.select(cols).where(
        (st.columns[id_col] == id) &
        (st.columns[time_col].between(ts_from, ts_to))
    ).group_by(*[ac, id_c])
    with eng.connect() as c:
        res = pd.read_sql(qr.compile(), c)
    return res


def get_ref_data(
        eng: sqa.engine.Engine,
        md: sqa.MetaData,
        location_id: str,
        ref_table: str,
        ts_from: int,
        ts_to: int,
        aggregations: Dict[str, str],
        avg_time: int,
        location_col: str = "LocationName",
        time_col: str = 'timestamp'):
    # Get the table
    ref_t: sqa.Table = md.tables[ref_table]
    # Make the aggregation column
    ac = make_aggregation_column(ref_t, time_col, avg_time)
    id_c = ref_t.c[location_col]
    # Make a list of selections
    id_cols = [ac, id_c]
    cols = id_cols + make_aggregate_columns(ref_t, aggregations)
    # Query
    qr = sqa.select(cols).where(
        (ref_t.columns[time_col].between(ts_from, ts_to)) &
        (ref_t.columns[location_col] == location_id)).group_by(*id_cols)
    logger.debug(f"The query is:\n {qr.compile()}")
    with eng.connect() as c:
        res = pd.read_sql(qr.compile(), c)
    return res


def get_calibration_periods(
    eng: sqa.engine.Engine,
    md: sqa.MetaData,
    id: int,
    sensor_table: str,
    cal_table: str = "Calibration",
    id_col: str = "SensorUnit_ID",
    start_col: str = "Date_UTC_from",
    end_col: str = "Date_UTC_to",
) -> pd.DataFrame:
    """
    Gets the calibration data for a given sensor
    :obj:`pandas.DataFrame`
    Parameters
    ----------
    id: int
        The sensor id to process
    md: sqlalchemy.MetaData
        the db metadata
    eng: sqalchemy.engine.Engine
        A sqlalchemy engine for the ORM session
    sensor_table: str
        The table with sensor information (serialnumbers etc)
    cal_table: str
        The name of the table containing the calibration information
    """
    cal_tab = md.tables[cal_table]
    # Query calibration table
    ct = sqa.select(cal_tab.c +
                    [func.unix_timestamp(cal_tab.c[start_col]).label('timestamp_from'),
                        func.unix_timestamp(func.least(cal_tab.c[end_col], func.curdate())).label('timestamp_to')]
                    ).where(
        cal_tab.c[id_col] == id)
    # Execute query
    ct_com = ct.compile()
    logger.debug(f"The query is:\n {ct_com}")
    with eng.connect() as con:
        res = pd.read_sql(ct_com, con)
    return res


def get_cal_data(eng: sqa.engine.Engine,
                 md: sqa.MetaData, 
                 id: int, 
                 sensor_type: 
                 str, 
                 agg_time: 
                 int, 
                 ref_agg: Dict[str, str], 
                 sens_agg: Dict[str, str]) -> pd.DataFrame:
    ref_dt = get_calibration_periods(eng, md, id,  sensor_type)

    def get_and_combine(tw:NamedTuple) -> pd.DataFrame:
        sensor_data = get_sensor_data(
            eng, md, id, sensor_type, tw.timestamp_from, tw.timestamp_to, sens_agg, agg_time)
        # Import reference data
        picarro_data = get_ref_data(eng, md, tw.LocationName,
                                    "picarro_data", tw.timestamp_from, tw.timestamp_to, ref_agg, agg_time)
        # Join data
        cal_data = sensor_data.set_index('time').join(
            picarro_data.set_index('timestamp'))
        return cal_data
    # Iterate over calibration data
    return pd.concat([get_and_combine(tw) for tw in ref_dt.itertuples()])


# Setup parser
parser = ap.ArgumentParser(description="Calibrate or process HPP/LP8 data")
#parser.add_argument('config', type=pl.Path, help="Path to configuration file")
parser.add_argument('mode', type=str, choices=[
                    'calibrate', 'process'], help="Processing mode")
parser.add_argument('sensor_type', type=str, choices=[
                    'HPP', 'LP8'], help="Sensor type")
parser.add_argument('--plot', type=pl.Path, help='Path to save plots')
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
if len(ids_to_process) == 0:
    logger.warn(f'The sensor with id {args.id} does not exist')
logger.debug(f"Processing mode: '{args.mode}'")
logger.debug(
    f"Processing sensor type '{args.sensor_type}' with ids:\n {ids_to_process}")

tm = {'HPP': 'hpp_data', 'LP8': 'lp8_data'}

# Expression for HPP averaging
hpp_agg = {
    'hpp_co2': 'AVG(NULLIF(senseair_hpp_co2_filtered, -999))',
    'hpp_pressure': 'AVG(NULLIF(senseair_hpp_pressure_filtered, -999))',
    'hpp_RH': 'AVG(NULLIF(sensirion_sht21_humidity, -999))',
    'hpp_t': 'AVG(NULLIF(sensirion_sht21_temperature, -999))',
    'hpp_ir': 'AVG(NULLIF(senseair_hpp_ir_signal, -999))',
    'hpp_detector_t': 'AVG(NULLIF(senseair_hpp_temperature_detector, -999))',
    'hpp_calibration_a': 'MAX(calibration_a)',
    'hpp_calibration_b': 'MAX(calibration_b)',
}
picarro_agg = {
    'CO2_DRY': 'AVG(CO2_DRY)',
    'T': 'AVG(T)',
    'RH': 'AVG(RH)',
    'pressure': 'AVG(pressure)',
    'H2O': 'AVG(H2O)',
}


def prepare_HPP_columns(dt: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare features for HPP calibration / predictions
    """
    dt['date'] = pd.to_datetime(dt['time'], unit='s')
    dt["hpp_log_ir"] = - np.log(dt['hpp_ir'])
    dt["hpp_inverse_ir"] = 1/(dt['hpp_ir'])
    dt['hpp_t_abs'] = calc.absolute_temperature(dt['hpp_t'])
    dt['hpp_H2O'] = calc.rh_to_molar_mixing(dt['hpp_RH'], calc.absolute_temperature(dt['hpp_t']), dt['hpp_pressure'] / 100)
    return dt


def H2O_calibration(dt: pd.DataFrame, id:int, base_path: pl.Path) -> None:
    dt_train, dt_test = train_test_split(dt)
    #Plot scatter
    fig, ax = plt.subplots()
    ax.scatter(dt['H2O'], dt['hpp_H2O'], 0.4)
    ax.set_xlabel('Reference water mixing ratio [ppm]')
    ax.set_ylabel('SHT21 water mixing ratio [ppm]')
    fig.savefig(base_path.with_name(f"{id}_H2O_cal_scatter.png"))
    #Plot timeseries

for id in ids_to_process:
    logger.debug(f"Processing sensor id {id}")
    logger.debug(f"Getting reference data")
    ref_dt = get_calibration_periods(
        engine, db_metadata, id,  tm[args.sensor_type])
    # Iterate over calibration data
    cal_data = get_cal_data(engine, db_metadata, id,  tm[args.sensor_type], 600, picarro_agg, hpp_agg)
    cal_feature = prepare_HPP_columns(cal_data.reset_index())
    res_path = args.plot
    H2O_calibration(cal_feature, id, res_path)
    logger.debug(ref_dt)
