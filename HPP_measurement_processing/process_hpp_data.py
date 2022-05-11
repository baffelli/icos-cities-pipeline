"""
This script processes the LP8/ Picarro data to produce calibration data
respectively calibrated data. The type of sensors respectively the processing mode
are choosen with a command line arguments, while the calibration model and the tables
are specified using a configuration file
"""

import pdb
import enum
from pymysql import DataError
import sensorutils.db as db_utils
import sensorutils.files as fu
import sensorutils.data as du
import sensorutils.calc as calc
import sensorutils.models as mods
import sensorutils.calibration as cal

from sensorutils.log import logger
import pathlib as pl
import pandas as pd
import sqlalchemy as sqa
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker, Session, Query
import datetime as dt
import argparse as ap
from typing import Dict, List, NamedTuple, Tuple, Callable, Optional, Union
import numpy as np
import math

import matplotlib.pyplot as plt
import matplotlib as mpl

from matplotlib.backends.backend_pdf import PdfPages

import copy as cp
import statsmodels.api as sm
from mpl_toolkits.axes_grid.anchored_artists import AnchoredText

import re


from sklearn.linear_model import QuantileRegressor

# Utility functions


def map_sensor_to_table(sensor: str):
    """
    Given a sensor type, returns the table name
    containing the sensor data
    """
    tm = {'HPP': 'hpp_data', 'LP8': 'lp8_data'}
    return tm[sensor]


def make_aggregation_column(table: sqa.sql.Selectable, column: str, avg_time: int) -> sqa.sql.elements.Label:
    """
    Given a table and a column name,
    return an expression to use the column as aggregation time
    with the duration `avg`time
    """
    return (func.floor(table.columns[column] / avg_time) * avg_time).label(column)


def make_aggregate_columns(table: sqa.sql.Selectable, aggregations: Dict[str, str]) -> List[sqa.sql.expression.ColumnClause]:
    """
    Given a table and a dictionary of functions (one per each column),
    returns a a list of :obj:`sqlalchemy.sql.expression.ColumnClause` to use
    in a select statement
    """
    return [sqa.sql.literal_column(query).label(name) for name, query in aggregations.items()]


def get_sensor_data(
        eng: sqa.engine.Engine,
        md: sqa.MetaData,
        sensor_id: int,
        sensor_table: str,
        start: dt.datetime,
        end: dt.datetime,
        aggregations: Dict[str, str],
        avg_time: int,
        time_col: str = 'time',
        batch: Optional[int] = None,
        id_col: str = "SensorUnit_ID",
        cyl: bool = True,
        cyl_tb: str = "RefGasCylinder",
        cyl_dep_tb: str = "RefGasCylinder_Deployment") -> Tuple[sqa.Table, sqa.sql.Selectable, List[sqa.sql.ColumnElement]]:
    """
    Gets the data for a given sensor id and a duration period.
    Returns a selectable that can be  further used and a list of columns to group by
    """
    # Get the table
    st: sqa.Table = md.tables[sensor_table]
    # Make an aggegation column
    ac = make_aggregation_column(st, time_col, avg_time)
    id_c = st.c[id_col]
    # Make a list of selections
    cols = [ac, id_c] + make_aggregate_columns(st, aggregations)
    # Ask for the data
    qr = sqa.select(cols).where(
        (st.columns[id_col] == sensor_id) &
        (st.columns[time_col].between(start.timestamp(), end.timestamp()))
    )
    return (st, qr, [ac, id_c])


def get_hpp_calibration_data(eng: sqa.engine.Engine,
                             md: sqa.MetaData,
                             sensor_id: int,
                             start: dt.datetime,
                             end: dt.datetime,
                             avg_time: int,
                             aggregations: dict,
                             cal_cols: List[str] = [
                                 'calibration_a', 'calibration_b'],
                             id_col: str = 'SensorUnit_ID',
                             date_col: str = 'time',
                             sensor_table: str = 'hpp_data') -> pd.DataFrame:
    """
    Gets the HPP data during the calibration periods (including reference cylinder concentrations)
    """
    st: sqa.Table = md.tables[sensor_table]
    ac = make_aggregation_column(st, date_col, avg_time)
    id_c = st.c[id_col]
    # Make a list of selections
    cols = [ac, id_c] + make_aggregate_columns(st, aggregations)
    # Get data during calibration times
    data_query = sqa.select(cols).where(
        ((st.c[cal_cols[0]] == 1) | (st.c[cal_cols[1]] == 1)) &
        (st.c[id_col] == sensor_id) &
        (st.c[date_col].between(start.timestamp(), end.timestamp()))
    ).group_by(*[ac, id_c])
    # Get cylinder table
    cylinder_query = get_cylinder_data(eng, md, str(sensor_id)).cte("cyl")
    cyl_col = [cylinder_query.c["CO2_DRY_CYL"],
               cylinder_query.c["cylinder_id"], cylinder_query.c['inlet']]
    qr_cyl = sqa.select(*data_query.c, *cyl_col).join(cylinder_query,
                                                      (data_query.c['SensorUnit_ID'] == cylinder_query.c['sensor_id']) &
                                                      (data_query.c[date_col].between(cylinder_query.c['cylinder_start'],  cylinder_query.c['cylinder_end'])), isouter=True)
    with eng.connect() as c:
        res = pd.read_sql(qr_cyl.compile(), c)
    return res


def pivot_cylinder_data(dt: pd.DataFrame,
                        index_col: List[str] = ['time'],
                        cyl_col: List[str] = ['CO2_DRY_CYL'],
                        cyl_index_col: List[str] = ['cylinder_id'],
                        inlet_col: str = 'inlet') -> pd.DataFrame:
    """
    Pivots the cylinder information in the calibration data to deliver one columns per cylinder id / inlet with the values
    of cylinder concentration and active inlet
    """
    # Column with the cylinder value
    all_col = [c for c in dt.columns if c not in cyl_col +
               [inlet_col] + index_col]
    # Pivot the sensor data
    dt_sens = pd.pivot_table(dt, index=index_col, values=all_col)
    # Pivot the cylinder data
    dt_cyl = pd.pivot_table(dt, index=index_col,
                            columns=inlet_col, values=cyl_col)
    dt_cyl_out = dt_cyl.copy().reset_index().set_index(index_col)
    dt_cyl_out.columns = [f"{a}_{b}" for a, b in dt_cyl.columns]
    # Join
    dt_wide = (dt_sens.join(dt_cyl_out, on=index_col)
               ) if not dt_cyl_out.empty else dt_sens
    #assert len(dt_wide) == len(dt.reset_index())
    return dt_wide.reset_index()


def map_cylinder_concentration(dt: pd.DataFrame) -> pd.DataFrame:
    """
    Maps the cylinder concentrations
    to one column *CO2_DRY_CYL* according to the values of the columns 'calibration_a' and 'calibration_b', which now will be in a single
    column called *calibration_inlet*
    """
    # Reverse mapping from dummy columns of calibration into one columns
    dt_out = dt.copy()
    inlet_col_re = 'calibration_([a-z])'
    dummies = dt.filter(regex=inlet_col_re)
    dt_out['calibration_inlet'] = pd.Series(
        dummies.columns[np.where(dummies != 0)[1]]).str.extract(inlet_col_re)
    # Filter matching rows
    dt_valid = dt_out[(dt_out["inlet"] ==
                       dt_out["calibration_inlet"]) | dt_out['inlet'].isna()]
    cols_to_remove = ['inlet', ] + \
        [c for c in dummies.reset_index().columns if c in dt.columns]
    return dt_valid.drop(columns=cols_to_remove)


def get_cylinder_data(
        eng: sqa.engine.Engine,
        md: sqa.MetaData,
        inlet: str,
        cyl_table: str = "RefGasCylinder",
        dep_table: str = "RefGasCylinder_Deployment") -> sqa.sql.Selectable:
    """
    Get calibration cylinder data
    for a given sensor id and period
    """
    cyl_tb = md.tables[cyl_table]
    dep_tb = md.tables[dep_table]
    cols = [cyl_tb.c["CO2"].label("CO2_DRY_CYL"),
            dep_tb.c['SensorUnit_ID'].label("sensor_id"),
            dep_tb.c['CylinderID'].label("cylinder_id"),
            func.unix_timestamp(dep_tb.c['Date_UTC_from']).label(
                "cylinder_start"),
            func.least(func.unix_timestamp(dep_tb.c['Date_UTC_to']), func.unix_timestamp(
                func.current_timestamp())).label("cylinder_end"),
            dep_tb.c['inlet']]
    st = sqa.select(cols).select_from(cyl_tb, dep_tb).join(cyl_tb,
                                                           (cyl_tb.c["CylinderID"] == dep_tb.c["CylinderID"]) &
                                                           (dep_tb.c["Date_UTC_from"] >= cyl_tb.c["Date_UTC_from"]) &
                                                           (dep_tb.c["Date_UTC_to"] <=
                                                            cyl_tb.c["Date_UTC_to"])
                                                           )
    return st


def get_ref_data(
        eng: sqa.engine.Engine,
        md: sqa.MetaData,
        location_id: str,
        ref_table: str,
        start: dt.datetime,
        end: dt.datetime,
        aggregations: Dict[str, str],
        avg_time: int,
        location_col: str = "LocationName",
        time_col: str = 'timestamp') -> pd.DataFrame:
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
        (ref_t.columns[time_col].between(func.unix_timestamp(start), func.unix_timestamp(end))) &
        (ref_t.columns[location_col] == location_id)).group_by(*id_cols)
    logger.debug(f"The query is:\n {qr.compile()}")
    with eng.connect() as c:
        res = pd.read_sql(qr.compile(), c)
    return res






def sensor_info_subquery(
        eng: sqa.engine.Engine,
        md: sqa.MetaData,
        sensor_table: str = 'Sensors',
        id_col: str = 'SensorUnit_ID',
        serial_col: str = 'Serialnumber',
        start_col: str = 'Date_UTC_from',
        end_col: str = 'Date_UTC_to',
        sensor_type_col: str = 'Type') -> sqa.sql.Selectable:
    """
    Returns sensor information table as a subquery
    to use it in other queries
    """
    sens_tab = md.tables[sensor_table]
    cols = [
        sens_tab.c[id_col].label("sensor_id"),
        sens_tab.c[serial_col].label("serial_number"),
        sens_tab.c[start_col].label('sensor_start'),
        sens_tab.c[end_col].label('sensor_end'),
        sens_tab.c[sensor_type_col].label('sensor_type')
    ]
    return sqa.select(cols).select_from(sens_tab)


def get_sensor_info(
        eng: sqa.engine.Engine,
        md: sqa.MetaData,
        id: int,
        sensor_type: du.AvailableSensors) -> List[SensInfoRow]:
    q = sensor_info_subquery(eng, md).subquery()
    ct = sqa.select(*q.c).where(
        (q.c['sensor_id'] == id) & (q.c['sensor_type'] == sensor_type.value)
    )
    ct_com = ct.compile()
    with eng.connect() as con:
        rs = con.execute(ct_com)
        res = [cal.SensInfoRow(**rw) for rw in rs]
    return res


def get_calibration_info(session: sqa.orm.Session,
                         id: int, start: dt.datetime, end: dt.datetime) -> List[cal.CalDataRow]:
    return session.query(mods.Calibration).join(mods.Sensor, mods.Sensor.id == mods.Calibration.id).first()

def get_calibration_periods(
    eng: sqa.engine.Engine,
    md: sqa.MetaData,
    sensor_id: int,
    sensor_type: du.AvailableSensors,
    start: dt.datetime,
    end: dt.datetime,
    sensor_table: str = 'Sensors',
    cal_table: str = "Calibration",
    id_col: str = "SensorUnit_ID",
    serial_col: str = "Serialnumber",
    start_col: str = "Date_UTC_from",
    end_col: str = "Date_UTC_to",
    loc_col: str = "LocationName",
    mode_col: str = "CalMode",
    sensor_type_col: str = "Type"
) -> List[cal.CalDataRow]:
    """
    Gets the calibration data for a given sensor
    :obj:`pandas.DataFrame`
    Parameters
    ----------
    sensor_id: int
        The sensor id to process
    start: datetime.datetime
        The first datetime to filter the data
    end: datetime.datetime
        The last datetime to filter the data
    md: sqlalchemy.MetaData
        the db metadata
    eng: sqalchemy.engine.Engine
        A sqlalchemy engine for the ORM session
    sensor_table: str
        The table with sensor information (serialnumbers etc)
    cal_table: str
        The name of the table containing the calibration information
    """
    import pdb
    pdb.set_trace()
    # Get table references
    cal_tab = md.tables[cal_table]
    #sens_tab = md.tables[sensor_table]
    sens_query_ft = sensor_info_subquery(eng, md).subquery()
    #ens_query_ft = sqa.select(sens_query)
    # Define columns to get
    cols = [
        cal_tab.c[loc_col].label("location"),
        *sens_query_ft.c,
        cal_tab.c[start_col].label('cal_start'),
        (func.least(
            cal_tab.c[end_col], func.current_timestamp())).label('cal_end'),
        cal_tab.c[mode_col].label(
            "cal_mode") if mode_col in cal_tab.c else sqa.literal(1).label("cal_mode")
    ]
    st_str = str(sensor_type.value)
    # Query calibration table
    ct = sqa.select(*cols).select_from(cal_tab, sens_query_ft).join(
        sens_query_ft, (sens_query_ft.c["sensor_id"] == cal_tab.c[id_col]) &
        (sens_query_ft.c["sensor_start"] < cal_tab.c[start_col]) &
        (sens_query_ft.c["sensor_end"] >= cal_tab.c[end_col])
    ).filter((cal_tab.c[id_col] == sensor_id) &
             (sens_query_ft.c["sensor_type"] == st_str) &
             (cal_tab.c[start_col] >= start) & (cal_tab.c[end_col] <= end))
    # Execute query
    ct_com = ct.compile()
    logger.debug(f"The query is:\n {ct_com}")
    with eng.connect() as con:
        res = [cal.CalDataRow(**rw._asdict()) for rw in con.execute(ct_com)]
    return res


def get_picarro_data(eng: sqa.engine.Engine,
                     md: sqa.MetaData,
                     location: str,
                     start: dt.datetime,
                     end: dt.datetime,
                     agg_duration: int,
                     table: str = "picarro_data") -> pd.DataFrame:
    """
    Get data for the Picarro CRDS 
    instrument at the given location `location` for the period
    between `start` and `end`. Returns aggregate data aggregated 
    by `agg_time`
    """
    picarro_agg = {
        'ref_CO2_DRY': 'AVG(IF(CO2_DRY = -999 OR CO2_DRY_F = 0, NULL, CO2_DRY))',
        'ref_CO2_DRY_SD': 'STDDEV(IF(CO2_DRY = -999 OR CO2_DRY_F = 0, NULL, CO2_DRY))',
        'ref_T': 'AVG(NULLIF(T, -999))',
        'ref_RH': 'AVG(NULLIF(RH, -999))',
        'ref_pressure': 'AVG(NULLIF(pressure * 100, -999))',
        'chamber_mode': 'MAX(calibration_mode)',
        'chamber_status': 'MAX(chamber_status)',
        'ref_H2O': 'AVG(IF(H2O = -999 OR H2O_F = 0, NULL, H2O))', }
    picarro_data = get_ref_data(
        eng, md, location, table, start, end, picarro_agg, agg_duration)
    return picarro_data


def get_HPP_data(
        engine: sqa.engine.Engine,
        md: sqa.MetaData,
        id: int,
        start: dt.datetime,
        end: dt.datetime,
        agg_duration: int,
        only_cyl: bool = False,
        **kwargs):
    """
    Get the data
    for a senseair HPP instrument for the selected
    duration. The duration is given by `start` and `end`.
    If `only_cyl` is set to true, only returns data during cylinder calibration
    """
    hpp_agg = {
        'sensor_CO2': 'AVG(NULLIF(senseair_hpp_co2_filtered, -999))',
        'sensor_pressure': 'AVG(NULLIF(senseair_hpp_pressure_filtered, -999))',
        'sensor_RH': 'AVG(NULLIF(sensirion_sht21_humidity, -999))',
        'sensor_t': 'AVG(NULLIF(sensirion_sht21_temperature, -999))',
        'sensor_ir_lpl': 'AVG(NULLIF(senseair_hpp_lpl_signal, -999))',
        'sensor_ir': 'AVG(NULLIF(senseair_hpp_ir_signal, -999))',
        'sensor_detector_t': 'AVG(NULLIF(senseair_hpp_temperature_detector, -999))',
        'sensor_mcu_t': 'AVG(NULLIF(senseair_hpp_temperature_mcu, -999))',
        'sensor_calibration_a': 'MAX(COALESCE(calibration_a, 0))',
        'sensor_calibration_b': 'MAX(COALESCE(calibration_b, 0))',
    }
    time_col = 'time'
    sensor_table, sensor_query, agg_col = get_sensor_data(
        engine, md, id, 'hpp_data', start, end, hpp_agg, agg_duration, time_col=time_col, **kwargs)
    # Select only calibration data
    if only_cyl:
        final_stmt = sensor_query.where((sensor_table.c['calibration_a'] == 1) | (
            sensor_table.c['calibration_b'] == 1))
    else:
        final_stmt = sensor_query
    grouped_stmt = final_stmt.group_by(*agg_col)
    # Add cylinder information
    # Get cylinder table
    cylinder_query = get_cylinder_data(engine, md, str(id)).cte("cyl")
    cyl_col = [cylinder_query.c["CO2_DRY_CYL"],
               cylinder_query.c["cylinder_id"], cylinder_query.c['inlet']]
    qr_cyl = sqa.select(*grouped_stmt.c, *cyl_col).join(cylinder_query,
                                                        (grouped_stmt.c['SensorUnit_ID'] == cylinder_query.c['sensor_id']) &
                                                        (grouped_stmt.c[time_col].between(cylinder_query.c['cylinder_start'],  cylinder_query.c['cylinder_end'])), isouter=True)
    with engine.connect() as con:
        res = pd.read_sql_query(qr_cyl.compile(), con)
    return res


def get_LP8_data(
        engine: sqa.engine.Engine,
        md: sqa.MetaData,
        id: int,
        start: dt.datetime,
        end: dt.datetime,
        agg_duration: int,
        **kwargs):
    """
    Get the data
    for a senseair LP8 instrument for the selected
    duration and aggregation duration
    """
    lp8_agg = {
        'sensor_CO2': 'AVG(NULLIF(senseair_lp8_co2, -999))',
        'sensor_RH': 'AVG(NULLIF(sensirion_sht21_humidity, -999))',
        'sensor_t': 'AVG(NULLIF(sensirion_sht21_temperature, -999))',
        'sensor_ir': 'AVG(NULLIF(senseair_lp8_ir, -999))',
        'sensor_detector_t': 'AVG(NULLIF(senseair_lp8_temperature, -999))',
    }
    tb, sensor_query, agg_col = get_sensor_data(
        engine, md, id, "lp8_data", start, end, lp8_agg, agg_duration, **kwargs)
    with engine.connect() as con:
        sensor_data = pd.read_sql_query(
            sensor_query.group_by(*agg_col).compile(), con)
    return sensor_data


def get_cal_data(eng: sqa.engine.Engine,
                 md: sqa.MetaData,
                 sensor_id: int,
                 start: dt.datetime,
                 end: dt.datetime,
                 sensor_type:
                 du.AvailableSensors,
                 agg_duration:
                 int,
                 cal: bool = True,
                 #only_active: bool = True
                 ) -> List[Tuple[Tuple[str, pd.Timestamp, pd.Timestamp], pd.DataFrame]]:
    """
    Gets all the calibration data for a sensor
    with given type and id and returns it as a dataframe.
    Parameters
    ----------
    eng: sqlachemy.engine.Engine
        The sqlalchemy database engine where to run the query
    md: sqlachemy.MetaData
        Database metadata
    sensor_id: int
        Gets data for the given sensor
    start: dt.datetime
        The starting date to retreive calibration data
    end: dt.datetime or none
        The end datetime to retreive calibration data
    sensor_type: sensorutils.data.AvailableSensors
        The sensor type to process
    agg_duration: int
        The duration of the aggregation interval for the data
    cal: bool
        If true, get the data from the calibration table, otherwise from the deployment
    only_active: bool
        If set to True, only get data for currently active sensor serial numbers
    data_start: date or none
    data_end: date or none
    """
    # Get the calibration periods from the calibration resp from the deployment table
    if cal:
        ref_dt_orig = get_calibration_periods(
            eng, md, sensor_id,  sensor_type, cal_table='Calibration')
    else:
        ref_dt_orig = get_calibration_periods(
            eng, md, sensor_id,  sensor_type, cal_table='Deployment')
    # Filter only active deployments / calibration if desired
    #ref_dt = [rw for rw in ref_dt_orig if rw.sensor_end >= dt.datetime.now()] if only_active else ref_dt_orig
    # Filter only valid rows (where the calibration / deployment is still active or the calibration end is larger than `start`)
    valid_rows = [rw for rw in ref_dt_orig if rw.cal_start >=
                  start and rw.cal_end <= end]
    import pdb
    pdb.set_trace()
    if len(valid_rows) == 0:
        raise ValueError(
            'There are no calibration entries for sensor {sensor_id} after {start}')
    # Select sensor data getter
    match sensor_type:
        case du.AvailableSensors.HPP:
            def getter(u, v, w, x, y, z): return map_cylinder_concentration(
                get_HPP_data(u, v, w, x, y, z))
        case du.AvailableSensors.LP8:
            def getter(u, v, w, x, y, z): return get_LP8_data(u, v, w, x, y, z)

    def get_and_combine(tw: cal.CalDataRow) -> pd.DataFrame:
        sensor_data = getter(
            eng, md, sensor_id, tw.cal_start, tw.cal_end, agg_duration)
        # Import reference data
        picarro_data = get_picarro_data(
            eng, md, tw.location, tw.cal_start, tw.cal_end, agg_duration)
        # Join data
        cal_data = sensor_data.set_index('time').join(
            picarro_data.set_index('timestamp'), how='left')
        cal_data['cal_mode'] = tw.cal_mode
        cal_data['cal_start'] = tw.cal_start
        cal_data['cal_end'] = tw.cal_end
        cal_data['sensor_start'] = tw.sensor_start
        cal_data['sensor_end'] = tw.sensor_end
        cal_data['sensor_type'] = tw.sensor_type
        cal_data['cal_end'] = tw.cal_end
        cal_data['serial_number'] = tw.serial_number
        return cal_data

    # Iterate over calibration data
    pd_con = pd.concat([get_and_combine(tw) for tw in valid_rows])
    grps = ["serial_number", "sensor_start", "sensor_end"]
    return list(pd_con.groupby(grps))


def get_prediction_data(eng: sqa.engine.Engine,
                        md: sqa.MetaData,
                        sensor_id: int,
                        sensor_type:
                        du.AvailableSensors,
                        start: Optional[dt.datetime],
                        end: dt.datetime,
                        agg_duration:
                        int) -> List[Tuple[Tuple[str, pd.Timestamp, pd.Timestamp], pd.DataFrame]]:
    si = get_sensor_info(eng, md, id, sensor_type)
    ad = get_averaging_time(sensor_type, fit=False)
    match sensor_type:
        case du.AvailableSensors.HPP:
            getter = get_HPP_data
        case du.AvailableSensors.LP8:
            getter = get_LP8_data

    def get_inner(rw: SensInfoRow) -> pd.DataFrame:
        """
        Get data for the given sensor and time
        """
        query_start = max([rw.sensor_start, start])
        query_end = min([rw.sensor_end, end])
        dt = getter(eng, md, rw.sensor_id,  query_start, query_end, ad)
        dt['serial_number'] = rw.serial_number
        dt['sensor_start'] = rw.sensor_start
        dt['sensor_end'] = rw.sensor_end
        return dt
    pd_con = pd.concat([get_inner(tw) for tw in si])
    grps = ["serial_number", "sensor_start", "sensor_end"]
    return list(pd_con.groupby(grps))


def prepare_features(dt: pd.DataFrame, ir_col: str = "sensor_ir_lpl", fit: bool = True, plt: str = '1d') -> pd.DataFrame:
    """
    Prepare features for CO2 calibration / predictions
    """
    dt_new = dt.copy()
    dt_new['date'] = pd.to_datetime(dt_new['time'], unit='s')
    dt_new["sensor_ir_log"] = - np.log(dt_new[ir_col])
    dt_new["sensor_ir_inverse"] = 1/(dt_new[ir_col])
    dt_new["sensor_ir_interaction_t"] = dt_new[f"sensor_t"] * \
        dt_new[ir_col]
    dt_new["sensor_ir_interaction_inverse_pressure"] = dt_new['sensor_pressure'] / \
        (dt_new[ir_col])
    dt_new['sensor_t_abs'] = calc.absolute_temperature(dt_new['sensor_t'])
    # Water content
    dt_new['sensor_H2O'] = calc.rh_to_molar_mixing(
        dt_new['sensor_RH'], dt_new['sensor_t_abs'], dt_new['sensor_pressure'] / 100)
    # Referenced CO2
    nc_sens = (dt_new['sensor_pressure']) / calc.P0 * \
        dt_new['sensor_t_abs'] / calc.T0
    dt_new['sensor_CO2_comp'] = dt["sensor_CO2"] * nc_sens
    # Dummy variable every `plt` days
    dt_new['time_dummy'] = dt_new['date'].dt.round(plt).dt.date.astype('str')
    # Start of cylinder calibration (anytime the inlet changes)
    dt_new['inlet_change'] = dt_new['calibration_inlet'].shift(
    ) != dt_new['calibration_inlet']
    dt_new['cal_num'] = dt_new['inlet_change'].cumsum()
    dt_new['cal_start'] = dt_new.groupby(
        dt_new['cal_num']).apply(lambda x: x['date'].min())
    dt_new['cal_elapsed'] = dt_new.groupby(dt_new['cal_num']).apply(
        lambda x: (x['date'] - x['date'].min())).reset_index()['date'].dt.seconds
    # Additional features needed for fitting
    if fit:
        dt_new['ref_t_abs'] = calc.absolute_temperature(dt_new['ref_T'])
        # Normalisation constant
        nc = (dt_new['ref_pressure']) / calc.P0 * dt_new['ref_t_abs'] / calc.T0
        # Compute normalised concentrations using ideal gas law
        dt_new['sensor_H2O_comp'] = dt_new['sensor_H2O'] * nc
        dt_new['ref_CO2'] = calc.dry_to_wet_molar_mixing(
            dt_new['ref_CO2_DRY'], dt_new['ref_H2O'])
        dt_new['ref_CO2_comp'] = calc.dry_to_wet_molar_mixing(
            dt_new['ref_CO2_DRY'], dt_new['ref_H2O']) * nc
        dt_new['ref_H2O_comp'] = dt_new['ref_H2O'] * nc

    return dt_new


def prepare_LP8_features(dt: pd.DataFrame, fit: bool = True,  plt: str = '1d') -> pd.DataFrame:
    """
    Prepare features for LP8 calibration
    """
    dt_new = dt.copy().reset_index()
    dt_new['date'] = pd.to_datetime(dt_new['time'], unit='s')
    dt_new["sensor_ir_log"] = - np.log(dt_new["sensor_ir"])
    dt_new["sensor_ir_interaction_t"] = dt_new[f"sensor_t"] * \
        dt_new["sensor_ir"]
    dt_new["sensor_ir_interaction_t_sq"] = dt_new[f"sensor_t"]**2 * \
        dt_new["sensor_ir"]
    dt_new["sensor_ir_interaction_t_cub"] = dt_new[f"sensor_t"]**3 * \
        dt_new["sensor_ir"]
    dt_new['sensor_CO2_interaction_t'] = dt_new[f"sensor_CO2"] / \
        dt_new[f"sensor_t"]
    dt_new["sensor_t_sq"] = dt_new[f"sensor_t"]**2
    dt_new["sensor_t_cub"] = dt_new[f"sensor_t"]**3
    dt_new['time_dummy'] = dt_new['date'].dt.round(plt).dt.date.astype('str')
    dt_new['const'] = -1
    # Additional features needed for fitting but not for prediction
    if fit:
        try:
            dt_new['ref_t_abs'] = calc.absolute_temperature(dt_new['ref_T'])
            # Normalisation constant
            nc = dt_new['ref_t_abs'] / calc.T0
            # Compute normalised concentrations using ideal gas law
            dt_new['ref_CO2'] = calc.dry_to_wet_molar_mixing(
                dt_new['ref_CO2_DRY'], dt_new['ref_H2O'])
            dt_new['ref_CO2_comp'] = calc.dry_to_wet_molar_mixing(
                dt_new['ref_CO2_DRY'], dt_new['ref_H2O']) * nc
        except KeyError:
            pass
    return dt_new


def H2O_calibration(dt: pd.DataFrame,
                    target_col: str = 'ref_H2O_comp',
                    cal_col: str = 'sensor_H2O_comp') -> sm.regression.linear_model.RegressionModel:
    """
    Fits an ols model to calibrate the H2O sensor and returns the 
    fit model
    """
    # Fit model
    valid = dt['ref_T'] > 0 & ~(dt[target_col].isna()) & ~(
        dt[cal_col].isna()) & (dt[cal_col] >= 5)
    dt_valid = dt[valid][[target_col, cal_col]].dropna()
    cm = sm.OLS(dt_valid[target_col], sm.add_constant(dt_valid[cal_col]))
    cm_fit = cm.fit()
    return cm_fit


def add_dummies(dt_in: pd.DataFrame, dummy_name: str) -> Tuple[pd.DataFrame, List[str]]:
    dt = dt_in.copy()
    dms = pd.get_dummies(dt[dummy_name], prefix=dummy_name, drop_first=True)
    dt_dm = pd.concat([dt, dms], axis=1)
    dm_cols = [str(c) for c in dms.columns]
    return dt_dm, dm_cols


def CO2_calibration(dt_in: pd.DataFrame, reg: List[str], target: str = 'ref_CO2', dummy: Optional[str] = 'time_dummy') -> sm.regression.linear_model.RegressionModel:
    """
    Fit the HPP/LP8 CO2 calibration

    Parameters
    ----------
    dt_in: pandas.DataFrame
        A dataframe with features and outcome variable for the regression
    """
    dt = dt_in.copy()
    if dummy:
        dt_dm, dm_cols = add_dummies(dt, dummy)
        tg_col = target
        reg_col = reg + dm_cols
    else:
        reg_col = reg
        tg_col = target
        dt_dm = dt.copy()
    # Drop any missing regressors
    all_cs = reg_col + [tg_col]
    valid = ~(dt_dm[all_cs].isna().any(axis=1) |
              np.isinf(dt_dm[all_cs]).any(axis=1))
    dt_valid = dt_dm[valid]
    if len(dt_valid) == 0:
        raise ValueError('All regressors are null')
    cm = sm.OLS(dt_valid[tg_col], dt_valid[reg_col])
    return cm.fit()


def HPP_CO2_calibration(dt_in: pd.DataFrame):
    """
    Computes the HPP CO2 calibration with the model
    proposed by Michael Müller
    Using the `lpl` flag you can use either the long path or
    the short path IR value
    """
    reg = [
        "sensor_ir_log", "sensor_ir_inverse", "sensor_pressure", "sensor_t_abs", "sensor_detector_t", "sensor_ir_interaction_inverse_pressure", 'sensor_H2O']
    return CO2_calibration(dt_in, reg)


def LP8_CO2_calibration(dt_in: pd.DataFrame) -> sm.regression.linear_model.RegressionResultsWrapper:
    """
    """
    reg = ["sensor_ir_log", "const", "sensor_t", "sensor_t_sq",
           "sensor_ir_interaction_t", "sensor_ir_interaction_t_sq"]
    #reg = ["sensor_ir_log", "sensor_ir_interaction_t", "sensor_t", "sensor_t_sq"]
    return CO2_calibration(dt_in, reg, target="ref_CO2", dummy=None)


def predict_CO2(dt_in: pd.DataFrame, model: sm.regression.linear_model.RegressionResultsWrapper) -> pd.DataFrame:
    """
    Apply the model in `model` to
    predict the CO2 concentration
    """
    # dt_out = sm.add_constant(dt_in.copy(),
    #                          has_constant='add', prepend=False)
    dt_out = dt_in.copy()
    dt_out["CO2_pred"] = model.predict(
        dt_out[model.model.exog_names])

    return dt_out


def bias(ref: pd.Series, value: pd.Series) -> float:
    """
    Computes the bias (mean difference)
    of two `pandas.Series`
    """
    return (ref - value).mean()


def rmse(ref: pd.Series, value: pd.Series) -> float:
    """
    Computes the rmse (mean squared difference)
    of two `pandas.Series`
    """
    return np.sqrt(((ref - value)**2).mean())


def plot_CO2_calibration(dt: pd.DataFrame,
                         model: sm.regression.linear_model.RegressionResults,
                         ref_col='ref_CO2',
                         pred_col='CO2_pred',
                         orig_col="sensor_CO2_comp",
                         extra_res: Optional[List[str]] = None) -> Tuple[plt.Figure, plt.Figure]:
    """
    Plot detailed CO2 calibration results:
    - Time series of sensor and reference
    - Scatter plot
    - Residuals of regressors
    """
    # Compute residuals statistics
    residuals = dt[ref_col] - dt[pred_col]
    res_rmse = rmse(dt[ref_col],  dt[pred_col])
    res_bias = bias(dt[ref_col],  dt[pred_col])
    res_cor = dt[ref_col].corr(dt[pred_col])
    # Make annotation
    ann = AnchoredText(
        f"RMSE: {res_rmse:5.2f} ppm,\n Bias: {res_bias:5.2f} ppm,\n corr: {res_cor:5.3f}", loc=3)
    cs = plt.cm.get_cmap('Set2')
    # Create plot for timeseries
    fig = mpl.figure.Figure()
    ax = fig.add_subplot(211)
    ax.scatter(dt[ref_col], dt[pred_col], 0.4,
               color=cs.colors[0], label='Calibrated')
    ax.scatter(dt[ref_col], dt[orig_col],
               0.4, color=cs.colors[1], label='Uncalibrated')
    ax.axline((0, 0), slope=1., color='C0')
    ax.set_xlabel('Reference mixing ratio [ppm]')
    ax.set_ylabel('Sensor mixing ratio [ppm]')
    lms = [300, 1000]
    ax.set_xlim(lms)
    ax.set_ylim(lms)
    # Add annotation
    ax.add_artist(ann)
    ax.legend()
    # Plot timeseries
    dt_col = dt['date'].dt.to_pydatetime()
    ts_ax = fig.add_subplot(212)
    ts_ax.scatter(dt_col, dt[orig_col],
                  0.4, color=cs.colors[1], label='Uncalibrated')
    ts_ax.scatter(dt_col, dt[pred_col],
                  0.4, color=cs.colors[0], label='Calibrated')
    ts_ax.scatter(dt_col, dt[ref_col],
                  0.4, color=cs.colors[2], label='Reference')
    ts_ax.set_xlabel('Date')
    ts_ax.set_ylabel('CO2 mixing ratio [ppm]')
    ts_ax.set_ylim(lms)
    ts_ax.legend()
    ts_ax.tick_params(axis='x', labelrotation=45)
    # Plot residuals vs various parameters
    fig2: mpl.Figure = mpl.figure.Figure()
    reg_names = [k for k, n in model.params.items()] + [ref_col, ] + extra_res
    nr = math.ceil(math.log2(len(reg_names))) + 1
    for i, par_name in enumerate(reg_names, start=1):
        current_ax = fig2.add_subplot(nr, nr, i)
        current_ax.scatter(dt[par_name], residuals, 0.4)
        current_ax.set_xlabel(f"Feature: {par_name}")
        current_ax.set_ylabel('Residual [ppm]')
        current_ax.set_ylim(-30, 30)
    fig2.tight_layout()
    fig.tight_layout()
    return fig, fig2


def compute_quality_indices(pred: pd.DataFrame, ref_col='ref_CO2', pred_col='CO2_pred',) -> dict:
    """
    Compute model quality indicators for a dataframe of model predictions
    and return a dict of the statistics
    """
    res_rmse = rmse(pred[ref_col],  pred[pred_col])
    res_bias = bias(pred[ref_col],  pred[pred_col])
    res_cor = pred[ref_col].corr(pred[pred_col])
    return {'rmse': res_rmse, 'bias': res_bias, 'correlation': res_cor}


def cleanup_data(dt_in: pd.DataFrame) -> pd.DataFrame:
    """
    Preliminary filter of data:
    - replacing of -999 with NaN
    - Removing negative concentrations
    """
    dt = dt_in.copy()
    dt = dt.replace([None, du.ICOS_MISSING], [0, np.NaN])
    # Condition
    con = dt['ref_CO2_DRY'].le(0) | dt['ref_CO2_DRY'].isna() | np.abs(
        dt['ref_CO2_DRY_SD']) > 4
    return dt[~con]


def split_HPP_cal_data(dt_in: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split the dataframe into two groups of dataframes
    - One group where the bottle calibration is on
    - One group where the bottle calibration is off
    """
    cal_period = (dt_in['sensor_calibration_a'] == 1) | (
        dt_in['sensor_calibration_b'] == 1)
    return dt_in[cal_period], dt_in[~cal_period]


class CalModes(enum.Enum):
    """
    Enum to represent different CV modes for the calibration
    """
    CHAMBER = [2, 3]
    COLLOCATION = [1]
    MIXED = [1, 2, 3]


def split_stable_conditions(dt_in: pd.DataFrame, col: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """
    Takes a dataframe and a boolean series of the same length
    and returns two sets of indices, *validation* and *training* each of which begin at the point of
    change in the series and contain half of this period
    """


def cal_cv_split(dt_in: pd.DataFrame,
                 sensor: du.AvailableSensors,
                 mode: CalModes = CalModes.MIXED,
                 duration: float = 7,
                 date_col: str = 'date') -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Splits the data in a training and testing dataset.
    Different splits are choosen depending on the calibration mode

    For MIXED (chamber and collocation), uses:

    - Climate chamber for training
    - Collocation for assessment

    For COLLOCATION, uses:

    - First `duration` days data  for calibration
    - The remaining time for assessment

    For CHAMBER, uses:

    - First `duration` days of data for training
    - The remaining time for assessment

    """
    dt_copy = dt_in.copy()
    match mode:
        case CalModes.COLLOCATION, _:
            cal_period = dt_in['cal_mode'].isin(mode.value)
            cal_start = dt_in[cal_period][date_col].min()
            cal_end = dt_in[cal_period][date_col].max() + \
                dt.timedelta(days=duration)
        case CalModes.CHAMBER:
            cal_period = dt_in['cal_mode'].isin(mode.value)
            cal_start = dt_in[cal_period][date_col].min()
            cal_end = cal_start + \
                dt.timedelta(days=duration)
        case CalModes.MIXED:
            cal_period = dt_in['cal_mode'].isin(mode.value)
            cal_start = dt_in[cal_period][date_col].min()
            cal_end = cal_start + dt.timedelta(days=duration)
    if not cal_period.any():
        raise DataError(
            f"There are no values for the calibration type {mode} in this dataset")
    train: pd.Series = dt_in[date_col].between(cal_start, cal_end)
    match sensor:
        case du.AvailableSensors.LP8:
            # Find periods of stable concentration
            stable_cols = dt_in['chamber_status'] == 'MEASURE'
            # Only take mode 1, mode 2 is RH calibration
            cal_mode = (dt_in['cal_mode'] == 0)
            # Find periods of change
            dt_copy['mode_change'] = ((dt_in['chamber_status'] != dt_in['chamber_status'].shift(
                -1)) & (dt_in['chamber_status'] != 'MEASURE')).cumsum()
            # For each "plateau", take 1/4 of the sample for training and half for testing
            plateaus = dt_copy.groupby("mode_change").apply(
                lambda x:  x.reset_index().index.to_series() < len(x) * 0.25).reset_index()[0]
            fit = train & stable_cols & plateaus
            test = train & stable_cols & ~plateaus
        case du.AvailableSensors.HPP:
            fit = train
            test = ~train
    return dt_in[fit], dt_in[test]


def remove_dummies(mod_fit: sm.regression.linear_model.OLSResults, dummy_prefix: str) -> sm.regression.linear_model.OLSResults:
    """
    Remove dummy variables from model fit
    By removing all variables starting with `dummy_prefix`
    """
    new_names = [n for n in mod_fit.params.keys(
    ) if not dummy_prefix in n]
    new_names = new_names + \
        ['const'] if 'const' not in mod_fit.params else new_names
    new_pars = pd.Series(
        {k: v for k, v in mod_fit.params.items() if k in new_names})
    dummy_names = [n for n in mod_fit.params.keys() if dummy_prefix in n]
    # Add intercept
    if dummy_names:
        loc_c = mod_fit.params['const'] if 'const' in mod_fit.params else 0
        new_pars['const'] = loc_c + mod_fit.params[dummy_names][-1]
    else:
        new_pars = mod_fit.params
    mod_fit_cp = cp.deepcopy(mod_fit.model)
    mod_fit_cp.data.xnames = new_names
    return sm.regression.linear_model.OLSResults(mod_fit_cp, new_pars)


def save_multipage(plots: List[plt.Figure], base_path: pl.Path, names: List[str]) -> None:
    """
    Save a list of plots as multipage pdf
    """
    with PdfPages(base_path) as pdf:
        for p, n in zip(plots, names):
            p.suptitle(n)
            pdf.savefig(p)


def convert_calibration_parameters(cp: sm.regression.linear_model.OLSResults,
                                   species: str,
                                   type: du.AvailableSensors,
                                   valid_from: dt.datetime,
                                   valid_to: dt.datetime,
                                   computed: dt.datetime,
                                   device: str) -> mods.CalibrationParameters:
    """
    Convert the fit results into a :obj:`sensorutils.data.CalibrationParameters`
    object that can be persisted in the database in a human-readable format
    thank to SQLAlchemy
    """
    pl = [mods.CalibrationParameter(parameter=n, value=v)
          for n, v in cp.params.items()]
    return mods.CalibrationParameters(parameters=pl,
                                    species=species,
                                    type=type.value, valid_from=valid_from,
                                    valid_to=valid_to,
                                    computed=computed, device=device)


def get_closest_calibration_parameters(engine: sqa.engine.Engine,
                                       md: sqa.MetaData,
                                       device: str,
                                       date: dt.datetime) -> mods.CalibrationParameters:
    """
    Get the calibration parameters closest to the given date
    for the given device id
    """
    Query(mods.CalibrationParameters)


def get_averaging_time(sensor: du.AvailableSensors, fit: bool = True) -> int:
    match sensor, fit:
        case du.AvailableSensors.HPP, True:
            av_sec = 600
        case du.AvailableSensors.LP8, True:
            av_sec = 600
        case du.AvailableSensors.HPP, False:
            av_sec = 60
        case du.AvailableSensors.LP8, False:
            av_sec = 600
    return av_sec


def filter_valid_sensor_ids(data: pd.DataFrame, sensor_type: du.AvailableSensors, sc: str = 'SensorUnit_ID') -> pd.DataFrame:
    """
    Filter a list of sensor ids removing the old HPP sensors (390 and less)
    """
    match sensor_type:
        case du.AvailableSensors.HPP:
            vd = data[data[sc].between(400, 500)]
        case du.AvailableSensors.LP8:
            vd = data
    return vd


def make_valid_sensors(ids: pd.Series) -> enum.Enum:
    """
    Creates an enumeration of all valid ids for the given
    pandas series. This is used to only allow certain
    sensor ids to be selected for processing
    """
    return enum.Enum('ValidSensors', [(f, int(f)) for f in ids.astype(str).tolist()])


def calibrate_HPP_cylinder(dt: pd.DataFrame) -> sm.regression.linear_model.OLSResults:
    """
    Computes the two point HPP calibration using the cylinder information
    """
    sm.OLS(dt['CO2_REF'])


def deserialize_parameters(session: sqa.orm.Session, serialnumber: Union[str, int], latest: bool = True, when: Optional[dt.datetime] = None) -> mods.CalibrationParameters:
    """
    Deserialize parameters from the DB into a CalibrationParameters object.
    If `latest` is set, returns the latest computed model for the given `serialnumber`,
    otherwise gets the one for the `when` date

    Parameters
    ----------
    session: :obj:`sqlalchemy.orm.Session`
    serialnumber: int or str
    latest: bool
        if set to `True` return the latest calibration parameter that was stored, otherwise
    when: dt.datetime or none
        If passed, get the parameters for the given date
    """
    qr = session.query(mods.CalibrationParameters).filter(
        mods.CalibrationParameters.device == serialnumber)
    if latest and not when:
        final_qr = qr.order_by(
            mods.CalibrationParameters.computed.desc()).first()
    elif not latest and when:
        final_qr = qr.filter(mods.CalibrationParameters.computed == when).first()
    else:
        raise ValueError(
            f"You need to set `latest` to True or pass a date to `when` in order to unambigously retreive one instance of `CalibrationParameters`")
    return final_qr


def save_lp8_predictions(eng: sqa.engine.Engine, md: sqa.MetaData, data: pd.DataFrame, table='co2_level2') -> None:
    """
    Save the table of predictions for the lp8 sensor to the database in the specified table
    """
    col_names = {'CO2_pred': 'CO2',
                 'temperature': 'sensor_temperature',
                 'sensor_rh': 'relative_humidity',
                 'timestamp': 'timestamp',
                 'SensorUnit_ID': 'sensor_id'}
    data.rename(columns=col_names)
    upsert = db_utils.create_upsert_metod(md)
    with eng.connect() as con:
        data.to_sql("co2_level2", con, method=upsert)


def fit_rh_threshold(data: pd.DataFrame) -> None:
    """
    Fits a RH threshold for the sensor to ensure that 
    the data quality 
    """
    data_in = data.copy()
    train_data = data_in[data_in['chamber_mode'] ==
                         '1' & data_in['chamber_status'] == 'MEASURE']
    train_data['ref_rh'] = calc.molar_mixing_to_rh(
        train_data['ref_H2O'], train_data['ref_t_abs'], train_data['ref_pressure'])
    valid_rh_range = train_data['ref_rh'].between(0, 100)
    dt = train_data[valid_rh_range]
    X = dt[['ref_RH', 'ref_t_abs']].to_numpy()
    y = dt['CO2_pred'] - dt['ref_CO2_comp']
    qp = QuantileRegressor(quantile=0.95, alpha=0).fit(X, y)
    fig = mpl.figure.Figure()
    ax = fig.add_subplot(211)
    ax.scatter(X[:, 0], y)
    y_p = qp.predict(X)
    ax.scatter(X[:, 0], y_p, color='red')
    ax.set_ylim(-100, 100)
    fig.savefig('/newhome/basi/plot.png')
    fig = mpl.figure.Figure()
    ax = fig.add_subplot(211)
    ax.scatter(dt['ref_H2O'], dt['sensor_RH'])
    fig.savefig('/newhome/basi/plot.png')


# Setup parser
parser = ap.ArgumentParser(description="Calibrate or process HPP/LP8 data")
parser.add_argument('config', type=pl.Path, help="Path to configuration file")
parser.add_argument('mode', type=str, choices=[
                    'calibrate', 'process'], help="Processing mode")
parser.add_argument(
    'sensor_type', type=du.AvailableSensors, help="Sensor type")
parser.add_argument('--plot', type=pl.Path, help='Path to save plots')
parser.add_argument('id', type=int, nargs='?',
                    help="If passed, only process one sensor")
parser.add_argument('--full', default=False, action='store_true',
                    help="Partial processing or full data")
args = parser.parse_args()

# Load configuration
data_mapping = fu.DataMappingFactory.read_config(args.config)[
    args.sensor_type.name]

# Connect to metadata/measurement database
logger.info('Connecting to the DB')
engine = db_utils.connect_to_metadata_db()
db_metadata = sqa.MetaData(bind=engine)
db_metadata.reflect()
# Attach the db connection to the data mapping
data_mapping.connect_all_db(source_eng=engine, dest_eng=engine)


# Create ORM session
Session = sessionmaker(engine)
with Session() as ses:
    dp = 
    pdb.set_trace()
# List all ids
ids = filter_valid_sensor_ids(db_utils.list_all_sensor_ids(
    args.sensor_type.value, engine), args.sensor_type)

# Make an enum of all valid ids
valid_ids = make_valid_sensors(ids['SensorUnit_ID'])
# Find the ids to process
ids_to_process = [valid_ids(args.id)] if args.id else [
    v for k, v in valid_ids.__members__.items()]
logger.debug(f"Processing mode: '{args.mode}'")
logger.debug(
    f"Processing sensor type '{args.sensor_type}' with ids:\n {ids_to_process}")


b_pth: pl.Path = args.plot
# Starting date for calibration
start = du.CS_START if args.full else dt.datetime.now() - dt.timedelta(days=60)
for current_id in ids_to_process:
    id = current_id.value
    logger.debug(f"Processing sensor id {id}")
    logger.debug(f"Getting reference data")
    match args.mode, args.sensor_type:
        case "process", (du.AvailableSensors.LP8 as st):
            # Create mapping
            serialnumber = db_utils.get_serialnumber(
                engine, db_metadata, id, st.value)
            # List files missing in destination (only process them)
            missing_dates = data_mapping.list_files_missing_in_dest(group=id)
            # Get calibration parameters
            with Session() as session:
                pm = deserialize_parameters(session, serialnumber, latest=True)
                if pm is None:
                    #raise ValueError(f'There are no calibration parameters for the sensor with serialnumber {serialnumber} in the database')
                    continue
                # Remove the dummy if the model has any
                model_fit = remove_dummies(pm.to_statsmodel(), "time_dummy")
            for date in missing_dates:
                logger.info(f"Getting raw data for {id} on {date}")
                # Get data
                cal_data, *rest = [data for (sn, start, end), data in get_cal_data(
                    engine, db_metadata, id, date, dt.datetime.now(), st, 600, cal=False) if end > dt.datetime.now()]
                # Prepare regressors
                prediction_features = prepare_LP8_features(cal_data, fit=True)
                # Preict
                predicted = predict_CO2(prediction_features, model_fit)
            # Plot
            ts_plot, scatter_plot = plot_CO2_calibration(
                predicted, model_fit, orig_col="sensor_CO2", ref_col="ref_CO2")
            wp_path = b_pth.with_name(
                f'LP8_predictions_{id}_{serialnumber}.pdf')
            ts_plot.savefig(wp_path)
        case "calibrate",  (du.AvailableSensors.LP8 as st):
            # Iterate over calibration data
            av_t = get_averaging_time(st, True)
            try:
                cal_data_all_serial = get_cal_data(
                    engine, db_metadata, id, start, st, av_t)
            except ValueError as e:
                logger.info(
                    f"There is not data for sensor {id} between {start} and today")
                continue
            for (serialnumber, sensor_start, sensor_end), cal_data in cal_data_all_serial:
                logger.info(f"Processing sensor {serialnumber} of unit {id}")
                cal_data_clean = cleanup_data(cal_data)
                cal_features = prepare_LP8_features(
                    cal_data_clean.reset_index())
                cal, train = cal_cv_split(
                    cal_features, duration=5, mode=CalModes.CHAMBER, sensor=args.sensor_type)
                if len(cal) == 0 or len(train) == 0:
                    continue
                try:
                    cal_fit = LP8_CO2_calibration(cal)
                except ValueError:
                    continue

                cal_fit_nd = remove_dummies(cal_fit, 'time_dummy')
                # Predict
                co2_pred = predict_CO2(train, cal_fit_nd)
                # Predict the full series
                co2_pred_all = predict_CO2(cal_features, cal_fit_nd)
                # Fit RH model to find the sensors threshold
                fit_rh_threshold(co2_pred_all)
                #Plot and save
                ts_plot, scatter_plot = plot_CO2_calibration(
                    co2_pred, cal_fit, orig_col="sensor_CO2", extra_res=['ref_H2O'])
                wp_path = b_pth.with_name(f'LP8_{id}_{serialnumber}.pdf')
                wp_path_res = b_pth.with_name(
                    f'LP8_res_{id}_{serialnumber}.pdf')
                ts_plot.savefig(wp_path)
                scatter_plot.savefig(wp_path_res)
                # Persist
                # Persist parameters
                computed_when = dt.datetime.now()
                cal_obj = convert_calibration_parameters(
                    cal_fit, "CO2", args.sensor_type, sensor_start.to_pydatetime(), sensor_end.to_pydatetime(), computed_when, serialnumber)
                # Compute model statistics
                model_statistics = compute_quality_indices(co2_pred)
                # Persist fit and model statistics
                with Session() as session:
                    logger.info(
                        f"Persisting calibration parameters for sensor {id}")
                    session.add(cal_obj)
                    session.commit()
                    cal_performance = du.ModelFitPerformance(
                        model_id=cal_obj.id, **model_statistics)
                    session.add(cal_performance)
                    session.commit()

        case "calibrate", (du.AvailableSensors.HPP as st):
            # Durations of calibration
            start = dt.datetime.now() - dt.timedelta(days=15)
            end = dt.datetime.now()
            # Get averaging duration
            av_t = get_averaging_time(st, True)
            # Get data during bottle calibration
            cal_data = map_cylinder_concentration(
                get_HPP_data(engine, db_metadata, id, start, end, 30, only_cyl=True))
            # Compute calibration features
            cal_features = prepare_features(cal_data, fit=False)
            cal_features.to_csv(b_pth.with_name(f'HPP_{id}_two_point.csv'))
            # Apply calibration
            for (serialnumber, sensor_start, sensor_end), cal_data in cal_data_all_serial:
                logger.info(f"Processing sensor {serialnumber} of unit {id}")
                # Prepare features
                cal_data_clean = cleanup_data(cal_data)
                cal_feature = prepare_features(cal_data_clean.reset_index())
                # Split into training and testing data
                bottle_cal, field_cal = split_HPP_cal_data(cal_feature)
                cal_feature_train, cal_feature_test = cal_cv_split(field_cal)
                H2O_cal = H2O_calibration(cal_feature_train)
                co2_cal = HPP_CO2_calibration(cal_feature_train)
                # Remove dummies from model fit
                co2_cal_nd = remove_dummies(co2_cal, 'time_dummy')
                # Persist parameters
                computed_when = dt.datetime.now()
                cal_obj = convert_calibration_parameters(
                    co2_cal_nd, "CO2", args.sensor_type, sensor_start.to_pydatetime(), sensor_end.to_pydatetime(), computed_when, serialnumber)
                with Session() as session:
                    logger.info("Persisting calibration parameters")
                    session.add(cal_obj)
                    session.commit()
                # Predict and plot the prediction
                pred_df = predict_CO2(cal_feature_test, co2_cal_nd)
                pred_df_bottle = predict_CO2(bottle_cal, co2_cal_nd)
                # Plot prediction week by week (for the co-location)
                wp = pred_df.resample('W', on='date').apply(
                    lambda x: pd.Series({'plot': plot_CO2_calibration(x, co2_cal_nd)})).reset_index()
                wp_path = b_pth.with_name(f'HPP_{id}_{serialnumber}.pdf')
                wp_res_path = b_pth.with_name(
                    f'HPP_res_{id}_{serialnumber}.pdf')
                # Save calibration results plot
                save_multipage([u for u, w in wp['plot']], wp_path, wp['date'])
                # Save residuals
                save_multipage([w for u, w in wp['plot']],
                               wp_res_path, wp['date'])

                # Plot prediction for bottle times
                wp = pred_df_bottle.resample('W', on='date').apply(lambda x: pd.Series(
                    {'plot': plot_CO2_calibration(x, co2_cal_nd, ref_col='CO2_DRY_CYL_a')})).reset_index()
                wp_path = b_pth.with_name(
                    f'HPP_bottle_{id}_{serialnumber}.pdf')

                save_multipage([w for u, w in wp['plot']], wp_path, wp['date'])
                logger.debug(ref_dt)
        case "process", _:
            av_t = get_averaging_time(args.sensor_type, True)
            logger.info(f"Applying calibration for sensor {id}")
            si = get_sensor_info(engine, db_metadata, id, args.sensor_type)
            # Iterate over sensor units
            for row in si:
                1
                # get_sensor_data()

            # Dates
            end = dt.datetime.now()
            start = None if args.full else (end - dt.timedelta(days=30))
            pred_data = get_prediction_data(
                engine, db_metadata, id, args.sensor_type, start, end, av_t)
            pdb.set_trace()
            # Get sensor data
