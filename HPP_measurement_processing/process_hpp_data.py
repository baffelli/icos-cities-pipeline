"""
This script processes the LP8/ Picarro data to produce calibration data
respectively calibrated data. The type of sensors respectively the processing mode
are choosen with a command line arguments, while the calibration model and the tables
are specified using a configuration file
"""

import sqlite3
import enum
from pymysql import DataError
import sensorutils.db as db_utils
import sensorutils.files as fu
import sensorutils.data as du
import sensorutils.calc as calc
from sensorutils.log import logger
import pathlib as pl
import pandas as pd
import sqlalchemy as sqa
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker, Session
import datetime as dt
import argparse as ap
from typing import Dict, List, NamedTuple, Tuple, Callable, Optional
import numpy as np
import math

import matplotlib.pyplot as plt
import matplotlib as mpl

from matplotlib.backends.backend_pdf import PdfPages

import copy as cp
import statsmodels.api as sm
from mpl_toolkits.axes_grid.anchored_artists import AnchoredText


# Utility functions

def map_sensor_to_table(sensor: str):
    """
    Given a sensor type, returns the table name
    containing the sensor data
    """
    tm = {'HPP': 'hpp_data', 'LP8': 'lp8_data'}
    return tm[sensor]


def make_aggregation_column(table: sqa.Table, column: str, avg_time: int) -> sqa.sql.elements.Label:
    """
    Given a table and a column name,
    return an expression to use the column as aggregation time
    with the duration `avg`time
    """
    return (func.floor(table.columns[column] / avg_time) * avg_time).label(column)


def make_aggregate_columns(table: sqa.table, aggregations: Dict[str, str]) -> List[sqa.sql.expression.ColumnClause]:
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
        id_col: str = "SensorUnit_ID",
        cyl: bool = True,
        cyl_tb: str = "RefGasCylinder",
        cyl_dep_tb: str = "RefGasCylinder_Deployment") -> pd.DataFrame:
    """
    Gets the data for a given sensor id and a duration period.
    Optionally returns the reference cylinder data
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
    ).group_by(*[ac, id_c])
    if cyl:
        qr_ct = qr.cte("agg")
        # If asked for, also add the cylinder values
        cd = get_cylinder_data(eng, md, sensor_id).cte("cyl")
        cyl_col = [cd.c["CO2_DRY_CYL"], cd.c["cylinder_id"], cd.c['inlet']]
        qr_cyl = sqa.select(*qr_ct.c, *cyl_col).join(cd,
                                                (qr_ct.c['SensorUnit_ID'] == cd.c['sensor_id']) &
                                                (qr_ct.c[time_col].between(cd.c['cylinder_start'],  cd.c['cylinder_end'])), isouter=True)
    else:
        qr_cyl = qr
    with eng.connect() as c:
        res = pd.read_sql(qr_cyl.compile(), c)
    # Pivot cylinder data (if any)
    return pivot_cylinder_data(res) if cyl else res


def pivot_cylinder_data(dt: pd.DataFrame, 
                        index_col: List[str] = ['time'],
                        cyl_col: List[str] = ['CO2_DRY_CYL'], 
                        cyl_index_col:List[str]=['cylinder_id'], 
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
    dt_cyl = pd.pivot_table(dt, index=index_col, columns=inlet_col, values=cyl_col)
    dt_cyl_out = dt_cyl.copy().reset_index().set_index(index_col)
    dt_cyl_out.columns = [f"{a}_{b}" for a,b in dt_cyl.columns]
    #Join
    dt_wide = (dt_sens.join(dt_cyl_out , on=index_col)) if not dt_cyl_out.empty else dt_sens
    assert len(dt_wide) == len(dt.reset_index())
    return dt_wide.reset_index()


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
                func.curdate())).label("cylinder_end"),
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


class CalDataRow(NamedTuple):
    """
    Class to represent a row in the calibration table
    """
    sensor_id: int
    serial_number: str
    location: str
    cal_start: dt.datetime
    cal_end: dt.datetime
    cal_mode: int
    sensor_start: dt.datetime
    sensor_end: dt.datetime


def get_calibration_periods(
    eng: sqa.engine.Engine,
    md: sqa.MetaData,
    sensor_id: int,
    sensor_type: str,
    sensor_table: str = 'Sensors',
    cal_table: str = "Calibration",
    id_col: str = "SensorUnit_ID",
    serial_col: str = "Serialnumber",
    start_col: str = "Date_UTC_from",
    end_col: str = "Date_UTC_to",
    loc_col: str = "LocationName",
    mode_col: str = "CalMode",
    sensor_type_col: str = "Type"
) -> List[CalDataRow]:
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
    # Get table references
    cal_tab = md.tables[cal_table]
    sens_tab = md.tables[sensor_table]
    # Define columns to get
    cols = [
        cal_tab.c[id_col].label("sensor_id"),
        cal_tab.c[loc_col].label("location"),
        sens_tab.c[serial_col].label("serial_number"),
        sens_tab.c[start_col].label('sensor_start'),
        sens_tab.c[end_col].label('sensor_end'),
        cal_tab.c[start_col].label('cal_start'),
        (func.least(
            cal_tab.c[end_col], func.curdate())).label('cal_end'),
        cal_tab.c[mode_col].label("cal_mode")
    ]
    # Query calibration table
    ct = sqa.select(*cols).select_from(cal_tab, sens_tab).join(
        sens_tab, (sens_tab.c[id_col] == cal_tab.c[id_col]) &
        (sens_tab.c[start_col] < cal_tab.c[start_col]) &
        (sens_tab.c[end_col] > cal_tab.c[end_col])
    ).where((cal_tab.c[id_col] == sensor_id) & (sens_tab.c[sensor_type_col] == sensor_type))
    # Execute query
    ct_com = ct.compile()
    logger.debug(f"The query is:\n {ct_com}")
    with eng.connect() as con:
        res = [CalDataRow(**rw)for rw in con.execute(ct_com)]
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
    'ref_H2O': 'AVG(IF(H2O = -999 OR H2O_F = 0, NULL, H2O))',}
    picarro_data = get_ref_data(eng, md,location, table , start, end, picarro_agg, agg_duration)
    return picarro_data

def get_HPP_data(
                engine: sqa.engine.Engine, 
                md: sqa.MetaData, 
                id: int,
                start: dt.datetime,
                end: dt.datetime,
                agg_duration:int,
                table:str = 'hpp_data'):
    """
    Get the data
    for a senseair HPP instrument for the selected
    duration
    """
    hpp_agg = {
        'sensor_CO2': 'AVG(NULLIF(senseair_hpp_co2_filtered, -999))',
        'sensor_pressure': 'AVG(NULLIF(senseair_hpp_pressure_filtered, -999))',
        'sensor_RH': 'AVG(NULLIF(sensirion_sht21_humidity, -999))',
        'sensor_t': 'AVG(NULLIF(sensirion_sht21_temperature, -999))',
        'sensor_ir_lpl': 'AVG(NULLIF(senseair_hpp_lpl_signal, -999))',
        'sensor_ir': 'AVG(NULLIF(senseair_hpp_ir_signal, -999))',
        ' ': 'AVG(NULLIF(senseair_hpp_temperature_detector, -999))',
        'sensor_mcu_t': 'AVG(NULLIF(senseair_hpp_temperature_mcu, -999))',
        'sensor_calibration_a': 'MAX(COALESCE(calibration_a, 0))',
        'sensor_calibration_b': 'MAX(COALESCE(calibration_b, 0))',
        }
    sensor_data = get_sensor_data(engine, md, id, table, start, end, hpp_agg, agg_duration, cyl=True)
    return sensor_data

def get_LP8_data(
                engine: sqa.engine.Engine, 
                md: sqa.MetaData, 
                id: int,
                start: dt.datetime,
                end: dt.datetime,
                agg_duration:int,
                table:str = 'lp8_data'):
    """
    Get the data
    for a senseair LP8 instrument for the selected
    duration and aggregation duration
    """
    lp8_agg = {
        'sensor_CO2': 'AVG(NULLIF(senseair_lp8_co2_filtered, -999))',
        'sensor_RH': 'AVG(NULLIF(sensirion_sht21_humidity, -999))',
        'sensor_t': 'AVG(NULLIF(sensirion_sht21_temperature, -999))',
        'sensor_ir': 'AVG(NULLIF(senseair_lp8_ir, -999))',
        'sensor_detector_t': 'AVG(NULLIF(senseair_lp8_temperature, -999))',
        }
    sensor_data = get_sensor_data(engine, md, id, table, start, end, lp8_agg, agg_duration, cyl=True)
    return sensor_data


def get_cal_data(eng: sqa.engine.Engine,
                 md: sqa.MetaData,
                 sensor_id: int,
                 sensor_type:
                 str,
                 agg_duration:
                 int) -> List[Tuple[Tuple[str, dt.datetime, dt.datetime], pd.DataFrame]]:
    #Get the calibration periods
    ref_dt = get_calibration_periods(eng, md, sensor_id,  sensor_type)
    #Select getter
    match sensor_type:
        case 'HPP':
            getter = get_HPP_data
        case 'LP8':
            getter = get_LP8_data
    def get_and_combine(tw: CalDataRow) -> pd.DataFrame:
        sensor_data = getter(
            eng, md, sensor_id, tw.cal_start, tw.cal_end, agg_duration)
        # Import reference data
        picarro_data = get_picarro_data(eng, md, tw.location, tw.cal_start, tw.cal_end, agg_duration)
        # Join data
        cal_data = sensor_data.set_index('time').join(
            picarro_data.set_index('timestamp'))
        cal_data['cal_mode'] = tw.cal_mode
        cal_data['cal_start'] = tw.cal_start
        cal_data['cal_end'] = tw.cal_end
        cal_data['sensor_start'] = tw.sensor_start
        cal_data['sensor_end'] = tw.sensor_end
        cal_data['cal_end'] = tw.cal_end
        cal_data['serial_number'] = tw.serial_number
        return cal_data
    # Iterate over calibration data
    pd_con = pd.concat([get_and_combine(tw) for tw in ref_dt])
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
    nc_sens = (dt_new['sensor_pressure']) / calc.P0 * dt_new['sensor_t_abs'] / calc.T0
    dt_new['sensor_CO2_comp'] = dt["sensor_CO2"] * nc_sens
    # Dummy variable every `plt` days
    dt_new['time_dummy'] = dt_new['date'].dt.round(plt).dt.date.astype('str')
    # Additional features needed for fitting
    if fit:
        dt_new['ref_t_abs'] = calc.absolute_temperature(dt_new['ref_T'])
        # Normalisation constant
        nc = (dt_new['ref_pressure']) / calc.P0 * dt_new['ref_t_abs'] / calc.T0
        # Compute normalised concentrations using ideal gas law
        dt_new['sensor_H2O_comp'] = dt_new['sensor_H2O'] * nc
        dt_new['ref_CO2'] = calc.dry_to_wet_molar_mixing(
            dt_new['ref_CO2_DRY'], dt_new['ref_H2O']) * nc
        dt_new['ref_H2O_comp'] = dt_new['ref_H2O'] * nc

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


def CO2_calibration(dt_in: pd.DataFrame, reg: List[str]) -> sm.regression.linear_model.RegressionModel:
    """
    Fit the HPP CO2 calibration

    Parameters
    ----------
    dt_in: pandas.DataFrame
        A dataframe with features and outcome variable for the regression
    """
    dt = dt_in.copy()
    dt_dm, dm_cols = add_dummies(dt, 'time_dummy')
    tg_col = ['ref_CO2']
    reg_col = reg + dm_cols
    # Drop any missing regressors
    valid = ~(dt_dm[reg_col+tg_col].isna().any(axis=1))
    dt_valid = dt_dm[valid]
    cm = sm.OLS(dt_valid[tg_col], dt_valid[reg_col])
    return cm.fit()


def HPP_CO2_calibration(dt_in: pd.DataFrame, lpl: bool = True):
    """
    Computes the HPP CO2 calibration with the model
    proposed by Michael Müller
    Using the `lpl` flag you can use either the long path or
    the short path IR value
    """
    reg = [
        "sensor_ir_log", "sensor_ir_inverse", "sensor_detector_t", "sensor_ir_interaction_inverse_pressure", 'sensor_H2O']
    return CO2_calibration(dt_in, reg)


def predict_CO2(dt_in: pd.DataFrame, model: sm.regression.linear_model.RegressionResultsWrapper) -> pd.DataFrame:
    """
    Apply the model in `model` to
    predict the CO2 concentration
    """
    dt_out = sm.add_constant(dt_in.copy(),
                             has_constant='add', prepend=False)
    dt_out["CO2_pred"] = model.predict(
        dt_out[model.model.exog_names])

    return dt_out


def plot_CO2_calibration(dt: pd.DataFrame, 
                        model: sm.regression.linear_model.RegressionResults,
                        ref_col='ref_CO2', 
                        pred_col='CO2_pred',
                        orig_col="sensor_CO2_comp") -> Tuple[plt.Figure, plt.Figure]:
    """
    Plot detailed CO2 calibration results:
    - Time series of sensor and reference
    - Scatter plot
    - Residuals of regressors
    """
    # Compute residuals statistics
    residuals = dt[ref_col] - dt[pred_col]
    res_sd = np.sqrt(residuals.var())
    res_bias = residuals.mean()
    res_cor = dt[ref_col].corr(dt[pred_col])
    # Make annotation
    ann = AnchoredText(
        f"RMSE: {res_sd:5.2f} ppm\n, Bias: {res_bias:5.2f} ppm,\n corr: {res_cor:5.2f}", loc=3)
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
    lms = [0, 1000]
    ax.set_xlim(lms)
    ax.set_ylim(lms)
    # Add annotation
    ax.add_artist(ann)
    ax.legend()
    # Plot timeseries
    ts_ax = fig.add_subplot(212)
    ts_ax.scatter(dt['date'].dt.to_pydatetime(), dt[orig_col],
                  0.4, color=cs.colors[1], label='Uncalibrated')
    ts_ax.scatter(dt['date'].dt.to_pydatetime(), dt[pred_col],
                  0.4, color=cs.colors[0], label='Calibrated')
    ts_ax.scatter(dt['date'].dt.to_pydatetime(), dt[ref_col],
                  0.4, color=cs.colors[2], label='Reference')
    ts_ax.set_xlabel('Date')
    ts_ax.set_ylabel('CO2 mixing ratio [ppm]')
    ts_ax.legend()
    ts_ax.tick_params(axis='x', labelrotation=45)
    #Plot residuals vs various parameters
    fig2:mpl.Figure = mpl.figure.Figure()
    reg_names = [k for k,n in model.params.items()]
    nr = math.ceil(math.log2(len(reg_names)))
    for i, par_name in enumerate(reg_names, start=1):
        current_ax = fig2.add_subplot(nr, nr, i)
        current_ax.scatter(dt[par_name], residuals, 0.4)
        current_ax.set_xlabel(f"Feature: {par_name}")
        current_ax.set_ylabel('Residual [ppm]')
        current_ax.set_ylim(-30, 30)
    fig2.tight_layout()
    fig.tight_layout()
    return fig, fig2


def cleanup_data(dt_in: pd.DataFrame) -> pd.DataFrame:
    """
    Preliminary filter of data:
    - replacing of -999 with Nan
    - Removing negative concentrations
    """
    dt = dt_in.copy()
    dt = dt.replace([None, -999], [0, np.NaN])
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
    CHAMBER = 'CHAMBER'
    COLLOCATION = 'COLLOCATION'
    MIXED = 'MIXED'



def cal_cv_split(dt_in: pd.DataFrame,
                 mode: CalModes = CalModes.MIXED,
                 duration: int = 7,
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
    match mode:
        case CalModes.COLLOCATION:
            cal_period = dt_in['cal_mode'].isin([1])
            cal_start = dt_in[cal_period][date_col].min()
            cal_end = dt_in[cal_period][date_col].max() + \
                dt.timedelta(days=duration)
        case CalModes.CHAMBER:
            cal_period = dt_in['cal_mode'].isin([2, 3])
            cal_start = dt_in[cal_period][date_col].min()
            cal_end = dt_in[cal_period][date_col].max() + \
                dt.timedelta(days=duration)
        case CalModes.MIXED:
            cal_period = dt_in['cal_mode'].isin([2, 3])
            cal_start = dt_in[cal_period][date_col].min()
            cal_end = dt_in[cal_period][date_col].max()
    if not cal_period.any():
        raise DataError(
            f"There are no values for the calibration type {mode} in this dataset")
    cal_start = dt_in[cal_period][date_col].min()
    cal_end = dt_in[cal_period][date_col].max() + dt.timedelta(days=duration)
    train: pd.Series = dt_in[date_col].between(cal_start, cal_end)
    return dt_in[train], dt_in[~train]


def remove_dummies(mod_fit: sm.regression.linear_model.OLSResults, dummy_prefix: str) -> sm.regression.linear_model.OLSResults:
    """
    Remove dummy variables from model fit
    By removing all variables starting with `dummy_prefix`
    """
    new_names = [n for n in mod_fit.params.keys(
    ) if not dummy_prefix in n] + ['const']
    new_pars = pd.Series(
        {k: v for k, v in mod_fit.params.items() if k in new_names})
    # Add intercept
    new_pars['const'] = mod_fit.params[-1]
    mod_fit_cp = cp.copy(mod_fit.model)
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
                                   type: str,
                                   valid_from: dt.datetime,
                                   valid_to: dt.datetime,
                                   computed: dt.datetime,
                                   device: str) -> du.CalibrationParameters:
    """
    Convert the fit results into a :obj:`sensorutils.data.CalibrationParameters`
    object that can be persisted in the database in a human-readable format
    thank to SQLAlchemy
    """
    pl = [du.CalibrationParameter(parameter=n, value=v)
          for n, v in cp.params.items()]
    return du.CalibrationParameters(parameters=pl,
                                    species=species,
                                    type=type, valid_from=valid_from,
                                    valid_to=valid_to,
                                    computed=computed, device=device)

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
db_metadata = sqa.MetaData(bind=engine)
db_metadata.reflect()
# Create ORM session
Session = sessionmaker(engine)

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


# Expression for HPP averaging






for id in ids_to_process:
    logger.debug(f"Processing sensor id {id}")
    logger.debug(f"Getting reference data")
    match args.mode:
        case "calibrate":
            ref_dt = get_calibration_periods(
                engine, db_metadata, id,  args.sensor_type)
            # Iterate over calibration data
            cal_data_all_serial = get_cal_data(engine, db_metadata, id, args.sensor_type, 600)
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
                    co2_cal_nd, "CO2", args.sensor_type, sensor_start, sensor_end, computed_when, serialnumber)
                with Session() as session:
                    logger.info("Persisting calibration parameters")
                    session.add(cal_obj)
                    session.commit()
                # Predict and plot the prediction
                pred_df = predict_CO2(cal_feature_test, co2_cal_nd)
                pred_df_bottle = predict_CO2(bottle_cal, co2_cal_nd)
                # Plot prediction week by week (for the collocation)
                wp = pred_df.resample('W', on='date').apply(
                    lambda x: pd.Series({'plot': plot_CO2_calibration(x, co2_cal_nd)})).reset_index()
                b_pth: pl.Path = args.plot
                wp_path = b_pth.with_name(f'HPP_{id}_{serialnumber}.pdf')
                wp_res_path = b_pth.with_name(f'HPP_res_{id}_{serialnumber}.pdf')
                #Save calibration results plot
                save_multipage([u for u,w in wp['plot']], wp_path, wp['date'])
                #Save residuals 
                save_multipage([w for u,w in wp['plot']], wp_res_path, wp['date'])
                import pdb; pdb.set_trace()
                # Plot prediction for bottle times
                wp = pred_df_bottle.resample('W', on='date').apply(lambda x: pd.Series({'plot': plot_CO2_calibration(x,ref_col='CO2_DRY_CYL_a')})).reset_index()
                wp_path = b_pth.with_name(f'HPP_bottle_{id}_{serialnumber}.pdf')
                save_multipage(wp['plot'], wp_path, wp['date'])

                logger.debug(ref_dt)
