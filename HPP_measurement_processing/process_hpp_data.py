"""
This script processes the LP8/ Picarro data to produce calibration data
respectively calibrated data. The type of sensors respectively the processing mode
are choosen with a command line arguments, while the calibration model and the tables
are specified using a configuration file
"""
import sqlite3

from requests import session
import sensorutils.db as db_utils
import sensorutils.files as fu
import sensorutils.data as du
import sensorutils.calc as calc
from sensorutils.log import logger
import pathlib as pl
import pandas as pd
import sqlalchemy as sqa
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
import datetime as dt
import argparse as ap
from typing import Dict, List, NamedTuple, Tuple
import numpy as np

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
        (st.columns[id_col] == sensor_id) &
        (st.columns[time_col].between(start.timestamp(), end.timestamp()))
    ).group_by(*[ac, id_c])
    with eng.connect() as c:
        res = pd.read_sql(qr.compile(), c)
    return res


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


def get_cal_data(eng: sqa.engine.Engine,
                 md: sqa.MetaData,
                 sensor_id: int,
                 sensor_type:
                 str,
                 agg_duration:
                 int,
                 ref_agg: Dict[str, str],
                 sens_agg: Dict[str, str]) -> List[Tuple[Tuple[str, dt.datetime, dt.datetime], pd.DataFrame]]:

    ref_dt = get_calibration_periods(eng, md, sensor_id,  sensor_type)

    def get_and_combine(tw: CalDataRow) -> pd.DataFrame:
        sensor_data = get_sensor_data(
            eng, md, sensor_id, map_sensor_to_table(sensor_type), tw.cal_start, tw.cal_end, sens_agg, agg_duration)
        # Import reference data
        picarro_data = get_ref_data(eng, md, tw.location,
                                    "picarro_data", tw.cal_start, tw.cal_end, ref_agg, agg_duration)
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
#Create ORM session
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
hpp_agg = {
    'sensor_CO2': 'AVG(NULLIF(senseair_hpp_co2_filtered, -999))',
    'sensor_pressure': 'AVG(NULLIF(senseair_hpp_pressure_filtered, -999))',
    'sensor_RH': 'AVG(NULLIF(sensirion_sht21_humidity, -999))',
    'sensor_t': 'AVG(NULLIF(sensirion_sht21_temperature, -999))',
    'sensor_ir_lpl': 'AVG(NULLIF(senseair_hpp_lpl_signal, -999))',
    'sensor_ir': 'AVG(NULLIF(senseair_hpp_ir_signal, -999))',
    'sensor_detector_t': 'AVG(NULLIF(senseair_hpp_temperature_detector, -999))',
    'sensor_mcu_t': 'AVG(NULLIF(senseair_hpp_temperature_mcu, -999))',
    'sensor_calibration_a': 'MAX(calibration_a)',
    'sensor_calibration_b': 'MAX(calibration_b)',
}
picarro_agg = {
    'ref_CO2_DRY': 'AVG(IF(CO2_DRY = -999 OR CO2_DRY_F = 0, NULL, CO2_DRY))',
    'ref_T': 'AVG(NULLIF(T, -999))',
    'ref_RH': 'AVG(NULLIF(RH, -999))',
    'ref_pressure': 'AVG(NULLIF(pressure * 100, -999))',
    'ref_H2O': 'AVG(IF(H2O = -999 OR H2O_F = 0, NULL, H2O))',
}


def prepare_features(dt: pd.DataFrame, ir_col:str="sensor_ir_lpl", fit: bool = True, plt: str = '1d') -> pd.DataFrame:
    """
    Prepare features for CO2 calibration / predictions
    """
    dt_new = dt.copy()
    import pdb; pdb.set_trace()
    dt_new['date'] = pd.to_datetime(dt_new['time'], unit='s')
    dt_new["sensor_ir_log"] = - np.log(dt_new[ir_col])
    dt_new["sensor_ir_inverse"] = 1/(dt_new[ir_col])
    dt_new["sensor_ir_interaction_t"] = dt_new[f"sensor_detector_t"] * dt_new[ir_col]
    dt_new["sensor_ir_interaction_inverse_pressure"] = dt_new['sensor_pressure']/(dt_new[ir_col])
    dt_new['sensor_t_abs'] = calc.absolute_temperature(dt_new['sensor_t'])
    #Water content
    dt_new['sensor_H2O'] = calc.rh_to_molar_mixing(
        dt_new['sensor_RH'], dt_new['sensor_t_abs'], dt_new['sensor_pressure'] / 100)
    #Referenced CO2
    nc_sens = (dt_new['sensor_pressure']) / calc.P0 
    dt_new['sensor_CO2_comp'] = dt["sensor_CO2"] * nc_sens
    # Dummy variable every `plt` days
    import pdb; pdb.set_trace()
    dt_new['time_dummy'] = dt_new['date'].dt.round(plt).dt.date.astype('str')
    # Additional features needed for fitting
    if fit:
        dt_new['ref_t_abs'] = calc.absolute_temperature(dt_new['ref_T'])
        # Normalisation constant
        nc = (dt_new['ref_pressure']) / calc.P0 
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


def CO2_calibration(dt_in: pd.DataFrame, reg:List[str]) -> sm.regression.linear_model.RegressionModel:
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
    import pdb; pdb.set_trace()
    # Drop any missing regressors
    valid = ~(dt_dm[reg_col+tg_col].isna().any(axis=1))
    dt_valid = dt_dm[valid]
    cm = sm.OLS(dt_valid[tg_col], dt_valid[reg_col])
    return cm.fit()

def HPP_CO2_calibration(dt_in:pd.DataFrame, lpl:bool=True):
    """
    Computes the HPP CO2 calibration with the model
    proposed by Michael Müller
    Using the `lpl` flag you can use either the long path or
    the short path IR value
    """
    reg =  [
        "sensor_ir_log","sensor_ir_inverse",
        "sensor_detector_t","sensor_t_abs","sensor_ir_interaction_inverse_pressure",'sensor_H2O']
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


def plot_CO2_calibration(dt: pd.DataFrame) -> plt.Figure:
    """
    Plot detailed CO2 calibration results:
    - Time series of sensor and reference
    - Scatter plot
    """
    ref_col = "ref_CO2"
    pred_col = "CO2_pred"
    orig_col = "sensor_CO2_comp"
    # Compute residuals statistics
    residuals = dt[ref_col] - dt[pred_col]
    res_sd = np.sqrt(residuals.var())
    res_bias = residuals.mean()
    res_cor = dt[ref_col].corr(dt[pred_col])
    # Make annotation
    ann = AnchoredText(
        f"RMSE: {res_sd:5.2f} ppm\n, Bias: {res_bias:5.2f} ppm,\n corr: {res_cor:5.2f}", loc=3)
    cs = plt.cm.get_cmap('Set2')
    # Create plot
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
    return fig


def cleanup_data(dt_in: pd.DataFrame) -> pd.DataFrame:
    """
    Preliminary filter of data:
    - replacing of -999 with Nan
    - Removing negative concentrations
    """
    dt = dt_in.copy()
    dt = dt.replace([None, -999], [0, np.NaN])
    con = dt['ref_CO2_DRY'].le(0) | dt['ref_CO2_DRY'].isna()
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


def cal_cv_split(dt_in: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Splits the data in a training and testing dataset
    The default mode uses:
    - Data in the climate chamber + 2 week before and after
    - Data outside for testing
    Fallback if no chamber:c
    - First two months of data for calibration
    """
    is_chamber = dt_in['cal_mode'].isin([2, 3])
    chamber_start = dt_in[is_chamber].date.min() - dt.timedelta(days=14)
    chamber_end = dt_in[is_chamber].date.max() + dt.timedelta(days=14)
    train: pd.Series = dt_in.date.between(chamber_start, chamber_end)
    split = train if train.any() else dt_in.date.le(
        (dt_in.date.min() + dt.timedelta(days=60)))
    return dt_in[split], dt_in[~split]


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
                                device: str) -> du.CalibrationParameters:
    """
    Convert the fit results into a :obj:`sensorutils.data.CalibrationParameters`
    object that can be persisted in the database in a human-readable format
    thank to SQLAlchemy
    """
    pl = [du.CalibrationParameter(parameter=n, value=v)
          for n, v in cp.params.items()]
    return du.CalibrationParameters(parameters = pl, species=species, type=type, valid_from=valid_from, valid_to=valid_to, device=device)


for id in ids_to_process:
    logger.debug(f"Processing sensor id {id}")
    logger.debug(f"Getting reference data")
    ref_dt = get_calibration_periods(
        engine, db_metadata, id,  args.sensor_type)
    # Iterate over calibration data
    cal_data_all_serial = get_cal_data(
        engine, db_metadata, id, args.sensor_type, 600, picarro_agg, hpp_agg)
    for (serialnumber, sensor_start, sensor_end), cal_data in cal_data_all_serial:
        logger.info(f"Processing sensor {serialnumber} of unit {id}")
        #Prepare features
        cal_data_clean = cleanup_data(cal_data)
        cal_feature = prepare_features(cal_data_clean.reset_index())
        #Split into training and testing data
        bottle_cal, field_cal = split_HPP_cal_data(cal_feature)
        cal_feature_train, cal_feature_test = cal_cv_split(field_cal)

        H2O_cal = H2O_calibration(cal_feature_train)
        co2_cal = HPP_CO2_calibration(cal_feature_train)
        # Remove dummies from model fit
        co2_cal_nd = remove_dummies(co2_cal, 'time_dummy')
        # Persist parameters
        cal_obj = convert_calibration_parameters(co2_cal_nd, "CO2", "HPP", sensor_start, sensor_end, serialnumber)
        with Session() as session:
            logger.info("Persisting calibration parameters")
            session.add(cal_obj)
            session.commit()
        #Predict and plot the prediction
        pred_df = predict_CO2(cal_feature_test, co2_cal_nd)
        pred_df_bottle = predict_CO2(bottle_cal, co2_cal_nd)
        # Plot prediction week by week
        wp = pred_df.resample('W', on='date').apply(
            lambda x: pd.Series({'plot': plot_CO2_calibration(x)})).reset_index()
        b_pth: pl.Path = args.plot
        wp_path = b_pth.with_name(f'HPP_{id}_{serialnumber}.pdf')
        save_multipage(wp['plot'], wp_path, wp['date'])
        import pdb
        pdb.set_trace()
        logger.debug(ref_dt)
