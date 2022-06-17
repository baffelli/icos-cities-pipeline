"""
This script processes the LP8/ Picarro data to produce calibration data
respectively calibrated data. The type of sensors respectively the processing mode
are choosen with a command line arguments, while the calibration model and the tables
are specified using a configuration file
"""



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


import matplotlib.pyplot as plt
import matplotlib as mpl

from matplotlib.backends.backend_pdf import PdfPages

import copy as cp
import statsmodels.api as sm

import re

from itertools import groupby

from sklearn.linear_model import QuantileRegressor

import dataclasses as dc


from statsmodels.regression.rolling import RollingOLS


# Utility functions



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
    """
    Adds dummy columns from the factor column `dummy_name`
    """
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


def LP8_CO2_calibration(dt_in: pd.DataFrame, regs: List[str], target: str = 'ref_CO2_comp', dummy: bool = False) -> sm.regression.linear_model.RegressionResultsWrapper:
    """
    Linear regression model for LP8 calibration.
    The names of `reg` correspond to the values of the column `parameter` in the
    `model_parameters` table. How the features are computed is found in :obj:`prepare_LP8_features`.
    """
    dummies = "time_dummy" if dummy else None
    return CO2_calibration(dt_in, regs, target=target, dummy=dummies)


def predict_CO2(dt_in: pd.DataFrame, model: sm.regression.linear_model.RegressionResultsWrapper) -> pd.DataFrame:
    """
    Apply the model in `model` to
    predict the CO2 concentration
    """
    dt_out = dt_in.copy()
    dt_out["CO2_pred"] = model.predict(dt_out[model.model.exog_names]) 
    dt_out["CO2_pred_comp"] = dt_out["CO2_pred"] * 1/dt_out["nc_sens"]

    return dt_out


def cleanup_data(dt_in: pd.DataFrame, fit: bool = True, rh_threshold: float = 80) -> pd.DataFrame:
    """
    Preliminary filter of data:
    - replacing of -999 with NaN
    - Removing negative concentrations
    """
    dt = dt_in.copy()
    dt = dt.replace([None, du.ICOS_MISSING], [0, np.NaN])
    # Condition
    con = (dt['sensor_ir'].isnull() |
           dt['sensor_t'].isna()) & dt['sensor_RH'] > rh_threshold
    if fit:
        con = con & (dt['ref_CO2_DRY'].le(0) | dt['ref_CO2_DRY'].isna() | np.abs(
            dt['ref_CO2_DRY_SD']) > 4)
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





def split_stable_conditions(dt_in: pd.DataFrame, col: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """
    Takes a dataframe and a boolean series of the same length
    and returns two sets of indices, *validation* and *training* each of which begin at the point of
    change in the series and contain half of this period
    """


def cal_cv_split(dt_in: pd.DataFrame,
                 sensor: du.AvailableSensors,
                 mode: cal.CalModes = cal.CalModes.MIXED,
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
        case cal.CalModes.COLLOCATION, _:
            cal_period = dt_in['cal_mode'].isin(mode.value)
            cal_start = dt_in[cal_period][date_col].min()
            cal_end = dt_in[cal_period][date_col].max() + \
                dt.timedelta(days=duration)
        case cal.CalModes.CHAMBER:
            cal_period = dt_in['cal_mode'].isin(mode.value)
            cal_start = dt_in[cal_period][date_col].min()
            cal_end = cal_start + \
                dt.timedelta(days=duration)
        case cal.CalModes.MIXED:
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
            chamber_mode = (dt_in['chamber_mode'] == 0)
            # Find periods of change
            dt_copy['mode_change'] = ((dt_in['chamber_status'] != dt_in['chamber_status'].shift(
                -1)) & (dt_in['chamber_status'] != 'MEASURE')).cumsum()
            # For each "plateau", take 1/4 of the sample for training and half for testing
            plateaus = dt_copy.groupby("mode_change").apply(
                lambda x:  x.reset_index().index.to_series() < len(x) * 0.25).reset_index()[0]
            # Find periods where RH does not vary
            fit = (train & stable_cols & plateaus) | (train & plateaus)
            test = (train & stable_cols & ~plateaus) | (train & ~plateaus)
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





def get_averaging_time(sensor: du.AvailableSensors, fit: bool = True) -> int:
    match sensor, fit:
        case du.AvailableSensors.HPP, True:
            av_sec = 60
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
    elif when:
        final_qr = qr.filter((mods.CalibrationParameters.valid_from <= when) & (
            mods.CalibrationParameters.valid_to >= when)).order_by(mods.CalibrationParameters.computed.desc()).first()
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
    train_data = data_in[(data_in['chamber_mode'] == '1')
                         & (data_in['chamber_status'] == 'MEASURE')]
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
    fig = mpl.figure.Figure()
    ax = fig.add_subplot(211)
    ax.scatter(dt['ref_H2O'], dt['sensor_RH'])



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
parser.add_argument('--backfill', type=int, default=60,
                    help="Number of days to backfill")
parser.add_argument('--end', type=dt.datetime.fromisoformat, default=dt.datetime.now(),
                    help="Date of end")
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
start = du.ICOS_START if args.full else dt.datetime.now() - dt.timedelta(days=args.backfill)
end = dt.datetime.now() if not args.end else args.end
backfill_days = (end - start).days
for current_id in ids_to_process:
    id = current_id.value
    logger.debug(f"Processing sensor id {id}")
    logger.debug(f"Getting reference data")
    match args.mode, args.sensor_type:
        case "process", (du.AvailableSensors.LP8 as st):
            av_t = get_averaging_time(st, fit=False)
            # List files missing in destination (only process them)
            missing_dates = data_mapping.list_files_missing_in_dest(
                group=dict(sensor_id=id), all=args.full, backfill=backfill_days)
            logger.info(f"The missing dates for {id} are {missing_dates}")
            # Get calibration parameters
            wp_path = b_pth.with_name(f'LP8_predictions_{id}.pdf')
            pdf = PdfPages(wp_path, 'a')
            # Iterate over missing dates
            for date in missing_dates:
                # Find serial number for this date
                date_start, date_end = du.day_range(date)
                serialnumber = db_utils.get_serialnumber(
                    engine, id, st.value, date_start, date_end)
                # #Prepare plot
                logger.info(f"Getting raw data for {id} on {date}")
                # Get data
                with Session() as ses:
                    pm = deserialize_parameters(
                        ses, serialnumber, when=date_start)
                    if pm is None:
                        logger.info(
                            f"No calibration parameters for unit {id} with serialnumber {serialnumber} on {date}")
                        continue
                    # Remove the dummy if the model has any
                    model_fit = remove_dummies(
                        pm.to_statsmodel(), "time_dummy")
                    cal_sets = [d.data for d in cal.get_cal_ts(
                        ses, id, st, date_start, date_end, av_t, dep=True) if d]
                    if not cal_sets:
                        continue
                    else:
                        cal_data = pd.concat(cal_sets)
                    # Prepare regressors
                    cal_data_clean = cleanup_data(cal_data, fit=False)
                    prediction_features = cal.prepare_LP8_features(
                        cal_data_clean, fit=True)
                    # Predict
                    ref_col = 'ref_CO2_comp'
                    pred_col = 'CO2_pred'
                    orig_col = 'sensor_CO2'
                    predicted = predict_CO2(prediction_features, model_fit)
                    l2_dt = cal.prepare_level2_data(predicted, pm.id, pred_col=pred_col)
                    cal.persist_level2_data(ses, l2_dt)
                    #Check prediction quality
                    if 'ref_CO2' in predicted.columns:
                        pred_quality = cal.compute_quality_indices(predicted, ref_col=ref_col, fit=False)
                        pred_quality_st = dc.replace(pred_quality, **{'date': date.date(), 'model_id': pm.id, 'sensor_id':id})
                        ses.merge(pred_quality_st)
                    ses.commit()
                # Plot
                ts_plot = cal.plot_CO2_calibration(
                    predicted, orig_col=orig_col, ref_col=ref_col)
                ts_plot.suptitle(f"LP8 {id}, {date} at {cal_data.location.unique()}")
                pdf.savefig(ts_plot)

            pdf.close()
            # Persist

        case "calibrate",  (du.AvailableSensors.LP8 as st):
            # Define regressors and target columns 
            tg_col = 'ref_CO2_comp'
            orig_col = 'sensor_CO2'
            pred_col = 'CO2_pred'
            #These columns are generated in cal.prepare_LP8_features in the calibration module
            reg = ["sensor_ir_log", "const", "sensor_t", "sensor_t_sq", "sensor_t_cub",
                   "sensor_ir_interaction_t", "sensor_ir_interaction_t_sq",
                   "sensor_ir_interaction_t_cub", "pressure_interpolation"]
            # Get averaging time for calibration mode
            av_t = get_averaging_time(st, fit=True)
            # Get galibration data
            logger.info(f"Getting calibration data for {id}")
            try:
                with Session() as ses:
                    cal_data_all_serial = cal.get_cal_ts(
                        ses, id, st, start, dt.datetime.now(), av_t * 2, modes=cal.CalModes.CHAMBER, dep=True)
            except ValueError as e:
                logger.info(
                    f"There is not measurement data for sensor {id} between {start} and {dt.datetime.now()}")
                continue
            for cd in cal_data_all_serial:
                logger.info(f"Processing sensor {cd.serial} of unit {id}")
                cal_data_clean = cleanup_data(cd.data)
                cal_features_unclean = cal.prepare_LP8_features(cal_data_clean)
                cal_features = cal.filter_LP8_features(cal_features_unclean)
                dur = (cal_features['date'].max() - cal_features['date'].min())
                try:
                    cal_df, test_df = cal_cv_split(
                        cal_features.reset_index(), duration=dur.days, mode=cal.CalModes.CHAMBER, sensor=args.sensor_type)
                    cal_fit = LP8_CO2_calibration(
                        cal_df, reg, target=tg_col, dummy=False)
                    cal_df.to_csv( b_pth.with_name(f'LP8_features_{id}.pdf'))
                except (ValueError, DataError) as E:
                    logger.exception(E)
                    logger.info("No valid data for {id}")
                    continue
                cal_fit_nd = remove_dummies(cal_fit, 'time_dummy')
                # Predict
                co2_pred = predict_CO2(test_df, cal_fit_nd)
                # Predict the full series
                co2_pred_all = predict_CO2(cal_features, cal_fit_nd)
                # Fit RH model to find the sensors threshold
                #TODO determine an indvididual RH threshold for each sensor
                # fit_rh_threshold(co2_pred_all)
                #Plot full timeseries
                ts_plot = cal.plot_CO2_calibration(co2_pred, pred_col=pred_col, orig_col=orig_col, ref_col=tg_col)
                base_name = f"{st.name}_{id}_{cd.serial}_{cd.start:%Y%m%d}_{cd.end:%Y%m%d}"
                wp_path = b_pth.with_name(f'{base_name}.pdf')
                ts_plot.savefig(wp_path)
                # Persist parameters
                computed_when = dt.datetime.now()
                cal_obj = cal.convert_calibration_parameters(
                    cal_fit, "CO2", args.sensor_type, cd.data.cal_end.max(), cd.next, computed_when, cd.serial)
                # Compute model statistics
                model_statistics = cal.compute_quality_indices(
                    co2_pred, ref_col=tg_col, pred_col=pred_col)
                # Persist fit and model statistics
                with Session() as session:
                    if not model_statistics.valid():
                        logger.info(
                            f"The fitted model for {cd.serial} is not valid: ")
                        continue
                    logger.info(
                        f"Persisting calibration parameters for sensor {id} and serialnumber {cd.serial}")
                    session.add(cal_obj)
                    session.commit()
                    # Set model id
                    model_statistics.model_id = cal_obj.id
                    session.add(model_statistics)
                    session.commit()

        case "calibrate", (du.AvailableSensors.HPP as st):    
            # Get averaging duration
            av_t = get_averaging_time(st, True)
            # Get data during bottle calibration
            with Session() as session:
                cal_data = cal.get_HPP_calibration_data(session, id, start, end, av_t)
                if cal_data.empty:
                    logger.info(f"No calibration data for {id} between {start} and {end} ")
                    continue
            # Compute calibration features
            target = ['cyl_CO2']
            features = ['sensor_CO2']
            cal_features = cal.prepare_HPP_features(cal_data, fit=True)
            cal_features_filtered = cal.filter_cal(cal_features, endog=features, exog=target)
            if not cal_features_filtered.empty:
                #Calibration window
                interval = 2
                #Call bottle calibration
                logger.info(f"Computing two point calibration for {id}")
                cal_fit = cal.HPP_two_point_calibration(cal_features_filtered, target, features, window = interval)
                #Predict
                cal_params  = [a for a,c in cal_fit]
                cal_pred = cal.apply_HPP_calibration(cal_features, cal_params)
                #Plot (one plot by day)
                figs = cal_pred.set_index('date').groupby(pd.Grouper(freq='1d')).apply(lambda x: pd.Series({'plot':cal.plot_HPP_calibration(x.reset_index())})).reset_index()
                wp_path = b_pth.with_name(f'HPP_bottle_cal_{id}_.pdf')
                #Create titles
                titles = figs.apply(lambda x: f"{x.date}, cycle: {x.date}", axis=1).tolist()
                save_multipage(figs['plot'].tolist(), wp_path,  titles)
                with Session() as session:
                    logger.info(f"Storing calibration parameters for {id} in the database")
                    for el, qual in cal_fit:
                        if el:
                            el_ad = cal.update_hpp_calibration(session, el)
                            qual.model_id = el_ad.id
                            session.add(qual)
                            session.commit()
            else:
                logger.info(f"No valid calibrations for {id} between {start} and {end}")
                continue
        case "process", (du.AvailableSensors.HPP as st):
            wp_path = b_pth.with_name(f'HPP_predictions_{id}.pdf')
            pdf = PdfPages(wp_path, 'a')
            av_t = get_averaging_time(st, True)
            #List missing dates
            missing_dates = data_mapping.list_files_missing_in_dest(
            group=dict(sensor_id=id), all=args.full,  backfill=args.backfill)
            #Iterate over dates
            for current_date in missing_dates:
                day_start, day_end = du.day_range(current_date)
                serialnumber = db_utils.get_serialnumber(engine, id, st.value, day_start, day_end)
                with Session() as session:
                    cp = cal.get_HPP_calibration(session, serialnumber, current_date)
                    if not cp:
                        logger.info(f"No calibration for HPP {id} on {current_date}, skipping")
                        continue
                    cal_data_all = cal.get_cal_ts(session, id, st, day_start, day_end, av_t, dep=True)
                    cal_sets = [d.data for d in cal_data_all if d]
                    if not cal_sets:
                        continue
                    else:
                        cal_data = pd.concat(cal_sets)
                        cal_features =  cal.prepare_HPP_features(cal_data, fit=False)
                        cal_pred = cal.apply_HPP_calibration(cal_features, [cp])
                        l2_dt = cal.prepare_level2_data(cal_pred, cp.id)
                        cal.persist_level2_data(session, l2_dt)
                        session.commit()
                        #Plot
                        #Filter the first minute afte the beginnig of a measurement cycle before plotting
                        min_elapsed = 240
                        cal_pred_plot = cal_pred[(cal_pred['inlet'] == '') & (cal_pred.normal_elapsed > min_elapsed)]
                        ts_plot = cal.plot_CO2_calibration(cal_pred_plot, orig_col="sensor_CO2", ref_col="ref_CO2")
                        loc = cal_pred.location.unique()
                        ts_plot.suptitle(f"HPP {id} on {current_date} at {loc}")
                        pdf.savefig(ts_plot)
                        logger.info(f"Storing processed data for {id} on {current_date}")
                        if 'ref_CO2' in cal_pred.columns:
                            pred_quality = cal.compute_quality_indices(cal_pred, ref_col="ref_CO2", fit=False)
                            pred_quality_st = dc.replace(pred_quality, **{'date': current_date.date(), 'model_id': cp.id, 'sensor_id':id})
                            session.merge(pred_quality_st)
                            session.commit()
            pdf.close()