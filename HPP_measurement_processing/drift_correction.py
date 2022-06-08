"""
Compute a bias adjustment of LP8 sensors in two different ways
depending on where they are deployed:
- If they are co-located with HPP/Picarro, just computes the average difference.
- If they are located somewhere else, compare the percentiles as done for BEACO2N 
(https://amt.copernicus.org/preprints/amt-2021-120/amt-2021-120.pdf)
"""

from calendar import c
from typing import Dict, List, NamedTuple, Tuple, Callable, Optional, Union
import numpy as np
import argparse as ap
import pathlib as pl
import sensorutils.db as db_utils
import sensorutils.files as fu
import sensorutils.data as du
import sensorutils.calc as calc
import sensorutils.models as mods
import sensorutils.calibration as cal
from sensorutils.log import logger

import datetime as dt

from sqlalchemy.orm import sessionmaker, Session, Query

import pandas as pd

import matplotlib.pyplot as plt

from statsmodels.regression.linear_model import OLS, OLSResults
from statsmodels.tools import add_constant

import matplotlib as mpl

from matplotlib.backends.backend_pdf import PdfPages


# Setup parser
parser = ap.ArgumentParser(description="Drift correction of SenseAir LP8 Data")
parser.add_argument(
    'sensor_type', type=du.AvailableSensors, help="Sensor type")
parser.add_argument('--plot', type=pl.Path, help='Path to save plots')
parser.add_argument('id', type=int, help="If passed, only process one sensor")
parser.add_argument('--full', default=False, action='store_true',
                    help="Partial processing or full data (partial: last 60 days, full: from beginning of measurements)")
args = parser.parse_args()

start = du.CS_START if args.full else dt.datetime.now() - dt.timedelta(days=60)
end = dt.datetime.now()
backfill_days = (end - start).days

# Connect to metadata/measurement database
logger.info('Connecting to the DB')
engine = db_utils.connect_to_metadata_db()
# Create ORM session
Session = sessionmaker(engine)



def prepare_data(in_data: pd.DataFrame, quant:float=0.1, duration:str = '21d', fit:bool=True) -> pd.DataFrame:
    """
    Inner function to generate the parameters used for the LP8 drift
    correction
    """
    def elapsed(s: pd.Series) -> pd.Series:
        vl = pd.to_datetime(s, unit='s')
        return (vl - vl.min()).dt.days
    out_data = in_data.copy().set_index('date')
    if fit:
        drift_data_offset = in_data.set_index('date')[['CO2', 'temperature', 'ref_DUE_CO2']].rolling(duration).apply(lambda x: x.quantile(quant))
        drift_data_offset['offset'] =  drift_data_offset['ref_DUE_CO2'] - drift_data_offset['CO2'] 
        out_data['offset'] = drift_data_offset['offset']
        out_data['temperature_quantile'] = drift_data_offset['temperature']
    out_data['temperature_sq'] = out_data['temperature']**2
    out_data['location_group'] = (out_data['location'].shift(-1) != out_data['location']).cumsum()
    out_data['location_elapsed'] =  out_data.groupby('location_group')['timestamp'].transform(elapsed)
    return out_data

def offset_regression(in_data: pd.DataFrame, target:str = 'offset', regs: List[str] = ['location_elapsed'] ) -> OLSResults:
    """
    Fits a regression model to estimate the time-dependent sensor drift
    """
    valid = in_data.dropna(subset= regs + [target])
    breakpoint()
    return OLS(valid[target],  add_constant(valid[regs])).fit()

def predict_offset(in_data: pd.DataFrame, model: OLSResults) -> pd.DataFrame:
    """
    Given a fitted drift model, predict the offset for every entry in the dataframe
    `in_data`
    """
    out_data = in_data.copy()
    cols = [c for c in model.params.keys() if c in in_data.columns]
    out_data['CO2_drift_corrected'] = out_data['CO2'] +  model.predict(add_constant(in_data[cols]))
    return out_data

def plot_drift_correction(dt: pd.DataFrame ) -> plt.figure:
    """
    Plot the drift correction results as timeseries
    """
    fig = mpl.figure.Figure()
    ax = fig.add_subplot(211)
    ax.plot(dt['date'], dt['CO2'], label='Original', color=cal.get_line_color(cal.measurementType.ORIG))
    ax.plot(dt['date'], dt['ref_CO2'], label='Reference (Co-located)', color=cal.get_line_color(cal.measurementType.REF))
    ax.plot(dt['date'], dt['ref_DUE_CO2'], label='Reference (LAEG)', ls='--', color=cal.get_line_color(cal.measurementType.REF))
    ax.plot(dt['date'], dt['CO2_drift_corrected'], label='Corrected', color=cal.get_line_color(cal.measurementType.CAL))
    ax.legend(loc='upper left')
    ax.set_ylim([300, 600])
    ax.tick_params(axis='x', labelrotation=90)
    ax = fig.add_subplot(212)
    ax.scatter(dt['ref_CO2'], dt['CO2'], label='Original', c=cal.get_line_color(cal.measurementType.ORIG))
    ax.scatter(dt['ref_CO2'], dt['CO2_drift_corrected'], label='Corrected', c=cal.get_line_color(cal.measurementType.CAL))
    ax.legend(loc='upper left')
    ax.set_ylim([300, 600])
    ax.set_xlim([300, 600])
    st = dict(slope=1., color='lightgray', lw=0.5)
    ax.axline((0, 0), **st)
    ax.axline((0, -3), **st)
    ax.axline((0, 3), **st)
    return fig



with Session() as session:
    drift_data = cal.get_drift_correction_data(session, args.id, start, end)

breakpoint()
drift_data_offset = prepare_data(drift_data.set_index('date').groupby('location').resample('1h').mean().reset_index())

offset_fits = offset_regression(drift_data_offset)
data_corrected = predict_offset(prepare_data(drift_data, fit=False), offset_fits)


plt_path = args.plot.with_name(f"drift_correction_{args.id}.pdf")
with PdfPages(plt_path) as pdf:
    data_corrected.resample('7d').apply(lambda x: pdf.savefig(plot_drift_correction(x.reset_index())))








