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
import sqlalchemy

from sqlalchemy.orm import sessionmaker, Session, Query

import pandas as pd

import matplotlib.pyplot as plt

from statsmodels.regression.linear_model import OLS, OLSResults
from statsmodels.tools import add_constant

import matplotlib as mpl

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.gridspec as gridspec



# Setup parser
parser = ap.ArgumentParser(description="Drift correction of SenseAir LP8 Data")
parser.add_argument(
    'sensor_type', type=du.AvailableSensors, help="Sensor type")
parser.add_argument('--plot', type=pl.Path, help='Path to save plots')
parser.add_argument('id', type=int, help="If passed, only process one sensor")
parser.add_argument('--full', default=False, action='store_true',
                    help="Partial processing or full data (partial: last 60 days or specified with --backfill, full: from beginning of measurements)")
parser.add_argument('--backfill', type=int, default=60,
                    help="Number of days to backfill")
args = parser.parse_args()

#Get last dazs
start = du.CS_START if args.full else dt.datetime.now() - dt.timedelta(days=args.backfill)
end = dt.datetime.now()
backfill_days = (end - start).days

# Connect to metadata/measurement database
logger.info('Connecting to the DB')
engine = db_utils.connect_to_metadata_db()
md = sqlalchemy.MetaData(bind=engine)
md.reflect()
# Create ORM session
Session = sessionmaker(engine)

def pivot_drift_data(in_data: pd.DataFrame, ref_loc=['LAEG', 'DUE1']) -> pd.DataFrame:
    """
    Makes the table of data wider by removining the repeated entries
    due to the several reference locations
    """
    #Get unique sensor data
    sens_keys = ['timestamp', 'sensor_id', 'location']
    sens_data = in_data.drop_duplicates(subset=sens_keys)[sens_keys + ['CO2', 'temperature', 'pressure_interp', 'calibration_model_id',  'relative_humidity', 'pressure', 'inlet']]
    #Unpivot the reference data
    ref_keys = ['timestamp', 'ref_location']
    ref_data = in_data.drop_duplicates(subset=ref_keys)[ref_keys + ['ref_loc_CO2', 'ref_loc_pressure', 'ref_h']]
    ref_data_wide = ref_data.pivot_table(index='timestamp', columns='ref_location').reset_index().set_index('timestamp')
    ref_data_wide.columns = ref_data_wide.columns.to_flat_index()
    #Join and add date column
    out_data = sens_data.set_index('timestamp').join(ref_data_wide)
    out_data['date'] = pd.to_datetime(out_data.index, unit='s').values
    return out_data

def prepare_data(in_data: pd.DataFrame, quant:float=0.1, duration:str = '11d', fit:bool=True, master_stn='DUE1', ref_stn='LAEG') -> pd.DataFrame:
    """
    Inner function to generate the parameters used for the LP8 drift
    correction
    """
    #Function to fit elapsed time at a given location
    def elapsed(s: pd.Series) -> pd.Series:
        return (s - s.min()).dt.days
    out_data = in_data.copy()
    #Pressure interpolation
    if fit:
        drift_data_offset = out_data.set_index('date').rolling(duration)[[('ref_loc_CO2', ref_stn), ('ref_loc_CO2', master_stn), ('CO2'), ('temperature')]].apply(lambda x: x.quantile(quant))
        drift_data_offset['offset'] =  drift_data_offset[('ref_loc_CO2', ref_stn)] - drift_data_offset[('CO2')] 
        out_data['offset'] = drift_data_offset.reset_index()['offset']
        for stn in [master_stn, ref_stn]:
            out_data[('ref_loc_CO2_quantile', stn)] = drift_data_offset.reset_index()[('ref_loc_CO2', stn)]
        out_data['temperature_quantile'] = drift_data_offset.reset_index()['temperature']
    out_data['temperature_sq'] = out_data['temperature']**2
    out_data['hour'] = out_data['date'].dt.hour
    out_data['location_elapsed'] =  out_data.reset_index().groupby('location')['date'].transform(elapsed)
    return out_data

def fit_drift(in_data: pd.DataFrame, target:Union[str, Tuple[str,str]] = 'offset', regs: List[str] = ['location_elapsed', 'temperature', 'temperature_sq'] ) -> mods.CalibrationParameters:
    """
    Fits a regression model to estimate the time-dependent sensor drift
    """
    valid = in_data.dropna(subset= regs + [target])
    fit = OLS(valid[target],  add_constant(valid[regs])).fit()
    pms = [mods.CalibrationParameter(parameter=n, value=v) for n, v in fit.params.items()]
    #FIXME get the correct serialnumber as part of the query
    sensor_id = str(int(in_data.sensor_id.unique()[0]))
    return mods.CalibrationParameters(species="CO2", 
    valid_from = in_data.date.min(), 
    valid_to = in_data.date.max(), 
    computed = dt.datetime.now(), 
    device = sensor_id, parameters = pms)

def predict_drift(in_data: pd.DataFrame, model: OLSResults, two_point:bool=False) -> pd.DataFrame:
    """
    Given a fitted drift model, predict the offset for every entry in the dataframe
    `in_data`
    """
    out_data = in_data.copy()
    cols = [c for c in model.params.keys() if c in in_data.columns]
    if two_point:
        out_data['CO2_drift_corrected'] = model.predict(add_constant(in_data[cols]))
    else:
        out_data['CO2_drift_corrected'] = out_data['CO2'] + model.predict(add_constant(in_data[cols]))
    return out_data


def compute_diurnal_cycle(data: pd.DataFrame, date_column:str = 'date') -> pd.DataFrame:
    """
    Computes the diurnal cycle as the average of the data frame in `data` grouped by the
    hour in the column `date_column`
    """
    return data.groupby(data[date_column].dt.hour).agg(np.mean)

def plot_drift_correction(dt: pd.DataFrame ) -> plt.figure:
    """
    Plot the drift correction results as timeseries
    """
    fig = mpl.figure.Figure()
    gs = gridspec.GridSpec(nrows=2, ncols=2)
    ax = fig.add_subplot(gs[0, :])
    ax.plot(dt['date'], dt['CO2'], label='Original', color=cal.get_line_color(cal.measurementType.ORIG))
    ax.plot(dt['date'], dt[('ref_loc_CO2', 'DUE1')], label='Reference (Co-located)', color=cal.get_line_color(cal.measurementType.REF))
    ax.plot(dt['date'], dt[('ref_loc_CO2', 'LAEG')], label='Reference (LAEG)', ls='--', color=cal.get_line_color(cal.measurementType.REF))
    ax.plot(dt['date'], dt['CO2_drift_corrected'], label='Corrected', color=cal.get_line_color(cal.measurementType.CAL))
    ax.legend(loc='upper left')
    lms = [300, 600]
    ax.set_ylim(lms)
    ax.tick_params(axis='x', labelrotation=90)
    #Compute diurnal cycle
    ax1 = fig.add_subplot(gs[1, 0])
    dt_dc = compute_diurnal_cycle(dt)
    ax1.plot(dt_dc.index, dt_dc['CO2'], label='Original', color=cal.get_line_color(cal.measurementType.ORIG))
    ax1.plot(dt_dc.index, dt_dc[('ref_loc_CO2', 'DUE1')], label='Reference (Co-located)', color=cal.get_line_color(cal.measurementType.REF))
    ax1.plot(dt_dc.index, dt_dc[('ref_loc_CO2', 'LAEG')], label='Reference (LAEG)', ls='--', color=cal.get_line_color(cal.measurementType.REF))

    ax1.plot(dt_dc.index, dt_dc['CO2_drift_corrected'], label='Corrected', color=cal.get_line_color(cal.measurementType.CAL))
    ax1.set_ylim(lms)
    #Plot CO2 vs temp
    ax2 = fig.add_subplot(gs[1, 1])
    ax2.scatter(dt['temperature'], dt['CO2'], label='Original', color=cal.get_line_color(cal.measurementType.ORIG))
    ax2.scatter(dt['temperature'], dt['CO2_drift_corrected'], label='Original', color=cal.get_line_color(cal.measurementType.CAL))
    ax2.set_ylim(lms)
    ax2.set_xlim([-10,40])
    return fig


def apply_drift_correction(data: pd.DataFrame, fitted: mods.CalibrationParameters, date_col = 'date', two_point: bool = False) -> pd.DataFrame:
    """
    Given a :obj:`pandas.DataFrame` and a fitted model in the form of :obj:`sensorutils.models.CalibrationParameters`, applies
    the model `fitted` to `data` only keeping the portion of the data within the validity period of the model fit.
    """
    valid = data[date_col].between(fitted.valid_from, fitted.valid_to)
    data_valid = data[valid]
    return predict_drift(data_valid, fitted.to_statsmodel(), two_point)


#Reference stations
refs = ['LAEG', 'DUE1']
#Sampling time for quantiles
group_freq = '1w'
resample_duration = '1h'


with Session() as session:
    drift_data = pivot_drift_data(cal.get_drift_correction_data(session, args.id, start, end, ref_loc=refs), ref_loc=refs)


#Apply correction for each deployment
for loc, local_offset_data in drift_data.groupby('location'):
    #Decide the type of calibration depending on where the sensor was deployed
    #If the sensor was collocated with a picarro sensor, compute a two point 
    #regression versus that reference, otherwise only fit
    #the offset (computed as the difference between reference and sensor value over a sliding window using `ref_stn` location as reference value)
    if loc in refs:
        target = ('ref_loc_CO2', loc)
        regressors = ['CO2', 'temperature']
        two_point = True
    else:
        target = 'offset'
        regressors = ['location_elapsed', 'temperature']
        two_point = False
    #Prepare data
    drift_data_offset = prepare_data(local_offset_data.set_index('date').groupby('location').resample(resample_duration).mean().reset_index(), master_stn=refs[0], ref_stn=refs[1])
    
    offset_fits = drift_data_offset.set_index('date').groupby(pd.Grouper(freq=group_freq)).apply(lambda x: fit_drift(x.reset_index(), target=target, regs=regressors))
    prediction_data = prepare_data(local_offset_data.reset_index(), fit=False, master_stn=refs[0], ref_stn=refs[1]).set_index('date')
    #Iterate over the fits and apply the correction
    data_corrected = pd.concat([apply_drift_correction(prediction_data.reset_index(), x, two_point=two_point) for x in offset_fits.tolist()])

plt_path = args.plot.with_name(f"drift_correction_{args.id}.pdf")
with PdfPages(plt_path) as pdf:
    data_corrected.set_index('date').groupby(pd.Grouper(freq='7d')).apply(lambda x: pdf.savefig(plot_drift_correction(x.reset_index())))


#Combine columns
data_corrected['pressure'] = data_corrected['pressure'] .combine_first(data_corrected['pressure_interp'] )

#Store data
with Session() as ses:
    #Create an upsert method
    upsert = db_utils.create_upsert_metod(md)
    #Rename columns
    names = {
        'timestamp' : 'timestamp',
        'sensor_id' : 'sensor_id',
        'location' : 'location',
        'CO2' : 'CO2',
        'temperature' : 'temperature',
        'relative_humidity' : 'relative_humidity',
        'pressure_final': 'pressure',
        'CO2_drift_corrected': "CO2_drift_corrected",
        'inlet': 'inlet'
    }
    data_final = data_corrected.rename(columns=names)[names.values()]
    data_final.to_sql(mods.Level3Data.__tablename__, ses.connection() , method=upsert, index=False, if_exists='append')
    ses.commit()
