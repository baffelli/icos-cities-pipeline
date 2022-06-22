"""
This module contains functions and classes to operate with CarboSense / ICOS-Cities measurement data
It includes classes to map calibration parameters to the DB using SQLalchemy (ORM) so that calibrations
can be persisted in a format that can be easily exchanged with other systems.
"""
from argparse import ArgumentError
import datetime as dt
import enum
import imp
import itertools
import pathlib as pl
import sqlite3 as sqllite
from ast import Str
from curses import meta
from dataclasses import dataclass, field
from enum import Enum
from statistics import correlation
from typing import (Callable, Dict, Iterable, List, Optional, Pattern, Set, TypeVar,
                    Union, Tuple)
from typing_extensions import Self

import influxdb
import numpy as np
import openpyxl as opx
import pandas as pd
import sqlalchemy as sqa
import statsmodels as sm
import statsmodels.formula.api as smf
import yaml
from matplotlib.pyplot import axis
from openpyxl.worksheet.worksheet import Cell
from pyparsing import col
from sqlalchemy import (Column, DateTime, Float, ForeignKey, Integer, String,
                        engine)
from sqlalchemy.orm import relationship
from statsmodels import tools as sttools
from statsmodels.regression import linear_model

from sensorutils import files as fu
from sensorutils import models as mods
from sensorutils import calc

import csv

"""
Represents the missing (or unset) time in ICOS-cities database
"""
NAT = pd.Timestamp('2100-01-01 00:00:00')

"""
Missing data
"""
ICOS_MISSING = -999


"""
Start date of carbosense and of ICOS-Cities
"""
CS_START = dt.datetime(2018, 1, 1)
ICOS_START = dt.datetime(2022, 1, 1)

class ClimateChamberStatusCode(enum.Enum):
    """
    Enum class to list possible error
    codes from the new Dübendorf calibration chamber
    """
    START:str = 'Start'
    STARTUP:str = 'Einlauf'
    ERROR:str = 'Messfehler'
    MEASURE:str = 'MESSUNG'
    

class AvailableSensors(enum.Enum):
    """
    Enum class to list all available
    sensor types for the calibration.
    This prevents the user to pass a wrong
    sensor type to the calibration method
    """
    HPP:str = "HPP"
    LP8:str = "LP8"
    PICARRO:str = "Picarro"
    Vaisala:str = "Vaisala"


@dataclass
class CalType(Enum):
    """
    Enumeration to define
    calibration type
    """
    ONE_POINT = 1
    TWO_POINT = 2
    OTHER = 3





def cells_to_df(cells: Iterable[Cell]) -> pd.DataFrame:
    return [(c.value, c.row, c.column) for c in cells]


def read_picarro_calibration_parameters(path: pl.Path) -> pd.DataFrame:
    """
    Reads the picarro CRD calibration parameters
    from an excel file (this is usually filled by the NABEL team)
    and returns a pandas dataframe with the parameters in a sensible format.
    It may be replaced  with another script if the NABEL team can export calibration
    events in a more useful format

    Parameters
    ----------
    path: Path or string
        The path of the file to read


    """
    # Import the first columns as an index

    colnames = {'Datum': 'date', 'Ort': 'location', 'Nr.': 'cylinder_id',
                'Compound': 'compound', 'Soll-Konz. [ppm]': 'target_concentration'}
    index_skip = 4
    cal_data = pd.read_excel(path, engine="openpyxl",  header=[
                             index_skip]).rename(columns=colnames)
    # Import device ids (second row of the data)
    device_ids = pd.read_excel(path, engine="openpyxl", nrows=3,  header=[3], usecols=range(
        5, 19, 2)).dropna(axis=0, how='all').iloc[0:1].to_dict(orient='records')[0]
    device_mapping = [k for k, v in device_ids.items()]
    # Find column index which is not na (this gives us the corresponding sensor ID)
    cal_data['device'] = cal_data.filter(regex='Ist*', axis=1).set_axis(
        device_mapping, axis=1, inplace=False).dropna(how='all').notna().idxmax(axis=1)
    # Fill column by backward filling
    cal_data['measured_concentration'] = cal_data.filter(
        regex='Ist*', axis=1).bfill(axis=1).iloc[:, 0].astype(float)
    # remove na and rename columns
    return cal_data.dropna(how='all')[list(colnames.values()) + ['measured_concentration', 'device']]


def read_picarro_parameters_new(path: pl.Path) -> pd.DataFrame:
    """
    Reads the picarro CRD calibration parameters
    from an excel file (this is usually filled by the NABEL team)
    and returns a pandas dataframe with the parameters in a sensible format. It is very similar
    to :obj:`read_picarro_calibration_parameters` but reads the file in a different (wide) format.
    An example of this file is found in `K:\\Nabel\\Daten\\Stationen\\Kalibrierwerte Picarro G1301 DUE Test.xlsx`
    """
    # TODO make reading of column names more flexible
    skip = 5
    # Manually define the columns (assuming the data does not change)
    cols = ['date', 'CO2-target_1', 'CO2-meas_1', 'CO2_dry-meas_1', 'CO2-target_2', 'CO2-meas_2',
            'CO2_dry-meas_2',  'CH4-target_1', 'CH4-meas_1', 'CH4-target_2', 'CH4-meas_2']
    data = pd.read_excel(path, engine="openpyxl",
                         header=None, names=cols, skiprows=skip)
    # Turn wide to long (using - as a separator)
    vv = ['CO2', 'CH4', 'CO2_dry']
    data_long = pd.wide_to_long(data.dropna(
        how='all'), stubnames=vv, i='date', j='type', sep='-', suffix='\\w+').reset_index()
    data_longer = pd.melt(data_long, id_vars=['date', 'type'], value_vars=vv)
    data_longer[['type', 'cylinder_id']
                ] = data_longer.type.str.split('_', expand=True)
    target_values = data_longer[data_longer.type.str.contains('target')].rename(
        columns={'value': 'target_concentration'}).drop('type', axis=1)
    measured_values = data_longer[data_longer.type.str.contains('meas')].rename(
        columns={'value': 'measured_concentration'}).drop('type',  axis=1)
    cal_data = target_values.set_index(['date', 'cylinder_id', 'variable']).join(measured_values.set_index(
        ['date', 'cylinder_id', 'variable'])).reset_index().rename(columns={'variable': 'compound'}).reset_index()
    return cal_data.dropna(subset=['measured_concentration', 'target_concentration'], how='all').drop('index', axis=1)


"""
Dictionary of file readers
"""
cal_readers = {'new': read_picarro_parameters_new,
               'old': read_picarro_calibration_parameters}


@dataclass
class CalibrationConfiguration():
    """
    Class to represent a device calibration
    configuration file

    Attributes
    ----------
    source_path: str
        The path where the calibration measurements are taken from (without the drive)
    drive: str
        The drive where the `source_path` file is stored. This is to make the function compatible with
        unix shares, where the *K:* and *G:* drives are mounted on `/mnt/{user}`. For `K:` use `K`
        for `G:` use G
    device: str or None
        The id of the device or None if the id is found in the file itself
    location: str or None
        The location of the device or None if the location is found in the file itself
    reader: Callable
        The function used to read the calibration parameters into a pandas dataframe
    """
    drive: str
    source_path: str
    device: Optional[str]
    location: Optional[str]
    reader: Callable

    def get_path(self) -> pl.Path:
        """
        Return the fully resolved path to the configuration file
        """
        dv = fu.get_drive(
            '') if self.drive == 'K' else fu.get_drive(self.drive)
        return pl.Path(dv, self.source_path)


def read_calibration_config(pt: pl.Path) -> List[CalibrationConfiguration]:
    """
    Reads a calibration configuration file (a yaml file) and returns a list of CalibrationConfiguration 
    objects.

    """
    with open(pt, 'r') as content:
        configs = yaml.load(content)
    for e in configs:
        e['reader'] = cal_readers[e['reader']]
    return [CalibrationConfiguration(**c) for c in configs]


def read_bottle_calibrations(pt: pl.Path) -> List[mods.CylinderAnalysis]:
    """
    Reads the calibration values from the bottle 
    calibration performed by Christoph Zellweger (GAW)
    and returns a list of :obj:`sensorutils.models.CylinderAnalysis` objects.
    """
    match pt.suffix:
        case '.xls' |  '.xlsx': 
            dt_in = pd.read_excel(pt, engine=None)
        case '.csv':
            dt_in = pd.read_csv(pt,  sep="[;,]", engine='python', 
                quoting=csv.QUOTE_NONE, doublequote=True, quotechar="'").rename(lambda x: x.replace('"',""), axis='columns')
        case _:
            raise NotImplementedError("Only CSV and Excel files can be read")
    names = {
        'cylinder': 'cylinder_id',
        'fillnr': 'fillnr',
        'p': 'pressure',
        'dtm2': 'analysed',
        'H2O': 'H2O',
        'H2O.sd': 'H2O_sd',
        'CO2': 'CO2',
        'CO2.sd': 'CO2_sd',
        'standard': 'standard'
    }
    dt_out = dt_in.rename(columns=names).applymap(lambda x: x.replace('"',"") if isinstance(x, str) else x)
    #Make a date for the fill number
    dt_out['analysed'] = pd.to_datetime(dt_out['analysed']) if 'analysed' in dt_out.columns else None
    dt_out['filled'] = pd.to_datetime(dt_out['fillnr'], format='%y%m%d', errors='coerce')
    entries = [
        mods.CylinderAnalysis(
            cylinder_id=i.cylinder_id, 
            pressure=calc.psi_to_bar(i.pressure), 
            analysed=i.analysed.to_pydatetime(), 
            fill_from=i.filled.to_pydatetime(), 
            CO2 = i.CO2,
            CO2_sd = i.CO2_sd,
            H2O = i.H2O,
            H2O_sd = i.H2O_sd,
            fillnr = i.fillnr,
            fill_to= NAT.to_pydatetime()) for i in dt_out[~dt_out['standard']].reset_index().itertuples()
        ]
    return entries
    

def apply_calibration_parameters(table:sqa.Table, session:sqa.orm.Session, compound:str, target_compound:str, zero:float, span:float, valid_from:dt.datetime, valid_to:dt.datetime, temp:bool=True) -> None:
    """
    Apply two point calibration parameters to the table specified by the `table` parameters. The calibration is applied to the column
    with the name `compound`, producing (corresponds to SQL column updating!) the column `target_compound`

    Parameters
    ----------
    table:  sqlalchemy.Table
        A sqlalchemy Table object to which the calibration should be applied
    session:  sqlachemy.orm.Session
        The sqlalchemy session where the processing is applied
    compound: str
        The name of the column where the calibration should be applied
    zero: float
        The intercept of the calibration
    span: float
        The slope of the calibration
    valid_from: datetime
        The calibration validity start
    valid_to: datetime
        The calibration validity end
    temp: boolean
        If set to true, the update is rolled back after application
    """
    # Generate the update expression
    ud = {target_compound: table.columns[compound]  * span + zero}
    ic = 'timestamp'
    cs = [compound, target_compound]

    od = session.query(table).filter(table.columns.timestamp.between(valid_from.timestamp(), valid_to.timestamp()))
    orig_data = pd.read_sql(od.statement, session)

    us = table.update().values(**ud).where(table.columns.timestamp.between(valid_from.timestamp(), valid_to.timestamp()))
    res = us.execute()
    modif_data = pd.read_sql(od.statement, session)
    print(orig_data[cs] - modif_data[cs])
    if temp:
        session.rollback()



def average_df(dt: pd.DataFrame, funs:Dict[str, Callable]={'max':np.max, 'min':np.min, 'mean':np.mean, 'std':np.std}, date_col:str='date', groups:Optional[List[str]]=[], av:str='1 min') -> pd.DataFrame:
    """
    Averages a dataframe to the given time interval and applies
    a set of aggregation functions specified by the dictionary `funs` to all columns in the dataframe.
    If the new sampling interval is smaller than the older one, the data is forward filled before.
    """
    cols = set(dt.columns) - set([date_col])
    fns_name = {col: [(name, fn) for name, fn in funs.items()] for  col in cols if col not in groups}
    dt_ix = dt.set_index([date_col])
    if groups:
        dt_grp = dt_ix.groupby(groups)
    else:
        dt_grp = dt_ix
    #TODO fix when new numpy is released that can aggregate a resampled groupby
    # dt_agg = [dt_grp.resample(av).agg(fn) for fn in funs.items()]
    # new_names = ["_".join(c) for c in dt_agg.columns]
    # dt_agg.columns = new_names
    dt_agg = pd.concat([dt_grp.resample(av).agg(fn).add_suffix(f"_{nm}") for nm,fn in funs.items()], axis=1)
    return dt_agg

def date_to_timestamp(dt: pd.DataFrame, dt_col:str, target_name:str='timestamp') -> pd.DataFrame:
    """
    Create a new dataframe where the date column specified in `dt_col`
    is replaced by a unix timestamp column with the name "timestamp"
    """
    dt[dt_col] = pd.to_datetime(dt[dt_col]).apply(lambda x: x.timestamp())
    return dt.rename(columns={dt_col:target_name})


def day_range(moment: dt.datetime) -> Tuple[dt.datetime, dt.datetime]:
    start = moment.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + dt.timedelta(1)
    return (start, end)



def influxql_results_to_df(res: influxdb.client.ResultSet) -> pd.DataFrame:
    """
    Converts an :obj:`influxdb.client.ResultSet` into a pandas.DataFrame
    """
    return pd.DataFrame([r for r in res.get_points()])

def reshape_influxql_results(res: pd.DataFrame, keys:List[str], names:str, value:str) -> pd.DataFrame:
    """
    Reshapes a :obj:`pandas.DataFrame` of results from an influxql
    query into a wider object using the given keys
    """
    return pd.pivot_table(res, index=keys, columns=names, values=value).reset_index()

def prepare_df_for_influxdb(data: pd.DataFrame, keys=['time', 'node']) -> Dict:
    """
    Prepares a dataframe to be stored in influxdb by converting
    from long to wide and adding information on the series
    """


def map_climate_chamber_status_code(ec: ClimateChamberStatusCode) -> int:
    match ec:
        case ClimateChamberStatusCode.STARTUP:
            value = 0
        case ClimateChamberStatusCode.START:
            value = 0
        case ClimateChamberStatusCode.ERROR:
            value = 0
        case ClimateChamberStatusCode.MEASURE:
            value = 1
    return value


def replace_inf(dt: pd.DataFrame) -> pd.DataFrame:
    """
    Remove all inf values from the DataFrame and replaces
    them with None
    """
    return dt.replace([np.inf, -np.inf, np.nan], None)