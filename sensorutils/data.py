"""
This module contains functions and classes to operate with CarboSense / ICOS-Cities measurement data
It includes classes to map calibration parameters to the DB using SQLalchemy (ORM) so that calibrations
can be persisted in a format that can be easily exchanged with other systems.
"""
from curses import meta
import enum
import imp
from typing import Callable, Iterable, Union, List, Optional, Pattern, Dict, Set
from dataclasses import dataclass, field
import influxdb
from matplotlib.pyplot import axis
from pyparsing import col
from . import files as fu
import yaml
import pandas as pd
import datetime as dt
import pathlib as pl
import statsmodels.formula.api as smf
import openpyxl as opx
from openpyxl.worksheet.worksheet import Cell
from statsmodels.regression import linear_model
from statsmodels import tools as sttools

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Float
from sqlalchemy import DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import engine 
import sqlalchemy as sqa

import sqlite3 as sqllite

from enum import Enum

import yaml

import numpy as np

import itertools

"""
Represents the missing (or unset) time in ICOS-cities database
"""
NAT = pd.Timestamp('2100-01-01 00:00:00')


@dataclass
class CalType(Enum):
    """
    Enumeration to define
    calibration type
    """
    ONE_POINT = 1
    TWO_POINT = 2
    OTHER = 3


Base = declarative_base()


@dataclass
class CalibrationParameter(Base):
    """
    Represents a single component (parameter)
    of a linear calibration model for the model with id `model_id` .
    This classed is used in combination with SQLalchemy to keep the calibration
    parameters synchronised in the db

    Attributes
    ----------
    id: int
        The unique id of the parameter
    model_id: int
        The id of the model
    parameter: str
        The name of the parameter (= feature) for the coefficient
    value: float
        The value of the coefficient
    """
    __tablename__ = "model_parameter"
    __sa_dataclass_metadata_key__ = "sa"
    # id: int = field(init=False, metadata={"sa": Column(Integer, primary_key=True)})
    # #model_id: int = field(init=False, metadata={"sa": Column(ForeignKey("calibration_parameters.id"))})
    # parameter: str = field(init=False, metadata={"sa": Column(String(64), primary_key=True)})
    # value: float = field(init=False, metadata={"sa": Column(Float())})
    id: int = Column(Integer, primary_key=True)
    model_id: int = Column(ForeignKey("calibration_parameters.id"))
    parameter: str = Column(String(64))
    value: float = Column(Float())


@dataclass
class CalibrationParameters(Base):
    """
    Simple class to represent calibration parameters
    for a given species and instrument.
    By using SQLalchemy the instances of this class are syncronised with 
    the database table with the name `calibration_parameters`
    Attributes
    ----------
    id: int
        The id of the calibration model
    type: CalType
        The type of calibration
    species: str
        The species to be calibrated
    valid_from: date
        The start of the validity range of this calibration
    valid_to:  date
        The end of the validity range of this calibration
    device: str
        The device under calibration
    parameters: list of CalibrationParameter objects
        A list of parameters, one for each component in the model
    """
    __tablename__ = "calibration_parameters"
    __sa_dataclass_metadata_key__ = "sa"
    id: int = Column(Integer, primary_key=True)
    type: str = Column(String(64))
    species: str = Column(String(64))
    valid_from: dt.datetime = Column(DateTime())
    valid_to: dt.datetime = Column(DateTime())
    device: str = Column(String(64))
    parameters: List[CalibrationParameter] = relationship(
        "CalibrationParameter")

    def serialise(self, path: Union[str, pl.Path]) -> Dict:
        """
        Save the calibration parameters as a dict
        """
        return (self.__dict__)


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

def temp_query(table: pd.DataFrame, query: str, name:str='temp') -> pd.DataFrame:
    """
    Applies a query on a dataframe by
    copying it into an in-memory sqllite db
    and running the query there.
    The query must refere
    """
    with sqllite.connect(":memory:") as con:
        table.to_sql(name=name, con=con)
        res = pd.read_sql_query(sql=query, con=con)
    return res

def make_calibration_parameters_table(table: pd.DataFrame) -> List[CalibrationParameters]:
    """
    From  the table of calibration parameters, make
    an object representing the calibration parameters as a InstrumentCalibration type
    with validity periods and bias/sensitivity values
    """
    def get_cal_params(grp: pd.DataFrame, grouping: tuple) -> CalibrationParameters:
        device, species, valid_from = grouping
        valid_to = grp.iloc[0]['next_date']
        if grp.shape[0] == 1:
            zero = grp['measured_concentration'].iloc[0] - \
                grp['target_concentration'].iloc[0]
            sensitivity = 1
            cal_type = CalType.ONE_POINT
        else:
            lm = linear_model.OLS(grp['target_concentration'], sttools.add_constant(
                grp['measured_concentration'])).fit()
            zero = lm.params['const']
            sensitivity = lm.params['measured_concentration']
            cal_type = CalType.TWO_POINT
        valid_to = grp['next_date'].iloc[0]
        param_z = CalibrationParameter(parameter='Intercept', value=zero)
        param_i = CalibrationParameter(parameter=species, value=sensitivity)
        return CalibrationParameters(species=species, device=device, parameters=[param_z, param_i], valid_from=valid_from, valid_to=valid_to, type=str(cal_type))
    # Add the previous date as begin of validity

    # This query add the validity period
    q = \
    """
    WITH cg as
    (
        SELECT
            *,
            DENSE_RANK() OVER (PARTITION BY compound,device ORDER BY date) AS cal_group
        FROM temp
    ) 
    SELECT
        c.*,
        d.cal_group,
        d.date AS next_date
        FROM cg AS c
    LEFT JOIN cg AS d ON c.cal_group = d.cal_group - 1 AND c.compound = d.compound AND c.device = d.device
    """
    res = temp_query(table, q).drop_duplicates()
    res['next_date'] = pd.to_datetime(res['next_date'].fillna(NAT))
    res['date'] = pd.to_datetime(res['date'])
    import pdb; pdb.set_trace()
    objs = res.reset_index().groupby(["device", "compound", "date"]).apply(
        lambda x: get_cal_params(x, x.name))
    return objs


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

def get_calibration_parameters(device: str, metadata: sqa.MetaData, eng: Union[engine.Engine, engine.Connection]) -> pd.DataFrame:
    """
    Get the sensor calibration parameters for a given device id
    """
    cp = sqa.Table('calibration_parameters', metadata, autoload_with=eng)
    mp = sqa.Table('model_parameter', metadata, autoload_with=eng)
    dep = sqa.Table('Deployment', metadata, autoload_with=eng)
    sens = sqa.Table('Sensors', metadata, autoload_with=eng)

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



def average_df(dt: pd.DataFrame, funs:Dict[str, Callable]={'max':np.max, 'min':np.min, 'mean':np.mean, 'std':np.std}, date_col:str='date', av:str='1 min') -> pd.DataFrame:
    """
    Averages a dataframe to the given time interval and applies
    a set of aggregation functions specified by the dictionary `funs` to all columns in the dataframe.
    If the new sampling interval is smaller than the older one, the data is forward filled before.
    """
    cols = set(dt.columns) - set([date_col])
    fns_name = {col: [(name, fn) for name, fn in funs.items()] for  col in cols}
    dt_agg = dt.set_index(date_col).resample(av).agg(fns_name)
    new_names = ["_".join(c) for c in dt_agg.columns]
    dt_agg.columns = new_names
    return dt_agg

def date_to_timestamp(dt: pd.DataFrame, dt_col:str, target_name:str='timestamp') -> pd.DataFrame:
    """
    Create a new dataframe where the date column specified in `dt_col`
    is replaced by a unix timestamp column with the name "timestamp"
    """
    dt[dt_col] = pd.to_datetime(dt[dt_col]).apply(lambda x: x.timestamp())
    return dt.rename(columns={dt_col:target_name})

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
