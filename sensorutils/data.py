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
from pyparsing import col
import sensorutils.files as fu
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


import sqlite3 as sqllite

from enum import Enum

import yaml


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
