"""
Functions used for calibration / sensor data processing
"""
from . import models as mods
from . import data as du
from . import base

from typing import List, NamedTuple, Optional, Union, Dict

from collections import namedtuple

import sqlalchemy as sqa
import sqlalchemy.sql.functions as func

import datetime as dt

import pandas as pd


class CalDataRow(NamedTuple):
    """
    Class to represent a row in the calibration table or in the deployment table
    *
    """
    sensor_id: int
    serial_number: str
    location: str
    cal_start: dt.datetime
    cal_end: dt.datetime
    cal_mode: Optional[int]
    sensor_start: dt.datetime
    sensor_end: dt.datetime
    sensor_type: du.AvailableSensors


class SensInfoRow(CalDataRow):
    """
    Class to represent a row
    of the sensor deployment table
    """
    sensor_id: int
    serial_number: str
    sensor_start: dt.datetime
    sensor_end: dt.datetime
    sensor_type: du.AvailableSensors


def grouper_factory(table: mods.TimeseriesData) -> base.Base:
    class GroupedTimeseries(base.Base, mods.TimeseriesData):
        __table__ = table
        id: Union[str, int] = table.id
        time: int = table.time
    return GroupedTimeseries


def make_aggregate_columns(aggregations: Dict[str, str]) -> List[sqa.sql.expression.ColumnClause]:
    """
    Given a table and a dictionary of functions (one per each column),
    returns a a list of :obj:`sqlalchemy.sql.expression.ColumnClause` to use
    in a select statement
    """
    return [sqa.sql.literal_column(query).label(name) for name, query in aggregations.items()]



def get_table(type: du.AvailableSensors) -> mods.TimeseriesData:
    """
    Return the ORM mapped object for the choosen sensor type
    """
    match type:
        case du.AvailableSensors.HPP:
            return mods.HPPData
        case du.AvailableSensors.LP8:
            return mods.LP8Data
        case du.AvailableSensors.PICARRO:
            return mods.PicarroData

def group_data(session: sqa.orm.Session, id: Union[int, str], type: du.AvailableSensors, start: dt.datetime, end: dt.datetime, averaging_time: int, aggregations: Dict):
    tb = get_table(type)
    aggs = make_aggregate_columns(aggregations)
    import pdb; pdb.set_trace()

def get_calibration_info(session: sqa.orm.Session,
                         id: int, type: str, end: dt.datetime) -> List[CalDataRow]:
    """
    Get the calibration info for a sensor before the ending date
    """
    objects = session.query(mods.Calibration, mods.Sensor).join(mods.Sensor,
                                                                (mods.Sensor.id == mods.Calibration.id) &
                                                                (mods.Sensor.end >= mods.Calibration.end) &
                                                                (mods.Calibration.start >=
                                                                 mods.Calibration.start)
                                                                ).\
        filter((mods.Sensor.id == id) & (mods.Sensor.type == type)
               & (mods.Calibration.end >= end)).all()
    return [CalDataRow(sensor_id=c.id, serial_number=s.serial,
                       location=c.location, sensor_type=s.type,
                       cal_start=c.start, cal_end=c.end,
                       sensor_start=s.start, sensor_end=s.end, cal_mode=c.mode) for c, s in objects]


def query_sensor_data(session: sqa.orm.Session, id: Union[str, int], type: du.AvailableSensors, start: dt.datetime, end: dt.datetime):
    tb = get_table(type)
    qr = session.query(tb).filter(
        (tb.time.between(int(start.timestamp()), int(end.timestamp()))) &
        (tb.id == id)
    )


def get_sensor_data(session: sqa.orm.Session, id: int, type: du.AvailableSensors, start: dt.datetime, end: dt.datetime) -> pd.DataFrame:
    """
    Get the sensor data for a given period of time
    """
    match type:
        case du.AvailableSensors.HPP:
            tb = mods.HPPData
        case du.AvailableSensors.LP8:
            tb = mods.LP8Data
    qr = session.query(tb).filter(
        (tb.time.between(int(start.timestamp()), int(end.timestamp()))) &
        (tb.id == id)
    )
    import pdb
    pdb.set_trace()
    with session.connection() as con:
        dt = pd.read_sql(qr.statement, con)
    return dt
