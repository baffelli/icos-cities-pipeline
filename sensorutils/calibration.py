"""
Functions used for calibration / sensor data processing
"""
from asyncio.log import logger
from dataclasses import dataclass
from operator import mod
import pdb
from tkinter import W

import sqlalchemy
from . import models as mods
from . import data as du
from . import base
from . import log
from . import db
from . import utils

from typing import List, NamedTuple, Optional, Union, Dict, Tuple

from collections import namedtuple

import sqlalchemy as sqa
from sqlalchemy import func as func
from sqlalchemy.orm import aliased

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


@dataclass
class CalData:
    """
    Class to represent a calibration entry
    """
    data: pd.DataFrame
    start: pd.Timestamp
    end: pd.Timestamp
    serial: Union[int, str]


def make_aggregate_columns(aggregations: Dict[str, str]) -> List[sqa.sql.expression.ColumnClause]:
    """
    Given a table and a dictionary of functions (one per each column),
    returns a a list of :obj:`sqlalchemy.sql.expression.ColumnClause` to use
    in a select statement
    """
    return [sqa.sql.literal_column(query).label(name) for name, query in aggregations.items()]


def make_aggregate_expression(cl: sqa.Column, label: str, avg_time: int) -> sqa.sql.ColumnElement:
    """
    Make an expression for time aggregation of timestamp (int)
    columns
    """
    return (func.floor(cl / avg_time) * avg_time).label(label)


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


def get_sensor_data_agg(session: sqa.orm.Session, id: Union[int, str],
                        type: du.AvailableSensors, start: dt.datetime, end: dt.datetime,
                        averaging_time: int, aggregations: Dict) -> pd.DataFrame:
    """
    Get the sensor data and aggregate it by :obj:`averaging_time` (in seconds), returning a :obj:`pandas.DataFrame`
    aggregated according to the SQL expression in the `aggregations` dictionary.
    """
    tb = get_table(type)
    aggs = make_aggregate_columns(aggregations)
    ae = make_aggregate_expression(tb.time, "time", avg_time=averaging_time)
    qr = query_sensor_data(session, id, type, start, end)
    qr_agg = qr.with_only_columns(*(aggs + [ae])).group_by(ae)
    log.logger.debug(f"The query is {qr_agg}")
    dt = pd.read_sql(qr_agg, session.connection(), index_col=ae.key)
    return dt


def get_calibration_info(session: sqa.orm.Session,
                         id: int, type: du.AvailableSensors, dep: bool = False, latest: bool = True) -> List[CalDataRow]:
    """
    Get the calibration info for a sensor
    """
    match dep:
        case False:
            tb = mods.Calibration
        case True:
            tb = mods.Deployment
        case _:
            raise ValueError("Dep can be only boolean")
    logger.info(tb)
    ss = sqa.select(mods.Sensor).filter(
        (mods.Sensor.type == type.value) & (mods.Sensor.id == id)).subquery()
    tb = aliased(tb)
    filtered_sens = aliased(mods.Sensor, alias=ss)
    objects = sqa.select(filtered_sens, tb).\
        join_from(filtered_sens, tb,
                  (filtered_sens.id == tb.id) &
                  (tb.start >= filtered_sens.start) &
                  (filtered_sens.end >= tb.end)
                  ).\
        filter(tb.start >= filtered_sens.start).\
        order_by(tb.start.desc())

    sd = [CalDataRow(sensor_id=c.id, serial_number=s.serial,
                     location=c.location, sensor_type=s.type,
                     cal_start=c.start, cal_end=c.end,
                     sensor_start=s.start, sensor_end=s.end, cal_mode=c.mode) for s, c in session.execute(objects)]
    return sd


def query_sensor_data(session: sqa.orm.Session, id: Union[str, int], type: du.AvailableSensors, start: dt.datetime, end: dt.datetime) -> sqlalchemy.sql.expression.Selectable:
    """
    Gets the sensor data for a given id and time period. Returns a :obj:`sqalchemy.orm.Query` to be
    used downstream in other queries.
    """
    tb = get_table(type)
    qr = sqa.select(tb).filter(
        (tb.time.between(int(start.timestamp()), int(end.timestamp()))) &
        (tb.id == id)
    )
    return qr


def get_aggregations(type: du.AvailableSensors) -> dict:
    match type:
        case du.AvailableSensors.HPP:
            agg = {
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
        case du.AvailableSensors.LP8:
            agg = {
                'sensor_CO2': 'AVG(NULLIF(senseair_lp8_co2, -999))',
                'sensor_RH': 'AVG(NULLIF(sensirion_sht21_humidity, -999))',
                'sensor_t': 'AVG(NULLIF(sensirion_sht21_temperature, -999))',
                'sensor_ir': 'AVG(NULLIF(senseair_lp8_ir, -999))',
                'sensor_detector_t': 'AVG(NULLIF(senseair_lp8_temperature, -999))',
            }
        case du.AvailableSensors.PICARRO:
            agg = {
                'ref_CO2_DRY': 'AVG(IF(CO2_DRY = -999 OR CO2_DRY_F = 0, NULL, CO2_DRY))',
                'ref_CO2_DRY_SD': 'STDDEV(IF(CO2_DRY = -999 OR CO2_DRY_F = 0, NULL, CO2_DRY))',
                'ref_T': 'AVG(NULLIF(T, -999))',
                'ref_RH': 'AVG(NULLIF(RH, -999))',
                'ref_pressure': 'AVG(NULLIF(pressure * 100, -999))',
                'chamber_mode': 'MAX(calibration_mode)',
                'chamber_status': 'MAX(chamber_status)',
                'ref_H2O': 'AVG(IF(H2O = -999 OR H2O_F = 0, NULL, H2O))', }
    return agg


def get_sensor_data(session: sqa.orm.Session, id: int, type: du.AvailableSensors, start: dt.datetime, end: dt.datetime) -> pd.DataFrame:
    """
    Get the sensor data for a given period of time and returns it as a pandas dataframe
    """
    qr = query_sensor_data(session, id, type, start, end)
    dt = pd.read_sql(qr, session.connection())
    return dt


def limit_cal_entry(entries: List[CalDataRow], start: dt.datetime, end: dt.datetime, modes: Optional[List[int]] = None, replace_dates:bool = True) -> List[CalDataRow]:
    """
    Iterate through a list of :obj:`CalDataRow` objects and keep only those that overlap with :obj:`start` and 
     :obj:`end`.
    If `mode` is set, only return the calibration entries with the choosen calibration modes 
    """
    entries_sort = sorted(entries, key=lambda x: x.cal_start, reverse=True)
    di = utils.TimeInterval(start, end)
    periods = [utils.TimeInterval(e.cal_start, e.cal_end) for e in entries_sort]
    # valid = [CalDataRow(sensor_id=e.sensor_id, location=e.location, sensor_type=e.sensor_type, serial_number=e.serial_number, sensor_start=e.sensor_start,
    #                     sensor_end=e.sensor_end, cal_mode=e.cal_mode, cal_start=max([e.cal_start, start]), cal_end=min([e.cal_end, end])) 
    #                     for e in entries_sort if di.contains(utils.TimeInterval(e.cal_start, e.cal_end)) or di.overlaps(utils.TimeInterval(e.cal_start, e.cal_end)) or di.starts(utils.TimeInterval(e.cal_start, e.cal_end))]
    valid = [CalDataRow(sensor_id=e.sensor_id, location=e.location, sensor_type=e.sensor_type, serial_number=e.serial_number, sensor_start=e.sensor_start,
                        sensor_end=e.sensor_end, cal_mode=e.cal_mode, cal_start=max([e.cal_start, start]), cal_end=min([e.cal_end, end])) 
                        for e in entries_sort if (o:=utils.TimeInterval(e.cal_start, e.cal_end)) and (o.contains(di) or di.contains(o))
                        ]
    if valid:
        breakpoint()
    if modes:
        valid = [v for v in valid if v.cal_mode in modes]
    return valid


def get_cal_ts(session: sqa.orm.Session, id: int, type: du.AvailableSensors, start: dt.datetime, end: dt.datetime, averaging_time: int, dep: bool = False, modes: Optional[List[int]] = None) -> List[Optional[CalData]]:
    """
    Gets the timeseries of (raw) calibration data for a given `id` between the `start` and `end`
    times. If the sensor is collocated with a picarro instrument, also returns the data
    for the calibration sensor. If `dep` is set, gets the data from the `Deployment` table
    instead than from the calibration.
    If modes
    """
    cal_entries = get_calibration_info(session, id, type, dep=dep)
    valid = limit_cal_entry(cal_entries, start, end, modes=modes)
    aggs = get_aggregations(type)
    picarro_aggs = get_aggregations(du.AvailableSensors.PICARRO)

    def iterate_cal_info(e: CalDataRow) -> pd.DataFrame:
        picarro_res = get_sensor_data_agg(session, e.location, du.AvailableSensors.PICARRO,
                                          e.cal_start, e.cal_end, averaging_time, picarro_aggs)
        st = du.AvailableSensors[e.sensor_type]
        sensor_res = get_sensor_data_agg(session, e.sensor_id, st,
                                         e.cal_start, e.cal_end, averaging_time, aggs)
        if not picarro_res.empty:
            all_dt = sensor_res.join(picarro_res, how='left').reset_index()
        else:
            all_dt = sensor_res.reset_index()
        return all_dt.assign(**e._asdict())
    if len(valid) > 0:
        all_dt = pd.concat([iterate_cal_info(e) for e in valid], axis=0)
        grps = ["serial_number", "sensor_start", "sensor_end"]
        cd = [CalData(data=data, serial=serial, start=start, end=end)
              for (serial, start, end), data in all_dt.groupby(grps)]
    else:
        cd = []
    return cd


def make_calibration_parameters_table(table: pd.DataFrame) -> List[mods.CalibrationParameters]:
    """
    From  the table of calibration parameters, make
    an object representing the calibration parameters as a InstrumentCalibration type
    with validity periods and bias/sensitivity values
    """
    def get_cal_params(grp: pd.DataFrame, grouping: tuple) -> mods.CalibrationParameters:
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
        param_z = mods.CalibrationParameters(parameter='Intercept', value=zero)
        param_i = mods.CalibrationParameters(
            parameter=species, value=sensitivity)
        return mods.CalibrationParameters(species=species, device=device, parameters=[param_z, param_i], valid_from=valid_from, valid_to=valid_to, type=str(cal_type))
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
    res = db.temp_query(table, q).drop_duplicates()
    res['next_date'] = pd.to_datetime(res['next_date'].fillna(du.NAT))
    res['date'] = pd.to_datetime(res['date'])
    import pdb
    pdb.set_trace()
    objs = res.reset_index().groupby(["device", "compound", "date"]).apply(
        lambda x: get_cal_params(x, x.name))
    return objs


def prepare_level2_data(dt_in: pd.DataFrame, model_id: Union[str, int]) -> List[mods.Level2Data]:
    """
    Rename columns to store and produce a Leve2Data object
    """
    new_names = {
        'location': 'location',
        'time': 'time',
        'sensor_id': 'id',
        'CO2_pred': 'CO2',
        'H2O': 'H2O',
        'sensor_RH': 'relative_humidity',
        'sensor_t': 'temperature',
        'sensor_pressure': 'pressure'
    }
    dt_out = dt_in.copy()[[k for k, v in new_names.items()
                           if k in dt_in.columns]].rename(columns=new_names)
    dt_out['model_id'] = model_id
    du_clean = du.replace_inf(dt_out)
    return [mods.Level2Data(**tup) for tup in du_clean.to_dict(orient="records")]


def persist_level2_data(session: sqa.orm.Session, data: List[mods.Level2Data]) -> None:
    """
    Persists the level2 data to the database
    """
    for row in data:
        session.merge(row)