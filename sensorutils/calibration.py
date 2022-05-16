"""
Functions used for calibration / sensor data processing
"""
from operator import mod
import pdb
from tkinter import W
from . import models as mods
from . import data as du
from . import base
from . import log
from . import db

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
                        averaging_time: int, aggregations: Dict):
    """
    Get the sensor data and aggregate it by :obj:`averaging_time` (in seconds), returning a :obj:`pandas.DataFrame`
    aggregated according to the SQL expression in the `aggregations` dictionary.
    """
    tb = get_table(type)
    aggs = make_aggregate_columns(aggregations)
    ae = make_aggregate_expression(tb.time, "time", avg_time=averaging_time)
    qr = query_sensor_data(session, id, type, start, end)
    qr_agg = qr.with_only_columns(*(aggs + [ae])).group_by(ae)
    log.logger.info(f"The query is {qr_agg}")
    dt = pd.read_sql(qr_agg, session.connection())
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


def query_sensor_data(session: sqa.orm.Session, id: Union[str, int], type: du.AvailableSensors, start: dt.datetime, end: dt.datetime):
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


def limit_cal_entry(entries: List[CalDataRow], start: dt.datetime, end: dt.datetime) -> List[CalDataRow]:
    """
    Iterate through a list of :obj:`CalDataRow` objects and keep only those that overlap with :obj:`start` and 
     :obj:`end`
    """
    valid = [CalDataRow(sensor_id=e.sensor_id, location=e.location, sensor_type=e.sensor_type, serial_number=e.serial_number, sensor_start=e.sensor_start,
                        sensor_end=e.sensor_end, cal_mode=e.cal_mode, cal_start=max([e.cal_start, start]), cal_end=min([e.cal_end, end])) for e in entries if e.cal_start < end and e.cal_end > start]
    breakpoint()
    return valid


def get_cal_ts(session: sqa.orm.Session, id: int, type: du.AvailableSensors, start: dt.datetime, end: dt.datetime, averaging_time: int) -> List[Tuple[Tuple[str, pd.Timestamp, pd.Timestamp], pd.DataFrame]]:
    """
    Gets the timeseries of (raw) calibration data for a given `id` between the `start` and `end`
    times. If the sensor is collocated with a picarro instrument, also returns the data
    for the calibration sensor.
    """
    cal_entries = get_calibration_info(session, id, type)
    valid = limit_cal_entry(cal_entries, start, end)
    aggs = get_aggregations(type)
    picarro_aggs = get_aggregations(du.AvailableSensors.PICARRO)
    # Get sensor data
    data_agg = get_sensor_data_agg(
        session, id, type, start, end,  averaging_time, aggs)
    # Get reference data

    def iterate_cal_info(e: CalDataRow) -> pd.DataFrame:
        res = get_sensor_data_agg(session, e.location, du.AvailableSensors.PICARRO,
                                  e.cal_start, e.cal_end, averaging_time, picarro_aggs)
        return res.assign(**e._asdict())
    picarro_data = pd.concat([iterate_cal_info(e) for e in valid], axis=0)
    all_dt = data_agg.set_index('time').join(
        picarro_data.set_index('time'), how='left').reset_index()
    grps = ["serial_number", "sensor_start", "sensor_end"]
    return list(all_dt.groupby(grps))
    # for d in query_dates:
    #     cal = [c for c in cal_entries if c.cal_start < d < c.cal_end]
    #     match cal:
    #         case CalDataRow() as cd:
    #             get_sensor_data_agg(id, type, d)
    #         case _:
    #             print("No valid dates")
    #     import pdb; pdb.set_trace()


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
