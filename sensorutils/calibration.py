"""
Functions used for calibration / sensor data processing
"""
import datetime as dt
import enum

from asyncio.log import logger

from dataclasses import asdict, dataclass
from operator import mod
from statistics import correlation
from typing import Dict, List, NamedTuple, NoReturn, Optional, Tuple, Union

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sqlalchemy
import sqlalchemy as sqa
import statsmodels.tools as smtools
from sqlalchemy import alias, func as func
from sqlalchemy.orm import aliased
from statsmodels.regression.linear_model import OLS, RegressionResults, OLSResults
from statsmodels.regression.rolling import RollingOLS


from sensorutils import data as du
from sensorutils import db, log
from sensorutils import models as mods
from sensorutils import utils
from sensorutils import calc

import copy as cp 

from mpl_toolkits.axes_grid.anchored_artists import AnchoredText
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates


class CalDataRow(NamedTuple):
    """
    Class to represent a row in the calibration table or in the deployment table
    Attributes:
    ----------
    sensor_id: int
        The sensor id
    serial_number: int
        The sensor serialnumber
    location: int
        The location of deployment / calibration. Serves as a key to find reference
        data in `picarro_data`
    cal_start: date
        Start of calibration
    cal_end: date 
        End of calibration
    cal_mode: int
        The calibration mode (2 = Chamber, 1 = Co-Location, 3 = Pressure chamber)
    sensor_start: date
        Start of sensor validity period
    sensor_end: date
        End of sensor validity period
    sensor_type: sensorutils.data.AvailableSensors
        The type of sensor being calibrated
    next_cal: date or none
        Date of the next calibration if available
        (used to limit validity period of computed parameters until the next
        calibration)

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
    next_cal: Optional[dt.datetime]


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
    next: pd.Timestamp
    serial: Union[int, str]

class measurementType(enum.Enum):
    """
    Enumeration to list measurement types, 
    used for plotting functions
    """
    REF = 'ref'
    CAL = 'cal'
    ORIG = 'orig'


class CalModes(enum.Enum):
    """
    Enum to represent different CV modes for the calibration
    """
    CHAMBER = [2, 3]
    COLLOCATION = [1]
    MIXED = [1, 2, 3]


def get_line_color(tp: measurementType, cm: mpl.colors.Colormap = plt.cm.get_cmap('Set2')) -> Tuple:
    match tp:
        case measurementType.CAL:
            cl = cm.colors[0]
        case measurementType.REF:
            cl = cm.colors[1]
        case measurementType.ORIG:
            cl = cm.colors[2]
    return cl

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
        case du.AvailableSensors.Vaisala:
            return mods.VaisalaData


def query_sensor_data_agg(session: sqa.orm.Session, id: Union[int, str],
                        type: du.AvailableSensors, start: dt.datetime, end: dt.datetime,
                        averaging_time: int, aggregations: Dict) -> sqa.sql.Selectable:
    """
    Get the sensor data and aggregate it by :obj:`averaging_time` (in seconds), returning a :obj:`sqlalchemy.sql.Selectable`
    aggregated according to the SQL expression in the `aggregations` dictionary.
    """
    tb = get_table(type)
    aggs = make_aggregate_columns(aggregations)
    qr = query_sensor_data(session, id, type, start, end).cte()
    qr_as = aliased(tb, qr)
    ae = make_aggregate_expression(qr_as.time, "time", avg_time=averaging_time)
    qr_agg = sqa.select(qr_as).with_only_columns(*(aggs + [ae])).group_by(ae)
    log.logger.debug(f"The query is {qr_agg}")
    return qr_agg

def get_pressure_interpolation(session: sqa.orm.Session, location: str, id: int, start: dt.datetime, end: dt.datetime, averaging_time: int) -> pd.DataFrame:
    """
    Gets the (interpolated) pressure value for the location `location` and the sensor `id`
    between the times `start` and `end`
    """
    tb = mods.PressureInterpolation
    aggregations = {'pressure_interpolation':'AVG(pressure * 100)'}
    aggs = make_aggregate_columns(aggregations)
    ae = make_aggregate_expression(tb.time, "time", avg_time=averaging_time)
    qr = sqa.select(tb).filter(
        (tb.time.between(start.timestamp(), end.timestamp())) &
        (tb.location == location) &
        (tb.sensor_id == id)
        ).with_only_columns(*(aggs + [ae])).group_by(ae)
    return pd.read_sql(qr, session.connection())

def get_sensor_data_agg(session: sqa.orm.Session, id: Union[int, str],
                        type: du.AvailableSensors, start: dt.datetime, end: dt.datetime,
                        averaging_time: int, aggregations: Dict) -> pd.DataFrame:
    """
    Get the sensor data and aggregate it by :obj:`averaging_time` (in seconds), returning a :obj:`pandas.DataFrame`
    aggregated according to the SQL expression in the `aggregations` dictionary.
    """
    qr_agg = query_sensor_data_agg(session, id, type, start, end, averaging_time, aggregations)
    dt = pd.read_sql(qr_agg, session.connection())
    return dt

def get_LP8_data_agg(session: sqa.orm.Session, id: Union[int, str], start: dt.datetime, end: dt.datetime, averaging_time: int) -> pd.DataFrame:
    """
    Get the data for the LP8 sensor and return a dataset averaged by `averaging_time`
    """
    aggs = get_aggregations(du.AvailableSensors.LP8)
    qr_agg = query_sensor_data_agg(session, id, type, start, end, averaging_time, aggs)
    return pd.read_sql(qr_agg, session.connection())


def get_HPP_data_agg(session: sqa.orm.Session, id: Union[int, str], start: dt.datetime, end: dt.datetime, averaging_time: int, cal: bool = False) -> pd.DataFrame:
    """
    Get the data for the LP8 sensor and return a dataset averaged by `averaging_time`.
    If `cal` is set, returns only the calibration times
    """
    aggs = get_aggregations(du.AvailableSensors.HPP)
    qr_agg = query_sensor_data_agg(session, id, type, start, end, averaging_time, aggs)
    return pd.read_sql(qr_agg, session.connection())

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
    ss = sqa.select(
        mods.Sensor
    ).filter(
        (mods.Sensor.type == type.value) & (mods.Sensor.id == id)).cte()
    tb1 = aliased(tb)
    filtered_sens = aliased(mods.Sensor, alias=ss)
    objects = sqa.select(filtered_sens, tb1).\
        join_from(filtered_sens, tb1,
                  (filtered_sens.id == tb1.id) &
                  (tb1.start >= filtered_sens.start) &
                  (filtered_sens.end >= tb1.end)
                  ).\
        filter(tb1.start >= filtered_sens.start).\
        order_by(tb1.start.desc())
    sd = [CalDataRow(sensor_id=c.id, serial_number=s.serial,
                     location=c.location, sensor_type=s.type,
                     cal_start=c.start, cal_end=c.end,
                     sensor_start=s.start, sensor_end=s.end, cal_mode=c.mode, next_cal=c.next_cal) for s, c in session.execute(objects)]
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


def query_level2_data(session: sqa.orm.Session, id: Union[str, int], start: dt.datetime, end: dt.datetime) -> sqlalchemy.sql.expression.Selectable:
    """
    Gets the calibrated sensor data for a given id and time period. Returns a :obj:`sqalchemy.orm.Query` to be
    used downstream in other queries.
    """
    tb = mods.Level2Data
    qr = sqa.select(tb).filter(
        (tb.time.between(int(start.timestamp()), int(end.timestamp()))) &
        (tb.id == id)
    )
    return qr


def get_aggregations(type: du.AvailableSensors, fit: bool=True) -> dict:
    match type, fit:
        case du.AvailableSensors.HPP, False:
            agg = {
                'sensor_CO2': 'AVG(NULLIF(senseair_hpp_co2_filtered, -999))',
                'sensor_pressure': 'AVG(NULLIF(senseair_hpp_pressure_filtered, -999))',
                'sensor_RH': 'AVG(NULLIF(sensirion_sht21_humidity, -999))',
                'sensor_t': 'AVG(NULLIF(sensirion_sht21_temperature, -999))',
                'sensor_ir_lpl': 'AVG(NULLIF(senseair_hpp_lpl_signal, -999))',
                'sensor_ir': 'AVG(NULLIF(senseair_hpp_ir_signal, -999))',
                'sensor_detector_t': 'AVG(NULLIF(senseair_hpp_temperature_detector, -999))',
                'sensor_mcu_t': 'AVG(NULLIF(senseair_hpp_temperature_mcu, -999))',
                'sensor_calibration_a': 'MIN(COALESCE(calibration_a, 0))',
                'sensor_calibration_b': 'MIN(COALESCE(calibration_b, 0))',
                'sensor_previous_calibration_a': 'MIN(COALESCE(previous_calibration_a, 0))',
                'sensor_previous_calibration_b': 'MIN(COALESCE(previous_calibration_b, 0))',
            }
        case du.AvailableSensors.HPP, True:
            agg = {
                'sensor_CO2': 'AVG(NULLIF(senseair_hpp_co2_filtered, -999))',
                'sensor_pressure': 'AVG(NULLIF(senseair_hpp_pressure_filtered, -999))',
                'sensor_RH': 'AVG(NULLIF(sensirion_sht21_humidity, -999))',
                'sensor_t': 'AVG(NULLIF(sensirion_sht21_temperature, -999))',
                'sensor_ir_lpl': 'AVG(NULLIF(senseair_hpp_lpl_signal, -999))',
                'sensor_ir': 'AVG(NULLIF(senseair_hpp_ir_signal, -999))',
                'sensor_detector_t': 'AVG(NULLIF(senseair_hpp_temperature_detector, -999))',
                'sensor_mcu_t': 'AVG(NULLIF(senseair_hpp_temperature_mcu, -999))',
                'sensor_calibration_a': 'MIN(COALESCE(calibration_a, 0))',
                'sensor_calibration_b': 'MIN(COALESCE(calibration_b, 0))',
                'sensor_previous_calibration_a': 'MIN(COALESCE(previous_calibration_a, 0))',
                'sensor_previous_calibration_b': 'MIN(COALESCE(previous_calibration_b, 0))',
            }
        case du.AvailableSensors.Vaisala, _:
            agg = {
                'sensor_CO2': 'AVG(NULLIF(vaisala_gmp343_co2, -999))',
                'sensor_pressure': 'AVG(NULLIF(bosch_bmp280_pressure, -999))',
                'sensor_RH': 'AVG(NULLIF(sensirion_sht21_humidity, -999))',
                'sensor_t': 'AVG(NULLIF(sensirion_sht21_temperature, -999))',
                'sensor_calibration_a': 'MIN(COALESCE(calibration_a, 0))',
                'sensor_calibration_b': 'MIN(COALESCE(calibration_b, 0))',
                'sensor_previous_calibration_a': 'MIN(COALESCE(previous_calibration_a, 0))',
                'sensor_previous_calibration_b': 'MIN(COALESCE(previous_calibration_b, 0))',
            }
        case du.AvailableSensors.LP8, _:
            agg = {
                'sensor_CO2': 'AVG(NULLIF(senseair_lp8_co2, -999))',
                'sensor_RH': 'AVG(NULLIF(sensirion_sht21_humidity, -999))',
                'sensor_t': 'AVG(NULLIF(sensirion_sht21_temperature, -999))',
                'sensor_ir': 'AVG(NULLIF(senseair_lp8_ir, -999))',
                'sensor_detector_t': 'AVG(NULLIF(senseair_lp8_temperature, -999))',
            }
        case du.AvailableSensors.PICARRO, _:
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


def get_HPP_calibration_data(session: sqa.orm.Session, sensor_type: du.AvailableSensors, id: int, start: dt.datetime, end: dt.datetime, avg_time: int = 120, fit=True) -> pd.DataFrame:
    """
    Get the data for the  HPP/Vaisala/Licor sensor. For each calibration period, also returns the corresponding
    cylinder analysis
    """
    tb = get_table(sensor_type)
    qr = query_sensor_data(session, id, sensor_type, start, end)
    qr_as = aliased(tb, qr.subquery())
    #Add the previous calibration value
    qr_as_extra = sqa.select(qr_as).add_columns(
                func.lag(qr_as.calibration_a).over(partition_by=qr_as.id, order_by=qr_as.time).label("previous_calibration_a"),
                func.lag(qr_as.calibration_b).over(partition_by=qr_as.id, order_by=qr_as.time).label("previous_calibration_b")
    ).subquery()
    if fit:
        qr_filt = sqa.select(qr_as_extra).filter(
            ((qr_as_extra.c.calibration_a == 1) |
            (qr_as_extra.c.calibration_b == 1)) 
        ).subquery()
    else:
        sqa.select(qr_as_extra).subquery()
    hpp_as = aliased(tb, qr_filt)
    ae = make_aggregate_expression(qr_filt.c.time, "time", avg_time)
    agg = get_aggregations(sensor_type, fit=True)
    ac = make_aggregate_columns(agg)
    hpp_agg = sqa.select(qr_filt).group_by(*[ae]).with_only_columns(*[ac + [ae, hpp_as.id]]).cte()
    sf = sqa.select(mods.Sensor).filter(mods.Sensor.type == sensor_type.value).subquery()
    sens_as = aliased(mods.Sensor, sf)
    #Get sensor info
    jq = sqa.select(hpp_agg, mods.CylinderDeployment, sens_as).outerjoin(
        mods.CylinderDeployment,
        (hpp_agg.c.id == mods.CylinderDeployment.sensor_id) &
        (sqa.func.from_unixtime(hpp_agg.c.time) >= mods.CylinderDeployment.start) &
        (sqa.func.from_unixtime(hpp_agg.c.time) <= mods.CylinderDeployment.end)
    ).join(
        sens_as,
        (hpp_agg.c.id == sens_as.id) &
        hpp_agg.c.time.between(
            sqa.func.unix_timestamp(sens_as.start),
            sqa.func.coalesce(sqa.func.unix_timestamp(sens_as.end), sqa.func.unix_timestamp()))
    ).where(
        (((hpp_agg.c.sensor_calibration_a == 1) & (mods.CylinderDeployment.inlet == 'a'))) |
        (((hpp_agg.c.sensor_calibration_b == 1) & (mods.CylinderDeployment.inlet == 'b')))
    ).order_by(hpp_agg.c.time).add_columns(sens_as.serial)
    #get the data
    res = session.execute(jq).all()
    def unpack_row(rw:dict) -> dict:
        return {k:(rw[k] if k not in ('CylinderDeployment') else ([{f"cyl_{k}":v for k,v in asdict(c).items()} for c in rw[k].cylinders] if rw[k] else None)) for k in rw.keys()}
    #Unpack and only keep the analysis
    res_unpack = [ unpack_row(b) for b in res]
    if res_unpack:
        md = {k:type(v) for k,v in res_unpack[0].items() if k not in ('CylinderDeployment')}
        dt = pd.json_normalize(res_unpack, record_path='CylinderDeployment', meta=list(md.keys())).convert_dtypes()
    else:
        dt = pd.DataFrame()
    return dt

def limit_cal_entry(entries: List[CalDataRow], start: dt.datetime, end: dt.datetime, modes: Optional[List[int]] = None, replace_dates: bool = True) -> List[CalDataRow]:
    """
    Iterate through a list of :obj:`CalDataRow` objects and keep only those that overlap with :obj:`start` and 
     :obj:`end`.
    If `mode` is set, only return the calibration entries with the choosen calibration modes 
    """
    entries_sort = sorted(entries, key=lambda x: x.cal_start, reverse=True)
    di = utils.TimeInterval(start, end)
    periods = [utils.TimeInterval(e.cal_start, e.cal_end)
               for e in entries_sort]
    valid = [CalDataRow(sensor_id=e.sensor_id, location=e.location, sensor_type=e.sensor_type, serial_number=e.serial_number, sensor_start=e.sensor_start,
                        sensor_end=e.sensor_end, next_cal=e.next_cal, cal_mode=e.cal_mode, cal_start=max([e.cal_start, start]), cal_end=min([e.cal_end, end]))
             for e in entries_sort if (o := utils.TimeInterval(e.cal_start, e.cal_end)) and (o.contains(di) or di.contains(o))
             ]
    if modes:
        valid = [v for v in valid if v.cal_mode in modes]
    return valid


def get_cal_ts(session: sqa.orm.Session, id: int, type: du.AvailableSensors, start: dt.datetime, end: dt.datetime, averaging_time: int, dep: bool = False, modes: Optional[CalModes] = None) -> List[Optional[CalData]]:
    """
    Gets the timeseries of (raw) calibration data for a given `id` between the `start` and `end`
    times. If the sensor is collocated with a picarro instrument, also returns the data
    for the calibration sensor. If `dep` is set, gets the data from the `Deployment` table
    instead than from the calibration.
    If modes
    """
    cal_entries = get_calibration_info(session, id, type, dep=dep)
    valid = limit_cal_entry(cal_entries, start, end, modes=(modes.value if modes else None))
    aggs = get_aggregations(type, fit=False)
    picarro_aggs = get_aggregations(du.AvailableSensors.PICARRO)

    def iterate_cal_info(e: CalDataRow) -> pd.DataFrame:
        picarro_res = get_sensor_data_agg(session, e.location, du.AvailableSensors.PICARRO,
                                          e.cal_start, e.cal_end, averaging_time, picarro_aggs)
        st = du.AvailableSensors[e.sensor_type]
        #Call pressure interpolation
        press = get_pressure_interpolation(session, e.location, e.sensor_id, e.cal_start, e.cal_end, averaging_time)
        sensor_res = get_sensor_data_agg(session, e.sensor_id, st,
                                         e.cal_start, e.cal_end, averaging_time, aggs)
        if not picarro_res.empty:
            all_dt = sensor_res.set_index('time').join(picarro_res.set_index('time'), how='left')
        else:
            all_dt = sensor_res
        if not press.empty:
            final_dt = all_dt.join(press.set_index('time'), how='left')
        else:
            final_dt = all_dt
        return final_dt.assign(**e._asdict())
    if len(valid) > 0:
        all_dt = pd.concat([iterate_cal_info(e) for e in valid], axis=0)
        grps = ["serial_number", "cal_start", "cal_end", "next_cal"]
        cd = [
            CalData(
                data=data,
                serial=serial,
                start=cal_start,
                end=cal_end,
                next=next_cal)
            for (serial, cal_start, cal_end, next_cal), data in all_dt.groupby(grps)
        ]
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

    objs = res.reset_index().groupby(["device", "compound", "date"]).apply(
        lambda x: get_cal_params(x, x.name))
    return objs


def prepare_level2_data(dt_in: pd.DataFrame, model_id: Union[str, int],  pred_col: str = 'CO2_pred_comp') -> List[mods.Level2Data]:
    """
    Rename columns to store and produce a Leve2Data object
    """
    new_names = {
        'location': 'location',
        'time': 'time',
        'sensor_id': 'id',
        pred_col: 'CO2',        
        'H2O': 'H2O',
        'sensor_RH': 'relative_humidity',
        'sensor_t': 'temperature',
        'sensor_pressure': 'pressure',
        'inlet': 'inlet'
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


@dataclass
class CalibrationQuality:
    """
    Dataclass to store calibration quality parameters
    """
    rmse: float
    bias: float
    correlation: float


def bias(ref: pd.Series, value: pd.Series) -> float:
    """
    Computes the bias (mean difference)
    of two `pandas.Series`
    """
    return (value - ref).mean() 


def rmse(ref: pd.Series, value: pd.Series) -> float:
    """
    Computes the rmse (mean squared difference)
    of two `pandas.Series`
    """
    return np.sqrt(((value - ref)**2).mean())

def none_if_nan(value: np.number) -> Optional[float]:
    return None if np.isnan(value) else float(value)

def compute_quality_indices(pred: pd.DataFrame, ref_col='ref_CO2', pred_col='CO2_pred', fit=True) -> mods.ModelFitPerformance:
    """
    Compute model quality indicators for a dataframe of model predictions
    and return a dict of the statistics.
    """

    res_rmse = none_if_nan(rmse(pred[ref_col],  pred[pred_col]))
    res_bias = none_if_nan(bias(pred[ref_col],  pred[pred_col]))
    res_cor = none_if_nan(pred[ref_col].corr(pred[pred_col]))
    dit = {'rmse': res_rmse, 'bias': res_bias, 'correlation': res_cor}
    if fit: 
        res = mods.ModelFitPerformance(**dit)
    else:
        res = mods.PredictionPerformance(**dit)
    return res


def convert_calibration_parameters(cp: OLSResults,
                                   species: str,
                                   type: du.AvailableSensors,
                                   valid_from: dt.datetime,
                                   valid_to: dt.datetime,
                                   computed: dt.datetime,
                                   device: str) -> mods.CalibrationParameters:
    """
    Convert the fit results into a :obj:`sensorutils.data.CalibrationParameters`
    object that can be persisted in the database in a human-readable format
    thank to SQLAlchemy
    """
    pl = [mods.CalibrationParameter(parameter=n, value=v)
          for n, v in cp.params.items()]
    return mods.CalibrationParameters(parameters=pl,
                                      species=species,
                                      type=type.value, valid_from=valid_from,
                                      valid_to=valid_to,
                                      computed=computed, device=device)


def filter_cal(data: pd.DataFrame, endog='sensor_CO2', exog='cyl_CO2', duration_threshold: int = 120) -> pd.DataFrame:
    """
    Filters the data for the HPP two point calibration
    """
    valid = ~data[endog+exog].isna().any(axis=1)
    max_dur = dt.timedelta(hours=2).total_seconds()
    valid_final = valid & \
    (data['inlet_elapsed'].between(duration_threshold, max_dur)) & \
    data.inlet.isin(['a', 'b']) & (data['cal_elapsed'].between(duration_threshold, max_dur))
    data_valid = data[valid_final]
    return data_valid


def HPP_two_point_calibration(dt_in: pd.DataFrame, st: du.AvailableSensors, endog: List[str], exog: List[str], 
window:int = 10, duration_threshold: int = 240, species:str='CO2') -> Optional[List[Tuple[Optional[mods.CalibrationParameters], Optional[mods.ModelFitPerformance]]]]:
    """
    Estimate the HPP two point calibration parameter
    using a grouped regression (grouped by `window` days) and returns a list
    of `sensorutils.models.CalibrationParameters`.
    Only use the measurement value after 240 seconds from the inlet change
    """
    def inner_cal(data: pd.DataFrame) -> pd.Series:
        if not data.empty:
            co2_levels = data[endog[0]].unique()
            span = np.abs(np.diff(co2_levels))[0] if len(co2_levels) > 1 else 0
            if span > 100:
                fit = OLS(data[endog].astype(float), smtools.add_constant(data[exog].astype(float))).fit()
            else:
                diff = data[endog].values - data[exog].values
                data['const'] = 1
                fit_orig = OLS(diff.astype(float), data['const']).fit()
                params = fit_orig.params
                new_params =  pd.concat([params, pd.Series({exog[0]:1})])
                fit = cp.deepcopy(fit_orig)
                fit.params = new_params
            cq = mods.ModelFitPerformance(rmse=np.sqrt(np.mean(fit.resid**2)), bias=np.mean(np.abs(fit.resid)), correlation=fit.rsquared)
        else:
            cq = None
            fit = None
        return pd.Series({'parameters':fit, 'quality':cq})
    #Group by time window and apply calibration
    cal_par = dt_in.set_index('date').\
        groupby(["id", "serial", pd.Grouper(freq=f'{window}d')]).\
        apply(lambda x: inner_cal(x.convert_dtypes()))
    #Add next calibration date
    if not cal_par.empty:
        cal_par_out = cal_par.reset_index()
        cal_par_out['next_cal'] = cal_par_out.date.shift(-1).fillna(du.NAT)
        cal_par_out['species'] = species
        par_objs = [
            (convert_calibration_parameters(
                m.parameters,
                m.species,
                st,
                m.date.to_pydatetime(),
                m.next_cal.to_pydatetime(),
                dt.datetime.now(),
                m.serial), m.quality) for m in cal_par_out.itertuples() if m.parameters
        ]
    else:
        par_objs = []

    return par_objs


def apply_HPP_calibration(dt_in: pd.DataFrame, cal_pars: List[mods.CalibrationParameters]) -> pd.DataFrame:
    """
    Apply the calibration parameters
    contained in the list `cal_pars` to the data frame `dt_in`
    and returns a data frame of calibrated data
    """
    def inner_pred(di: pd.DataFrame, pms: mods.CalibrationParameters) -> pd.DataFrame:
        valid = di['date'].between(pms.valid_from, pms.valid_to)
        dt_pred = di[valid].copy()
        dt_pred['const'] = 1
        pms_sm = pms.to_statsmodel()
        reg_names = [n for n in pms_sm.params.keys()]
        dt_pred['CO2_pred'] = pms_sm.predict(dt_pred[reg_names].astype(float))
        return dt_pred
    pred_objs = pd.concat([inner_pred(dt_in, pm) for pm in cal_pars]).replace({pd.NA: np.nan})
    return pred_objs


def plot_CO2_ts(ax: mpl.axes.Axes,
                time: pd.Series, meas: pd.Series,
                orig: Optional[pd.Series] = None , 
                ref: Optional[pd.Series] = None) -> mpl.axes.Axes:
    """
    Plot the CO2 timeseries with the given axes, time span and measurements. Optionally,
    you can pass a second series to plot a reference line
    """
    ax.plot(time, meas, color=get_line_color(measurementType.CAL), label='Calibrated')
    if orig is not None:
        ax.plot(time, orig, color=get_line_color(measurementType.ORIG), label='Uncalibrated')
    if ref is not None:
        ax.plot(time, ref, color=get_line_color(measurementType.REF), label='Reference')
    ax.set_xlabel('Time')
    ax.set_ylabel('CO2 [ppm]')
    return ax

def plot_CO2_scatter(ax: mpl.axes.Axes, meas: pd.Series,
                ref: pd.Series , 
                orig: Optional[pd.Series] = None) -> mpl.axes.Axes:
    ax.scatter(ref, meas, color=get_line_color(measurementType.CAL), s=0.5,  label='Calibrated')
    if orig is not None:
        ax.scatter(ref, orig, color=get_line_color(measurementType.ORIG),  s=0.5, label='Reference')
    st = dict(slope=1., color='lightgray', lw=0.5)
    ax.axline((0, 0), **st)
    ax.axline((0, -3), **st)
    ax.axline((0, 3), **st)
    ax.set_xlabel('Reference CO2 [ppm]')
    ax.set_ylabel('Measured CO2 [ppm]')
    return ax

def plot_RH_ts(ax: mpl.axes.Axes, time: pd.Series, rh: pd.Series, t: pd.Series) -> mpl.axes.Axes:
    """
    Plot the timeseries of relative humidity
    """
    cm = plt.cm.get_cmap('Set2')
    ax.plot(time, rh, color = cm.colors[4])
    ax.set_ylabel("H2O [ppm]")
    ax.set_ylim([0, 2])
    ax_t = ax.twinx()
    ax_t.plot(time, t,  color = cm.colors[3])
    ax_t.set_ylim([-5, 40])
    ax_t.set_ylabel("T [C]")
    ax_t.tick_params(axis='y', labelcolor=cm.colors[3])

    return ax

def plot_CO2_calibration(dt_in: pd.DataFrame,
                         ref_col='ref_CO2',
                         pred_col='CO2_pred',
                         orig_col="sensor_CO2",
                         daily: bool = True) -> plt.Figure:
    """
    Plot detailed CO2 calibration results:
    - Time series of sensor and reference
    - Scatter plot
    - Residuals of regressors
    Depending on the mode, 
    """
    dt_min, dt_max = dt_in['date'].max(), dt_in['date'].min()
    time_span = abs(dt_max - dt_min)
    if time_span.days <= 1:
        date_form = DateFormatter("%H")
        loc = mdates.HourLocator(interval=2)
        xl = [dt_min.replace(hour=0, minute=0), dt_min.replace(hour=23, minute=59)]
    else:
        date_form = DateFormatter("%Y-%M-%d %H")
        loc = mdates.DayLocator(interval=time_span.days // 2)
        xl = [dt_in['date'].min(), dt_in['date'].max()]
    fig = mpl.figure.Figure()
    gs = fig.add_gridspec(3,1)
    #Plot timeseries
    ax1 = fig.add_subplot(gs[0,:])
    rc = dt_in[ref_col] if ref_col in dt_in.columns else None
    plot_CO2_ts(ax1, dt_in['date'], dt_in[pred_col], dt_in[orig_col], ref=rc)
    lms = [300, 600]
    ax1.set_ylim(lms)
    ax1.xaxis.set_major_formatter(date_form)
    ax1.xaxis.set_major_locator(loc)
    ax1.set_xlim(xl)
    #Plot timeseries of RH/T
    ax_w = fig.add_subplot(gs[1,:], sharex=ax1)
    ax_w = plot_RH_ts(ax_w, dt_in['date'], dt_in['sensor_H2O'], dt_in['sensor_t'])
    if rc is not None:
        ax2 = fig.add_subplot(gs[2, :])
        ax2 = plot_CO2_scatter(ax2, dt_in[pred_col], rc, dt_in[orig_col])
        s_bias = bias(rc, dt_in[pred_col])
        s_rmse = rmse(rc, dt_in[pred_col])
        cor = rc.corr(dt_in[pred_col])
        ann = AnchoredText(
        f"RMSE: {s_rmse:5.2f} ppm,\n Bias: {s_bias:5.2f} ppm,\n corr: {cor:5.3f}", loc='upper left')
        ax2.add_artist(ann)
        ax2.set_ylim(lms)
        ax2.set_xlim(lms)
    ax1.legend(loc='upper right')
    return fig
    



def plot_HPP_calibration(dt: pd.DataFrame) -> plt.Figure:
    """
    Plots the Senseair HPP two point calibration timeseries
    (for one day)
    """
    # dt_min, dt_max = dt['date'].min(), dt['date'].max()
    # date_form = DateFormatter("%H-%M")
    fig = mpl.figure.Figure()
    dt_valid = dt[~dt.sensor_CO2.isna()]
    if not dt_valid.empty and 'sensor_CO2' in dt_valid.columns:
        #dt_scaled = dt['cal_elapsed'] * (dt['cal_cycle'] - dt['cal_cycle'].min() + 1)
        dt_scaled = dt_valid['date']
        ax = fig.add_subplot(211)
        sz = 0.5
        ax.scatter(dt_scaled, dt_valid['sensor_CO2'], color=get_line_color(measurementType.ORIG), label='Uncalibrated', s=sz)
        ax.scatter(dt_scaled, dt_valid['CO2_pred'], color=get_line_color(measurementType.CAL), label='Calibrated', s=sz)
        ax.scatter(dt_scaled, dt_valid['cyl_CO2'], color=get_line_color(measurementType.REF), label='Reference', s=sz)
        ax.set_ylabel('CO2 [ppm]')
        ax.set_ylim([300, 700])
        ax.legend()
        ax1 = fig.add_subplot(212)
        ax1.scatter(dt_scaled, dt_valid['sensor_RH'], color=get_line_color(measurementType.ORIG), s=sz)
        ax1.set_ylim([0, 30])
        ax1.set_ylabel('RH [%]')
        ax1.set_xlabel('Elapsed time')

    return fig

def get_HPP_calibration(session: sqa.orm.Session, id: Union[int, str], dt: dt.datetime, species='CO2') -> Optional[mods.CalibrationParameters]:
    """
    Get the valid HPP calibration parameters for a given date and sensor  id
    """
    ds, de = du.day_range(dt)
    par_query = sqa.select(mods.CalibrationParameters).filter(
        (mods.CalibrationParameters.device == id) &
        (mods.CalibrationParameters.valid_from <= ds) &
        (mods.CalibrationParameters.valid_to >= de) &
        (mods.CalibrationParameters.species == "CO2")
    ).order_by(
        mods.CalibrationParameters.computed.desc()
    )

    return session.execute(par_query).scalar()

def update_hpp_calibration(session: sqa.orm.Session, pm: mods.CalibrationParameters) -> mods.CalibrationParameters:
    """
    Updates an existing entry of HPP Calibration parameters in the database.
    If no entry exists, add an entry
    """
    #Select the last computed valuex§
    sq = sqa.select(mods.CalibrationParameters).filter(
        (mods.CalibrationParameters.valid_from == pm.valid_from) &
        (mods.CalibrationParameters.valid_to == pm.valid_to) &
        (mods.CalibrationParameters.species == pm.species) &
        (mods.CalibrationParameters.device == pm.device)).order_by(
            mods.CalibrationParameters.computed.desc()
        )
    obj: mods.CalibrationParameters = session.execute(sq).scalar()

    if obj is not None:
        obj.parameters = pm.parameters
        obj.computed = pm.computed
        obj_up = obj
    else:
        obj_up = session.add(pm)
        obj_up = pm
    session.commit()
    return obj_up


def filter_LP8_features(features: pd.DataFrame) -> pd.DataFrame:
    """
    Filter the SenseAir LP8 features and returns a dataframe
    with only the valid times, that is where:
    - RH < 85 %
    - no abrupt temperature changes
    - no abrupt IR changes
    """
    rh_cond  = features['sensor_RH'] < 85 
    ir_diff = (features.set_index('date')['sensor_ir'].diff().abs()).values < 1000
    return features[rh_cond & ir_diff]


def get_drift_correction_data(session: sqa.orm.Session, sensor_id: Union[int, str], start: dt.datetime, end: dt.datetime, ref_loc=['LAEG', 'DUE1']) -> pd.DataFrame:
    """
    Get data for the senseair LP8 drift correction. To do so,
    take the data from `co2_level2`, join it with
    `picarro_data` with the locations in `ref_loc`. This produces a cartesian product,
    this should be taken care by the drift correction function.
    """
    #Get data from picarro
    ref_data = sqa.select(mods.PicarroData, mods.Location).join(
        mods.Location,
        (mods.Location.id == mods.PicarroData.id) &
        (mods.PicarroData.time.between(func.unix_timestamp(mods.Location.start), func.coalesce(func.unix_timestamp(), func.unix_timestamp(mods.Location.end))))
    ).with_only_columns(
        mods.PicarroData.time.label(f"ref_loc_time"),
        mods.PicarroData.CO2_DRY.label("ref_loc_CO2_DRY"),
        mods.PicarroData.CO2.label("ref_loc_CO2"),
        mods.PicarroData.pressure.label("ref_loc_pressure"),
        mods.PicarroData.id.label("ref_location"),
        mods.Location.x.label('ref_loc_x'),
        mods.Location.x.label('ref_loc_y'),
        mods.Location.h.label("ref_h")
    ).filter(
        (mods.PicarroData.id.in_(ref_loc))
        ).subquery()
    ref_data_as = aliased(mods.PicarroData, ref_data)
    pressure_as = aliased(mods.PressureInterpolation, sqa.select(mods.PressureInterpolation).with_only_columns(
        mods.PressureInterpolation.pressure.label("pressure_interp"),
        mods.PressureInterpolation.location,
        mods.PressureInterpolation.sensor_id,
        mods.PressureInterpolation.time
    ).subquery())
    qr = sqa.select(mods.Level2Data, ref_data, mods.Deployment, pressure_as.pressure).join(
        ref_data,
        (ref_data.c.ref_loc_time == mods.Level2Data.time),
        isouter=True
    ).join(
        mods.Deployment,
        (mods.Deployment.id == mods.Level2Data.id) &
        (mods.Level2Data.time.between(
            sqa.func.unix_timestamp(mods.Deployment.start), 
            func.coalesce(sqa.func.unix_timestamp(mods.Deployment.end), sqa.func.unix_timestamp())
            )
        )
    ).join(
        pressure_as,
        (pressure_as.time == mods.Level2Data.time) &
        (pressure_as.location == mods.Level2Data.location) &
        (pressure_as.sensor_id == mods.Level2Data.id) 
    ).filter(
        mods.Level2Data.time.between(start.timestamp(), end.timestamp()) &
        (mods.Level2Data.id == sensor_id) &
        (mods.Deployment.mode.notin_([2,3]))
    )
    #Data is in long format ((n_obs * n_ref) x n_columns) -> make wide
    data = pd.read_sql_query(qr, session.connection())
    #Add information
    data['date'] = pd.to_datetime(data['timestamp'], unit='s')
    return data

def map_inlets(inlet_a: pd.Series, inlet_b: pd.Series) -> pd.Series:
    """
    Combines two series with binary values into one series with the values
    'a' and 'b' depending on wether `inlet_a` or `inlet_b` are set to 1.
    """
    return pd.Series([['', 'a', 'b', ''][i] for i in (inlet_a + inlet_b * 2)],  dtype="string")
def prepare_HPP_features(dt: pd.DataFrame, ir_col: str = "sensor_ir_lpl", fit: bool = True, plt: str = '1d') -> pd.DataFrame:
    """
    Prepare features for CO2 calibration / predictions
    """
    dt_new = dt.copy().reset_index()
    dt_new['date'] = pd.to_datetime(dt_new['time'], unit='s')
    dt_new['sensor_t_abs'] = calc.absolute_temperature(dt_new['sensor_t'])
    # Water content
    dt_new['sensor_H2O'] = calc.rh_to_molar_mixing(
        dt_new['sensor_RH'], dt_new['sensor_t_abs'], dt_new['sensor_pressure'])
    # Referenced CO2
    nc_sens = calc.normalisation_constant(dt_new['sensor_pressure'], dt_new['sensor_t_abs'])
    dt_new['nc_sens'] = nc_sens
    dt_new['sensor_CO2_comp'] = dt_new['sensor_CO2'] * 1/nc_sens
    #Map to single inlet
    dt_new['inlet'] = map_inlets(dt_new.sensor_calibration_a, dt_new.sensor_calibration_b)
    dt_new['previous_inlet'] = map_inlets(dt_new.sensor_previous_calibration_a, dt_new.sensor_previous_calibration_b)
    # Start of cylinder calibration (anytime the inlet changes)
    dt_new['inlet_change'] = (dt_new['inlet'] != dt_new['previous_inlet'])
    #One full calibration cycle corresponds to two inlet changes
    dt_new['inlet_cycle'] = dt_new['inlet_change'].cumsum()
    dt_new['cal_cycle'] = (((dt_new['inlet_change'].cumsum() % 2)).diff() == 1).cumsum() 
    #Calibration cycle end
    dt_new['cal_cycle_end'] = (~dt_new['inlet'].isnull()) & (dt_new['inlet'].shift(-1).isnull()) & (dt_new['inlet_cycle'] % 2 == 0)
    dt_new['normal_cycle'] = dt_new['cal_cycle_end'].cumsum()
    #Elapsed time (for calibration cycle)
    elaps_lm = lambda x: (x['date'] - x['date'].min()).dt.total_seconds()
    if dt_new.cal_cycle.max() > 0:
        dt_new['inlet_elapsed'] = dt_new.groupby('inlet_cycle').apply(elaps_lm).values
        dt_new['cal_elapsed'] = dt_new.groupby('cal_cycle').apply(elaps_lm).values
        
    else:
        dt_new['inlet_elapsed'] = 0
        dt_new['cal_elapsed'] = 0
    # Additional features needed for fitting
    if dt_new.normal_cycle.max() > 0:
        dt_new['normal_elapsed'] = dt_new.groupby('normal_cycle').apply(elaps_lm).values
    else:
        dt_new['normal_elapsed'] = (dt_new['date'] - dt_new['date'].min()).dt.total_seconds()
    if 'ref_CO2_DRY' in dt_new.columns:
        # Normalisation constant
        nc = calc.normalisation_constant(dt_new['ref_pressure'] * 100, dt_new['sensor_t_abs'])
        # Compute normalised concentrations using ideal gas law
        dt_new['sensor_H2O_comp'] = dt_new['sensor_H2O'] * nc
        dt_new['ref_CO2'] = calc.dry_to_wet_molar_mixing(dt_new['ref_CO2_DRY'], dt_new['ref_H2O']/100)
        dt_new['ref_CO2_comp'] =  dt_new['ref_CO2'] * nc
    return dt_new.reset_index()


def prepare_LP8_features(dt: pd.DataFrame, fit: bool = True,  plt: str = '1d') -> pd.DataFrame:
    """
    Prepare features for LP8 calibration
    """
    dt_new = dt.copy().reset_index()
    dt_new['date'] = pd.to_datetime(dt_new['time'], unit='s')
    dt_new["sensor_ir_log"] = - np.log(dt_new["sensor_ir"])
    dt_new["sensor_t_sq"] = dt_new[f"sensor_t"]**2
    dt_new["sensor_t_cub"] = dt_new[f"sensor_t"]**3
    dt_new["sensor_ir_interaction_t"] = dt_new[f"sensor_t"] / \
        dt_new["sensor_ir"]
    dt_new["sensor_ir_interaction_t_sq"] = dt_new[f"sensor_t_sq"] / \
        dt_new["sensor_ir"]
    dt_new["sensor_ir_interaction_t_cub"] = dt_new[f"sensor_t_cub"] / \
        dt_new["sensor_ir"]
    dt_new['sensor_CO2_interaction_t'] = dt_new[f"sensor_CO2"] / \
        dt_new[f"sensor_t"]
    dt_new["sensor_ir_product_t"] = dt_new["sensor_ir"] * dt_new["sensor_t"]
    dt_new["sensor_t_abs"] = calc.absolute_temperature(dt_new['sensor_t'])
    dt_new['time_dummy'] = dt_new['date'].dt.round(plt).dt.date.astype('str')
    dt_new['const'] = 1
    dt_new['sensor_H2O'] = calc.rh_to_molar_mixing(dt_new['sensor_RH'], dt_new['sensor_t_abs'], dt_new['pressure_interpolation'])
    # dt_new['sensor_ir_interaction_H2O'] = dt_new["sensor_ir"] / \
    # dt_new['sensor_H2O']
    dt_new['sensor_ir_interaction_pressure'] = dt_new['pressure_interpolation'] / dt_new['sensor_ir']
    dt_new["nc_sens"] = calc.normalisation_constant(dt_new['pressure_interpolation'] , dt_new['sensor_t_abs'])
    # Additional features needed for fitting but not for prediction
    if fit:
        try:
            dt_new['ref_t_abs'] = calc.absolute_temperature(dt_new['ref_T'])
            # Normalisation constant
            dt_new["nc"] = calc.normalisation_constant(dt_new['ref_pressure'], dt_new['ref_t_abs'])
            # Number of molecule (not concentration) at current conditions  using ideal gas law
            #Dry to wet
            dt_new['ref_CO2'] = calc.dry_to_wet_molar_mixing(
                dt_new['ref_CO2_DRY'], dt_new['ref_H2O']/100)
            #Compensate for pressure and temperature
            dt_new['ref_CO2_comp'] = dt_new['ref_CO2'] * dt_new["nc"]
        except KeyError:
            dt_new["nc"] = calc.T0 / dt_new['sensor_t_abs'] 
            pass
    return dt_new

