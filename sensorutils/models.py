"""
Module to store the ORM class models
to represent database objects
"""
from abc import ABC, ABCMeta
from dataclasses import dataclass, field
from tkinter.tix import Select
from sensorutils import data

import sqlalchemy
from . import base
import datetime as dt

from sqlalchemy import (Column, DateTime, Float, ForeignKey, Integer, String,
                        engine)
from sqlalchemy.orm import relationship, column_property, query_expression, aliased, object_session
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import func

import pandas as pd
import numpy as np

import statsmodels as sm

from statsmodels.regression import linear_model

import pathlib as pl

from typing import List, Union, Dict, Optional


@dataclass
class SensorDeploymentBase(object):
    """
    Base class for representing
    either Deployment or Calibration entries,
    as they use a very similar database format
    """
    id: int =  Column("SensorUnit_ID", Integer, primary_key=True)
    location: str = Column("LocationName", String)
    start: dt.datetime = Column("Date_UTC_from", DateTime, primary_key=True)
    end: dt.datetime = Column("Date_UTC_to", DateTime, primary_key=True)
    mode: int = Column(Integer)
    @property
    def next_cal(self) -> Optional[dt.datetime]:
        """
        Return the date of next (chamber) calibration
        """
        ses =  object_session(self)
        cls = self.__class__
        iq = sqlalchemy.select(
            cls,
        ).filter(cls.mode==2).cte()
        iq_as = aliased(cls, alias=iq)
        ft_dt = sqlalchemy.DateTime(data.NAT.to_pydatetime())
        stm = sqlalchemy.select(func.coalesce(iq_as.start, ft_dt)).filter(
            (iq_as.id == self.id) &  
            (iq_as.start > self.start)).order_by(iq_as.start.desc()).limit(1)
        final_stm = ses.scalar(func.coalesce(ses.scalar(stm), sqlalchemy.cast(data.NAT.to_pydatetime(), sqlalchemy.DateTime)))
        return final_stm
        
        

SensBase = declarative_base(cls=SensorDeploymentBase)

@dataclass
class Deployment(SensBase):
    """
    ORM class to represent a sensor deployment in the database
    """
    __tablename__ = "Deployment"
    __sa_dataclass_metadata_key__ = "sa"
    # id: int = Column("SensorUnit_ID", String, primary_key=True)
    # location: str = Column("LocationName", String)
    start: dt.datetime = Column("Date_UTC_from", DateTime, primary_key=True)
    end: dt.datetime = Column("Date_UTC_to", DateTime, primary_key=True)
    mode: Optional[int] = Column('cal_mode', Integer)
    height: float = Column("HeightAboveGround", Float)
    inlet_height: float = Column("Inlet_HeightAboveGround", Float)


@dataclass
class Calibration(SensBase):
    """
    ORM class to represent a sensor calibration deployment in the database
    """
    __tablename__ = "Calibration"
    __sa_dataclass_metadata_key__ = "sa"
    mode: int = Column("CalMode", Integer)
    table: str = Column("DBTableNameRefData", String)

@dataclass
class Sensor(base.Base):
    """
    ORM class to represent a sensor in the database
    """
    __tablename__ = "Sensors"
    __sa_dataclass_metadata_key__ = "sa"
    id: int = Column("SensorUnit_ID", String)
    serial: int = Column("Serialnumber", String, primary_key=True)
    type: int = Column("Type", String, primary_key=True)
    start: dt.datetime = Column("Date_UTC_from", DateTime)
    end: dt.datetime = Column("Date_UTC_to", DateTime)

@dataclass 
class Cylinder(base.Base):
    """
    ORM class to represent a gas cylinder
    """
    __tablename__ = "ref_gas_cylinder"
    __sa_dataclass_metadata_key__ = "sa"
    cylinder_id: str = Column("cylinder_id", String, primary_key=True)
    fill: int = Column("EMPA_fill_number", String)
    start: dt.datetime = Column("Date_UTC_from", DateTime, primary_key=True)
    end: dt.datetime = Column("Date_UTC_to", DateTime, primary_key=True)
    CO2: float  = Column(Float)
    H2O: float  = Column(Float)
    analysed: dt.datetime = Column(DateTime)
    pressure: float = Column(Float)

@dataclass
class CylinderDeployment(base.Base):
    """
    ORM class to represent a gas cylinder deployment
    to a certain location and time range
    """
    __tablename__ = "cylinder_deployment"
    __sa_dataclass_metadata_key__ = "sa"
    cylinder_id: str = Column("cylinder_id", String, primary_key=True)
    start: dt.datetime = Column("Date_UTC_from", DateTime, primary_key=True)
    end: dt.datetime = Column("Date_UTC_to", DateTime, primary_key=True)
    sensor_id: int = Column("SensorUnit_ID", Integer, primary_key=True)
    location: int = Column("LocationName", Integer)
    inlet: str = Column("inlet", String)
    cylinders: List[Cylinder] = relationship("Cylinder",  
    primaryjoin= lambda: (CylinderDeployment.cylinder_id == Cylinder.cylinder_id) & (CylinderDeployment.start > Cylinder.start),
    foreign_keys= lambda: Cylinder.cylinder_id)

@dataclass
class CylinderAnalysis(base.Base):
    """
    ORM class to represent the analyes of a given gas cylinder
    for a selected date
    """
    __tablename__ = "ref_gas_cylinder_analysis"
    __sa_dataclass_metadata_key__ = "sa"
    

@dataclass
class CalibrationParameter(base.Base):
    """
    Represents a single component (parameter)
    of a calibration model for the model with id `model_id` .
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
    id: int = Column(Integer, primary_key=True)
    model_id: int = Column(Integer)
    parameter: str = Column(String(64))
    value: float = Column(Float())


@dataclass
class CalibrationParameters(base.Base):
    """
    Simple class to represent calibration parameters
    for a given species and instrument.
    By using SQLalchemy the instances of this class are syncronised with 
    the database table with the name `calibration_parameters`
    Attributes
    ----------
    type: CalType
        The type of calibration
    species: str
        The species to be calibrated
    valid_from: date
        The start of the validity range of this calibration
    valid_to:  date
        The end of the validity range of this calibration
    computed: date
        The date of computation of this model
    device: str
        The device under calibration
    computation
    parameters: list of CalibrationParameter objects
        A list of parameters, one for each component in the model
    """
    __tablename__ = "calibration_parameters"
    __sa_dataclass_metadata_key__ = "sa"
    id: int = Column(Integer, primary_key=True, autoincrement=True)
    type: str = Column(String(64))
    species: str = Column(String(64))
    valid_from: dt.datetime = Column(DateTime())
    valid_to: dt.datetime = Column(DateTime())
    computed: dt.datetime = Column(DateTime())
    device: str = Column(String(64))
    parameters: List[CalibrationParameter] = relationship(
        CalibrationParameter, foreign_keys= lambda: CalibrationParameter.model_id, primaryjoin=lambda: CalibrationParameter.model_id == CalibrationParameters.id)

    def serialise(self) -> Dict:
        """
        Save the calibration parameters as a dict
        """
        return (self.__dict__)
    
    def to_statsmodel(self) -> sm.regression.linear_model.RegressionResultsWrapper:
        """
        Maps the model object to a :obj:statsmodel.regression.linear_model in order to use
        the object for prediction and fitting
        """
        #Create a dummymodel
        n = 10
        exog = pd.DataFrame(0, index = np.arange(0, n), columns = self.regressors(), dtype=np.float64, )
        endog = pd.DataFrame(0, index = np.arange(0, n), columns = [self.species],  dtype=np.float64)
        mod_obj = linear_model.OLS(endog=endog,exog=exog)
        params = pd.Series({s.parameter:s.value for s in self.parameters})
        fit_obj = linear_model.RegressionResults(model=mod_obj, params=params)
        return fit_obj
    
    def regressors(self) -> List[str]:
        """
        Returns the the models regressors as a list
        of names
        """
        return [s.parameter for s in self.parameters]

@dataclass
class ModelFitPerformance(base.Base):
    """
    ORM object to represent the model fit performance and serialise it
    in the database
    """
    __tablename__ = "calibration_performance"
    __sa_dataclass_metadata_key__ = "sa"
    id: int = Column(Integer, primary_key=True, autoincrement=True)
    model_id: int = Column(ForeignKey("calibration_parameters.id"))
    rmse: float = Column(Float)
    bias: float = Column(Float)
    correlation: float = Column(Float)
    
    def valid(self) -> bool:
        """
        Check if the calibration is valid
        """
        return np.isfinite(self.rmse) & np.isfinite(self.bias) & np.isfinite(self.correlation)

@dataclass
class TimeseriesData(object):
    """
    A base class to represent (grouped) timeseries data.
    The `time`attribute is assumed to be timestamps, the attirbute `id` represents
    the group which is either the sensor id or the location
    """
    __abstract__ = True
    id: Union[int, str]
    time: int 

@dataclass
class LP8Data(base.Base, TimeseriesData):
    """
    ORM object to represent the raw LP8 Data
    """
    __tablename__ = "lp8_data"
    __sa_dataclass_metadata_key__ = "sa"
    id: int = Column("SensorUnit_ID", Integer, primary_key=True, quote=False)
    time: int = Column("time", Integer, primary_key=True)
    battery: float = Column("battery", Float)
    senseair_lp8_temperature_last: float = Column(Float)
    senseair_lp8_temperature: float = Column(Float)
    sensirion_sht21_temperature: float = Column(Float)
    sensirion_sht21_temperature_last: float = Column(Float)
    sensirion_sht21_humidity: float = Column(Float)
    senseair_lp8_vcap2: float = Column(Float)
    senseair_lp8_vcap1: float = Column(Float)
    senseair_lp8_co2_filtered: float = Column(Float)
    senseair_lp8_co2: float = Column(Float)
    senseair_lp8_ir: float = Column(Float)
    senseair_lp8_ir_filtered: float = Column(Float)
    senseair_lp8_ir_last: float = Column(Float)
    senseair_lp8_status: int = Column(Integer)

@dataclass
class HPPData(base.Base, TimeseriesData):
    """
    ORM object to represent the raw LP8 Data
    """
    __tablename__ = "hpp_data"
    __sa_dataclass_metadata_key__ = "sa"
    id: int = Column("SensorUnit_ID", Integer, primary_key=True)
    time: int = Column("time", Integer, primary_key=True)
    battery: float = Column("battery", Float)
    calibration_a: int  = Column(Integer)
    calibration_b: int  = Column(Integer)
    senseair_hpp_co2_filtered: float = Column(Float)
    senseair_hpp_ir_signal: float = Column(Float)
    senseair_hpp_lpl_signal: float = Column(Float)
    senseair_hpp_ntc5_diff_temp: float = Column(Float)
    senseair_hpp_ntc6_se_temp: float = Column(Float)
    senseair_hpp_pressure_filtered: float = Column(Float)
    senseair_hpp_status: int = Column(Integer)
    senseair_hpp_temperature_detector: float = Column(Float)
    senseair_hpp_temperature_mcu: float = Column(Float)
    sensirion_sht21_temperature: float = Column(Float)
    sensirion_sht21_humidity: float = Column(Float)


@dataclass
class PicarroData(base.Base, TimeseriesData):
    """
    ORM object to represent the raw Picarro Data
    """
    __tablename__ = "picarro_data"
    __sa_dataclass_metadata_key__ = "sa"
    id: str = Column("LocationName", String, primary_key=True)
    time: int = Column("timestamp", Integer, primary_key=True)
    battery: float = Column("battery", Float)
    valvepos: float = Column("valvepos", Float)
    CO2: float = Column(Float)
    CO2_F: int = Column(Integer)
    CO2_DRY: float = Column(Float)
    CO2_DRY_F: int = Column(Integer)
    H2O: float = Column(Float)
    H2O_F: int = Column(Integer)
    pressure: float = Column(Float)
    pressure_F: int = Column(Integer)
    RH: float = Column(Float)
    RH_F: int = Column(Integer)
    calibration_mode: str = Column(String)
    chamber_status: int = Column(Integer)

@dataclass
class Level2Data(base.Base, TimeseriesData):
    __tablename__ = "co2_level2"
    __sa_dataclass_metadata_key__ = "sa"
    id: str = Column("sensor_id", String, primary_key=True)
    location: str = Column("location", String, primary_key=True)
    time: int = Column("timestamp", Integer, primary_key=True)
    model_id: int = Column("calibration_model_id", Integer)
    CO2: float = Column(Float)
    H2O: float = Column(Float)
    temperature: float = Column(Float)
    relative_humidity: float = Column(Float)
    pressure: float = Column(Float)
    inlet: float = Column(String)