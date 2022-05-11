"""
Module to store the ORM class models
to represent database objects
"""
from dataclasses import dataclass
from . import base
import datetime as dt

from sqlalchemy import (Column, DateTime, Float, ForeignKey, Integer, String,
                        engine)
from sqlalchemy.orm import relationship

@dataclass
class Deployment(base.Base):
    """
    ORM class to represent a sensor deployment in the database
    """
    __tablename__ = "Deployment"
    __sa_dataclass_metadata_key__ = "sa"
    id: int = Column("SensorUnit_ID", String, primary_key=True)
    start: dt.datetime = Column("Date_UTC_from", DateTime, primary_key=True)
    end: dt.datetime = Column("Date_UTC_to", DateTime, primary_key=True)
    location: str = Column("LocationName", String)
    height: float = Column("HeightAboveGround", Float)
    inlet_height: float = Column("Inlet_HeightAboveGround", Float)

@dataclass
class Calibration(base.Base):
    """
    ORM class to represent a sensor calibration deployment in the database
    """
    __tablename__ = "Calibration"
    __sa_dataclass_metadata_key__ = "sa"
    id: int = Column("SensorUnit_ID", String, primary_key=True)
    location: str = Column("LocationName", String)
    start: dt.datetime = Column("Date_UTC_from", DateTime, primary_key=True)
    end: dt.datetime = Column("Date_UTC_to", DateTime, primary_key=True)
    mode: int = Column("CalMode", int)
    table: str = Column("DBTableNameRefData", String)

@dataclass
class Sensor(base.Base):
    """
    ORM class to represent a sensor in the database
    """
    __tablename__ = "Sensors"
    __sa_dataclass_metadata_key__ = "sa"
    id: int = Column("SensorUnit_ID", String, primary_key=True)
    serial: int = Column("Serialnumber", String)
    type: int = Column("Type", String, primary_key=True)
    start: dt.datetime = Column("Date_UTC_from", DateTime)
    end: dt.datetime = Column("Date_UTC_to", DateTime)
