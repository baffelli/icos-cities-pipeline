
import sqlalchemy.engine as eng
import sqlalchemy as db
import sqlalchemy.orm as orm
import sqlalchemy as sqa
from typing import Callable, Union, Optional, List
import pandas as pd
import datetime as dt
import sqlite3 as sqllite

from sensorutils import models as mods
from sensorutils.log import logger

"""
This module contains function used to interact with the metadata DB,
"""

def get_first_deployment_at_location(session: orm.Session, id: str) -> mods.Deployment:
    """
    Given a certain location id, returns the first deployment at that location
    """
    qr = sqa.select(mods.Deployment).where(mods.Deployment.location == id).order_by(mods.Deployment.location.asc())
    return session.execute(qr).first()


def list_locations_with_deployment(session: orm.Session, start: Optional[dt.datetime] = None) -> List[mods.Location]:
    """
    List all locations in the database that had at least one deployment
    in :obj:`sensorutils.models.Deployment`
    """
    fls = (mods.Deployment.location == mods.Location.id, )
    if start is not None:
        fls = fls + (mods.Deployment.start > start,)
    qr = sqa.select(mods.Location).filter(
        sqa.select(mods.Deployment).filter(*fls).exists()
    )
    return session.execute(qr).all()

def create_upsert_metod(meta: db.MetaData) -> Callable:
    """
    Closure that creates an upsert function to be passed as a method to `pandas.write_sql`.
    This is useful when we want to upsert data in a table in order to use ``ON DUPLICATE KEY UPDATE`` 
    and replace existing data instead of returning an error message.

    Parameters
    ----------
    meta: sqlalchemy.MetaData
            The database  metadata as reflected by sqlalchemy. For more infos see `here <https://docs.sqlalchemy.org/en/14/core/metadata.html>`_

    Returns
    -------
    function
            The upsert functions to be passed to pandas function
    """
    def method(table: db.Table, conn: db.engine.Connection, keys, data_iter):
        sql_table = db.Table(table.name, meta, autoload=True)
        insert_stmt = db.dialects.mysql.insert(sql_table).values(
            [dict(zip(keys, data)) for data in data_iter])
        upsert_cols = {x.name: x for x in insert_stmt.inserted}
        upsert_cols_subset = {k: v for k,
                              v in upsert_cols.items() if k in keys}
        upsert_stmt = insert_stmt.on_duplicate_key_update(upsert_cols_subset)
        try:
            conn.execute(upsert_stmt)
        except db.exc.SQLAlchemyError as e:
            raise db.exc.SQLAlchemyError((str(e)[1:100]))
    return method

#TODO make it possible to use an alternate db e.g SQLLITE or duckdb
#TODO make it use the configuration from secrets.yaml
def connect_to_metadata_db(group: str = 'CarboSense_MySQL', conf_path: str = "~/.my.cnf") -> eng.Engine:
    """
    Connects to the ICOS-Cities/CarboSense MariaDB Database using
    the connection configuration stored in ``~/.my.cnf`.
    For more informations on how to prepare such a file, check `here <https://mariadb.com/kb/en/configuring-mariadb-with-option-files/>`_

    Parameters
    ----------
    conf_path: str
            The path to the configuration file. Only needed if the file is not in the default location
    group: str
            The options group name for the desired connection. 	The configruation files should contain a section called `group` to be able to connect.


    Returns
    -------
            sqlachemy.engine.Engine
    """
    logger.debug(f'Attempting connection to db using config file {conf_path} and configuration group {group}')
    engine = eng.create_engine('mariadb+pymysql://', connect_args={'read_default_file': conf_path, 'read_default_group': group})
    @sqa.event.listens_for(engine, "connect", insert=True)
    def connect(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        tz = '+00:00'
        logger.info(f'Setting session timezone to {tz}')
        cursor.execute("SET sql_mode = 'ANSI_QUOTES'")
        cursor.execute(f"SET time_zone = '{tz}'")
    logger.debug(f'Succesfully connected to db')
    return engine


def list_all_sensor_ids(type: str, eng: eng.Engine) -> pd.DataFrame:
    """
    List all sensors of a certain type that were previously deployed. 
    It looks up for the sensor types in the table given by the ORM mapping class :obj:`sensorutils.models.Sensor` and for
    deployment information in :obj:`sensorutils.models.Deployment`
    """
    md = db.MetaData(bind=eng)
    md.reflect()
    Session = orm.sessionmaker(eng)
    with Session() as ses:
        type_sq = ses.query(mods.Sensor).filter(mods.Sensor.type == type)
        if type == 'HPP':
            type_sq_final = type_sq.filter(mods.Sensor.id >= 400)
        else:
            type_sq_final = type_sq
        
        sens = type_sq_final.with_entities(mods.Sensor.id).distinct().subquery()
        dep = ses.query(mods.Deployment).with_entities(mods.Deployment.id).distinct().subquery()
        stmt = ses.query(sens).join(dep, dep.c.id == sens.c.id)
        with ses.connection() as con:
            df = pd.read_sql_query(stmt.statement, con)
    return df


def get_serialnumber(eng: eng.Engine, id: Union[int, str], type: str, start: Optional[dt.datetime], end: Optional[dt.datetime]) -> Optional[Union[str, int]]:
    """
    Get the  serialnumber for the given sensor id and sensor type. If `start` and `end` are set,
    find the serial number of the given date period.
    """
    Session = orm.sessionmaker(eng)
    with Session() as ses:
        qr = sqa.select(mods.Sensor).\
            filter((mods.Sensor.id == id) & (mods.Sensor.type == type))
        if start and end:
            qr_when = qr.filter((mods.Sensor.end >= end) & (mods.Sensor.start <= start))
        else: 
           qr_when = qr.filter(mods.Sensor.end > dt.datetime.now())
        qr_final = qr_when.with_only_columns(mods.Sensor.serial)
        id = ses.execute(qr_final).first()
        if id is None:
            return None
        else:
            return id[0]


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


def alter_all_dates(eng: eng.Engine):
    """
    For all tables where date_UTC_to is 2100-01-01, replace with NULL
    """