
import sqlalchemy.engine as eng
import sqlalchemy as db
import sqlalchemy.orm as orm
import sqlalchemy as sqa
from typing import Callable, Union
import pandas as pd
from .log import logger
import datetime as dt

from . import models as mods
"""
This module contains function used to interact with the metadata DB,
"""


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
            import pdb; pdb.set_trace()
            raise db.exc.SQLAlchemyError((str(e)[1:100]))
    return method


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
        sens = ses.query(mods.Sensor).filter(mods.Sensor.type == type).with_entities(mods.Sensor.id).distinct().subquery()
        dep = ses.query(mods.Deployment).with_entities(mods.Deployment.id).distinct().subquery()
        stmt = ses.query(sens).join(dep, dep.c.id == sens.c.id)
        with ses.connection() as con:
            df = pd.read_sql_query(stmt.statement, con)
    return df


def get_serialnumber(eng: eng.Engine, md: sqa.MetaData, id: Union[int, str], type: str) -> Union[str, int]:
    """
    Get the current sensor serialnumber for the given sensor id and sensor type
    """
    Session = orm.sessionmaker(eng)
    with Session() as ses:
        qr = ses.query(mods.Sensor).\
            filter((mods.Sensor.id == id) & (mods.Sensor.type == type) & (mods.Sensor.end > dt.datetime.now())).\
                with_entities(mods.Sensor.serial)
        id, *rest = qr.first()
    return id


    