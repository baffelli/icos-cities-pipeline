from asyncio import all_tasks
from os import lstat
import sensorutils.db as db_utils
import yaml
import pathlib as pl
import digdag
import datetime as dt


from sqlalchemy import orm

from typing import Any, Optional

def list_sensors(dest:str) -> None:
    """
    List all available sensors in the database and writes  them
    in the digdag env in the varialbe `dest`
    """
    con = db_utils.connect_to_metadata_db()
    all_types = ['LP8', 'HPP']
    lst = {sens: list(db_utils.list_all_sensor_ids(sens, con)['SensorUnit_ID']) for sens in all_types}
    env = {}
    env[dest] = lst
    print(dest)
    digdag.env.store(env)

def list_locations(dest:str, start: Optional[dt.datetime] = None) -> None:
    """
    List all available locations in the database that had at least
    one deployment and write it in the digdag env in the variable `dest`
    """
    print("a")
    eng = db_utils.connect_to_metadata_db()
    ses = orm.sessionmaker(bind=eng)
    with ses() as session:
        locs = db_utils.list_locations_with_deployment(session, start=start)
    loc_names = [l.id for l,*_ in locs]
    env = {}
    env[dest] = loc_names
    digdag.env.store(env)


def store_env(param:Any, key:str) -> None:
    pass

def load_config(file_path: pl.Path, var_name: str) -> None:
    """
    Load a yaml configuration file from `file_path` and store
    it the digdag env variable `var_name`
    """
    with open(file_path, 'r') as rf:
        config = yaml.load(rf, yaml.BaseLoader)
    env = {}
    env[var_name] = config
    print(env)
    digdag.env.store(env)