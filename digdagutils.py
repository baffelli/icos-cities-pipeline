from asyncio import all_tasks
from os import lstat
import sensorutils.db as db_utils
import yaml
import pathlib as pl
import digdag
import datetime as dt
import os

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


    eng = db_utils.connect_to_metadata_db()
    ses = orm.sessionmaker(bind=eng)
    start = dt.datetime.fromisoformat(start)
    with ses() as session:
        locs = db_utils.list_locations_with_deployment(session, start=start)
    loc_names = [l.id for l,*_ in locs]
    print(start)
    print(type(start))
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


def get_username(are: Any) -> None:
    """
    Stores the current username in the env
    variable `usernam 
    """
    lg = os.getlogin()
    digdag.env.store({'username': lg})

def configure_paths(base: str) -> None:
    """
    Configure paths used in subsequent tasks
    """
    lg = os.getlogin()
    username = lg
    proj_folder = f"{base}"
    base_folder = f'{base}/Software'
    data_folder = f'{base}/Data'
    icos_folder = f'/mnt/{lg}/ICOS-Cities'
    plot_folder = f'{username}/Network/Processing/'
    reference_config = f'{base_folder}/config/picarro_mapping.yml'
    cal_config =  f'{base_folder}/config/calibration_mapping.yml'
    digdag.env.store({k:v for k,v in locals().items()})


def show_params():
    print(digdag.env.params)