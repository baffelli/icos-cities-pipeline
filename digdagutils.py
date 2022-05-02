from asyncio import all_tasks
from os import lstat
import sensorutils.db as db_utils
import yaml
import pathlib as pl
import digdag


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