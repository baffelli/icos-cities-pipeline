"""
Module to manage secrets (API keys, etc)
used to access different locations
"""
import yaml
from . import files as fu

def get_key(service: str, user:str=None, file:str=f'/newhome/{fu.get_user()}/secrets.yml') -> str:
    with open(file) as input:
        config = yaml.load(input)
    try:
        pw = config[service]['key']
    except KeyError as e:
        raise KeyError(f"There is no key for the service '{service}' in '{file}'")
    return pw