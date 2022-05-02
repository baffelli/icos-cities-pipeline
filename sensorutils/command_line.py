
from typing import Union
import pathlib as pl
import yaml

def path_or_config(arg: Union[pl.Path, dict, str]) -> dict:
    """
    Util to parse an argument as yaml or to take a dict and return as it is.
    Used to allow different type of command type utilities
    """
    match arg:
        case pl.Path() as pt:
            res = load_yaml(pt)
        case str(x):
            pat = pl.Path(x)
            res = load_yaml(pat)
        case {**rest} as mapping:
                res = rest
        case _:
            raise TypeError("Input is neither a path to a YAML file nor a dictionary")
    return res

def load_yaml(pt: pl.Path) -> dict:
    with pt.open('r') as input:
        res = yaml.load(input, yaml.Loader)
    return  res