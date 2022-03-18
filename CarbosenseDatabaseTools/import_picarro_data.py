from locale import strcoll
from numpy import True_
import pandas as pd
import pymysql
import sqlalchemy.engine as eng
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sqa
import sqlalchemy as db 
import itertools as ito

import sensorutils.decentlab as dl
import sensorutils.db as db_utils
import sensorutils.files as fu
import pathlib as pl


from datetime import datetime as dt
from datetime import timedelta 

import re


import pathlib as pl

import glob

from typing import Optional, List, Union


import argparse as ap

"""
This script is used to copy datasets from the various reference instruments into the database.
The configuration is specified in a yaml file, see the repository for an example
"""



def load_and_map(source_dest_mapping: List[fu.SourceMapping], temporary: bool=False) -> None:
    #Iterate over the mapping
    for mapping in source_dest_mapping:
        #Get Missing files
        missing_files = mapping.list_files_missing_in_dest(backfill=5)
        #Iterate over files
        for date in missing_files:
            mapped_data = mapping.transfer_file(date, temporary=temporary)

parser = ap.ArgumentParser()
parser.add_argument("config", type=str, help='Path of configuration file')
parser.add_argument("--all", type=bool, help='If set, reimport all data from all sources')
parser.add_argument("--temporary", type=bool, help='If set, only temporary copy data to destination (used for testin)')
args = parser.parse_args()

source_dest_mapping = fu.DataMappingFactory.read_config(args.config, temporary=args.temporary)

load_and_map(source_dest_mapping)
