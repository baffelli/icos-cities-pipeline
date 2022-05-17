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
import sensorutils.log as log

from datetime import datetime as dt
from datetime import timedelta 

import re


import pathlib as pl

import glob

from typing import Optional, List, Union, Dict


import argparse as ap

import sensorutils.command_line as cu

"""
This script is used to copy datasets from the various reference instruments into the database.
The configuration is specified in a yaml file, see the repository for an example
"""



def load_and_map(mapping: fu.SourceMapping, temporary: bool=False, import_all: bool=False) -> None:
    #Iterate over the mapping
    log.logger.info(f'Listing missing files for {mapping.dest}')
    #Get Missing files
    missing_files = mapping.list_files_missing_in_dest(backfill=5, all=import_all, group=mapping.dest.group)
    #Iterate over files
    for date in missing_files:
        log.logger.info(f'Transfering file for {mapping.dest} at date {date}')
        mapped_data = mapping.transfer_file(date, temporary=temporary)

parser = ap.ArgumentParser()
parser.add_argument("config", type=cu.path_or_config, help='Path of configuration file')
parser.add_argument("location", type=str, help='Import data for given location')
#parser.add_argument("dest", type='str', help='Destination table')
parser.add_argument("--import-all", type=bool, help='If set, reimport all data from all sources')
parser.add_argument("--temporary", type=bool, help='If set, only temporary copy data to destination (used for testin)')
args = parser.parse_args()
source_dest_mapping = fu.DataMappingFactory.create_mapping(**args.config[args.location])
eng = db_utils.connect_to_metadata_db()
source_dest_mapping.dest.attach_db(eng)
load_and_map(source_dest_mapping)
