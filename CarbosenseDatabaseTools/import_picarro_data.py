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





def load_and_map(source_dest_mapping: List[fu.SourceMapping]) -> None:
    #Iterate over the mapping
    for mapping in source_dest_mapping:
        #Get Missing files
        missing_files = mapping.list_files_missing_in_dest(backfill=5)
        #Iterate over files
        import pdb; pdb.set_trace()

        for date in missing_files:
            mapped_data = mapping.transfer_file(date, temporary=True)
            import pdb; pdb.set_trace()

parser = ap.ArgumentParser()
parser.add_argument("config", type=str)
args = parser.parse_args()

source_dest_mapping = fu.DataMappingFactory.read_config(args.config)

load_and_map(source_dest_mapping)
