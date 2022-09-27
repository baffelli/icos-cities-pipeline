"""
This script processes the Picarro/Reference instrumente data specified
in the `config` argument (and stored in the database tables specified there) and performs the following steps:
- Apply calibration data
- Compute 10 minute average
- Compute wet CO2 concentration
"""
import sensorutils.db as db_utils
import sensorutils.files as fu


import argparse as ap

parser = ap.ArgumentParser()
parser.add_argument("config", type=str, help='Path of configuration file')
parser.add_argument("--all", type=bool, help='If set, reimport all data from all sources')
parser.add_argument("--temporary", type=bool, help='If set, only temporary copy data to destination (used for testin)')
args = parser.parse_args()