"""
Transfers the meteo observations from the meteoswiss daily dumps
to the database into the table meteo_observations
"""
import argparse as ap
import sensorutils.files as fu
import sensorutils.db as du
import sensorutils.log as log
import pathlib as pl
import re
import pdb

parser = ap.ArgumentParser(description='Transfer meteo data from folder to database')

parser.add_argument('config', type=pl.Path, help='Datasource mapping configuration file')
parser.add_argument('base_path', type=pl.Path, help='Base path of the meteo files')
parser.add_argument('re', type=str, help='Regex to find meteo files')
parser.add_argument('--import_all', default=False, action='store_true', help='If set, perform bulk import, otherwise incremental import')

#Parse arguments
args = parser.parse_args()
#Connect to db
eng = du.connect_to_metadata_db()

#List all meteo data
mapping = fu.DataMappingFactory.read_config(args.config)
meteo_mapping = mapping['MCH']
meteo_mapping.dest.attach_db(eng)
missing = meteo_mapping.list_files_missing_in_dest(all=False)
for m in missing:
    log.logger.info(f"transfering meteo file for date {m}")
    meteo_mapping.transfer_file(m, temporary=False)