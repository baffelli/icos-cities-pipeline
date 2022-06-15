"""
This script imports the picarro CRDS calibration parameters from these paths:
- K:\\Nabel\\Daten\\Stationen\\Kalibrierwerte Picarro G1301 DUE Test.xlsx
- G:\\503_Themen\\CarboSense\\Kalibrationen Picarros_clean.xlsx
After importing the data, it computes :obj:`sensorutils.data.CalibrationParameters` and serialises them in the database for later use.
To obtain one table with both validity and parameter values, use the following query 
```
SELECT
*
FROM calibration_parameters as cp
JOIN model_parameter AS mp ON cp.id = mp.model_id
```
"""
import sensorutils.db as db_utils
import sensorutils.files as fu
import sensorutils.data as du
import pathlib as pl
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sqa

import argparse as ap
parser = ap.ArgumentParser()
parser.add_argument('config', type=str)

args = parser.parse_args()

eng = db_utils.connect_to_metadata_db()

md = du.Base.metadata.create_all(eng, checkfirst=True)
du.Base.metadata.create_all(eng, checkfirst=True)
Session = sessionmaker(bind=eng)
session = Session() 
metadata = sqa.MetaData(bind=eng)
metadata.reflect()


for source in du.read_calibration_config(args.config):
    #Load calibration parameters
    cal_data = source.reader(source.get_path())
    cal_data['location'] = cal_data['location'] if 'location' in cal_data.columns else source.location
    cal_data['device'] = cal_data['device'] if 'device' in cal_data.columns else source.device
    #Replace CO2_DRY with CO2
    cal_data['compound'] = cal_data['compound'].replace('CO2_DRY', 'CO2')
    cal_params = du.make_calibration_parameters_table(cal_data.dropna().query("compound == 'CO2'"))
    #Serialise objects
    for cp in cal_params:
        session.add(cp)
        session.commit()

