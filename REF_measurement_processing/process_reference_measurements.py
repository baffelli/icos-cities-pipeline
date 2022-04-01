"""
This script processes the PICARRO data on the database
The table and data to configure are specified as a yaml file passed
as a command line argument.
The script applies the following processing:
- Compute the 10 minute average
- Apply the calibration parameters
- Compute wet values (when not available)
"""
import sensorutils.db as db_utils
import sensorutils.files as fu
import sensorutils.data as du
import pathlib as pl
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sqa

import datetime as dt
import argparse as ap
parser = ap.ArgumentParser()
parser.add_argument('config', type=str)



args = parser.parse_args()

#Helper
def apply_calibration(table:sqa.Table, session:sqa.orm.Session, species:str, target_species:str, zero:float, span:float, valid_from:dt.datetime, valid_to:dt.datetime, temp:bool=True) -> None:
    """
    Apply the given calibration parameters to the table
    """
    ud = {target_species: table.columns[species]  * span + zero}
    cs = ['timestamp', species, target_species]

    od = session.query(table).filter(table.columns.timestamp.between(valid_from.timestamp(), valid_to.timestamp()))
    orig_data = pd.read_sql(od.statement, eng)

    us = table.update().values(**ud).where(table.columns.timestamp.between(valid_from.timestamp(), valid_to.timestamp()))
    res = us.execute()
    modif_data = pd.read_sql(od.statement, eng)
    print(orig_data[cs] - modif_data[cs])
    if temp:
        session.rollback()

    


# Load processing configuration
config = fu.read_picarro_processing_configuration(args.config)
# Connect to db and reflect metadata
eng = db_utils.connect_to_metadata_db()
Session = sessionmaker(bind=eng)
session = Session()
metadata = sqa.MetaData(bind=eng)
metadata.reflect()
#Represent tables
cp = sqa.Table('calibration_parameters', metadata, autoload_with=eng)
mp = sqa.Table('model_parameter', metadata, autoload_with=eng)
dep = sqa.Table('Deployment', metadata, autoload_with=eng)
sens = sqa.Table('Sensors', metadata, autoload_with=eng)

#Sen

# Iterate over all configuration (representing different tables) and apply processing
for table in config:
    #Table in the database representing this species
    db_table = metadata.tables[table.source]

    # Get calibration parameters
    if not table.calibrated:
        cal_params = session.query(cp.columns.valid_from, cp.columns.species,cp.columns.valid_to,cp.columns.device,mp).join(mp).subquery()
        pq = (session.query(dep.columns.LocationName, dep.columns.SensorUnit_ID,cal_params).filter(dep.columns.LocationName == table.location).
              join(sens, sens.columns.SensorUnit_ID == dep.columns.SensorUnit_ID).
              join(cal_params, cal_params.columns.device  == sens.columns.Serialnumber)
              ).filter((sens.columns.Type == 'picarro') & (cal_params.columns.species == 'CO2') & (dep.columns.Date_UTC_from > sens.columns.Date_UTC_from) & (cal_params.columns.valid_from.between(dep.columns.Date_UTC_from, dep.columns.Date_UTC_to)) ).order_by(cal_params.columns.valid_from)
        print(pq.statement.compile(compile_kwargs={"literal_binds": True}))
        #Get the parameters as pandas dataframe
        pm = pd.read_sql_query(pq.statement, eng)
        cp_wide = pd.pivot_table(pm, index=['device','valid_from','valid_to','LocationName'], columns='parameter', values='value')
        #Iterate over parameters
        for r in cp_wide.reset_index().itertuples():
            apply_calibration(db_table, session, table.CO2, table.CO2_DRY, r.Intercept, r.CO2, r.valid_from, r.valid_to )
  
