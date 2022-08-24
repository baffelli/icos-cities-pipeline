
import sqlalchemy.engine as eng
import pandas as pd
from pathlib import Path
import numpy as np

import sqlalchemy.engine as eng
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sqa
import datetime as dt
import re 

#Create DB engine
engine = eng.create_engine('mysql+pymysql://emp-sql-cs1', connect_args={'read_default_file': '~/.my.cnf','read_default_group':'CarboSense_MySQL'})


base_path = Path("~/icos-cities-mid-cost/data")


def get_available_data_in_db(con):
    all_hpp_query = "SELECT DISTINCT CAST(FROM_UNIXTIME(time) AS DATE) AS date, MIN(time) AS first_of_day, MAX(time) AS last_of_day FROM hpp_data WHERE YEAR(CAST(FROM_UNIXTIME(time) AS DATE)) BETWEEN 2019 AND 2020 GROUP BY date"
    dt = pd.read_sql(all_hpp_query, con)
    dt.date = pd.to_datetime(dt.date)
    return dt



def get_availabele_data_in_files(base_path):
    paths = base_path.expanduser().glob("hpp*.csv")
    pattern = re.compile(r'\d{8}')
    return pd.DataFrame([(p, dt.datetime.strptime(re.search(pattern, p.name).group(0),'%Y%m%d')) for p in base_path.expanduser().glob("hpp*.csv")], columns=['path','date'])

def get_hpp_data(con, params, chunksize=1000):
    field_types = {
        "SensorUnit_ID": np.int, 
        "time": np.int64, 
        "battery": np.float32, 
        "calibration": np.int, 
        "senseair_hpp_co2_filtered": np.float32, 
        "senseair_hpp_ir_signal": np.float32, 
        "senseair_hpp_lpl_signal": np.float32, 
        "senseair_hpp_ntc5_diff_temp": np.float32, 
        "senseair_hpp_ntc6_se_temp": np.float32, 
        "senseair_hpp_pressure_filtered": np.float32, 
        "senseair_hpp_status": np.float32, 
        "senseair_hpp_temperature_detector": np.float32, 
        "senseair_hpp_temperature_mcu": np.float32, 
        "sensirion_sht21_humidity": np.float32, 
        "sensirion_sht21_temperature": np.float32
    }
    return pd.read_sql("select * from CarboSense.hpp_data WHERE time BETWEEN %s AND %s", con, params=params)



field_types = {
    "SensorUnit_ID": pd.Int32Dtype(), 
    "time": pd.Int64Dtype(), 
    "battery": np.float32, 
    "calibration": pd.Int64Dtype(), 
    "senseair_hpp_co2_filtered": np.float32, 
    "senseair_hpp_ir_signal": np.float32, 
    "senseair_hpp_lpl_signal": np.float32, 
    "senseair_hpp_ntc5_diff_temp": np.float32, 
    "senseair_hpp_ntc6_se_temp": np.float32, 
    "senseair_hpp_pressure_filtered": np.float32, 
    "senseair_hpp_status": np.float32, 
    "senseair_hpp_temperature_detector": np.float32, 
    "senseair_hpp_temperature_mcu": np.float32, 
    "sensirion_sht21_humidity": np.float32, 
    "sensirion_sht21_temperature": np.float32
}


available_files = get_availabele_data_in_files(base_path)
with engine.connect() as con:
    #Read all available data
    available_in_db = get_available_data_in_db(con)
    #merge
    to_load = available_in_db.merge(available_files, how='outer', indicator=True)
    for row in to_load[to_load['_merge']=='left_only'].itertuples():
        hpd = get_hpp_data(con , [row.first_of_day, row.last_of_day]).set_index('time')
        of = Path(base_path) / f"hpp_{row.date.strftime('%Y%m%d')}.csv"
        hpd.to_csv(of, index=False)
    
