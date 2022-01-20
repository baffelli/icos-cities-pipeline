import influxdb
import pandas as pd
import pymysql
import sqlalchemy.engine as eng
import sqlalchemy as db 


from datetime import datetime as dt
from datetime import timedelta 


#Method to upsert
def create_method(meta):
    def method(table, conn, keys, data_iter):
        sql_table = db.Table(table.name, meta, autoload=True)
        insert_stmt = db.dialects.mysql.insert(sql_table).values([dict(zip(keys, data)) for data in data_iter])
        upsert_stmt = insert_stmt.on_duplicate_key_update({x.name: x for x in insert_stmt.inserted})
        conn.execute(upsert_stmt)
    return method

import_all = False

#TODO handle secrets / tokens for influxdb
passw = "eyJrIjoiN2lSN1VJQjg1OUkwOWJyeTZUUFBiSVNDRjh5WGxGZTMiLCJuIjoic2ltb25lLmJhZmZlbGxpQGVtcGEuY2giLCJpZCI6MX0="

#Create influxdb client
client = influxdb.InfluxDBClient("swiss.co2.live",443,None,None,"main", path='/api/datasources/proxy/6',ssl=True,verify_ssl=True,headers={'Authorization': 'Bearer ' + passw})
engine = eng.create_engine('mysql+pymysql://emp-sql-cs1', connect_args={'read_default_file': '~/.my.cnf','read_default_group':'CarboSense_MySQL'})




#Connect to mariadb
with engine.connect() as con:

    #Query to list all LP8 sensors (1010 is the first deployed)
    lp8_query = """
    WITH max_dt AS
    (
        SELECT
            SensorUnit_ID,
            MAX(time) AS lt
        FROM lp8_data
        GROUP BY SensorUnit_ID
    )

    SELECT DISTINCT 
        su.SensorUnit_ID, UNIX_TIMESTAMP(su.Date_UTC_from), max_dt.lt
    FROM SensorUnits AS su
    JOIN Sensors AS sens ON sens.SensorUnit_ID = su.SensorUnit_ID
    JOIN max_dt ON max_dt.SensorUnit_ID = su.SensorUnit_ID
    WHERE TYPE = 'LP8' AND EXISTS (
        SELECT DISTINCT SensorUnit_ID
        FROM Deployment AS dep
        WHERE dep.SensorUnit_ID = sens.SensorUnit_ID
    ) AND sens.SensorUnit_ID >= 1010
    """
    sens = con.execute(lp8_query)

    for current_sens, start_ts, end_ts, in sens:
        start_ts_part = start_ts if import_all else int((dt.now() - timedelta(days=10)).timestamp())
        data_query = f"""SELECT * FROM  (SELECT MEAN("value") AS value FROM "measurements" WHERE node=~ /{current_sens}/ AND sensor =~ /senseair*|sensirion*|battery/ AND time > {start_ts_part}s GROUP BY "sensor", time(10m))"""
        print(f"Getting data for sensor {current_sens}")
        res = client.query(data_query, epoch='s')
        result = pd.DataFrame(res.get_points())
        if not result.empty:
            #Pivot data and rename columns
            result_wide = result.pivot(index='time', columns='sensor', values='value').reset_index().rename(columns=lambda x: x.replace("-","_"))
            result_wide['SensorUnit_ID'] = current_sens
            #Write to database
            print(f"Writing data into SQL database for sensor {current_sens}")
            with con.begin() as cb:
                meta = db.MetaData(con)
                method = create_method(meta)
                result_wide.to_sql("lp8_data", con, schema="CarboSense", index=False, if_exists='append', method=method)

