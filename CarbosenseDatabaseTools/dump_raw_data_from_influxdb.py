import influxdb
import pandas as pd
import pymysql
from sqlalchemy.engine.url import URL


#TODO handle secrets / tokens for influxdb
passw = "eyJrIjoiN2lSN1VJQjg1OUkwOWJyeTZUUFBiSVNDRjh5WGxGZTMiLCJuIjoic2ltb25lLmJhZmZlbGxpQGVtcGEuY2giLCJpZCI6MX0="

#Create influxdb client
client = influxdb.InfluxDBClient("swiss.co2.live",443,None,None,"main", path='/api/datasources/proxy/6',ssl=True,verify_ssl=True,headers={'Authorization': 'Bearer ' + passw})




try:
    #Connect to mariadb
    con = pymysql.connect(read_default_file="~/.my.cnf", read_default_group="CarboSense_MySQL",charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    #Query to list all LP8 sensors (1010 is the first deployed)
    lp8_query = "SELECT DISTINCT SensorUnit_ID FROM Sensors AS sens WHERE Type = 'LP8' AND EXISTS (SELECT DISTINCT SensorUnit_ID FROM Deployment AS dep WHERE dep.SensorUnit_ID = sens.SensorUnit_ID) AND SensorUnit_ID >= 1010"
    with con.cursor() as cur:
        cur.execute(lp8_query)
        sens = cur.fetchall()
    for current_sens in sens:
        data_query = f"""SELECT * FROM  (SELECT MEAN("value") AS value FROM "measurements" WHERE node=~ /{current_sens['SensorUnit_ID']}/ AND sensor =~ /senseair*|sensirion*|battery/  GROUP BY "sensor", time(10m))"""
        res = client.query(data_query)
        import pdb; pdb.set_trace()
        result = pd.DataFrame(res.get_points())
        #Pivot data and rename columns
        result_wide = result.pivot(index='time', columns='sensor', values='value').reset_index().rename(columns=lambda x: x.replace("-","_"))
        #Write to database
        result_wide.to_sql("lp8_data", con, schema="CarboSense", index=False)



finally:
    con.close()
