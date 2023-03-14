import influxdb
import keyring

from datetime import datetime as dt

def decentlab_client(host="swiss.co2.live", port=443,  database:str='main', path:str='/api/datasources/proxy/uid/main', ssl=True, token=''):
    return influxdb.InfluxDBClient(host=host,port=port,username=None,password=None,database=database, path=path, ssl=ssl ,verify_ssl=True,headers={'Authorization': f'Bearer {token}'})

def format_influxdb_timestamp(timestamp):
	return f'{dt.fromtimestamp(timestamp).isoformat()}Z'

def format_list_regex(codes):
	return f"{ '|'.join(map(lambda x: str(x),codes))}"




