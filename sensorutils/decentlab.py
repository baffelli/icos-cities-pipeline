import influxdb
import keyring


def decentlab_client(host="swiss.co2.live", port=443,  database='main', path='/api/datasources/proxy/6', ssl=True, token=''):
    return influxdb.InfluxDBClient(host=host,port=port,None,None,database=database, path=path, ssl=ssl ,verify_ssl=True,headers={'Authorization': f'Bearer  {token}'})

def format_influxdb_timestamp(timestamp):
	return f'{dt.fromtimestamp(timestamp).isoformat()}Z'

def format_list_regex(codes):
	return f"{ '|'.join(map(lambda x: str(x),codes))}"



