import pymysql
import sqlalchemy.engine as eng
import sqlalchemy as db 
from typing import Callable

"""
This module contains function used to interact with the metadata DB,
"""

def create_upsert_metod(meta: db.MetaData) -> Callable: 
	"""
	Closure that creates an upsert function to be passed as a method to `pandas.write_sql`.
	This is useful when we want to upsert data in a table in order to use ``ON DUPLICATE KEY UPDATE`` 
	and replace existing data instead of returning an error message.
	
	Parameters
	----------
	meta: sqlalchemy.MetaData
		The database  metadata as reflected by sqlalchemy. For more infos see `here <https://docs.sqlalchemy.org/en/14/core/metadata.html>`_
	
	Returns
	-------
	function
		The upsert functions to be passed to pandas function
	"""
	def method(table: db.Table, conn: db.engine.Connection, keys, data_iter):
		sql_table = db.Table(table.name, meta, autoload=True)
		insert_stmt = db.dialects.mysql.insert(sql_table).values([dict(zip(keys, data)) for data in data_iter])
		upsert_cols = {x.name: x for x in insert_stmt.inserted}
		upsert_cols_subset = {k:v for k,v in upsert_cols.items() if k in keys}
		upsert_stmt = insert_stmt.on_duplicate_key_update(upsert_cols_subset)
		try:
			conn.execute(upsert_stmt)
		except db.exc.SQLAlchemyError as e:
			raise db.exc.SQLAlchemyError((str(e)[1:1000]))
	return method


def connect_to_metadata_db(group:str='CarboSense_MySQL', conf_path:str="~/.my.cnf") -> eng.Engine:
	"""
	Connects to the ICOS-Cities/CarboSense MariaDB Database using
	the connection configuration stored in ``~/.my.cnf`.
	For more informations on how to prepare such a file, check `here <https://mariadb.com/kb/en/configuring-mariadb-with-option-files/>`_

	Parameters
	----------
	conf_path: str
		The path to the configuration file. Only needed if the file is not in the default location
	group: str
		The options group name for the desired connection. 	The configruation files should contain a section called `group` to be able to connect.


	Returns
	-------
		sqlachemy.engine.Engine
	"""
	return eng.create_engine('mysql+pymysql://', connect_args={'read_default_file': conf_path,'read_default_group':group})