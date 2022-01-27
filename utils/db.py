import pymysql
import sqlalchemy.engine as eng
import sqlalchemy as db 


#Method to upsert
def create_upsert_metod(meta):
	def method(table, conn, keys, data_iter):
		sql_table = db.Table(table.name, meta, autoload=True)
		insert_stmt = db.dialects.mysql.insert(sql_table).values([dict(zip(keys, data)) for data in data_iter])
		upsert_stmt = insert_stmt.on_duplicate_key_update({x.name: x for x in insert_stmt.inserted})
		try:
			conn.execute(upsert_stmt)
		except db.exc.SQLAlchemyError as e:
			raise db.exc.SQLAlchemyError(print(str(e)[1:1000]))
	return method