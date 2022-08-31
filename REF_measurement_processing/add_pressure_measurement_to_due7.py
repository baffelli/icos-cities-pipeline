"""
Adds pressure measurements to the `picarro_data` table at the location `DUE7` by copying the pressure
in `DUE1` assuming the atmospheric pressure in Dubendorf to be the same in all buildings
"""

import argparse as ap
from numpy import source
import sensorutils.db as db_utils
import sensorutils.files as fu
import sensorutils.data as du
import sqlalchemy as sqa
parser = ap.ArgumentParser(description='Add pressure data for a location by copying it from another')
parser.add_argument('source', type=str, help='Source location')
parser.add_argument('dest', type=str, help='Destination location')
parser.add_argument('--table', default='picarro_data', type=str, help='Destination table')
args = parser.parse_args()


eng = db_utils.connect_to_metadata_db()
md = sqa.MetaData(bind=eng)
md.reflect()
tb = md.tables[args.table]
#Query for the source table
source_query = sqa.select(tb.c.pressure, tb.c.timestamp).where((tb.c['LocationName'] == args.source)).subquery()



update_stmt = tb.update().values(pressure = sqa.select(source_query.c.pressure).where(source_query.c.timestamp == tb.c.timestamp).scalar_subquery()).where(tb.c['LocationName']==args.dest)

with eng.connect() as con:
    con.execute(update_stmt.compile())