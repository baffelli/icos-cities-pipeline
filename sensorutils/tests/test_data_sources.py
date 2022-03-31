import imp
import unittest
import tempfile as tf
from sensorutils import files as fu
from sensorutils import db as du
import json
import yaml
import pandas as pd
class TestDataSourceMapping(unittest.TestCase):

    def setUp(self):
        self.mapping = {"DUE":
        {"source": {
            "type":"file",
            "re": "((DUE Test .*)|(CarboSens.*)|(DUE O3 .*))\\.(csv|CSV)",
            "path": "DUE",
            "date_from":"2018-01-01 00:00:00",
            "na":""
        },
        "dest": {
            "type": "DB",
            "db_prefix": "CarboSense_MySQL",
            "path": "NABEL_DUE",
            "date_column":"timestamp",
            "date_from":"2018-01-01 00:00:00",
            "na":-999
        },
        "columns": [
            {"name":"O3", "query":"O3", "datatype":"float", "na":-999}, 
            {"name":"O3_F", "query":"CASE WHEN O3_F IS NOT NULL THEN 1 ELSE 0 END", "datatype":"int", "na":0},
            {"name":"timestamp", "query":"CAST(strftime('%s', date) as integer)", "datatype":"int", "na":0}
            ]
        },
        "GIMM": {
        "source": {
            "type":"DB",
            "db_prefix": "empaGSN",
            "date_column":"timed / 1e3",
            "path":"gimmiz_1min_cal",
            "date_from":"2018-01-01 00:00:00",
            "na":""
        },
        "dest": {
            "type": "DB",
            "db_prefix": "CarboSense_MySQL",
            "path": "UNIBE_GIMM",
            "date_column":"timestamp",
            "date_from":"2018-01-01 00:00:00",
            "na":-999
        },
        "columns": [
            {"name":"CO2_DRY", "query":"CO2_DRY", "datatype":"float", "na":-999}, 
            {"name": "CO2_DRY_F", "query":  "1 - (((VALVEPOS = 5) OR (CO2_DRY IS NULL)))", "datatype": "int", "na": 0},
            {"name":"timestamp", "query":"timed / 1000", "datatype":"int", "na":0}
            ]
        }}
        self.engine = du.connect_to_metadata_db()
    
    def temp_mapping(self, type='json'):
        with tf.NamedTemporaryFile(mode='w+t') as cfg:
            json.dump(self.mapping, cfg) if type=='json' else yaml.dump(self.mapping, cfg)
            cfg.flush()
            mapping = fu.DataMappingFactory.read_config(cfg.name, type=type)
        return mapping
    
    def test_check_db(self):
        mapping_ym = self.temp_mapping(type='yaml')
        self.assertRaises(AttributeError, mapping_ym['GIMM'].source.list_files)

    def test_load_mapping(self):
        mapping_js = self.temp_mapping(type='json')
        mapping_ym = self.temp_mapping(type='yaml')
        self.assertEqual(len(mapping_js), 2)
        self.assertEqual(mapping_js, mapping_ym)
    
    def test_column_mapping(self):
        mapping = self.temp_mapping()
        source = mapping['GIMM']
        query = source.mapping_to_query()
        self.assertEqual(str(query[0]), "CO2_DRY AS CO2_DRY") 
        self.assertEqual(str(query[2]), "timed / 1000 AS timestamp") 
    
    def test_list_files(self):
        mapping = self.temp_mapping()
        fl = mapping['DUE']
        fl.connect_all_db()
        ds = fl.source.list_files()
        self.assertIsInstance(ds, pd.DataFrame)
    
    def test_list_db_files(self):
        mapping = self.temp_mapping()
        source = mapping['GIMM'].source
        source.attach_db_from_config()
        fl = mapping['GIMM'].source.list_files()
        self.assertTrue(len(fl) > 0)

    def test_map_file(self):
        mapping = self.temp_mapping()
        source = mapping['GIMM'].source
        source.attach_db_from_config()
        mf = source.list_files()
        data = source.read_file(mf.iloc[0].date)
        mapped_data = mapping['GIMM'].map_file(data)
        
        self.assertTrue(len(set(mapped_data.columns).intersection(['CO2_DRY','timestamp'])) !=0)


    def test_map_file_flag(self):
        mapping = self.temp_mapping()
        mapping = mapping['DUE']
        mapping.connect_all_db()
        mf = mapping.source.list_files()
        data = mapping.source.read_file(mf.iloc[0].date)
        mapped_data = mapping.map_file(data)
        self.assertTrue(len(set(mapped_data.columns).intersection(['CO2_DRY','timestamp'])) !=0)




    def test_list_files_from_db(self):
        mapping = self.temp_mapping()
        source = mapping['GIMM'].source
        source.attach_db_from_config()
        import pdb; pdb.set_trace()
        fl = source.list_files()
        self.assertIsInstance(fl, pd.DataFrame)

    def test_loading_db_file(self):
        mapping = self.temp_mapping()
        mp = mapping['DUE']
        mp.connect_all_db()
        mf = mp.dest.list_files()
        data = mp.dest.read_file(mf.iloc[0].date)
        self.assertTrue(len(set(data.columns).intersection(['CO2'])) != 0)
    
    def test_loading_csv_file(self):
        mapping = self.temp_mapping()
        mp = mapping['DUE']
        mp.connect_all_db()
        source = mp.source
        mf = source.list_files()
        data = source.read_file(mf.iloc[0].date)
        self.assertIsInstance(data, pd.DataFrame)
    
    def test_missing_files(self):
        mapping = self.temp_mapping()
        mp = mapping['DUE']
        mp.connect_all_db()
        missing = mp.list_files_missing_in_dest()
        self.assertIsInstance(missing, list)

    def test_write_file(self):
        mapping = self.temp_mapping()
        mp = mapping['DUE']
        mp.connect_all_db()
        source = mp.source
        dest = mp.dest
        mf = source.list_files()
        data = source.read_file(mf.iloc[0].date)
        data_mapped = mp.map_file(data)
        affected = dest.write_file(data_mapped, temporary=True)
        self.assertTrue((data_mapped['timestamp']==affected['timestamp']).all())




if __name__ == '__main__':
    unittest.main()