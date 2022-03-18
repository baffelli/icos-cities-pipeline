import imp
import unittest
import tempfile as tf
from sensorutils import files as fu
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
            "path": "NABEL_DUE",
            "date_column":"timestamp",
            "db_prefix":"CarboSense_MySQL",
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
            "db_prefix":"empaGSN",
            "date_column":"timed / 1e3",
            "path":"gimmiz_1min_cal",
            "date_from":"2018-01-01 00:00:00",
            "na":""
        },
        "dest": {
            "type": "DB",
            "path": "UNIBE_GIMM",
            "date_column":"timestamp",
            "db_prefix":"CarboSense_MySQL",
            "date_from":"2018-01-01 00:00:00",
            "na":-999
        },
        "columns": [
            {"name":"CO2_DRY", "query":"CO2_DRY", "datatype":"float", "na":-999}, 
            {"name": "CO2_DRY_F", "query":  "1 - (((VALVEPOS = 5) OR (CO2_DRY IS NULL)))", "datatype": "int", "na": 0},
            {"name":"timestamp", "query":"timed / 1000", "datatype":"int", "na":0}
            ]
        }}
    
    def temp_mapping(self, type='json'):
        with tf.NamedTemporaryFile(mode='w+t') as cfg:
            json.dump(self.mapping, cfg) if type=='json' else yaml.dump(self.mapping, cfg)
            cfg.flush()
            mapping = fu.DataMappingFactory.read_config(cfg.name, type=type)
        return mapping

    def test_load_mapping(self):
        mapping_js = self.temp_mapping(type='json')
        mapping_ym = self.temp_mapping(type='yaml')
        self.assertEqual(len(mapping_js), 2)
        self.assertEqual(mapping_js, mapping_ym)
    
    def test_column_mapping(self):
        mapping = self.temp_mapping()
        query = mapping[1].mapping_to_query()
        self.assertEqual(str(query[0]), "CO2_DRY AS CO2_DRY") 
        self.assertEqual(str(query[2]), "timed / 1000 AS timestamp") 
    
    def test_list_files(self):
        mapping = self.temp_mapping()
        fl = mapping[0].source.list_files()
        self.assertIsInstance(fl, pd.DataFrame)
    
    def test_list_db_files(self):
        mapping = self.temp_mapping()
        fl = mapping[1].source.list_files()
        self.assertTrue(len(fl) > 0)

    def test_map_file(self):
        mapping = self.temp_mapping()
        source = mapping[1].source
        mf = source.list_files()
        data = source.read_file(mf.iloc[0].date)
        mapped_data = mapping[1].map_file(data)
        
        self.assertTrue(len(set(mapped_data.columns).intersection(['CO2_DRY','timestamp'])) !=0)


    def test_map_file_flag(self):
        mapping = self.temp_mapping()
        source = mapping[0].source
        mf = source.list_files()
        data = source.read_file(mf.iloc[0].date)
        mapped_data = mapping[0].map_file(data)
        self.assertTrue(len(set(mapped_data.columns).intersection(['CO2_DRY','timestamp'])) !=0)




    def test_list_files_from_db(self):
        mapping = self.temp_mapping()
        fl = mapping[1].source.list_files()
        self.assertIsInstance(fl, pd.DataFrame)

    def test_loading_db_file(self):
        mapping = self.temp_mapping()
        dest = mapping[0].dest
        mf = dest.list_files()
        data = dest.read_file(mf.iloc[0].date)
        self.assertTrue(len(set(data.columns).intersection(['CO2'])) != 0)
    
    def test_loading_csv_file(self):
        mapping = self.temp_mapping()
        dest = mapping[0].source
        mf = dest.list_files()
        data = dest.read_file(mf.iloc[0].date)
    
    def test_missing_files(self):
        mapping = self.temp_mapping()
        mf = mapping[0].list_files_missing_in_dest()
        self.assertIsInstance(mf, set)

    def test_write_file(self):
        mapping = self.temp_mapping()
        source = mapping[0].source
        dest = mapping[0].dest
        mf = source.list_files()
        data = source.read_file(mf.iloc[0].date)
        data_mapped = mapping[0].map_file(data)
        affected = dest.write_file(data_mapped, temporary=True)
        self.assertTrue((data_mapped['timestamp']==affected['timestamp']).all())




if __name__ == '__main__':
    unittest.main()