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
            "path": "DUE"},
        "dest": {
            "type": "DB",
            "path": "NABEL_DUE",
            "date_column":"timestamp",
            "date_mult": 1,
            "db_prefix":"CarboSense_MySQL"
        },
        "columns": {
            "O3":"O3",
            "timestamp":"date"
        }
        },
        "GIMM": {
        "source": {
            "type":"DB",
            "db_prefix":"empaGSN",
            "date_column":"timed",
            "date_mult": 1e-3 ,
            "path":"gimmiz_1min_cal"},
        "dest": {
            "type": "DB",
            "path": "UNIBE_GIMM",
            "date_column":"timestamp",
            "date_mult": 1,
            "db_prefix":"CarboSense_MySQL"
        },
        "columns": {
            "CO2":"CO2_DRY",
            "timestamp":"timed / 1000"
        }
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
        self.assertEqual(str(query[0]), "CO2_DRY AS CO2") 
        self.assertEqual(str(query[1]), "timed / 1000 AS timestamp") 

    def test_map_file(self):
        mapping = self.temp_mapping()
        source = mapping[1].source
        mf = source.list_files()
        data = source.read_file(mf.iloc[0].date.strftime('%Y-%m-%d'))
        mapped_data = mapping[1].map_file(data)
        self.assertIn(mapped_data.columns, ['CO2_DRY','timestamp'])

    def test_list_files(self):
        mapping = self.temp_mapping()
        fl = mapping[0].source.list_files()
        self.assertIsInstance(fl, pd.DataFrame)

    def test_list_files_from_db(self):
        mapping = self.temp_mapping()
        fl = mapping[1].source.list_files()
        self.assertIsInstance(fl, pd.DataFrame)

    def test_loading_db_file(self):
        mapping = self.temp_mapping()
        dest = mapping[0].dest
        mf = dest.list_files()
        data = dest.read_file(mf.iloc[0].date.strftime('%Y-%m-%d %X'))
        self.assertIn(data.columns, ['CO2'])
    
    def test_loading_csv_file(self):
        mapping = self.temp_mapping()
        dest = mapping[0].source
        mf = dest.list_files()
        data = dest.read_file(mf.iloc[0].date)
    
    def test_missing_files(self):
        mapping = self.temp_mapping()
        mf = mapping[0].list_files_missing_in_dest()
        self.assertIsInstance(mf, pd.DataFrame)
        
if __name__ == '__main__':
    unittest.main()