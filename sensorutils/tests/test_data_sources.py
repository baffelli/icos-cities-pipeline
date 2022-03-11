import unittest
import tempfile as tf
from sensorutils import files as fu
import json
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
        }},
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
        }
        }}
    
    def temp_mapping(self):
        with tf.NamedTemporaryFile(mode='w+t') as cfg:
            json.dump(self.mapping, cfg)
            cfg.flush()
            mapping = fu.DataMappingFactory.read_json(cfg.name)
        return mapping

    def test_load_mapping(self):
        mapping = self.temp_mapping()
        self.assertEqual(len(mapping), 2)
    
    def test_list_files(self):
        mapping = self.temp_mapping()
        fl = mapping[0].source.list_files()
        self.assertIsInstance(fl, pd.DataFrame)

    def test_list_files_from_db(self):
        mapping = self.temp_mapping()
        fl = mapping[1].source.list_files()
        self.assertIsInstance(fl, pd.DataFrame)
        print(fl)

    def test_loading_db_file(self):
        mapping = self.temp_mapping()
        dest = mapping[0].dest
        mf = dest.list_files()
        data = dest.read_file(mf.iloc[0].date.strftime('%Y-%m-%d %X'))
        self.assertIn(data.columns, ['CO2'])
        print(mf)
    
    def test_loading_csv_file(self):
        mapping = self.temp_mapping()
        dest = mapping[0].source
        mf = dest.list_files()
        data = dest.read_file(mf.iloc[0].path)
        import pdb; pdb.set_trace()
        print(mf)
    
    def test_missing_files(self):
        mapping = self.temp_mapping()
        mf = mapping[0].list_files_missing_in_dest()
        self.assertIsInstance(mf, pd.DataFrame)
        print(mf)
if __name__ == '__main__':
    unittest.main()