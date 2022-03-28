import unittest
import tempfile as tf
from sensorutils import files as fu
from sensorutils import data as icos_data
from sensorutils import db as db
import json
import yaml
import pandas as pd
import pathlib as pl

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sqa

class TestParameters(unittest.TestCase):

    def setUp(self) -> None:
        self.param_path = pl.Path(fu.get_g_drive(), '503_Themen', 'CarboSense', 'Kalibrationen Picarros_clean.xlsx')
        self.new_param_path = pl.Path(fu.get_nabel_dir(), 'Daten', 'Stationen', 'Kalibrierwerte Picarro G1301 DUE Test.xlsx')
        self.mapping = [
        {'drive': 'K', 'source_path': "Nabel/Daten/Stationen/Kalibrierwerte Picarro G1301 DUE Test.xlsx", 'reader': 'new', 'location': 'DUT', 'device': '176-CFADS-050'},
        {'drive': 'G', 'source_path': "503_Themen/CarboSense/Kalibrationen Picarros_clean.xlsx", 'reader': 'old', 'location': None, 'device': None}
        ]

    def temp_mapping(self):
        with tf.NamedTemporaryFile(mode='w+t') as cfg:
            yaml.dump(self.mapping, cfg)
            cfg.flush()
            mapping = icos_data.read_calibration_config(cfg.name)
        return mapping

    def test_read_xls(self):
        data = icos_data.read_picarro_calibration_parameters(self.param_path)
        self.assertIsInstance(data, pd.DataFrame)
    
    def test_sql(self):
        data = icos_data.read_picarro_calibration_parameters(self.param_path)
        con = db.connect_to_metadata_db()
        res = icos_data.temp_query(data, 'SELECT * from temp')
        self.assertIsInstance(res, pd.DataFrame)

    def test_read_xls_new(self):
        data = icos_data.read_picarro_parameters_new(self.new_param_path)
        import pdb; pdb.set_trace()
        self.assertIsInstance(data, pd.DataFrame)
    
    def test_read_calibration_configuration(self):
        conf = self.temp_mapping()
        u = fu.get_user()
        self.assertEqual(conf[1].get_path(), pl.Path(f'/mnt/{u}/G/503_Themen/CarboSense/Kalibrationen Picarros_clean.xlsx'))
    
    def test_serialise(self):
        """
        Check that the calibration parameters are correctly serialised
        """
        data = icos_data.read_picarro_calibration_parameters(self.param_path)
        cal_obj = icos_data.make_calibration_parameters_table(data)
        engine = create_engine('sqlite:///:memory:')
        icos_data.Base.metadata.create_all(engine, checkfirst=True)
        metadata = sqa.MetaData(bind=engine)
        Session = sessionmaker(bind=engine)
        session = Session() 
        for cp in cal_obj:
            session.add(cp)
            session.commit()
        qr = session.query(icos_data.CalibrationParameters).all()
        for mapped, orig in zip(qr, cal_obj):
            self.assertEqual(mapped ,orig)
        import pdb; pdb.set_trace()




if __name__ == '__main__':
    unittest.main()