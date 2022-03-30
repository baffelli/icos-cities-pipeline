import imp
import unittest
import tempfile as tf
from sensorutils import files as fu
from sensorutils import secrets as se
import json
import yaml
import pandas as pd
class TestSecrets(unittest.TestCase):

    def setUp(self):
        self.k = 'AAB'
        self.secret = \
        f"""
        decentlab:
            key: {self.k}
        """
        with tf.NamedTemporaryFile(mode='w+t', delete=False) as cfg:
            cfg.writelines(self.secret)
            cfg.flush()
            pt = cfg.name
        self.path = pt

    def test_read(self):
        k = se.get_key('decentlab', file=self.path)
        self.assertEqual(k, self.k)
    
    def test_read_missing(self):
        self.assertRaises(KeyError, se.get_key, 'other',file=self.path)

   
if __name__ == '__main__':
    unittest.main()