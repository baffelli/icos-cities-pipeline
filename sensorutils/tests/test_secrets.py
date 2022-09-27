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
        self.user = 'pippo'
        self.k1 = 'pappo'
        self.secret = \
        f"""
        - service:
            name: decentlab
            secret: {self.k}
        - service:
            name: other
            user: {self.user}
            secret: {self.k1}
        """
        with tf.NamedTemporaryFile(mode='w+t', delete=False) as cfg:
            cfg.writelines(self.secret)
            cfg.flush()
            pt = cfg.name
        self.path = pt

    def test_read(self):
        k = se.get_key('decentlab', file=self.path)
        self.assertEqual(k, self.k)
    
    def test_read_user(self):
        self.assertEqual(se.get_key('other', self.user, file=self.path), self.k1)

    def test_read_missing(self):
        self.assertRaises(KeyError, se.get_key, 'obomp', file=self.path)

   
if __name__ == '__main__':
    unittest.main()