from unittest import TestCase
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.sql import select, func
from redshift_sqlalchemy.dialect import UnloadFromSelect
import re


class TestUnloadFromSelect(TestCase):

    def setUp(self):
        '''
            Sets up a table and associate meta data for the test queries to build against
        '''
        self.metadata = MetaData()
        self.t1 = Table('t1', self.metadata, Column('id', Integer, primary_key=True), Column('name', String))

    def test_basic_unload_case(self):
        '''
            Tests that the simplest type of UnloadFromSelect works
        '''
        expected_result = re.sub(r'\s+', ' ',
                                 "UNLOAD ('SELECT count(t1.id) AS count_1 \nFROM t1') TO 's3://bucket/key' "
                                 "CREDENTIALS 'aws_access_key_id=cookies;aws_secret_access_key=cookies;token=cookies' "
                                 "DELIMITER ',' ADDQUOTES ALLOWOVERWRITE PARALLEL ON;").strip()

        unload = UnloadFromSelect(select([func.count(self.t1.c.id)]), 's3://bucket/key', 'cookies', 'cookies',
                                  'cookies')

        unload_str = re.sub(r'\s+', ' ', str(unload)).strip()

        self.assertEqual(expected_result, unload_str)

    def test_unload_with_options(self):
        '''
            Tests that UnloadFromSelect handles options correctly
        '''
        expected_result = re.sub(r'\s+', ' ',
                                 "UNLOAD ('SELECT count(t1.id) AS count_1 \nFROM t1') TO 's3://bucket/key' "
                                 "CREDENTIALS 'aws_access_key_id=cookies;aws_secret_access_key=cookies;token=cookies' "
                                 "DELIMITER ',' ADDQUOTES NULL '---' ALLOWOVERWRITE PARALLEL OFF;").strip()

        unload = UnloadFromSelect(select([func.count(self.t1.c.id)]), 's3://bucket/key', 'cookies', 'cookies',
                                  'cookies', {'parallel': 'OFF', 'null_as': '---'})

        unload_str = re.sub(r'\s+', ' ', str(unload)).strip()

        self.assertEqual(expected_result, unload_str)
