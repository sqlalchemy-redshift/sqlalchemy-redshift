from unittest import TestCase
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.sql import select, func
from redshift_sqlalchemy.dialect import UnloadFromSelect


class TestUnloadFromSelect(TestCase):
    def setUp(self):
        ''' Sets up a table and associate meta data for the test queries to build against
        '''
        self.metadata = MetaData()
        self.t1 = Table('t1', self.metadata, Column('id', Integer, primary_key=True), Column('name', String))

    def test_basic_unload_case(self):
        ''' Tests that the simplest type of UnloadFromSelect works
        '''
        expected_result = "UNLOAD ('SELECT count(t1.id) AS count_1 \nFROM t1') TO 's3://bucket/key'\n           " \
                          "CREDENTIALS 'aws_access_key_id=cookies;aws_secret_access_key=cookies;token=cookies'\n" \
                          "           DELIMITER ','\n           ADDQUOTES\n           ALLOWOVERWRITE\n          " \
                          " PARALLEL ON;"
        insert = UnloadFromSelect(select([func.count(self.t1.c.id)]), 's3://bucket/key', 'cookies', 'cookies',
                                  'cookies')
        self.assertEqual(expected_result, str(insert).strip())

    def test_parallel_off_unload_case(self):
        ''' Tests that UnloadFromSelect handles parallel being set to off
        '''
        expected_result = "UNLOAD ('SELECT count(t1.id) AS count_1 \nFROM t1') TO 's3://bucket/key'\n           " \
                          "CREDENTIALS 'aws_access_key_id=cookies;aws_secret_access_key=cookies;token=cookies'\n" \
                          "           DELIMITER ','\n           ADDQUOTES\n           ALLOWOVERWRITE\n          " \
                          " PARALLEL OFF;"
        insert = UnloadFromSelect(select([func.count(self.t1.c.id)]), 's3://bucket/key', 'cookies', 'cookies',
                                  'cookies', {'parallel': 'OFF'})
        self.assertEqual(expected_result, str(insert).strip())
