from unittest import TestCase
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.sql import select, func
from redshift_sqlalchemy.dialect import CopyCommand


class TestCopyCommand(TestCase):
    def setUp(self):
        ''' Sets up a table and associate meta data for the test queries to build against
        '''
        pass

    def test_basic_copy_case(self):
        ''' Tests that the simplest type of UnloadFromSelect works
        '''
        expected_result = "copy t1 from 's3://mybucket/data/listing/' credentials 'aws_access_key_id=cookies;aws_secret_access_key=cookies'"
        insert = CopyCommand('t1', 's3://mybucket/data/listing/', 'cookies', 'cookies')
        self.assertEqual(expected_result, str(insert))
