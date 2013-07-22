from unittest import TestCase
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, create_engine
from sqlalchemy.sql import select, func
from sqlalchemy.types import NullType, VARCHAR
from redshift_sqlalchemy.dialect import RedshiftDialect, UnloadFromSelect, visit_unload_from_select


class TestUnloadFromSelect(TestCase):
    def setUp(self):
        ''' Sets up a in memory sqlite instance with a table
        '''
        self.metadata = MetaData()
        self.t1 = Table('t1', self.metadata, Column('id', Integer, primary_key=True), Column('name', String))
        self.engine = create_engine('sqlite:///:memory:', echo=True)
        self.metadata.create_all(self.engine)

    def test_basic_unload_case(self):
        ''' Tests that the simplest type of UnloadFromSelect works
        '''
        expected_result = "unload ('SELECT count(t1.id) AS count_1 \nFROM t1') to 'cookies' credentials 'aws_access_key_id=cookies;aws_secret_access_key=cookies'"
        insert = UnloadFromSelect(select([func.count(self.t1.c.id)]), 'cookies', 'cookies', 'cookies')
        self.assertEqual(expected_result, str(insert))
