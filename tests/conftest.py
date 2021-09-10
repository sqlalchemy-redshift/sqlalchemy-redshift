import os
import copy
import contextlib
import itertools
import uuid
import functools
import time

try:
    from urllib import parse as urlparse
except ImportError:
    import urlparse

import requests
import pytest
import sqlalchemy as sa
from sqlalchemy.engine.url import URL


from rs_sqla_test_utils import db
from packaging.version import Version


_unicode = type(u'')


def database_name_generator():
    template = 'testdb_{uuid}_{count}'
    db_uuid = _unicode(uuid.uuid1()).replace('-', '')
    for i in itertools.count():
        yield template.format(
            uuid=db_uuid,
            count=i,
        )


database_name = functools.partial(next, database_name_generator())


class DatabaseTool(object):
    """
    Abstracts the creation and destruction of migrated databases.
    """

    def __init__(self):
        self.engine = self.engine_definition.engine()

    @contextlib.contextmanager
    def _database(self):
        from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector

        db_name = database_name()
        with self.engine.connect() as conn:
            conn.execute('COMMIT')  # Can't create databases in a transaction
            if isinstance(conn.dialect, RedshiftDialect_redshift_connector):
                conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute('CREATE DATABASE {db_name}'.format(db_name=db_name))

        dburl = copy.deepcopy(self.engine.url)
        try:
            dburl.database = db_name
        except AttributeError:
            dburl = dburl.set(database=db_name)

        try:
            yield db.EngineDefinition(
                db_connect_url=dburl,
                connect_args=self.engine_definition.connect_args,
            )
        finally:
            with self.engine.connect() as conn:
                conn.execute('COMMIT')  # Can't drop databases in a transaction
                if isinstance(conn.dialect, RedshiftDialect_redshift_connector):
                    conn.execution_options(isolation_level="AUTOCOMMIT")
                conn.execute('DROP DATABASE {db_name}'.format(db_name=db_name))

    @contextlib.contextmanager
    def migrated_database(self):
        """
        Test fixture for testing real commits/rollbacks.

        Creates and migrates a fresh database for every test.
        """
        with self._database() as engine_definition:
            engine = engine_definition.engine()
            try:
                self.migrate(engine)
                yield {
                    'definition': engine_definition,
                    'engine': engine,
                }
            finally:
                engine.dispose()


def pytest_addoption(parser):
    """
    Pytest option to define which dbdrivers to run the test suite with.

    """
    parser.addoption("--dbdriver", action="append")


def make_mock_engine(name):
    """
    Creates a mock sqlalchemy engine for testing dialect functionality

    """
    if Version(sa.__version__) >= Version('1.4.0'):
        return sa.create_mock_engine(URL(
            drivername=name
        ), executor=None)
    else:
        return sa.create_engine(URL(
            drivername=name,
        ), strategy='mock', executor=None)


class DriverParameterizedTests:
    """
    Helper class for generating fixture params using pytest config opts.

    """
    DEFAULT_DRIVERS = ['psycopg2', 'psycopg2cffi']
    drivers = None
    stub_redshift_engines = None
    stub_redshift_dialects = None
    redshift_dialect_flavors = None

    @classmethod
    def set_drivers(cls,  _drivers):
        DriverParameterizedTests.drivers = _drivers
        DriverParameterizedTests.make_fixtures()

    @staticmethod
    def _make_drivername(driver):
        return 'redshift+{}'.format(driver)

    @classmethod
    def make_fixtures(cls):

        driver_names = [
            DriverParameterizedTests._make_drivername(x)
            for x in DriverParameterizedTests.drivers
        ]

        DriverParameterizedTests.stub_redshift_engines = [
                make_mock_engine(x)
                for x in driver_names
        ]

        DriverParameterizedTests.stub_redshift_dialects = [
            x.dialect for x in DriverParameterizedTests.stub_redshift_engines
        ]

        DriverParameterizedTests.redshift_dialect_flavors = driver_names


def pytest_generate_tests(metafunc):
    if DriverParameterizedTests.drivers is None:
        dbdrivers = metafunc.config.getoption("--dbdriver", default=DriverParameterizedTests.DEFAULT_DRIVERS)
        DriverParameterizedTests.set_drivers(dbdrivers)

    if 'stub_redshift_engine' in metafunc.fixturenames:
        metafunc.parametrize(
            "stub_redshift_engine",
            DriverParameterizedTests.stub_redshift_engines,
            scope="session")
    if 'stub_redshift_dialect' in metafunc.fixturenames:
        metafunc.parametrize(
            'stub_redshift_dialect',
            DriverParameterizedTests.stub_redshift_dialects,
            scope="session")
    if 'redshift_dialect_flavor' in metafunc.fixturenames:
        metafunc.parametrize(
            'redshift_dialect_flavor',
            DriverParameterizedTests.redshift_dialect_flavors,
            scope="session")


@pytest.yield_fixture(scope='session')
def _redshift_database_tool(request, redshift_dialect_flavor):
    from rs_sqla_test_utils import models
    if 'PGPASSWORD' not in os.environ:
        pytest.skip('This test will only work on Travis.')

    session = requests.Session()

    while True:
        resp = session.post('https://bigcrunch.herokuapp.com/session/')
        if resp.status_code != 503:
            break
        print('waiting for Redshift to boot up')
        time.sleep(15)

    resp.raise_for_status()
    config = resp.json()
    try:
        class RedshiftDatabaseTool(DatabaseTool):
            engine_definition = db.redshift_engine_definition(
                config['cluster'],
                redshift_dialect_flavor
            )

            def migrate(self, engine):
                models.Base.metadata.create_all(bind=engine)

        yield RedshiftDatabaseTool()
    finally:
        base_url = resp.request.url
        resp = session.delete(
            urlparse.urljoin(base_url, config['resource_url'])
        )
        resp.raise_for_status()


@pytest.yield_fixture(scope='function')
def _redshift_engine_and_definition(_redshift_database_tool):
    with _redshift_database_tool.migrated_database() as database:
        yield database


@pytest.fixture(scope='function')
def redshift_engine(_redshift_engine_and_definition):
    """
    A redshift engine for a freshly migrated database.
    """
    return _redshift_engine_and_definition['engine']


@pytest.fixture(scope='function')
def redshift_engine_definition(_redshift_engine_and_definition):
    """
    A redshift engine definition for a freshly migrated database.
    """
    return _redshift_engine_and_definition['definition']


@pytest.yield_fixture(scope='session')
def _session_scoped_redshift_engine(_redshift_database_tool):
    """
    Private fixture to maintain a db for the entire test session.
    """
    with _redshift_database_tool.migrated_database() as egs:
        yield egs['engine']


@pytest.yield_fixture(scope='function')
def redshift_session(_session_scoped_redshift_engine):
    """
    A redshift session that rolls back all operations.

    The engine and db is maintained for the entire test session for efficiency.
    """
    conn = _session_scoped_redshift_engine.connect()
    tx = conn.begin()

    RedshiftSession = sa.orm.sessionmaker()
    session = RedshiftSession(bind=conn)
    try:
        yield session
    finally:
        session.close()
        tx.rollback()
        conn.close()
