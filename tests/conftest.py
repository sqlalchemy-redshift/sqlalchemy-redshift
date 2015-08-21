import os
import copy
import contextlib
import itertools
import uuid
import functools

import pytest
import sqlalchemy as sa

from rs_sqla_test_utils import db, models


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
        db_name = database_name()
        with self.engine.connect() as conn:
            conn.execute('COMMIT')  # Can't create databases in a transaction
            conn.execute('CREATE DATABASE {db_name}'.format(db_name=db_name))

        dburl = copy.deepcopy(self.engine.url)
        dburl.database = db_name

        try:
            yield db.EngineDefinition(
                db_connect_url=dburl,
                connect_args=self.engine_definition.connect_args,
            )
        finally:
            with self.engine.connect() as conn:
                conn.execute('COMMIT')  # Can't drop databases in a transaction
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


@pytest.fixture(scope='session')
def _redshift_database_tool():
    if 'PGPASSWORD' in os.environ:
        class RedshiftDatabaseTool(DatabaseTool):
            engine_definition = db.redshift_engine_definition()

            def migrate(self, engine):
                models.Base.metadata.create_all(bind=engine)

        return RedshiftDatabaseTool()
    else:
        pytest.skip('This test will only work on Travis.')


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
