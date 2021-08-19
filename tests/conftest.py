import contextlib
import copy
import functools
import itertools
import os
import time
import uuid

try:
    from urllib import parse as urlparse
except ImportError:
    import urlparse

import pytest
import requests
import sqlalchemy as sa
from rs_sqla_test_utils import db

_unicode = str


def database_name_generator():
    template = "testdb_{uuid}_{count}"
    db_uuid = _unicode(uuid.uuid1()).replace("-", "")
    for i in itertools.count():
        yield template.format(
            uuid=db_uuid,
            count=i,
        )


database_name = functools.partial(next, database_name_generator())


class DatabaseTool:
    """
    Abstracts the creation and destruction of migrated databases.
    """

    def __init__(self):
        self.engine = self.engine_definition.engine()

    @contextlib.contextmanager
    def _database(self):
        db_name = database_name()
        with self.engine.connect() as conn:
            conn.execute("COMMIT")  # Can't create databases in a transaction
            conn.execute(f"CREATE DATABASE {db_name}")

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
                conn.execute("COMMIT")  # Can't drop databases in a transaction
                conn.execute(f"DROP DATABASE {db_name}")

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
                    "definition": engine_definition,
                    "engine": engine,
                }
            finally:
                engine.dispose()


@pytest.yield_fixture(scope="session")
def _redshift_database_tool():
    from rs_sqla_test_utils import models

    if "PGPASSWORD" not in os.environ:
        pytest.skip("This test will only work on Travis.")

    session = requests.Session()

    while True:
        resp = session.post("https://bigcrunch.herokuapp.com/session/")
        if resp.status_code != 503:
            break
        print("waiting for Redshift to boot up")
        time.sleep(15)

    resp.raise_for_status()
    config = resp.json()
    try:

        class RedshiftDatabaseTool(DatabaseTool):
            engine_definition = db.redshift_engine_definition(config["cluster"])

            def migrate(self, engine):
                models.Base.metadata.create_all(bind=engine)

        yield RedshiftDatabaseTool()
    finally:
        base_url = resp.request.url
        resp = session.delete(urlparse.urljoin(base_url, config["resource_url"]))
        resp.raise_for_status()


@pytest.yield_fixture(scope="function")
def _redshift_engine_and_definition(_redshift_database_tool):
    with _redshift_database_tool.migrated_database() as database:
        yield database


@pytest.fixture(scope="function")
def redshift_engine(_redshift_engine_and_definition):
    """
    A redshift engine for a freshly migrated database.
    """
    return _redshift_engine_and_definition["engine"]


@pytest.fixture(scope="function")
def redshift_engine_definition(_redshift_engine_and_definition):
    """
    A redshift engine definition for a freshly migrated database.
    """
    return _redshift_engine_and_definition["definition"]


@pytest.yield_fixture(scope="session")
def _session_scoped_redshift_engine(_redshift_database_tool):
    """
    Private fixture to maintain a db for the entire test session.
    """
    with _redshift_database_tool.migrated_database() as egs:
        yield egs["engine"]


@pytest.yield_fixture(scope="function")
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
