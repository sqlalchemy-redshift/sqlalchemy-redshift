import os
import contextlib
import datetime as dt

import boto3
from botocore import exceptions as botocore_exceptions
import sqlalchemy as sa
from sqlalchemy.engine import url as sa_url
from sqlalchemy.ext import hybrid as sa_hybrid

from sqlalchemy.ext import declarative
import uuid


_unicode = type(u'')


Base = declarative.declarative_base()
RedshiftSession = sa.orm.sessionmaker()


class TestSession(Base):
    __tablename__ = 'test_session'
    pk = sa.Column(
        sa.Unicode(36), nullable=False, primary_key=True,
        info={'distkey': True, 'sortkey': True},
    )
    created_time = sa.Column(
        sa.DateTime(), nullable=False, server_default=sa.text('SYSDATE'),
    )

    @sa_hybrid.hybrid_property
    def expiry_time(self):
        return self.created_time + dt.timedelta(hours=1)


class EngineDefinition(object):
    """
    Collect the construction arguments of a SQLAlchemy engine.

    Allows a global definition to be used to create identical engines in
    different threads/processes.
    """
    def __init__(self, db_connect_url, connect_args):
        self.db_connect_url = db_connect_url
        self.connect_args = connect_args

    def engine(self):
        return sa.create_engine(
            self.db_connect_url,
            connect_args=self.connect_args,
        )


@contextlib.contextmanager
def redshift_engine_definition():
    raise Exception()  # Stop this untested code from actually running
    username = 'travis'
    password = os.environ['PGPASSWORD']
    cluster_identifier = 'travis-test-server'

    client = boto3.client('redshift')
    try:
        response = client.create_cluster(
            ClusterIdentifier=cluster_identifier,
            NodeType='dc1.large',
            MasterUsername=username,
            MasterUserPassword=password,
        )
    except botocore_exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'ClusterAlreadyExists':
            raise e
        response = client.describe_clusters(
            ClusterIdentifier=cluster_identifier,
        )
        endpoint = response['Clusters'][0]['Endpoint']
    else:
        endpoint = response['Cluster']['Endpoint']

    engine_definition = EngineDefinition(
        db_connect_url=sa_url.URL(
            drivername='redshift+psycopg2',
            username=username,
            password=password,
            host=endpoint['Address'],
            port=endpoint['Port'],
            database='dev',
            query={'client_encoding': 'utf8'},
        ),
        connect_args={},
    )

    engine = engine_definition.engine()
    current_session_pk = _unicode(uuid.uuid1())

    session = RedshiftSession(bind=engine)

    with session.transaction:
        Base.metadata.create_all(bind=session.connection(), check_first=True)
        session.add(TestSession(pk=current_session_pk))

    try:
        yield engine_definition
    finally:
        with session.transaction:
            session.delete(TestSession(pk=current_session_pk))
        with session.transaction:
            live_sessions = session.query(TestSession).filter(
                TestSession.expiry_time <= sa.func.now(),
            ).count()

        if live_sessions == 0:
            try:
                response = client.delete_cluster(
                    ClusterIdentifier=cluster_identifier,
                    SkipFinalClusterSnapshot=True,
                )
            except botocore_exceptions.ClientError as e:
                if e.response['Error']['Code'] != 'InvalidClusterState':
                    raise e
