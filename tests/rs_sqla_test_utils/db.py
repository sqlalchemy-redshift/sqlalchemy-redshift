import os

import pkg_resources
import sqlalchemy as sa
from sqlalchemy.engine import url as sa_url


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


def redshift_engine_definition():
    return EngineDefinition(
        db_connect_url=sa_url.URL(
            drivername='redshift+psycopg2',
            username='travis',
            password=os.environ['PGPASSWORD'],
            host=(
                'redshift-sqlalchemy-test.cforsfjmjsja.us-west-2.redshift'
                '.amazonaws.com'
            ),
            port=5439,
            database='dev',
            query={'client_encoding': 'utf8'},
        ),
        connect_args={
            'sslmode': 'verify-full',
            'sslrootcert': pkg_resources.resource_filename(
                'redshift_sqlalchemy',
                'redshift-ssl-ca-cert.pem',
            )
        }
    )
