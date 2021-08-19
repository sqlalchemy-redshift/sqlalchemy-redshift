import os

import sqlalchemy as sa
from sqlalchemy.engine import url as sa_url


class EngineDefinition:
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


def redshift_engine_definition(cluster):
    return EngineDefinition(
        db_connect_url=sa_url.URL(
            drivername="redshift+psycopg2",
            username="travis",
            password=os.environ["PGPASSWORD"],
            host=cluster["Address"],
            port=cluster["Port"],
            database="dev",
            query={"client_encoding": "utf8"},
        ),
        connect_args={},
    )
