import sqlalchemy as sa

from rs_sqla_test_utils.utils import get_url_builder


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


def redshift_engine_definition(
    host: str,
    port: int,
    dialect: str,
    password: str,
    username: str,
    database: str,
):
    url_builder = get_url_builder()
    return EngineDefinition(
        db_connect_url=url_builder(
            drivername=dialect,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
            query={'client_encoding': 'utf8'},
        ),
        connect_args={},
    )
