import sqlalchemy as sa
from sqlalchemy import orm as sa_orm

from redshift_sqlalchemy import models

from rs_sqla_test_utils import utils


engine = sa.create_engine('redshift+psycopg2://mydb')
Session = sa_orm.sessionmaker(engine)
session = Session()


def test_stl_unload_log():
    stmt = session.query(models.STLUnloadLog).statement
    expected_result = utils.clean("""
    SELECT
        "STL_UNLOAD_LOG".userid,
        "STL_UNLOAD_LOG".query,
        "STL_UNLOAD_LOG".slice,
        "STL_UNLOAD_LOG".pid,
        "STL_UNLOAD_LOG".path,
        "STL_UNLOAD_LOG".start_time,
        "STL_UNLOAD_LOG".end_time,
        "STL_UNLOAD_LOG".line_count,
        "STL_UNLOAD_LOG".transfer_size
    FROM "STL_UNLOAD_LOG"
    """)
    assert utils.clean(utils.compile_query(stmt)) == expected_result
