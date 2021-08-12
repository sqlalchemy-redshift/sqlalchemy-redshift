from tests.rs_sqla_test_utils import models


def test_long_tablename(redshift_session):
    session = redshift_session
    examples = [models.LongTablename(metric=i) for i in range(5)]
    session.add_all(examples)

    rows = session.query(models.LongTablename.metric)
    assert set(row.metric for row in rows) == set([0, 1, 2, 3, 4])
