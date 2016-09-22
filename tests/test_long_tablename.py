from rs_sqla_test_utils import models


def test_long_tablename(redshift_session):
    session = redshift_session
    examples = [models.LongTablename(metric=i) for i in range(5)]
    session.add_all(examples)
    session.flush()

    rows = session.query(models.LongTablename.metric)
    assert {row.metric for row in rows} == {0, 1, 2, 3, 4}
