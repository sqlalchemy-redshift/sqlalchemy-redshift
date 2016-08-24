from rs_sqla_test_utils import models


def test_postfetch_lastrowid(redshift_session):
    session = redshift_session
    examples = [models.PostfetchExample(some_int=i) for i in range(5)]
    session.add_all(examples)
    session.flush()
    assert sorted(pfe.id for pfe in examples) == [0, 1, 2, 3, 4]
