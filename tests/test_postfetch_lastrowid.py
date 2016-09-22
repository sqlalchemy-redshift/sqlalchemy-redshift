from rs_sqla_test_utils import models


def test_postfetch_lastrowid(redshift_session):
    session = redshift_session
    for i in range(5):
        postfetchExample = models.PostfetchExample(some_int=i)
        session.add(postfetchExample)
    session.flush()
    assert postfetchExample.id == 4
