from rs_sqla_test_utils import models


def test_simple_query(redshift_session):
    redshift_session.add(models.Basic(name='Freda'))

    assert redshift_session.query(models.Basic.name).first().name == 'Freda'
