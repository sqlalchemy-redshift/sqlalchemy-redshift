from sqlalchemy import func

from sqlalchemy_redshift.compat import sa_select


def test_func_now(stub_redshift_dialect):
    dialect = stub_redshift_dialect
    s = sa_select(func.NOW().label("time"))
    compiled = s.compile(dialect=dialect)
    assert str(compiled) == "SELECT SYSDATE AS time"
