from sqlalchemy import func, select


def test_func_now(stub_redshift_dialect):
    dialect = stub_redshift_dialect
    s = select([func.NOW().label("time")])
    compiled = s.compile(dialect=dialect)
    assert str(compiled) == "SELECT SYSDATE AS time"
