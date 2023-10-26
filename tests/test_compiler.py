from sqlalchemy import func, select

from tests.rs_sqla_test_utils import models


def test_func_now(stub_redshift_dialect):
    dialect = stub_redshift_dialect
    s = select([func.NOW().label("time")])
    compiled = s.compile(dialect=dialect)
    assert str(compiled) == "SELECT SYSDATE AS time"

def test_unquoting_schema(stub_redshift_dialect):
    dialect = stub_redshift_dialect
    s = select(models.BasicInIncludingDotSchema.col1).where(models.BasicInIncludingDotSchema.col1 == 1)
    compiled = s.compile(dialect=dialect)
    assert str(compiled) == """SELECT dotted.schema.basic.col1 
FROM dotted.schema.basic 
WHERE dotted.schema.basic.col1 = %s"""
