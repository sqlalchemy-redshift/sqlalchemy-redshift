import pytest
from sqlalchemy_redshift.dialect import RelationKey

raw_names = [
    (
        "table",
        '"schema"'
    ),
    (
        '"table"',
        "schema"
    ),
    (
        '"table"',
        '"schema"'
    ),
    (
        "table",
        "schema"
    )
]


@pytest.mark.parametrize("raw_table, raw_schema", raw_names)
def test_unquoted(raw_table, raw_schema):
    key = RelationKey(raw_table, raw_schema)
    assert key.unquoted().__str__() == "schema.table"
