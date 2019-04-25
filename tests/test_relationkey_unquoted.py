from sqlalchemy_redshift.dialect import RelationKey


def test_unquoted_with_quoted_schema():
    key = RelationKey("table", '"schema"')
    assert key.unquoted() == "schema.table"


def test_unquoted_with_quoted_table():
    key = RelationKey('"table"', "schema")
    assert key.unquoted() == "schema.table"


def test_unquoted_with_both_quoted():
    key = RelationKey('"table"', '"schema"')
    assert key.unquoted() == "schema.table"


def test_unquoted_with_neither_quoted():
    key = RelationKey("table", "schema")
    assert key.unquoted() == "schema.table"
