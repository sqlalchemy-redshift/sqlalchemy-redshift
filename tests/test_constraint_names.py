from sqlalchemy_redshift.dialect import RedshiftDialect


def test_pk_name(redshift_session):
    dialect = RedshiftDialect()
    pk = dialect.get_pk_constraint(
        redshift_session,
        "reflection_named_pk_constraint"
    )
    assert pk['name'] == "reflection_named_pk_constraint__pkey"
    assert pk['constrained_columns'] == ["col1", "col2"]


def test_fk_name(redshift_session):
    dialect = RedshiftDialect()
    fks = dialect.get_foreign_keys(
        redshift_session,
        "reflection_named_fk_constraint"
    )
    assert len(fks) == 1
    assert fks[0]["name"] == "reflection_named_fk_constraint__fk"
    assert fks[0]["constrained_columns"] == ["col1"]
    assert fks[0]["referred_columns"] == ["col1"]
    assert fks[0]["referred_schema"] == ""
    assert fks[0]["referred_table"] == "reflection_unique_constraint"
