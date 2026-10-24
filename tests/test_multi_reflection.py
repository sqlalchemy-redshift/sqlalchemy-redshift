from sqlalchemy.engine import ObjectKind, ObjectScope


def test_get_multi_table_options_uses_redshift_options(
    stub_redshift_dialect, monkeypatch
):
    """
    Table reflection reads options through get_multi_table_options, which
    PGDialect implements with PostgreSQL-only SQL from SQLAlchemy 2.1. The
    Redshift options come from get_table_options, and no SQL is run here.
    """
    options = {
        "redshift_diststyle": "KEY",
        "redshift_distkey": "id",
        "redshift_sortkey": ("id",),
        "redshift_interleaved_sortkey": None,
    }
    monkeypatch.setattr(
        stub_redshift_dialect, "get_table_options", lambda *args, **kw: options
    )

    reflected = stub_redshift_dialect.get_multi_table_options(
        None,
        schema="public",
        filter_names=["example"],
        scope=ObjectScope.ANY,
        kind=ObjectKind.ANY,
    )

    assert dict(reflected) == {("public", "example"): options}


def test_has_multi_table_uses_has_table(stub_redshift_dialect, monkeypatch):
    """
    has_multi_table answers through has_table, which also finds external
    tables, rather than PGDialect's pg_class query from SQLAlchemy 2.1.
    """
    existing = {"example", "spectrum_table"}
    monkeypatch.setattr(
        stub_redshift_dialect,
        "has_table",
        lambda connection, table_name, schema=None, **kw: table_name in existing,
    )

    reflected = stub_redshift_dialect.has_multi_table(
        None, ["example", "spectrum_table", "missing"], schema="public"
    )

    assert dict(reflected) == {
        ("public", "example"): True,
        ("public", "spectrum_table"): True,
        ("public", "missing"): False,
    }
