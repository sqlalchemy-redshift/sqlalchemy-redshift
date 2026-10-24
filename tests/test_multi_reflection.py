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
