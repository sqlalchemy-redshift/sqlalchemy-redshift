"""
Reflection of datashare (consumer database) objects.

These objects live outside pg_catalog, so the dialect reads them from the
SVV_ALL_* views. The tests drive the dialect with a fake connection that
answers the catalog queries, so no Redshift cluster is needed.

Two situations are covered:

- local: the URL names the connected database. Datashare schemas are added
  on top of the pg_catalog reflection.
- remote: the URL names another database than the connected one (a
  connection pooler alias). Everything comes from SVV_ALL_* and pg_catalog
  is not queried at all.
"""

from collections import namedtuple

import pytest

from sqlalchemy_redshift import dialect as rs_dialect
from sqlalchemy_redshift.dialect import RedshiftDialect_psycopg2

SHARED_DB = "shared_db"
Relation = namedtuple(
    "Relation",
    "relkind schema_oid schema rel_oid relname diststyle "
    "owner_id owner_name view_definition privileges comment",
)
Column = namedtuple(
    "Column",
    "schema table_name name encode format_type distkey sortkey notnull "
    "comment adsrc attnum default schema_oid table_oid",
)


class Result(list):
    def scalar(self):
        return self[0][0]

    def fetchall(self):
        return list(self)

    def all(self):
        return list(self)


class FakeConnection:
    """Answers the catalog queries the dialect issues while reflecting."""

    def __init__(self, url_database, connected_database):
        self.connected = connected_database
        self.remote = url_database != connected_database
        self.engine = type(
            "Engine", (), {"url": type("URL", (), {"database": url_database})}
        )
        self.pg_catalog_queries = 0

    def _pg_catalog(self, rows):
        self.pg_catalog_queries += 1
        assert not self.remote, "pg_catalog queried for a remote database"
        return Result(rows)

    def scalars(self, statement, params=None):
        return Result(row[0] for row in self.execute(statement, params))

    def execute(self, statement, params=None):
        sql = str(statement)
        if "current_database()" in sql:
            return Result([(self.connected,)])
        if "svv_all_schemas" in sql:
            assert params == {"database": SHARED_DB}
            return Result([("public",), ("vendor",), ("vendor_test",)])
        if "svv_all_tables" in sql:
            assert params["database"] == SHARED_DB
            assert params["schemas"] == ["vendor"]
            rows = [
                Relation("v", None, "vendor", None, name, *([None] * 5), comment)
                for name, comment in (("orders", "Shared orders"), ("customers", None))
            ]
            if "table_name" in params:
                rows = [r for r in rows if r.relname == params["table_name"]]
            return Result(rows)
        if "svv_all_columns" in sql:
            assert params["database"] == SHARED_DB
            assert params["schemas"] == ["vendor"]
            rows = [
                Column(
                    "vendor",
                    "orders",
                    "id",
                    None,
                    "character varying(65535)",
                    False,
                    0,
                    False,
                    None,
                    None,
                    1,
                    None,
                    None,
                    None,
                ),
                Column(
                    "vendor",
                    "orders",
                    "amount",
                    None,
                    "numeric(18,2)",
                    False,
                    0,
                    True,
                    None,
                    None,
                    2,
                    None,
                    None,
                    None,
                ),
                Column(
                    "vendor",
                    "orders",
                    "created",
                    None,
                    "timestamp without time zone",
                    False,
                    0,
                    False,
                    None,
                    None,
                    3,
                    None,
                    None,
                    None,
                ),
            ]
            if "table_name" in params:
                rows = [r for r in rows if r.table_name == params["table_name"]]
            return Result(rows)
        if "regclass" in sql:
            raise AssertionError("::regclass must not run for datashare tables")
        if "pg_description" in sql:
            raise AssertionError("pg_description queried for a datashare table")
        if "FROM pg_catalog.pg_namespace" in sql:
            return self._pg_catalog([("information_schema",), ("public",)])
        if "nspname FROM pg_namespace" in sql:  # PGDialect.get_schema_names
            return self._pg_catalog([("information_schema",), ("public",)])
        if "pg_catalog.pg_class" in sql or "pg_get_late_binding" in sql:
            return self._pg_catalog([])  # nothing local in the vendor schema
        raise AssertionError("unexpected SQL: " + sql[:200])


@pytest.fixture
def dialect(monkeypatch):
    monkeypatch.setattr(
        rs_dialect,
        "inspect",
        lambda conn: type("Inspector", (), {"default_schema_name": "public"}),
    )
    d = RedshiftDialect_psycopg2()
    d._domains = {}  # skip _load_domains, not under test
    return d


@pytest.mark.parametrize(
    "url_database, connected_database, expected_schemas",
    [
        (
            SHARED_DB,
            SHARED_DB,
            ["information_schema", "public", "vendor", "vendor_test"],
        ),
        (SHARED_DB, "analytics", ["public", "vendor", "vendor_test"]),
    ],
    ids=["local", "remote"],
)
def test_datashare_reflection(
    dialect, url_database, connected_database, expected_schemas
):
    conn = FakeConnection(url_database, connected_database)
    cache = {}

    assert dialect.get_schema_names(conn, info_cache=cache) == expected_schemas

    assert dialect.get_table_names(conn, schema="vendor", info_cache=cache) == []
    assert dialect.get_view_names(conn, schema="vendor", info_cache=cache) == [
        "orders",
        "customers",
    ]
    assert dialect.has_table(conn, "orders", schema="vendor", info_cache=cache)

    columns = dialect.get_columns(conn, "orders", schema="vendor", info_cache=cache)
    assert [c["name"] for c in columns] == ["id", "amount", "created"]
    assert str(columns[0]["type"]) == "VARCHAR(65535)"
    assert str(columns[1]["type"]) == "NUMERIC(18, 2)"
    assert columns[1]["nullable"] is False
    assert str(columns[2]["type"]) == "TIMESTAMP"

    pk = dialect.get_pk_constraint(conn, "orders", schema="vendor", info_cache=cache)
    assert pk == {"constrained_columns": [], "name": ""}
    assert (
        dialect.get_foreign_keys(conn, "orders", schema="vendor", info_cache=cache)
        == []
    )
    assert dialect.get_table_comment(
        conn, "orders", schema="vendor", info_cache=cache
    ) == {"text": "Shared orders"}
    assert dialect.get_table_comment(
        conn, "customers", schema="vendor", info_cache=cache
    ) == {"text": None}

    if conn.remote:
        assert conn.pg_catalog_queries == 0
    else:
        assert conn.pg_catalog_queries > 0
