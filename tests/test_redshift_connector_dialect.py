from decimal import Decimal
from unittest import mock

import pytest
import redshift_connector
import sqlalchemy as sa
from sqlalchemy.pool import NullPool

from sqlalchemy_redshift.dialect import _restore_percents

URL = "redshift+redshift_connector://user:pw@example.invalid:5439/dev"


def connection_kwargs(url, **engine_kwargs):
    """The kwargs ``redshift_connector.connect`` passes to ``Connection``."""
    engine = sa.create_engine(url, poolclass=NullPool, **engine_kwargs)
    with mock.patch.object(redshift_connector, "Connection") as connection:
        connection.side_effect = RuntimeError("stop before the network")
        with pytest.raises(RuntimeError, match="stop before the network"):
            engine.connect()
    engine.dispose()
    return connection.call_args.kwargs


def test_url_query_values_are_typed():
    kwargs = connection_kwargs(URL + "?ssl=true&timeout=15&tcp_keepalive=false")
    assert kwargs["ssl"] is True
    assert kwargs["timeout"] == 15
    assert kwargs["tcp_keepalive"] is False
    assert kwargs["sslmode"] == "verify-full"


def test_string_connect_args_are_typed():
    kwargs = connection_kwargs(URL, connect_args={"ssl": "false", "port": "5439"})
    assert kwargs["ssl"] is False
    assert kwargs["port"] == 5439


def test_unrecognised_boolean_is_rejected():
    engine = sa.create_engine(URL + "?ssl=maybe", poolclass=NullPool)
    with pytest.raises(ValueError):
        engine.connect()


@pytest.mark.parametrize(
    "compiled, has_parameters, sent",
    [
        ("SELECT 'a%%'", False, "SELECT 'a%'"),
        ("SELECT 'a%%', %s", True, "SELECT 'a%', %s"),
        ("SELECT 10 %% %s", True, "SELECT 10 %% %s"),
        ("SELECT 10 %% 4", False, "SELECT 10 % 4"),
        ('SELECT 1 AS "a%%b", %s', True, 'SELECT 1 AS "a%b", %s'),
        ("SELECT 'it''s 5%%', %s", True, "SELECT 'it''s 5%', %s"),
        ("SELECT %s -- 100%%\n, 1 %% 2", True, "SELECT %s -- 100%\n, 1 %% 2"),
        ("SELECT /* 1%% */ %s", True, "SELECT /* 1% */ %s"),
        ("SELECT 1", True, "SELECT 1"),
    ],
)
def test_restore_percents(compiled, has_parameters, sent):
    assert _restore_percents(compiled, has_parameters) == sent


def test_compiled_statements_are_restored_driver_sql_is_not():
    dialect = sa.create_engine(URL).dialect
    cursor = mock.Mock()
    dialect.do_execute(cursor, "SELECT 'a%%', %s", ("x",), mock.Mock(compiled=object()))
    cursor.execute.assert_called_once_with("SELECT 'a%', %s", ("x",))
    cursor = mock.Mock()
    dialect.do_execute(cursor, "SELECT 'a%%'", (), mock.Mock(compiled=None))
    cursor.execute.assert_called_once_with("SELECT 'a%%'", ())


def test_numeric_results_are_not_routed_through_float():
    dialect = sa.create_engine(URL).dialect
    assert dialect.supports_native_decimal is True
    value = Decimal("12345678901234567890.123456789012345678")
    processor = sa.Numeric(38, 18).dialect_impl(dialect).result_processor(dialect, 1700)
    assert processor is None or processor(value) == value


@pytest.mark.parametrize(
    "error, expected",
    [
        (redshift_connector.InterfaceError("BrokenPipe: server socket closed."), True),
        (redshift_connector.InterfaceError("connection is closed"), True),
        (redshift_connector.InterfaceError("communication error", OSError()), True),
        (redshift_connector.InterfaceError("Only %s and %% are supported"), False),
        (redshift_connector.ProgrammingError({"M": "syntax error"}), False),
    ],
)
def test_is_disconnect(error, expected):
    dialect = sa.create_engine(URL).dialect
    assert dialect.is_disconnect(error, None, None) is expected
