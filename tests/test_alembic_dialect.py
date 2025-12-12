from alembic.ddl.base import RenameTable, ColumnComment
from alembic.ddl.postgresql import PostgresqlColumnType
from alembic import migration
from sqlalchemy import Integer, VARCHAR
from sqlalchemy.exc import CompileError
import pytest

from sqlalchemy_redshift import dialect


def test_configure_migration_context():
    context = migration.MigrationContext.configure(
        url='redshift+psycopg2://mydb'
    )
    assert isinstance(context.impl, dialect.RedshiftImpl)


def test_rename_table(stub_redshift_dialect):
    compiler = dialect.RedshiftDDLCompiler(stub_redshift_dialect, None)
    sql = compiler.process(RenameTable("old", "new", "schema"))
    assert sql == 'ALTER TABLE schema."old" RENAME TO "new"'


def test_alter_column_comment(stub_redshift_dialect):
    compiler = dialect.RedshiftDDLCompiler(stub_redshift_dialect, None)
    sql = compiler.process(
        ColumnComment("table_name", "column_name", "my comment")
    )
    assert sql == "COMMENT ON COLUMN table_name.column_name IS 'my comment'"


def test_alter_column_type_varchar_supported(stub_redshift_dialect):
    """
    Test VARCHAR size change - the only ALTER COLUMN TYPE operation 
    supported by Redshift. Should generate valid SQL without errors.
    """
    compiler = dialect.RedshiftDDLCompiler(stub_redshift_dialect, None)
    sql = compiler.process(
        PostgresqlColumnType("table_name", "column_name", VARCHAR(100))
    )
    assert sql == 'ALTER TABLE table_name ALTER COLUMN column_name TYPE VARCHAR(100)'


def test_alter_column_type_unsupported_raises(stub_redshift_dialect):
    """
    Test non-VARCHAR type change - unsupported by Redshift.
    
    Should raise CompileError immediately (fail-fast) with detailed
    migration instructions rather than generating invalid SQL that
    would fail at runtime.
    """
    compiler = dialect.RedshiftDDLCompiler(stub_redshift_dialect, None)

    with pytest.raises(CompileError) as exc_info:
        compiler.process(
            PostgresqlColumnType("table_name", "column_name", Integer())
        )

    # Verify the error message provides actionable guidance
    error_msg = str(exc_info.value)
    assert "Redshift does not support ALTER COLUMN TYPE" in error_msg
    assert "Only VARCHAR size changes are supported" in error_msg
    assert "multi-step migration" in error_msg
    assert "op.add_column" in error_msg
    assert "op.execute" in error_msg
    assert "op.drop_column" in error_msg
    assert "op.alter_column" in error_msg
