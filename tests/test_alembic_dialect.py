from alembic.ddl.base import RenameTable, ColumnComment
from alembic import migration

from sqlalchemy_redshift import dialect


def test_configure_migration_context():
    context = migration.MigrationContext.configure(
        url='redshift+psycopg2://mydb'
    )
    assert isinstance(context.impl, dialect.RedshiftImpl)


def test_rename_table():
    compiler = dialect.RedshiftDDLCompiler(dialect.RedshiftDialect(), None)
    sql = compiler.process(RenameTable("old", "new", "schema"))
    assert sql == 'ALTER TABLE schema."old" RENAME TO "new"'


def test_alter_column_comment():
    compiler = dialect.RedshiftDDLCompiler(dialect.RedshiftDialect(), None)
    sql = compiler.process(ColumnComment("table_name", "column_name", "my comment"))
    assert sql == "COMMENT ON COLUMN table_name.column_name IS 'my comment'"
