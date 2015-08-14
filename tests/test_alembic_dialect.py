from alembic.ddl.base import RenameTable
from alembic import migration

from redshift_sqlalchemy import dialect


def test_configure_migration_context():
    context = migration.MigrationContext.configure(url='redshift+psycopg2://mydb')
    assert isinstance(context.impl, dialect.RedshiftImpl)


def test_rename_table(redshift_dialect):
    compiler = dialect.RedShiftDDLCompiler(redshift_dialect, None)
    sql = compiler.process(RenameTable("old", "new", "scheme"))
    assert sql == 'ALTER TABLE scheme."old" RENAME TO "new"'
