from alembic import migration

from redshift_sqlalchemy import dialect


def test_configure_migration_context():
    context = migration.MigrationContext.configure(url='redshift+psycopg2://mydb')
    assert isinstance(context.impl, dialect.RedshiftImpl)
