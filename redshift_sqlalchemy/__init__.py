__version__ = '0.5.1a'

from sqlalchemy.dialects import registry

registry.register(
    'redshift', 'redshift_sqlalchemy.dialect', 'RedshiftDialect'
)
registry.register(
    'redshift+psycopg2', 'redshift_sqlalchemy.dialect', 'RedshiftDialect'
)
registry.register(
    'redshift+psycopg2cffi', 'redshift_sqlalchemy.dialect',
    'PyscopgCFFIRedshiftDialect',
)
registry.register(
    'redshift+pg8000', 'redshift_sqlalchemy.dialect', 'Pg8000RedshiftDialect'
)
registry.register(
    'redshift+pypostgresql', 'redshift_sqlalchemy.dialect',
    'PypostgresqlRedshiftDialect',
)
registry.register(
    'redshift+zxjdbc', 'redshift_sqlalchemy.dialect', 'ZxjdbcRedshiftDialect'
)
