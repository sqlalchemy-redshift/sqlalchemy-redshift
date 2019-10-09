from pkg_resources import DistributionNotFound, get_distribution, parse_version

try:
    import psycopg2  # noqa: F401
except ImportError:
    raise ImportError(
        'No module named psycopg2. Please install either '
        'psycopg2 or psycopg2-binary package for CPython '
        'or psycopg2cffi for Pypy.'
    )

for package in ['psycopg2', 'psycopg2-binary', 'psycopg2cffi']:
    try:
        if get_distribution(package).parsed_version < parse_version('2.5'):
            raise ImportError('Minimum required version for psycopg2 is 2.5')
        break
    except DistributionNotFound:
        pass
else:
    raise ImportError(
        'A module was found named psycopg2, '
        'but the version of it could not be checked '
        'as it was neither the Python package psycopg2, '
        'psycopg2-binary or psycopg2cffi.'
    )

__version__ = get_distribution('sqlalchemy-redshift').version

from sqlalchemy.dialects import registry

registry.register("redshift", "sqlalchemy_redshift.dialect", "RedshiftDialect")
registry.register(
    "redshift.psycopg2", "sqlalchemy_redshift.dialect", "RedshiftDialect"
)
