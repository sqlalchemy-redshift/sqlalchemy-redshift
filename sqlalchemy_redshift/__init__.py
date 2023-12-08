from importlib.metadata import distribution as get_distribution
from importlib.metadata import version

from packaging.version import parse as parse_version

for package in ['psycopg2', 'psycopg2-binary', 'psycopg2cffi']:
    try:
        package_version = version(package)
        if parse_version(package_version) < parse_version('2.5'):
            raise ImportError('Minimum required version for psycopg2 is 2.5')
        break
    except ModuleNotFoundError:
        pass

__version__ = get_distribution('sqlalchemy_redshift').version

from sqlalchemy.dialects import registry  # noqa

registry.register(
    "redshift", "sqlalchemy_redshift.dialect",
    "RedshiftDialect_psycopg2"
)
registry.register(
    "redshift.psycopg2", "sqlalchemy_redshift.dialect",
    "RedshiftDialect_psycopg2"
)
registry.register(
    'redshift+psycopg2cffi', 'sqlalchemy_redshift.dialect',
    'RedshiftDialect_psycopg2cffi',
)

registry.register(
    "redshift+redshift_connector", "sqlalchemy_redshift.dialect",
    "RedshiftDialect_redshift_connector"
)
