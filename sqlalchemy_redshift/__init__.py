from pkg_resources import DistributionNotFound, get_distribution, parse_version

for package in ['psycopg2', 'psycopg2-binary', 'psycopg2cffi']:
    try:
        if get_distribution(package).parsed_version < parse_version('2.5'):
            raise ImportError('Minimum required version for psycopg2 is 2.5')
        break
    except DistributionNotFound:
        pass

__version__ = get_distribution('sqlalchemy-redshift').version

from sqlalchemy.dialects import registry
from sqlalchemy.dialects.postgresql.base import PGDialect
import re

def _get_server_version_info(self, connection):
    v = connection.exec_driver_sql("select pg_catalog.version()").scalar()
    m = re.match(
            r".*(?:PostgreSQL|EnterpriseDB) "
            r"(\d+)\.?(\d+)?(?:\.(\d+))?(?:\.\d+)?(?:devel|beta)?",
            v,
    )
    if not m:
        if "Redshift" in v: # There is no way of extracting Postgres version from the new version format
            return (8, 0, 2)
        raise AssertionError(
            "Could not determine version from string '%s'" % v
        )
    return tuple([int(x) for x in m.group(1, 2, 3) if x is not None])


PGDialect._get_server_version_info = _get_server_version_info

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
