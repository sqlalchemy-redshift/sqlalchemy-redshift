import importlib.metadata as metadata
from packaging.version import Version

for package in ['psycopg2', 'psycopg2-binary', 'psycopg2cffi']:
    try:
        version = metadata.version(package)
        if Version(version) < Version('2.5'):
            raise ImportError('Minimum required version for psycopg2 is 2.5')
        break
    except metadata.PackageNotFoundError:
        pass

try:
    __version__ = metadata.version('sqlalchemy-redshift')
except metadata.PackageNotFoundError:
    __version__ = '0.8.15.dev0'  # fallback for development

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
