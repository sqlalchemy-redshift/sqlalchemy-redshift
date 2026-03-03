from importlib.metadata import PackageNotFoundError, version

from packaging.version import Version

for package in ["psycopg2", "psycopg2-binary", "psycopg2cffi"]:
    try:
        if Version(version(package)) < Version("2.5"):
            raise ImportError("Minimum required version for psycopg2 is 2.5")
        break
    except PackageNotFoundError:
        pass

__version__ = version("sqlalchemy-redshift")

from sqlalchemy.dialects import registry  # noqa

registry.register("redshift", "sqlalchemy_redshift.dialect", "RedshiftDialect_psycopg2")
registry.register(
    "redshift.psycopg2", "sqlalchemy_redshift.dialect", "RedshiftDialect_psycopg2"
)
registry.register(
    "redshift+psycopg2cffi",
    "sqlalchemy_redshift.dialect",
    "RedshiftDialect_psycopg2cffi",
)

registry.register(
    "redshift+redshift_connector",
    "sqlalchemy_redshift.dialect",
    "RedshiftDialect_redshift_connector",
)
