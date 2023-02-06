import sqlalchemy as sa
from packaging.version import Version


def sa_select(*args, **kwargs):
    """Return a SELECT statement.

    This is a compatibility shim for maintaining support between SQLAlchemy
    versions 1.3, 1.4, and 2.0.
    """
    sa_version = Version(sa.__version__)
    if sa_version >= Version('1.4.0'):
        return sa.select(*args, **kwargs)
    else:
        return sa.select([*args], **kwargs)
