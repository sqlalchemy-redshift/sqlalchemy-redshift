"""
System Tables and Views

Amazon Redshift has many system tables and views that contain information
about how the system is functioning. This module contains SQLAlchemy
Declarative models for these tables.
"""


import sqlalchemy as sa
from sqlalchemy.ext import declarative


Base = declarative.declarative_base()


class STLUnloadLog(Base):
    """
    Records the details for an unload operation.

    STL_UNLOAD_LOG records one row for each file created by an UNLOAD
    statement. For example, if an UNLOAD creates 12 files, STL_UNLOAD_LOG will
    contain 12 corresponding rows.

    This table is visible to all users.
    """

    __tablename__ = 'STL_UNLOAD_LOG'

    user_id = sa.Column('userid', sa.Integer())
    """ID of the user who generated the entry."""

    query_id = sa.Column('query', sa.Integer(), primary_key=True)
    """ID for the transaction."""

    slice = sa.Column(sa.Integer())
    """Number that identifies the slice where the query was running."""

    pid = sa.Column(sa.Integer())
    """Process ID associated with the query statement."""

    path = sa.Column(sa.Unicode(1280), primary_key=True)
    """The complete Amazon S3 object path for the file."""

    start_time = sa.Column(sa.DateTime())
    """Start time for the transaction."""

    end_time = sa.Column(sa.DateTime())
    """End time for the transaction."""

    line_count = sa.Column(sa.BigInteger())
    """Number of lines (rows) unloaded to the file."""

    transfer_size = sa.Column(sa.BigInteger())
    """Number of bytes transferred."""
