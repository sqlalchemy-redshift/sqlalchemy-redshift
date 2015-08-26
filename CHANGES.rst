
0.1.3 (unreleased)
------------------

- Don't visit indexes as Redshift doesn't support them.
  Thanks `bouk <https://github.com/bouk>`_.
  (`Issue #23 <https://github.com/graingert/redshift_sqlalchemy/pull/23>`_)
- Use SYSDATE instead of NOW().
  Thanks `bouk <https://github.com/bouk>`_.
  (`Issue #15 <https://github.com/graingert/redshift_sqlalchemy/pull/15>`_)
- Default to SSL with hardcoded AWS Redshift CA.
  (`Issue #20 <https://github.com/graingert/redshift_sqlalchemy/pull/20>`_)


0.1.2 (2015-08-11)
------------------

- Register postgresql.visit_rename_table for redshift's
  alembic RenameTable.
  Thanks `bouk <https://github.com/bouk>`_.
  (`Issue #7 <https://github.com/graingert/redshift_sqlalchemy/pull/7>`_)


0.1.1 (2015-05-20)
------------------

- Register RedshiftImpl as an alembic 3rd party dialect.


0.1.0 (2015-05-11)
------------------

- First version of sqlalchemy-redshift that can be installed from PyPI
