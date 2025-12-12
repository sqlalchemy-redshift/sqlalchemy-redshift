Alembic Support
===============

The sqlalchemy-redshift dialect provides support for `Alembic <https://alembic.sqlalchemy.org/>`_ migrations with awareness of Redshift's limitations compared to PostgreSQL.

Basic Usage
-----------

To use Alembic with Redshift, configure your ``alembic.ini`` with a Redshift connection string:

.. code-block:: ini

    sqlalchemy.url = redshift+psycopg2://user:password@host:5439/database

Or for redshift_connector:

.. code-block:: ini

    sqlalchemy.url = redshift+redshift_connector://user:password@host:5439/database

Supported Operations
--------------------

The following Alembic operations are fully supported:

* ``create_table()`` - Create new tables
* ``drop_table()`` - Drop tables
* ``add_column()`` - Add columns to tables
* ``drop_column()`` - Remove columns from tables
* ``rename_table()`` - Rename tables (via ``ALTER TABLE ... RENAME TO``)
* ``alter_column()`` with ``new_column_name`` - Rename columns
* ``alter_column()`` with ``type_=VARCHAR(n)`` - Resize VARCHAR columns (Redshift native support)

Limited Support Operations
--------------------------

ALTER COLUMN TYPE Limitations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Redshift has severe limitations on column type changes compared to PostgreSQL.**

Supported:
~~~~~~~~~~

* **VARCHAR size changes only**:

.. code-block:: python

    # This works - Redshift supports VARCHAR resizing
    op.alter_column('users', 'username',
                   type_=sa.VARCHAR(200),
                   existing_type=sa.VARCHAR(100))

Generates:

.. code-block:: sql

    ALTER TABLE users ALTER COLUMN username TYPE VARCHAR(200);

Not Supported:
~~~~~~~~~~~~~~

* **Arbitrary type changes** (e.g., VARCHAR to INTEGER, INTEGER to BIGINT):

.. code-block:: python

    # This will raise alembic.util.CommandError immediately
    op.alter_column('users', 'age',
                   type_=sa.Integer(),
                   existing_type=sa.VARCHAR(10))

* **USING clauses** for type conversion:

.. code-block:: python

    # This will raise alembic.util.CommandError - USING is not supported in Redshift
    op.alter_column('users', 'age',
                   type_=sa.Integer(),
                   existing_type=sa.VARCHAR(10),
                   postgresql_using='age::integer')

When you attempt unsupported operations, the dialect will:

1. **Raise a CommandError immediately** with detailed migration instructions
2. **Prevent invalid migrations from being generated**
3. **Provide working example code** for the manual migration approach

Manual Type Change Workaround
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For non-VARCHAR type changes, you must manually perform a multi-step migration:

.. code-block:: python

    def upgrade():
        # Step 1: Add new column with desired type
        op.add_column('users', sa.Column('age_new', sa.Integer(), nullable=True))
        
        # Step 2: Copy and cast data from old column to new column
        op.execute('UPDATE users SET age_new = age::integer')
        
        # Step 3: Drop the old column
        op.drop_column('users', 'age')
        
        # Step 4: Rename new column to original name
        op.alter_column('users', 'age_new', new_column_name='age')

    def downgrade():
        # Reverse the process
        op.add_column('users', sa.Column('age_new', sa.VARCHAR(10), nullable=True))
        op.execute('UPDATE users SET age_new = age::varchar(10)')
        op.drop_column('users', 'age')
        op.alter_column('users', 'age_new', new_column_name='age')

Other Limitations
-----------------

Redshift does not support the following PostgreSQL features:

* ``SET DEFAULT`` / ``DROP DEFAULT`` via ``ALTER COLUMN`` (add defaults during ``ADD COLUMN`` instead)
* ``SET NOT NULL`` / ``DROP NOT NULL`` via ``ALTER COLUMN`` (define constraints during ``ADD COLUMN`` instead)
* Check constraints (can be defined but not enforced)
* Foreign key constraints (can be defined but not enforced)
* Most PostgreSQL-specific types and features

Redshift Extensions
-------------------

The dialect supports Redshift-specific table options via dialect-specific arguments:

.. code-block:: python

    def upgrade():
        op.create_table(
            'events',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('user_id', sa.Integer()),
            sa.Column('event_time', sa.TIMESTAMP()),
            sa.Column('event_data', sa.VARCHAR(1000)),
            redshift_diststyle='KEY',
            redshift_distkey='user_id',
            redshift_sortkey=['event_time', 'user_id']
        )

See the :doc:`ddl-compiler` documentation for more details on Redshift-specific table options.

Best Practices
--------------

1. **Test migrations in a development environment** before applying to production
2. **Use VARCHAR size changes** when possible instead of full type changes
3. **Plan manual migrations** for complex type changes
4. **Review generated SQL** for unsupported operations
5. **Consider the data volume** when planning manual type changes (they require a full table copy)
6. **Use Redshift-specific features** like distribution keys and sort keys for better performance

Additional Resources
--------------------

* `Amazon Redshift ALTER TABLE documentation <https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_TABLE.html>`_
* `Alembic documentation <https://alembic.sqlalchemy.org/>`_
* `SQLAlchemy documentation <https://docs.sqlalchemy.org/>`_
