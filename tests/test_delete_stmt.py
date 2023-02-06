__author__ = 'Haleemur Ali'
"""
Tests to validate that the correct delete statement is
issued for the redshift dialect of SQL.

These tests use a simple transaction schema.

For simple delete statments that don't have a ``WHERE`` clause
or whose ``WHERE`` clause only refers to columns from the
target table, the emitted query should match that emitted
for the postgresql dialect.

However, for more complex queries, an extra ``USING`` clause is required.

For example, the following is valid in Postgresql:

.. :code-block: sql

    DELETE FROM customers
    WHERE customers.id = orders.customer_id
      AND orders.id < 100


This same query needs to be written like this in Redshift:

.. :code-block: sql
    DELETE FROM customers
    USING orders
    WHERE customers.id = orders.customer_id
      AND orders.id < 100

"""

import sqlalchemy as sa
from packaging.version import Version
from rs_sqla_test_utils.utils import clean, compile_query
from sqlalchemy_redshift.compat import sa_select

sa_version = Version(sa.__version__)


meta = sa.MetaData()

customers = sa.Table(
    'customers', meta,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=False),
    sa.Column('first_name', sa.String(128)),
    sa.Column('last_name', sa.String(128)),
    sa.Column('email', sa.String(255))
)

orders = sa.Table(
    'orders', meta,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=False),
    sa.Column('customer_id', sa.Integer),
    sa.Column('total_invoiced', sa.Numeric(12, 4)),
    sa.Column('discount_invoiced', sa.Numeric(12, 4)),
    sa.Column('grandtotal_invoiced', sa.Numeric(12, 4)),
    sa.Column('created_at', sa.DateTime),
    sa.Column('updated_at', sa.DateTime)
)

items = sa.Table(
    'items', meta,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=False),
    sa.Column('order_id', sa.Integer),
    sa.Column('product_id', sa.Integer),
    sa.Column('name', sa.String(255)),
    sa.Column('qty', sa.Numeric(12, 4)),
    sa.Column('price', sa.Numeric(12, 4)),
    sa.Column('total_invoiced', sa.Numeric(12, 4)),
    sa.Column('discount_invoiced', sa.Numeric(12, 4)),
    sa.Column('grandtotal_invoiced', sa.Numeric(12, 4)),
    sa.Column('created_at', sa.DateTime),
    sa.Column('updated_at', sa.DateTime)
)

product = sa.Table(
    'products', meta,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=False),
    sa.Column('parent_id', sa.Integer),
    sa.Column('name', sa.String(255)),
    sa.Column('price', sa.Numeric(12, 4))
)

ham = sa.Table(
    'ham', meta,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=False)
)

spam = sa.Table(
    'spam', meta,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=False)
)

hammy_spam = sa.Table(
    'ham, spam', meta,
    sa.Column('ham_id', sa.Integer, sa.ForeignKey('ham.id')),
    sa.Column('spam_id', sa.Integer, sa.ForeignKey('spam.id'))
)


def test_delete_stmt_nowhereclause(stub_redshift_dialect):
    del_stmt = sa.delete(customers)
    assert clean(compile_query(del_stmt, stub_redshift_dialect)) == \
        'DELETE FROM customers'


def test_delete_stmt_simplewhereclause1(stub_redshift_dialect):
    del_stmt = sa.delete(customers).where(
        customers.c.email == 'test@test.test'
    )
    expected = """
        DELETE FROM customers
        WHERE customers.email = 'test@test.test'"""
    assert clean(compile_query(del_stmt, stub_redshift_dialect)) == \
        clean(expected)


def test_delete_stmt_simplewhereclause2(stub_redshift_dialect):
    del_stmt = sa.delete(customers).where(
        customers.c.email.endswith('test.com')
    )
    if sa_version >= Version('1.4.0'):
        expected = """
            DELETE FROM customers
            WHERE (customers.email LIKE '%%' || 'test.com')"""
    else:
        expected = """
            DELETE FROM customers
            WHERE customers.email LIKE '%%' || 'test.com'"""
    assert clean(compile_query(del_stmt, stub_redshift_dialect)) == \
        clean(expected)


def test_delete_stmt_joinedwhereclause1(stub_redshift_dialect):
    del_stmt = sa.delete(orders).where(
        orders.c.customer_id == customers.c.id
    )
    expected = """
        DELETE FROM orders
        USING customers
        WHERE orders.customer_id = customers.id"""
    assert clean(compile_query(del_stmt, stub_redshift_dialect)) == \
        clean(expected)


def test_delete_stmt_joinedwhereclause2(stub_redshift_dialect):
    del_stmt = sa.delete(
        orders
    ).where(
        orders.c.customer_id == customers.c.id
    ).where(
        orders.c.id == items.c.order_id
    ).where(
        customers.c.email.endswith('test.com')
    ).where(
        items.c.name == 'test product'
    )
    expected = """
      DELETE FROM orders
      USING customers, items
      WHERE orders.customer_id = customers.id
      AND orders.id = items.order_id
      AND (customers.email LIKE '%%' || 'test.com')
      AND items.name = 'test product'"""

    assert clean(compile_query(del_stmt, stub_redshift_dialect)) == \
        clean(expected)


def test_delete_stmt_subqueryplusjoin(stub_redshift_dialect):
    del_stmt = sa.delete(
        orders
    ).where(
        orders.c.customer_id.in_(
            sa_select(
                customers.c.id
            ).where(customers.c.email.endswith('test.com'))
        )
    ).where(
        orders.c.id == items.c.order_id
    ).where(
        items.c.name == 'test product'
    )
    expected = """
      DELETE FROM orders
      USING items
      WHERE orders.customer_id IN
      (SELECT customers.id
      FROM customers
      WHERE (customers.email LIKE '%%' || 'test.com'))
      AND orders.id = items.order_id
      AND items.name = 'test product'"""
    assert clean(compile_query(del_stmt, stub_redshift_dialect)) == \
        clean(expected)


def test_delete_stmt_subquery(stub_redshift_dialect):
    del_stmt = sa.delete(
        orders
    ).where(
        orders.c.customer_id.in_(
            sa_select(
                customers.c.id
            ).where(customers.c.email.endswith('test.com'))
        )
    )
    expected = """
        DELETE FROM orders
        WHERE orders.customer_id IN
        (SELECT customers.id
        FROM customers
        WHERE (customers.email LIKE '%%' || 'test.com'))"""
    assert clean(compile_query(del_stmt, stub_redshift_dialect)) == \
        clean(expected)


def test_delete_stmt_on_subquerycomma(stub_redshift_dialect):
    del_stmt = sa.delete(
        ham
    ).where(
        ham.c.id.in_(
            sa_select(
                hammy_spam.c.ham_id
            )
        )
    )
    expected = """
        DELETE FROM ham
        WHERE ham.id IN
        (SELECT "ham, spam".ham_id
        FROM "ham, spam")"""
    assert clean(compile_query(del_stmt, stub_redshift_dialect)) == \
        clean(expected)


def test_delete_on_comma(stub_redshift_dialect):
    del_stmt = sa.delete(ham).where(ham.c.id == hammy_spam.c.ham_id)
    expected = """
        DELETE FROM ham USING "ham, spam"
        WHERE ham.id = "ham, spam".ham_id"""
    assert clean(compile_query(del_stmt, stub_redshift_dialect)) == \
        clean(expected)


def test_delete_stmt_on_alias(stub_redshift_dialect):
    parent_ = sa.alias(product)
    del_stmt = sa.delete(
        product
    ).where(product.c.parent_id == parent_.c.id)
    expected = """
        DELETE FROM products
        USING products AS products_1
        WHERE products.parent_id = products_1.id"""
    assert clean(compile_query(del_stmt, stub_redshift_dialect)) == \
        clean(expected)


def test_delete_stmt_with_comma_subquery_alias_join(stub_redshift_dialect):
    parent_ = sa.alias(product)

    del_stmt = sa.delete(
        items
    ).where(
        items.c.order_id == orders.c.id
    ).where(
        orders.c.customer_id.in_(
            sa_select(customers.c.id).where(
                customers.c.email.endswith('test.com')
            )
        )
    ).where(
        items.c.product_id == product.c.id
    ).where(
        product.c.parent_id == parent_.c.id
    ).where(
        parent_.c.id != hammy_spam.c.ham_id
    )

    expected = """
        DELETE FROM items
        USING orders, products, products AS products_1, "ham, spam"
        WHERE items.order_id = orders.id
        AND orders.customer_id IN
        (SELECT customers.id
        FROM customers
        WHERE (customers.email LIKE '%%' || 'test.com'))
        AND items.product_id = products.id
        AND products.parent_id = products_1.id
        AND products_1.id != "ham, spam".ham_id"""

    assert clean(compile_query(del_stmt, stub_redshift_dialect)) == \
        clean(expected)
