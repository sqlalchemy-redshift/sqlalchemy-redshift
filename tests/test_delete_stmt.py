from redshift_sqlalchemy.dialect import RedshiftDialect
import sqlalchemy as sa


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
    sa.Column('name', sa.String(255)),
    sa.Column('qty', sa.Numeric(12, 4)),
    sa.Column('price', sa.Numeric(12, 4)),
    sa.Column('total_invoiced', sa.Numeric(12, 4)),
    sa.Column('discount_invoiced', sa.Numeric(12, 4)),
    sa.Column('grandtotal_invoiced', sa.Numeric(12, 4)),
    sa.Column('created_at', sa.DateTime),
    sa.Column('updated_at', sa.DateTime)
)


def get_str(stmt):
    return str(stmt.compile(dialect=RedshiftDialect()))


def test_delete_stmt_nowhereclause():
    del_stmt = sa.delete(customers)
    assert get_str(del_stmt) == 'DELETE FROM customers'


def test_delete_stmt_simplewhereclause1():
    del_stmt = sa.delete(customers).where(customers.c.email == 'test@test.test')
    assert get_str(del_stmt) == "DELETE FROM customers WHERE customers.email = %(email_1)s"


def test_delete_stmt_simplewhereclause2():
    del_stmt = sa.delete(customers).where(customers.c.email.endswith('test.com'))
    assert get_str(del_stmt) == "DELETE FROM customers WHERE customers.email LIKE '%%' || %(email_1)s"


def test_delete_stmt_joinedwhereclause1():
    del_stmt = sa.delete(orders).where(orders.c.customer_id == customers.c.id)
    expected = "DELETE FROM orders USING customers WHERE orders.customer_id = customers.id"
    assert get_str(del_stmt) == expected


def test_delete_stmt_joinedwhereclause2():
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
    expected = [
        "DELETE FROM orders",
        "USING customers, items",
        "WHERE orders.customer_id = customers.id",
        "AND orders.id = items.order_id",
        "AND (customers.email LIKE '%%' || %(email_1)s)",
        "AND items.name = %(name_1)s"
    ]
    expected = ' '.join(expected)
    assert get_str(del_stmt) == expected
