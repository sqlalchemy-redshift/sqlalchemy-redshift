from redshift_sqlalchemy.dialect import RedshiftDialect
import sqlalchemy as sa


class TestDeleteStatement(object):

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

    @staticmethod
    def get_str(stmt):
        return str(stmt.compile(dialect=RedshiftDialect()))

    def test_delete_stmt_nowhereclause(self):
        del_stmt = sa.delete(self.customers)

        assert self.get_str(del_stmt) == 'DELETE FROM customers'

    def test_delete_stmt_simplewhereclause1(self):
        del_stmt = sa.delete(self.customers).where(self.customers.c.email == 'test@test.test')
        assert self.get_str(del_stmt) == "DELETE FROM customers WHERE customers.email = %(email_1)s"

    def test_delete_stmt_simplewhereclause2(self):
        del_stmt = sa.delete(self.customers).where(self.customers.c.email.endswith('test.com'))
        assert self.get_str(del_stmt) == "DELETE FROM customers WHERE customers.email LIKE '%%' || %(email_1)s"

    def test_delete_stmt_joinedwhereclause1(self):
        del_stmt = sa.delete(self.orders).where(self.orders.c.customer_id == self.customers.c.id)
        expected = "DELETE FROM orders USING customers WHERE orders.customer_id = customers.id"
        assert self.get_str(del_stmt) == expected

    def test_delete_stmt_joinedwhereclause2(self):
        del_stmt = sa.delete(
            self.orders
        ).where(
            self.orders.c.customer_id == self.customers.c.id
        ).where(
            self.orders.c.id == self.items.c.order_id
        ).where(
            self.customers.c.email.endswith('test.com')
        ).where(
            self.items.c.name == 'test product'
        )
        expected = [
            "DELETE FROM orders",
            "USING customers,items",
            "WHERE orders.customer_id = customers.id",
            "AND orders.id = items.order_id",
            "AND (customers.email LIKE '%%' || %(email_1)s)",
            "AND items.name = %(name_1)s"
        ]
        expected = ' '.join(expected)
        assert self.get_str(del_stmt) == expected
