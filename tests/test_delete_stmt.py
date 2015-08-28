import pytest
from redshift_sqlalchemy.dialect import RedshiftDialect
from sqlalchemy import Table, Column, Integer, DateTime, Numeric, String, MetaData, select, delete

class TestDeleteStatement(object):
    meta = MetaData()
    
    customers = Table(
        'customers', meta,
        Column('id', Integer, primary_key=True, autoincrement=False),
        Column('first_name', String(128)),
        Column('last_name', String(128)),
        Column('email', String(255))
    )
    
    orders = Table(
        'orders', meta,
        Column('id', Integer, primary_key=True, autoincrement=False),
        Column('customer_id', Integer),
        Column('total_invoiced', Numeric(12, 4)),
        Column('discount_invoiced', Numeric(12, 4)),
        Column('grandtotal_invoiced', Numeric(12, 4)),
        Column('created_at', DateTime),
        Column('updated_at', DateTime)
    )
    
    items = Table(
        'items', meta,
        Column('id', Integer, primary_key=True, autoincrement=False),
        Column('order_id', Integer),
        Column('name', String(255)),
        Column('qty', Numeric(12,4)),
        Column('price', Numeric(12, 4)),
        Column('total_invoiced', Numeric(12, 4)),
        Column('discount_invoiced', Numeric(12, 4)),
        Column('grandtotal_invoiced', Numeric(12, 4)),
        Column('created_at', DateTime),
        Column('updated_at', DateTime)
    )
    
    @staticmethod
    def get_str(stmt):
        return str(stmt.compile(dialect=RedshiftDialect()))
    
    def test_delete_stmt_nowhereclause(self):
        del_stmt = delete(self.customers)
        
        assert self.get_str(del_stmt) == 'DELETE FROM customers'

    def test_delete_stmt_simplewhereclause1(self):
        del_stmt = delete(self.customers).where(self.customers.c.email == 'test@test.test')
        assert self.get_str(del_stmt) == "DELETE FROM customers WHERE customers.email = %(email_1)s"
        
    def test_delete_stmt_simplewhereclause2(self):
        del_stmt = delete(self.customers).where(self.customers.c.email.endswith('test.com'))
        assert self.get_str(del_stmt) == "DELETE FROM customers WHERE customers.email LIKE '%%' || %(email_1)s"
        
    def test_delete_stmt_joinedwhereclause1(self):
        del_stmt = delete(self.orders).where(self.orders.c.customer_id==self.customers.c.id)
        assert self.get_str(del_stmt) == "DELETE FROM orders USING customers WHERE orders.customer_id = customers.id"
        
    def test_delete_stmt_joinedwhereclause2(self):
        del_stmt = delete(
            self.orders
        ).where(
            self.orders.c.customer_id==self.customers.c.id
        ).where(
            self.orders.c.id==self.items.c.order_id
        ).where(
            self.customers.c.email.endswith('test.com')
        ).where(
            self.items.c.name=='test product'
        )
        expected = "DELETE FROM orders USING customers, items WHERE orders.customer_id = customers.id AND orders.id = items.order_id AND (customers.email LIKE '%%' || %(email_1)s) AND items.name = %(name_1)s"
        assert self.get_str(del_stmt) == expected
