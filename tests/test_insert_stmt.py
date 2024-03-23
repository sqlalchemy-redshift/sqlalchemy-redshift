import sqlalchemy as sa
from packaging.version import Version

from rs_sqla_test_utils.utils import clean, compile_query

sa_version = Version(sa.__version__)


meta = sa.MetaData()

customers = sa.Table(
    'customers', meta,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=False),
    sa.Column('first_name', sa.String(128)),
    sa.Column('last_name', sa.String(128)),
    sa.Column('email', sa.String(255))
)


def test_insert_values(stub_redshift_dialect):
    insert_stmt = customers.insert().values(
        (1, "Firstname", "Lastname", "firstname.lastname@example.com")
    )

    expected = """
    INSERT INTO customers (id, first_name, last_name, email)
    VALUES
        (1, 'Firstname', 'Lastname', 'firstname.lastname@example.com')
    """

    assert clean(compile_query(insert_stmt, stub_redshift_dialect)) == \
        clean(expected)

    insert_stmt = customers.insert().values([
        (1, "Firstname", "Lastname", "firstname.lastname@example.com"),
        (2, "Firstname2", "Lastname2", "firstname2.lastname2@example.com"),
    ])
    expected = """
    INSERT INTO customers (id, first_name, last_name, email)
    VALUES
        (1, 'Firstname', 'Lastname', 'firstname.lastname@example.com'),
        (2, 'Firstname2', 'Lastname2', 'firstname2.lastname2@example.com')
    """

    assert clean(compile_query(insert_stmt, stub_redshift_dialect)) == \
        clean(expected)


def test_insert_from_select(stub_redshift_dialect):
    select_expr = sa.select(customers.columns)
    if sa_version >= Version('1.4.0'):
        columns = select_expr.selected_columns
    else:
        columns = select_expr.columns
    
    insert_stmt = customers.insert().from_select(columns, select_expr)

    expected = """
    INSERT INTO customers (id, first_name, last_name, email)
        SELECT
            customers.id,
            customers.first_name,
            customers.last_name,
            customers.email
        FROM customers
    """

    assert clean(compile_query(insert_stmt, stub_redshift_dialect)) == \
        clean(expected)


def test_insert_from_cte(stub_redshift_dialect):
    cte_expr = sa.select(customers.columns).cte("cte")

    select_expr = sa.select(cte_expr.columns).where(
        cte_expr.columns.last_name == "Lastname"
    )
    if sa_version >= Version('1.4.0'):
        columns = select_expr.selected_columns
    else:
        columns = select_expr.columns

    insert_stmt = customers.insert().from_select(columns, select_expr)

    expected = """
    INSERT INTO customers (id, first_name, last_name, email)
    WITH cte AS
        (SELECT
            customers.id AS id,
            customers.first_name AS first_name,
            customers.last_name AS last_name,
            customers.email AS email
        FROM customers)
    SELECT cte.id, cte.first_name, cte.last_name, cte.email
    FROM cte
    WHERE cte.last_name = 'Lastname'
    """

    assert clean(compile_query(insert_stmt, stub_redshift_dialect)) == \
        clean(expected)
