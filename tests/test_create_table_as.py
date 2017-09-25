import pytest
import sqlalchemy as sa

from sqlalchemy_redshift import dialect

from rs_sqla_test_utils.utils import clean, compile_query


EXAMPLE_QUERY = sa.select(['a', 'b']).select_from('table')


def test_basic_create_table_as():
    """Tests bare CreateTableAs works."""
    create = dialect.CreateTableAs('name', EXAMPLE_QUERY)

    expected_result = """
        CREATE TABLE name
        AS
        SELECT a, b FROM table
    """

    assert clean(compile_query(create)) == clean(expected_result)


def test_create_table_as_with_keys():
    """Tests CreateTableAs with dist & sort keys works."""
    create = dialect.CreateTableAs('name', EXAMPLE_QUERY,
                                   diststyle='KEY', distkey='a', sortkey='b')

    expected_result = """
        CREATE TABLE name
        DISTSTYLE KEY
        DISTKEY (a)
        SORTKEY (b)
        AS
        SELECT a, b FROM table
    """

    assert clean(compile_query(create)) == clean(expected_result)


def test_create_table_as_with_interleaved_sort_keys():
    """Tests CreateTableAs with interleaved sort keys works."""
    create = dialect.CreateTableAs('name', EXAMPLE_QUERY,
                                   sortkey=('a', 'b'),
                                   sortkey_type='INTERLEAVED')

    expected_result = """
        CREATE TABLE name
        INTERLEAVED SORTKEY (a,b)
        AS
        SELECT a, b FROM table
    """

    assert clean(compile_query(create)) == clean(expected_result)


def test_create_table_as_with_sa_column():
    """Ensure we can pass sa.Column objects for keys."""
    create = dialect.CreateTableAs('name', EXAMPLE_QUERY,
                                   diststyle='KEY',
                                   distkey=sa.Column('a'),
                                   sortkey=sa.Column('b'))

    expected_result = """
        CREATE TABLE name
        DISTSTYLE KEY
        DISTKEY (a)
        SORTKEY (b)
        AS
        SELECT a, b FROM table
    """

    assert clean(compile_query(create)) == clean(expected_result)


def test_create_table_as_with_column_names():
    """Test CreateTableAs with custom column names."""
    create = dialect.CreateTableAs('name', EXAMPLE_QUERY,
                                   columns=['c', sa.Column('d')])

    expected_result = """
        CREATE TABLE name
        (c,d)
        AS
        SELECT a, b FROM table
    """
    assert clean(compile_query(create)) == clean(expected_result)


def test_create_table_no_backup():
    """Test CreateTableAs with disabled backups."""
    create = dialect.CreateTableAs('name', EXAMPLE_QUERY, backup=False)

    expected_result = """
        CREATE TABLE name
        BACKUP NO
        AS
        SELECT a, b FROM table
    """
    assert clean(compile_query(create)) == clean(expected_result)


def test_create_table_as_temp():
    """Test creating temp tables in CreateTableAs."""
    create = dialect.CreateTableAs(
        'name', EXAMPLE_QUERY, temporary=True)
    expected_result = """
        CREATE TEMPORARY TABLE name
        AS
        SELECT a, b FROM table
    """
    assert clean(compile_query(create)) == clean(expected_result)


def test_create_table_as_quoting():
    """Test that we quote columns in CreateTableAs."""
    create = dialect.CreateTableAs('table', EXAMPLE_QUERY,
                                   schema='case',
                                   columns=['union'],
                                   distkey='select',
                                   sortkey='table')

    expected_result = """
        CREATE TABLE "case"."table"
        ("union")
        DISTKEY ("select")
        SORTKEY ("table")
        AS
        SELECT a, b FROM table
    """
    assert clean(compile_query(create)) == clean(expected_result)


def test_create_table_as_illegal_styles():
    """Tests CreateTableAs does not accept bad dist or sort styles."""
    with pytest.raises(ValueError):
        dialect.CreateTableAs('name', EXAMPLE_QUERY,
                              sortkey=('a', 'b'),
                              sortkey_type='ILLEGAL')

    with pytest.raises(ValueError):
        dialect.CreateTableAs('name', EXAMPLE_QUERY,
                              diststyle='ALSO ILLEGAl')
