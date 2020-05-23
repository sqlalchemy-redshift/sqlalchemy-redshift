import pytest
from sqlalchemy import exc as sa_exc, Integer

from sqlalchemy_redshift import ddl, dialect


@pytest.fixture
def preparer():
    compiler = dialect.RedshiftDDLCompiler(dialect.RedshiftDialect(), None)
    return compiler.preparer


def test_table_attributes_empty(preparer):
    # Test the null case
    text = ddl.get_table_attributes(preparer)
    expected = ""
    assert expected == text


@pytest.mark.parametrize('distkey', (
    "a_key",
    Column("a_key", Integer)))
def test_table_attributes_dist_key(preparer, distkey):
    text = ddl.get_table_attributes(preparer, distkey=distkey)
    assert text == " DISTKEY (a_key)"


@pytest.mark.parametrize('sortkey', (
    "b_key",
    ["b_key"],
    Column("b_key", Integer)))
def test_table_attributes_dist_key_one_sort_key(preparer, sortkey):
    text = ddl.get_table_attributes(preparer, distkey="a_key", sortkey=sortkey)
    assert text == " DISTKEY (a_key) SORTKEY (b_key)"


@pytest.mark.parametrize('sortkey', ("b_key", ["b_key"]))
def test_table_attributes__one_sort_key(preparer, sortkey):
    text = ddl.get_table_attributes(preparer, sortkey=sortkey)
    assert text == " SORTKEY (b_key)"


@pytest.mark.parametrize('sortkey', (("b_key", "c_key"), ["b_key", "c_key"]))
def test_table_attributes_two_sort_key(preparer, sortkey):
    text = ddl.get_table_attributes(preparer, sortkey=sortkey)
    assert text == " SORTKEY (b_key, c_key)"


@pytest.mark.parametrize('sortkey', ("b_key", ["b_key"]))
def test_table_attributes_one_sort_key_interleaved(preparer, sortkey):
    # A single interleaved key doesn't make too much sense, but redshift doesn't
    # complain, so neither should we.
    text = ddl.get_table_attributes(preparer, interleaved_sortkey=sortkey)
    assert text == " INTERLEAVED SORTKEY (b_key)"


@pytest.mark.parametrize('sortkey', (("b_key", "c_key"), ["b_key", "c_key"]))
def test_table_attributes_two_sort_key_interleaved(preparer, sortkey):
    text = ddl.get_table_attributes(preparer, interleaved_sortkey=sortkey)
    assert text == " INTERLEAVED SORTKEY (b_key, c_key)"


def test_table_attributes_one_sort_one_interleaved_raises(preparer):
    with pytest.raises(sa_exc.ArgumentError):
        ddl.get_table_attributes(preparer,
                                 sortkey="b_key",
                                 interleaved_sortkey="b_key")


def test_dist_key_with_key_diststyle(preparer):
    text = ddl.get_table_attributes(preparer, distkey="a_key", diststyle="KEY")
    assert text == " DISTSTYLE KEY DISTKEY (a_key)"


def test_no_distkey_with_key_diststyle(preparer):
    with pytest.raises(sa_exc.ArgumentError):
        ddl.get_table_attributes(preparer, diststyle="KEY")


def test_distkey_with_other_diststyles(preparer):
    for style in ("EVEN", "NONE", "ALL"):
        with pytest.raises(sa_exc.ArgumentError):
            ddl.get_table_attributes(preparer,
                                     diststyle=style,
                                     distkey="a_key")


def test_all_diststyle(preparer):
    text = ddl.get_table_attributes(preparer, diststyle="ALL")
    assert text == " DISTSTYLE ALL"


def test_even_diststyle(preparer):
    text = ddl.get_table_attributes(preparer, diststyle="EVEN")
    assert text == " DISTSTYLE EVEN"


def test_bad_diststyle(preparer):
    with pytest.raises(sa_exc.ArgumentError):
        ddl.get_table_attributes(preparer, diststyle="BAD_SHOULD_FAIL")
