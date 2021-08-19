import pytest
import sqlalchemy as sa
from rs_sqla_test_utils.utils import clean, compile_query

from sqlalchemy_redshift import dialect


@pytest.mark.parametrize(
    "kwargs,expected_query",
    (
        ({}, "ALTER TABLE trg APPEND FROM src"),
        ({"fill_target": True}, "ALTER TABLE trg APPEND FROM src FILLTARGET"),
        ({"ignore_extra": True}, "ALTER TABLE trg APPEND FROM src IGNOREEXTRA"),
    ),
)
def test_append__basic(kwargs, expected_query):
    source = sa.Table("src", sa.MetaData(), sa.Column("value", sa.Integer))
    target = sa.Table("trg", sa.MetaData(), sa.Column("value", sa.Integer))
    command = dialect.AlterTableAppendCommand(source, target, **kwargs)
    assert clean(compile_query(command)) == clean(expected_query)


def test_append__ignoreextra_and_filltarget():
    source = sa.Table("src", sa.MetaData(), sa.Column("value", sa.Integer))
    target = sa.Table("trg", sa.MetaData(), sa.Column("value", sa.Integer))

    with pytest.raises(
        ValueError, match='"ignore_extra" cannot be used with "fill_target".'
    ):
        dialect.AlterTableAppendCommand(
            source, target, fill_target=True, ignore_extra=True
        )
