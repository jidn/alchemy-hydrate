import pytest
import sqlalchemy as sa

from alchemy_hydrate.factory import dict_factory

table = sa.Table(
    "example",
    sa.MetaData(),
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("optional1", sa.String(100), nullable=True),  # Optional field
    sa.Column("optional2", sa.Integer, nullable=True),  # Optional field
    sa.Column(
        "parent_id", sa.Integer, sa.ForeignKey("parent.id")
    ),  # Foreign key to the Parent table
    sa.Column("required1", sa.String(50), nullable=False),  # Required field
    sa.Column("required2", sa.Integer, nullable=False),  # Required field
)


def test_factory_required_missing():
    # The func has two fields with default values.
    func = dict_factory(table, optional1="defaulted", required2=101038)

    with pytest.raises(ValueError) as ex:
        func()
    assert "required1" in str(ex)


def test_factory_values():
    func = dict_factory(table, optional1="defaulted", required2=101038)
    data = func(required1="required")
    assert data["id"] is None
    assert "defaulted" == data["optional1"]
    assert data["optional2"] is None
    assert "required" == data["required1"]
    assert 101038 == data["required2"]


def test_override_default():
    func = dict_factory(table, optional1="defaulted", required2=101038)
    data = func(required1="required", required2=6800)
    assert 6800 == data["required2"]


def test_factory_extra_allowed():
    func = dict_factory(table, optional1="defaulted", required2=101038)
    data = func(required1="required", extra=True)
    assert data["extra"] is True
