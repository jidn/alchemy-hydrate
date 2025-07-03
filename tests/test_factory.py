from typing import cast

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as orm

from alchemy_hydrate.factory import Make


class Base(orm.DeclarativeBase):
    pass


class MyModel(Base):
    __tablename__ = "my_model"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    optional1: orm.Mapped[str | None] = orm.mapped_column(sa.String(20))
    optional2: orm.Mapped[int | None]
    required1: orm.Mapped[str] = orm.mapped_column(sa.String(20))
    required2: orm.Mapped[int]


my_model = cast(orm.DeclarativeBase, MyModel)
my_model_table = cast(sa.Table, MyModel.__table__)
func_defaults = dict(optional1="defaulted", required2=101038)
make_model = Make(my_model, **func_defaults)
make_table = Make(my_model_table, **func_defaults)


def test_factory_required_missing():
    # The func has two fields with default values.
    with pytest.raises(ValueError) as ex:
        make_model()
    assert "required1" in str(ex)


def test_factory_dict_values():
    data = make_model.dict(required1="required")
    assert data["id"] is None
    assert "defaulted" == data["optional1"]
    assert data["optional2"] is None
    assert "required" == data["required1"]
    assert 101038 == data["required2"]


def test_factory_dict_override_default():
    data = make_model.dict(required1="required", required2=6800)
    assert 6800 == data["required2"]


def test_factory_extra_allowed():
    data = make_model.dict(required1="required", extra=True)
    assert data["extra"] is True


@pytest.mark.skip(reason="Using warnings not ValueError.")
def test_factory_inst_table():
    """A table can not create an instance."""
    with pytest.raises(ValueError) as ex:
        make_table(required1="required")
    assert "Use dict" in str(ex)


def test_factory_table_call():
    """A table should not use __call__."""
    with pytest.warns(UserWarning, match="Tables should use dict.*"):
        make_table(required1="required")


def test_factory_inst_mapper():
    obj = MyModel(
        optional1=None, optional2=None, required1="required1", required2="required2"
    )
    assert isinstance(obj, orm.DeclarativeBase)
    assert obj.optional1 is None

    obj = make_model(required1="required")
    assert isinstance(obj, MyModel)
