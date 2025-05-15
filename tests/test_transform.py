import datetime
import enum
import uuid

import pytest
from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from sqlalchemy.types import NULLTYPE

from alchemy_hydrate import TransformData
from alchemy_hydrate.transform import (
    create_enum_from_string,
    create_flag_from_string,
    is_int_enum,
)

Base = declarative_base()


class MultiTypeModel(Base):
    __tablename__ = "testor"

    id: Mapped[int] = mapped_column(unique=True, primary_key=True)
    int_or_none: Mapped[int | None]
    int_only_mapped_col = mapped_column(Integer, nullable=False)
    text: Mapped[str]
    text12 = mapped_column(String(12))
    dt: Mapped[datetime.datetime]
    correlation: Mapped[uuid.UUID]

    other_id = mapped_column(ForeignKey("other.id"))

    class StateStr(enum.Enum):
        Pending = "pending"
        Active = "active"
        Complete = "complete"

    state: Mapped[StateStr] = mapped_column(default=StateStr.Pending)

    class LevelInt(enum.Enum):
        LOW = 1
        MID = 5
        HIGN = 10

    level: Mapped[LevelInt] = mapped_column(default=LevelInt.MID)


def test_multi_type_parse():
    transformer = TransformData(MultiTypeModel)
    assert 10 == len(transformer)


def test_multi_type_convert():
    transformer = TransformData(MultiTypeModel)
    data = transformer(
        {
            "id": "123",
            "int_or_none": "",
            "int_only_mapped_col": "456",
            "text": "cjj",
            "text12": "hello",
            "dt": "20240401T131415.123456Z",
            "correlation": "00000000-0000-0000-0000-000000000000",
            "other_id": "68",
            "state": "pending",
            "level": "5",
        }
    )
    assert data == {
        "id": 123,
        "int_or_none": None,
        "int_only_mapped_col": 456,
        "text": "cjj",
        "text12": "hello",
        "dt": datetime.datetime(
            2024, 4, 1, 13, 14, 15, 123456, tzinfo=datetime.timezone.utc
        ),
        "correlation": uuid.UUID(int=0),
        "other_id": 68,
        "state": MultiTypeModel.StateStr.Pending,
        "level": MultiTypeModel.LevelInt.MID,
    }


class State(enum.Enum):
    Pending = "pending"
    Active = "active"
    Complete = "complete"


class Level(enum.Enum):
    ERROR = 1
    INFO = 5
    DEBUG = 10


class Strength(enum.IntEnum):
    WEAK = 1
    MID = 5
    STRONG = 8


class Permissions(enum.Flag):
    READ = enum.auto()
    WRITE = enum.auto()
    EXECUTE = enum.auto()
    ADMIN = READ | WRITE | EXECUTE


def test_enum_with_str():
    """Test str to enum.Enum with str member values."""

    assert State.Active == State("active")
    assert State.Complete == create_enum_from_string(State, "complete")

    with pytest.raises(ValueError):
        State("invalid")


def test_enum_with_int():
    """Test str to enum.Enum with int member values."""
    assert Level.INFO == create_enum_from_string(Level, "5")
    assert Level.INFO == create_enum_from_string(Level, "info")
    with pytest.raises(ValueError):
        assert Level.INFO == create_enum_from_string(Level, "invalid")


def test_enum_FlagEnum():
    """Test str to enum.FlagEnum with member values and comma seperated values."""
    assert Permissions.READ == create_flag_from_string(Permissions, "1")
    assert Permissions.WRITE == create_flag_from_string(Permissions, "write")
    assert Permissions.ADMIN == create_flag_from_string(Permissions, "7")
    assert Permissions.ADMIN == create_flag_from_string(
        Permissions, "read,write, execute"
    )
    with pytest.raises(ValueError):
        create_enum_from_string(Permissions, "101038")
    with pytest.raises(ValueError):
        create_enum_from_string(Permissions, "read,invalid")


def test_enum_type_detect():
    assert True == is_int_enum(Level)  # noqa 712
    assert False == is_int_enum(State)  # noqa 712
    assert True == is_int_enum(Permissions)  # noqa 712
    assert False == issubclass(Level, enum.Flag)  # noqa 712
    assert True == issubclass(Permissions, enum.Flag)  # noqa 712


def describe_table(table):
    from sqlalchemy.inspection import inspect

    table = inspect(MultiTypeModel).local_table
    pa = {"end": ", ", "sep": None}
    for col in table.columns:
        if col.name == "id":
            breakpoint()
        print("{name=", repr(col.name), **pa)
        print("nullable", col.nullable, **pa)
        print("type=", col.type, **pa)
        if col.type != NULLTYPE:
            print("python_type=", col.type.python_type, **pa)
        if col.foreign_keys:
            print("fk=", col.foreign_keys, **pa)
        print("}")


if __name__ == "__main__":
    transformer = TransformData(MultiTypeModel)

    for col in transformer.cols:
        print(col)
    output = transformer(
        {
            "id": "123",
            "int_or_none": "",
            "int_only_mapped_col": "456",
            "text": "cjj",
            "text12": "hello",
            "dt": "20240401T131415.123456Z",
            "correlation": "00000000-0000-0000-0000-000000000000",
            "other_id": "68",
            "state": "pending",
            "level": "5",
        }
    )
    print(output)
