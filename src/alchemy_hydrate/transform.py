"""Generate SQLAlchemy transform from model."""

import enum
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable

from sqlalchemy import inspect
from sqlalchemy.types import NullType

from . import registry


@dataclass
class ConvertCol:
    name: str
    nullable: bool
    typeof: Any
    from_str: Callable[[str], Any]

    def __call__(self, input: str | None) -> Any:
        if not input:
            if self.nullable:
                return None
            raise ValueError(f"{self.name} is required.")
        return self.from_str(input)


class TransformData:
    def __init__(self, table, extra_converters=None):
        self.cols: list[ConvertCol] = []
        tbl = inspect(table).local_table

        converters = registry
        if isinstance(extra_converters, dict):
            converters = registry | extra_converters

        for col in tbl.columns:
            try:
                if isinstance(col.type, NullType) and col.foreign_keys:
                    # TODO: the underlying type is int, but I don't see it
                    python_type = int
                elif hasattr(col, "python_type"):
                    python_type = col.python_type
                elif hasattr(col.type, "python_type"):
                    python_type = col.type.python_type
                else:
                    raise TypeError(f"What underlying type is {col}")
            except Exception:
                raise

            tbl_col = f"{tbl.name}.{col.name}"
            if tbl_col in converters:
                func = converters[tbl_col]
            elif col.name in converters:
                func = converters[col.name]
            elif python_type in converters:
                func = converters[python_type]
            elif isinstance(python_type, enum.EnumType):
                # func = lambda s, t=python_type: create_enum_from_string(t, s)
                def enum_type_from_str(s: str, t=python_type):
                    return create_enum_from_string(t, s)

                func = enum_type_from_str
            elif isinstance(python_type, enum.Flag):
                # func = lambda s, t=python_type: create_flag_from_string(t, s)
                def enum_flag_from_str(s, t=python_type):
                    return create_flag_from_string(t, s)

                func = enum_flag_from_str
            else:
                raise TypeError(f"No converter for {col}")

            converter = ConvertCol(col.name, col.nullable, python_type, func)
            self.cols.append(converter)

    def __len__(self) -> int:
        return len(self.cols)

    def __call__(self, input: dict[str, str]) -> dict[str, Any]:
        output = {}
        for converter in reversed(self.cols):
            try:
                output[converter.name] = converter(input[converter.name])
            except ValueError:
                breakpoint()
        return output


def create_enum_from_string(enum_cls, value_str):
    """
    Creates an enum member from a string value.

    Args:
        enum_cls: The Enum class (e.g., LevelInt, StateText, Permissions).
        value_str: The string value to match against enum member names or values.

    Raises:
        ValueError if no match is found.

    Returns:
        An instance of the enum member.
    """
    try:
        # Try to get the enum member directly by name (case-insensitive)
        return enum_cls[value_str.upper()]
    except KeyError:
        # If not found by name, try to match by value (converting if necessary)
        for member in enum_cls:
            if isinstance(member.value, (int, float)):
                try:
                    if int(value_str) == member.value:
                        return member
                except ValueError:
                    pass  # Ignore if the string cannot be converted to an integer
            elif isinstance(member.value, str):
                if value_str.lower() == member.value.lower():
                    return member
            elif isinstance(member.value, tuple):
                # Handle Flag enums with tuple values (less common for direct string input)
                if value_str in member.value:
                    return member
            else:
                if value_str == str(member.value):
                    return member
        raise ValueError(f"No conversion from {repr(value_str)} to {enum_cls}")


def create_flag_from_string(flag_cls, value: str):
    """Creates a Flag enum member from a string value.

    Args:
        flag_cls: enum.FlagEnum
        value: Can be comma-separated names or an integer.
            'READ,WRITE' or '3'

    Raises:
        ValueError if no match is found.

    Returns:
        An instance of the enum member.
    """
    try:
        # Try converting to integer directly
        int_value = int(value)
        return flag_cls(int_value)
    except ValueError:
        # Try matching by comma-separated names (case-insensitive)
        flags = 0
        for name in value.upper().replace(" ", "").split(","):
            try:
                flags |= flag_cls[name].value
            except KeyError:
                print(f"Warning: Flag '{name}' not found in {flag_cls.__name__}")
        return flag_cls(flags) if flags else None
    except Exception as e:
        print(f"Error creating Flag from '{value}': {e}")
        return None


def is_int_enum(enum_cls):
    """Programmatically determines if an Enum (or its subclasses)  uses integers as values."""
    if not issubclass(enum_cls, enum.Enum):
        return False
    for member in enum_cls:
        if not isinstance(member.value, int):
            return False
    return True
