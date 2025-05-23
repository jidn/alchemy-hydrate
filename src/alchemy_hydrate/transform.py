"""Generate SQLAlchemy transform from model."""

import enum
from dataclasses import dataclass
from typing import Any, Callable

from sqlalchemy import Column, Table
from sqlalchemy.types import NullType

from . import registry


@dataclass
class ConvertCol:
    name: str
    nullable: bool
    typeof: Any
    from_str: Callable[[str], Any]

    def __call__(self, input: str) -> Any:
        if not input and self.nullable:
            return None
        return self.from_str(input)


class TransformData:
    def __init__(self, table: Table, extra_converters=dict[Any, Callable] | None):
        """A transform for the fields in table.

        Args:
            table: from sqlalchemy
            extra_converters:
                A dict of additional converters.
                The dict key can be any of
                    fieldname: str, ie 'when_created'
                    type: ie datetime.datetime, enum.Enum

        Raises:
            ValueError for unavailable converter for table.columns.name
        """
        self.name: str = table.name
        self.columns: list[ConvertCol] = []

        self.converters = registry
        if isinstance(extra_converters, dict):
            self.converters = registry | extra_converters

        for col in table.columns:
            # Convert by field name, type is irrelevant
            if col.name in self.converters:
                python_type = None
                func = self.converters[col.name]
            # Seen when type is subclass instance of TypeDecorators
            elif getattr(col.type, "_is_type_decorator", None):
                python_type = col.type.__class__

                def enum_flag_from_str(s, instance=col.type):
                    return instance.process_result_value(s, None)

                func = enum_flag_from_str

            else:
                # Convert by column type
                python_type = self.get_column_type(col)
                func = self.get_coverter_by_type(col, python_type)

            converter = ConvertCol(col.name, bool(col.nullable), python_type, func)
            self.columns.append(converter)

    def __len__(self) -> int:
        return len(self.columns)

    def __call__(self, input: dict[str, str]) -> dict[str, Any]:
        output = {}
        for converter in self.columns:
            # Ignore Table fields not in CSV, they may not be required.
            if converter.name in input:
                output[converter.name] = converter(input[converter.name])
        return output

    def __iter__(self):
        return iter(self.columns)

    def __repr__(self):
        lines = [self.name]
        lines.extend(("  " + repr(_)) for _ in self.columns)
        return "\n".join(lines)

    def get_column_type(self, column: Column) -> Any:
        try:
            if isinstance(column.type, NullType) and column.foreign_keys:
                return int
            elif hasattr(column, "python_type"):
                return column.python_type
            elif hasattr(column.type, "python_type"):
                return column.type.python_type
            else:
                raise TypeError(f"What underlying type is {column}")
        except Exception:
            raise

    def get_coverter_by_type(
        self, column: Column, python_type: Any
    ) -> Callable[[str], Any]:
        if python_type in self.converters:
            # For basic types like Mapped[str]
            # It can be very difficult to have length violations so lets
            # check it here.
            if python_type is not str:
                return self.converters[python_type]
            else:
                size = getattr(column.type, "length", None)
                if size is None:
                    return self.converters[python_type]
                else:

                    def verify_str_length(value, max_len=size, field=column.name):
                        if len(value) <= max_len:
                            return value
                        raise ValueError(f"{repr(field)} exceeds length {max_len}")

                    return verify_str_length

        elif issubclass(python_type, enum.Flag):

            def enum_flag_from_str(s, t=python_type):
                return create_flag_from_string(t, s)

            return enum_flag_from_str
        elif isinstance(python_type, enum.EnumType):

            def enum_type_from_str(s: str, t=python_type):
                return create_enum_from_string(t, s)

            return enum_type_from_str
        else:
            raise TypeError(f"No converter for {column}")


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
        value: Can be pipe-separated names or an integer.
            'READ|WRITE' or '3'

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
        for name in value.upper().replace(" ", "").split("|"):
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
