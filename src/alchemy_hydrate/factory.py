from collections.abc import Iterator
from typing import Any

from sqlalchemy import Table

__all__ = ["model_factory", "model_dict_factory", "model_one_time"]


def model_factory(model, **defaults):
    """Make a factory for creating model instances from required fields.

    Args:
        model: SQLAlchemy decarative model
        defaults: value when creating object; required or optional

    Returns:
        factor(*args: dict[str, Any]) -> Generator(model)
    """
    dict_factory = model_dict_factory(model, **defaults)

    def factory(*args: dict[str, Any], model=model, dict_factory=dict_factory):
        for data in args:
            complete_data = dict_factory(data)
            obj = model(**complete_data)
            yield obj

    return factory


def model_one_time(model_cls, **values):
    factory = model_factory(model_cls)
    return factory(values)


def model_dict_factory(model, **defaults):
    """Make a factory for creating model data dictionaries.

    Args:
        model: SQLAlchemy decarative model
        defaults: values when creating dict; required or optional

    Returns:
        factor(*args: dict[str, Any]) -> Generator(model)
    """
    table: Table = model.__table__

    required = set()
    optional = set()

    for col in table.columns:
        if bool(col.nullable) or col.primary_key:
            optional.add(col.name)
        else:
            required.add(col.name)

    def factory(
        # obj: MyObjectType,
        *args: dict[str, Any],
        defaults: dict[str, Any] = defaults,
        required: set[str] = required,
        optional: set[str] = optional,
    ) -> Iterator[dict[str, Any]]:
        """
        Example function demonstrating type hints for the given signature.

        Args:
            *args: Positional arguments
            defaults: contains values for indefined fields
            kwarg1: The first keyword argument, a list of strings.
            kwarg2: The second keyword argument, a list of strings.

        Returns:
            A list of dictionaries with string keys and any values.
            factor(*args: dict[str, Any]) -> Generator(dict[str, Any])
        """
        for data in args:
            for field in optional:
                if field not in data:
                    data[field] = defaults[field] if field in defaults else None
            for field in required:
                if field not in data and field in defaults:
                    data[field] = defaults[field]
            yield data

    return factory
