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
    factory = dict_factory(model_cls)
    return factory(**values)


def dict_factory(table: Table, **defaults):
    """Create a function to create usable table dict.

    When creating an table row instance, all fields must be given.
    This returns a helper function to create dicts for instance.
    The helper takes Table column names as keyword arguments, merging
    them with the defaults, and supplying `null` to any unspecified
    columns.  Override defaults as needed.

    Args:
        table: SQLAlchemy Table
        defaults: Column values applied to each dict.

    Returns:
        factory(**column_value) -> dict

    Example:
        func = dict_factory(User.__table__, kind=User.Kind.STANDARD)
        data = func(name='cjj', email='cjj@example.com')
        str(data)
        {'kind':User.Kind.STANDARD, 'name':'cjj', 'email':'cjj@example.com',
        'gender':None, 'age':None, 'marital_status':None, 'income': None}
    """
    required = set()
    optional = set()

    # Partition the required and nullable columns.
    for col in table.columns:
        if bool(col.nullable) or col.primary_key:
            # Primary keys are considered optional as DB will generate those.
            optional.add(col.name)
        else:
            required.add(col.name)

    def factory(_table_data=(defaults, required, optional), **kwargs) -> dict[str, Any]:
        """
        Example function demonstrating type hints for the given signature.

        Args:
            *kwargs: Give data for Table column name/value.
            _table_data: Defaults and require/optional columns.

        Returns:
            A dict suitable for Table row instance creation.
        """
        defaults, required, optional = _table_data
        data = dict(defaults)
        data.update(kwargs)  # Merge given with defaults
        remaining_fields = (required.union(optional)).difference(data.keys())
        for name in remaining_fields:
            if name not in optional:
                raise ValueError(f"Not optional: {name}")
            data[name] = None
        return data

    return factory


def model_dict_factory(model, **defaults):
    """Make a factory for creating model data dictionaries.

    Args:
        model: SQLAlchemy decarative model
        defaults: values when creating dict; required or optional

    Returns:
        factor(*args: dict[str, Any]) -> Generator(model)
    """
    table: Table = model.__table__

    func = dict_factory(model.__table__, **defaults)

    def factory(
        # obj: MyObjectType,
        *args: dict[str, Any],
        _func=func,
    ) -> Iterator[dict[str, Any]]:
        """
        Example function demonstrating type hints for the given signature.

        Args:
            *args: given partial instance dictionaries

        Returns:
            A list of dictionaries with string keys and any values.
            factor(*args: dict[str, Any]) -> Generator(dict[str, Any])
        """

        for kw in args:
            yield func(**kw)

    return factory
