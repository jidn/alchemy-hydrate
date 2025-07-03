from typing import Any, Generic, TypeVar

import sqlalchemy as sa

_M = TypeVar("_M", bound=Any)


def type_is_ORM(mapper: Any):
    """Is mapper derived from SQLAlchemy ORM model.

    Args:
        mapper: Assumed SQLAlchemy DeclarativeBase ORM model.

    Raises:
        TypeError when mapper isn't expected type.
    """
    if not hasattr(mapper, "__table__") or not isinstance(mapper.__table__, sa.Table):
        name = getattr(mapper, "__name__", repr(mapper))
        tablename = getattr(mapper, "__tablename__", "?")
        raise TypeError(
            f"Expected a SQLAlchemy ORM model class with __table__ attribute. "
            f"Got {name} (tablename={tablename})."
        )


class Make(Generic[_M]):
    """Make ORM instance when called or ORM dict.

    The helper taketakes Table column names as keyword arguments, merging
    them with the defaults, and supplying `null` to any unspecified
    columns.  Override defaults as needed.

    Example:
        # Create a user with default values.  Unspecified optional fields are None.
        make_user = Make(User, 'id', kind=User.Kind.STANDARD)

        # Create the dictionary values for an instance
        >>> make_user.dict(name='cjj', email='cjj@example.com')
        {'kind':User.Kind.STANDARD, 'name':'cjj', 'email':'cjj@example.com',
        'gender':None, 'age':None, 'marital_status':None, 'income': None}

        >>> User(**dict)
        <User>
        >>> make_user(name='tfe', email='tfe@example.com')
        <User>
    """

    def __init__(self, mapper: type[_M], *ignore: str, **defaults: Any):
        """
        Args:
            mapper: SQLAlchemy DeclarationBase model to create instances.
            *ignore: Column names that should be ignored.
            **defaults: Default values for all instances or dict.
        """
        type_is_ORM(mapper)
        self.mapper: type[_M] = mapper
        self.required, self.optional = Make.required_optional(mapper, *ignore)
        self.defaults = defaults

    def __call__(self, **kwargs: Any) -> _M:
        """Create mapper instance from given arguments and defaults.

        Args:
            **kwargs: ORM field values.

        Returns:
            Instance of the mapper.
        """
        return self.mapper(**self.dict(**kwargs))

    def dict(self, **kwargs: Any) -> dict[str, Any]:
        """
        Example function demonstrating type hints for the given signature.

        Args:
            *kwargs: Give data for Table column name/value.

        Returns:
            A dict suitable for Table row instance creation.
        """
        data = self.defaults.copy()
        data.update(kwargs)  # Merge given with defaults
        remaining_fields = (self.required | self.optional) - data.keys()
        for name in remaining_fields:
            if name not in self.optional:
                raise ValueError(f"Missing required field: {name}")
            data[name] = None
        return data

    def __repr__(self):
        return (
            f"<mapper={self.mapper},"
            f"required={sorted(self.required)}, "
            f"optional={sorted(self.optional)}, "
            f"defaults={sorted(self.defaults.items())}>"
        )

    @staticmethod
    def required_optional(mapper: Any, *ignore: str) -> tuple[set[str], set[str]]:
        """Analyze a SQLAlchemy mapper and return the optional and required fields.

        Args:
            table: DeclarativeBase or Table to analyze.
            *ignore: Field names to ignore.

        Returns:
            Tuple of required field names and optional field names.

        Raise:
            TypeError for abstrace base class
        """
        type_is_ORM(mapper)
        required = set()
        optional = set()

        # Partition the required and nullable columns.
        for col in mapper.__table__.columns:
            if col.name in ignore:
                continue
            elif bool(col.nullable) or col.primary_key:
                # Primary keys are considered optional as DB will generate those.
                optional.add(col.name)
            else:
                required.add(col.name)
        return required, optional
