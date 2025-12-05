"""
Comprehensive tests for the `Make` ORM factory.

The `Make` class simplifies constructing SQLAlchemy ORM instances by:
- Automatically populating required fields with sensible defaults.
- Inferring defaults from type hints (int→0, str→"", bool→False, etc.).
- Handling Enums, Optionals, lists, datetimes, and relationships.
- Respecting `init=False` fields such as primary keys.
- Allowing custom per-instance defaults via `register_default()`.

Example
-------
>>> class Parent(Base):
...     __tablename__ = "parent"
...     id: Mapped[int] = mapped_column(primary_key=True, init=False)
...     name: Mapped[str]
...     kind: Mapped["Parent.Kind"]
...
...     class Kind(enum.IntEnum):
...         FATHER = 1
...         MOTHER = 2
...         GUARDIAN = 3
...         UNKNOWN = 0
...
>>> make_parent = Make(Parent)
>>> p = make_parent(name="James")
>>> p.kind
Parent.Kind.UNKNOWN
"""

import datetime
import enum

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as orm

from alchemy_hydrate.factory import BrokenRelationshipError, Make, type_is_ORM

# ---------------------------------------------------------------------------
# SQLAlchemy model setup
# ---------------------------------------------------------------------------


class Base(orm.DeclarativeBase, orm.MappedAsDataclass):
    pass


class Parent(Base):
    __tablename__ = "parent"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True, init=False)
    name: orm.Mapped[str]
    kind: orm.Mapped["Parent.Kind"]
    children: orm.Mapped[list["Child"]] = orm.relationship(back_populates="parent")

    class Kind(enum.IntEnum):
        UNKNOWN = 0
        FATHER = 1
        MOTHER = 2
        GUARDIAN = 3


class Child(Base):
    __tablename__ = "child"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True, init=False)
    name: orm.Mapped[str]
    age: orm.Mapped[int | None]
    parent_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("parent.id"))
    parent: orm.Mapped[Parent] = orm.relationship(back_populates="children", init=False)


class Role(enum.IntEnum):
    UNKNOWN = 0
    ADMIN = 1
    USER = 2


class Defaultable(Base):
    __tablename__ = "defaultable"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True, init=False)
    my_int: orm.Mapped[int]
    my_str: orm.Mapped[str]
    my_bool: orm.Mapped[bool]
    my_float: orm.Mapped[float]
    my_datetime: orm.Mapped[datetime.datetime]
    my_role: orm.Mapped[Role]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def session():
    engine = sa.create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = orm.sessionmaker(bind=engine)
    with Session() as s:
        yield s
    engine.dispose()


# ---------------------------------------------------------------------------
# Core behavior
# ---------------------------------------------------------------------------


def test_make_is_orm():
    """Is the object an SQLAlchemy ORM."""
    type_is_ORM(Parent)
    with pytest.raises(TypeError):
        type_is_ORM(Role)


def test_make_repr_is_readable():
    make = Make(Parent, name="alpha")
    r = repr(make)
    assert "Make(Parent)" in r
    assert str(make.defaults) in r


def test_make_repr_class_name():
    class Make2(Make):
        pass

    make = Make2(Parent, name="alpha")
    r = repr(make)
    assert "Make2(Parent)" in r


def test_make_creates_instance_with_inferred_defaults():
    """Required fields get reasonable inferred defaults."""
    make = Make(Defaultable)
    obj = make()
    assert obj.my_int == 0
    assert obj.my_str == ""
    assert obj.my_bool is False
    assert obj.my_float == 0.0
    assert isinstance(obj.my_datetime, datetime.datetime)
    assert obj.my_role == Role.UNKNOWN


def test_make_uses_registered_default_overrides():
    make = Make(Defaultable)
    make.register_default(int, lambda: 42)
    make.register_default(Role, lambda: Role.ADMIN)
    obj = make()
    assert obj.my_int == 42
    assert obj.my_role == Role.ADMIN


def test_make_respects_explicit_defaults_and_kwargs():
    make = Make(Defaultable, my_int=10, my_str="hi")
    assert make().my_int == 10
    assert make(my_int=99).my_int == 99
    assert make().my_str == "hi"


def test_make_resolves_relationships(session):
    """Relationships should link correctly and assign foreign keys."""
    make_parent = Make(Parent)
    make_child = Make(Child)

    parent = make_parent(name="Dad")
    child = make_child(name="Son", age=5, parent=parent)
    session.add_all([parent, child])
    session.flush()

    assert parent.id == 1
    assert child.parent_id == 1
    assert parent.children == [child]
    assert child.parent is parent


def test_make_ignores_init_false_fields(monkeypatch):
    """Ensure init=False fields are set post-construction, not in __init__."""
    make_parent = Make(Parent)
    called = {}

    orig_init = Parent.__init__

    def spy(self, *args, **kw):
        called.update(kw)
        orig_init(self, *args, **kw)

    monkeypatch.setattr(Parent, "__init__", spy)

    p = make_parent(id=99, name="X")
    assert "id" not in called
    assert p.id == 99


# ---------------------------------------------------------------------------
# Enum and optional behavior
# ---------------------------------------------------------------------------


def test_make_enum_prefers_zero_or_named_unknown_or_default():
    """Ensure enum inference prefers UNKNOWN=0 or DEFAULT."""
    make = Make(Parent)
    val = make._default_for_type(Parent.Kind)
    assert val == Parent.Kind.UNKNOWN


def test_make_optional_types_use_inner_default():
    """Optional[T] should infer T’s default."""

    class OptModel(Base):
        __tablename__ = "opt"
        id: orm.Mapped[int] = orm.mapped_column(primary_key=True, init=False)
        maybe_int: orm.Mapped[int | None]

    make = Make(OptModel)
    d = make.dict()
    assert d["maybe_int"] == 0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_make_invalid_default_field_raises():
    with pytest.raises(AttributeError):
        Make(Parent, bogus="value")


def test_make_handles_empty_table_without_relationships():
    class Empty(Base):
        __tablename__ = "empty"
        id: orm.Mapped[int] = orm.mapped_column(primary_key=True, init=False)
        name: orm.Mapped[str]

    make = Make(Empty)
    obj = make()
    assert obj.name == ""


def test_make_unmapped_field_invisible():
    class NoHint(Base):
        __tablename__ = "nohint"
        id: orm.Mapped[int] = orm.mapped_column(primary_key=True, init=False)
        something = sa.Column(sa.String)

    make = Make(NoHint)
    assert make.dict() == dict()


def test_make_empty_list_relationships_default_to_empty():
    make = Make(Parent)
    p = make(name="Dad")
    assert isinstance(p.children, list)
    assert p.children == []


def test_make_handles_custom_type_default():
    class Custom:
        pass

    make = Make(Defaultable)
    make.register_default(Custom, lambda: Custom())
    assert isinstance(make._default_for_type(Custom), Custom)


def test_make_datetime_default_is_recent():
    make = Make(Defaultable)
    now = datetime.datetime.now()
    dt = make._default_for_type(datetime.datetime)
    assert isinstance(dt, datetime.datetime)
    assert (dt - now).total_seconds() < 2


# ---------------------------------------------------------------------------
# Forward reference / late binding safety tests
# ---------------------------------------------------------------------------


def test_make_handles_forward_reference_isolated_registry():
    """Ensure Make works with forward references using a clean registry."""

    # --- Create an isolated registry and Base for this test -------------
    reg = orm.registry()

    class Base:
        __abstract__ = True
        registry = reg
        metadata = reg.metadata

    # --- Define models with late-bound relationship ---------------------
    @reg.mapped_as_dataclass
    class Parent(Base):
        __tablename__ = "parent"
        id: orm.Mapped[int] = orm.mapped_column(primary_key=True, init=False)
        name: orm.Mapped[str]
        # Forward ref — LateBound not yet defined
        late_bounds: orm.Mapped[list["LateBound"]] = orm.relationship(
            back_populates="parent"
        )

    @reg.mapped_as_dataclass
    class LateBound(Base):
        __tablename__ = "late_bound"
        id: orm.Mapped[int] = orm.mapped_column(primary_key=True, init=False)
        parent_id: orm.Mapped[int | None] = orm.mapped_column(
            sa.ForeignKey("parent.id")
        )
        parent: orm.Mapped["Parent | None"] = orm.relationship(
            back_populates="late_bounds"
        )

    # Explicitly configure all mappers in this isolated registry
    reg.configure()

    # --- Exercise Make --------------------------------------------------
    make_parent = Make(Parent)
    d = make_parent.dict(name="Isolated")

    # --- Assertions -----------------------------------------------------
    assert "late_bounds" in d
    assert isinstance(d["late_bounds"], list)
    assert d["late_bounds"] == []


def test_make_defaults_list_relationship_from_string_annotation():
    """Make should default list-relationships to [] even with string annotations.

    This simulates the case where get_type_hints(...) falls back to __annotations__
    and returns a *string* like "Mapped[list[Child]]" instead of a typing object.
    In that case, _default_for_type must still recognize this as a list and
    return [] as the default.
    """

    make_parent = Make(Parent)

    # Simulate the scenario where annotations are strings, e.g. from
    # __future__ import annotations and a failed get_type_hints(), so Make
    # falls back to __annotations__ giving "Mapped[list[Child]]".
    make_parent.hints["children"] = "Mapped[list[Child]]"

    data = make_parent.dict(name="Dad")

    # The key should be present and default to an empty list
    assert "children" in data
    assert isinstance(data["children"], list)
    assert data["children"] == []


def test_make_tolerates_unresolved_string_relationship():
    """Make should work even when a relationship points at an unknown class.

    This reproduces the case where production models use string-based
    relationships, but only a subset of models are imported in the test
    module. The relationship below references "UnknownModel", which is
    never defined, and Make should still be able to construct instances.
    """

    class Broken(Base):
        __tablename__ = "broken"
        id: orm.Mapped[int] = orm.mapped_column(primary_key=True, init=False)
        name: orm.Mapped[str]

        # IMPORTANT: "UnknownModel" is never defined/imported anywhere.
        # If Make.__init__ uses sa.inspect(mapper) and forces mapper
        # configuration, SQLAlchemy will raise InvalidRequestError when
        # it tries to resolve this string.
        unknowns: orm.Mapped[list["UnknownModel"]] = orm.relationship("UnknownModel")

    # Constructing Make(Broken) should not raise:
    make_broken = Make(Broken)

    # And we should be able to build a dict of defaults without configuring
    # the SQLAlchemy mapper, even though the relationship target is unresolved.
    data = make_broken.dict(name="x")

    assert data["name"] == "x"
    # The relationship-like attribute should be present in the dict; we don't
    # care about the exact default value here (could be None, [], or "").
    assert "unknowns" in data

    with pytest.raises(BrokenRelationshipError) as excinfo:
        make_broken(name="x")

    err = excinfo.value
    assert "UnknownModel" in str(err)
    assert err.mapper is Broken
    # Check that the unknown expression is captured
    assert err.expression == "UnknownModel"
    # And that it appears in args for easy introspection
    assert "UnknownModel" in err.args
