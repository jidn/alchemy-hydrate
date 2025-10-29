import pytest
import sqlalchemy as sa
import sqlalchemy.orm as orm

from alchemy_hydrate.factory import Make, type_is_ORM

# ----------------------------------------------------------------------
# Simple models for testing
# ----------------------------------------------------------------------


class BaseTest(orm.DeclarativeBase, orm.MappedAsDataclass):
    pass


class Parent(BaseTest):
    __tablename__ = "parent"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True, init=False)
    name: orm.Mapped[str]
    children: orm.Mapped[list["Child"]] = orm.relationship(back_populates="parent")


class Child(BaseTest):
    __tablename__ = "child"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True, init=False)
    name: orm.Mapped[str]
    age: orm.Mapped[int | None]
    parent_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("parent.id"))
    parent: orm.Mapped[Parent] = orm.relationship(back_populates="children")


# ----------------------------------------------------------------------
# Async fixture with in-memory DB and tables
# ----------------------------------------------------------------------


@pytest.fixture
def session():
    engine = sa.create_engine("sqlite:///:memory:", echo=False)
    BaseTest.metadata.create_all(engine)
    SessionLocal = orm.sessionmaker(engine, expire_on_commit=False)
    with SessionLocal() as session:
        yield session
    engine.dispose()


# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------


def test_factory_type_is_ORM_accepts_only_declarative():
    """The function `type_is_ORM` on works on class declarations."""
    # pass for proper ORM class
    type_is_ORM(Parent)

    # pass on instance
    type_is_ORM(Parent(name="p", children=[]))

    # should raise for non ORM or instances
    with pytest.raises(TypeError):
        type_is_ORM(object)


def test_factory_discovery():
    """Discovery find all field info."""
    p = Make(Parent)
    assert {"name"} == p.required
    assert {"id"} == p.optional
    assert {"id"} == p.no_init

    c = Make(Child, "id")
    assert {"name", "parent_id"} == c.required
    assert {"age"} == c.optional  # id was ignored
    assert {"parent"} == c.relationships
    assert {"id"} == c.no_init


def test_factory_invalid_default_raises_AttributeError():
    # defaults with non-existent column name
    with pytest.raises(AttributeError):
        Make(Parent, bogus_field="hi")


def test_factory_missing_required_fields_raises_ValueError():
    """Test missing required fields raises ValueError."""
    p = Make(Parent)
    with pytest.raises(ValueError, match="Missing required field"):
        p.dict()
    # once supplied -> no exception
    d = p.dict(name="ok")
    assert d["name"] == "ok"


def test_factory_default_and_overrides():
    """Test default values and overrides to defaults."""
    m = Make(Parent, name="default-name")
    assert "default-name" == m().name
    assert "override" == m(name="override").name


def test_factory_relationship(session: orm.Session):
    """Test parent/child relationship."""
    make_parent = Make(Parent)
    make_child = Make(Child)

    parent = make_parent(name="Parent")
    child = make_child(name="Child", age=5, parent=parent)
    session.add_all([parent, child])
    session.flush()

    assert 1 == child.id
    assert 1 == parent.id
    assert 1 == child.parent_id
    assert parent.children == [child]


def test_factory_missing_relationship_raises_ValueError():
    make_child = Make(Child, name="no-parent", age=1)
    # parent_id is required
    with pytest.raises(ValueError):
        make_child.dict()


def test_factory_ignore_fields_are_excluded():
    make_child = Make(Child, "age")  # ignore age
    assert "age" not in make_child.required
    assert "age" not in make_child.optional


def test_factory_repr_contains_key_info():
    m = Make(Parent, name="foo")
    r = repr(m)
    assert "Parent" in r
    assert "required" in r
    assert "optional" in r


def test_factory_example(session: orm.Session):
    """Test example in Make.__doc__"""
    # Create a user with default values; ignore `id`.
    parent = Parent(name="Dad", children=[])
    m = Make(Child, parent=parent)
    names = ["Anna", "Benjamin"]
    # `parent_id` is not required when relationship is given.
    parent.children = [m(name=name) for name in names]

    session.add(parent)
    session.flush()
    assert 1 == parent.id
    assert [1, 2] == [_.id for _ in parent.children]
    assert names == [_.name for _ in parent.children]


# ----------------------------------------------------------------------
# Edge cases
# ----------------------------------------------------------------------


def test_factory_empty_defaults_and_no_kwargs_optional_none():
    m = Make(Parent)
    data = m.dict(name="hi")
    assert {"name": "hi", "children": []} == data


def test_factory_list_relationships_default_to_empty():
    make_parent = Make(Parent, name="P1")
    parent = make_parent()
    assert isinstance(parent.children, list)
    assert parent.children == []


def test_factory_explicit_relationship():
    make_parent = Make(Parent, name="P2")
    make_child = Make(Child, name="C1")
    p = make_parent()
    c = make_child(parent=p)
    assert p.children == [c]


# def test_call_with_no_init_field_assignment(monkeypatch):
#     # Create a Make and fake that one field is "no-init"
#     m = Make(Parent)
#     m.no_init.add("custom_field")
#
#     called = {}
#
#     # Patch Python's builtin setattr so we can see what it tries to assign
#     import builtins
#
#     original_setattr = builtins.setattr
#
#     def fake_setattr(inst, key, val):
#         called[key] = val
#         return original_setattr(inst, key, val)
#
#     monkeypatch.setattr(builtins, "setattr", fake_setattr)
#
#     # Trigger Make.__call__; should hit the setattr loop
#     obj = m(custom_field="xyz", name="n")
#     assert called == {"custom_field": "xyz"}
#
#     # Restore (monkeypatch fixture auto-restores afterward)
#     assert isinstance(obj, Parent)
