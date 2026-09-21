"""The task registry: every task known to this process by its stable name."""

import pytest

from kabudachi.config import UNSET
from kabudachi.errors import DuplicateTaskError
from kabudachi.registry import TaskDefinition, TaskKind, TaskRegistry


def body(argument):
    return argument


def other_body(argument):
    return argument


def definition(name="billing.charge", func=body, **overrides):
    fields = {
        "name": name,
        "func": func,
        "kind": TaskKind.TASK,
        "serializer": "protobuf",
        "version": 0,
        "queue": UNSET,
        "input_type": int,
        "output_type": int,
        "is_async": False,
    }
    fields.update(overrides)
    return TaskDefinition(**fields)


def test_a_registered_definition_is_found_by_name():
    registry = TaskRegistry()
    charge = definition()

    registry.register(charge)

    assert registry.get("billing.charge") is charge


def test_an_unknown_name_is_not_found():
    assert TaskRegistry().get("nope") is None


def test_a_registry_knows_whether_it_has_a_name():
    registry = TaskRegistry()
    registry.register(definition())

    assert "billing.charge" in registry
    assert "nope" not in registry


def test_registering_a_name_twice_is_refused_and_keeps_the_first():
    registry = TaskRegistry()
    first = definition(func=body)
    registry.register(first)

    with pytest.raises(DuplicateTaskError, match=r"billing\.charge"):
        registry.register(definition(func=other_body))

    assert registry.get("billing.charge") is first


def test_definitions_come_out_in_registration_order():
    registry = TaskRegistry()
    names = ["c.task", "a.task", "b.task"]
    for name in names:
        registry.register(definition(name=name))

    assert [d.name for d in registry.definitions()] == names


def test_registries_are_independent():
    first, second = TaskRegistry(), TaskRegistry()

    first.register(definition())

    assert "billing.charge" not in second


def test_a_definition_cannot_be_changed_after_it_is_made():
    charge = definition()

    with pytest.raises(AttributeError):
        charge.version = 9


def test_a_definition_keeps_the_callable_it_wraps():
    assert definition(func=body).func is body
