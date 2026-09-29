"""The serializer registry: backends are found by name, and one that cannot
run here is reported when it is asked for, not when the package is imported."""

from types import SimpleNamespace

import pytest

import kabudachi
from kabudachi import serializers as serializers_module
from kabudachi.errors import (
    DuplicateSerializerError,
    SerializerUnavailableError,
    UnknownSerializerError,
)
from kabudachi.serializers import SerializerRegistry


class Utf8Text:
    """A stand-in backend: strings as UTF-8. Proves the registry is not tied to protobuf."""

    name = "utf8"

    def available(self):
        return True

    def supports(self, value_type):
        return value_type is str

    def encode(self, value, value_type):
        return value.encode()

    def decode(self, payload, target_type):
        return payload.decode()


class Missing:
    name = "missing"

    def available(self):
        return False

    def supports(self, value_type):
        return False

    def encode(self, value, value_type):
        raise AssertionError("never called")

    def decode(self, payload, target_type):
        raise AssertionError("never called")


def test_the_error_for_an_unknown_name_lists_what_is_registered():
    registry = SerializerRegistry()
    registry.register(Utf8Text())

    with pytest.raises(UnknownSerializerError, match="utf8"):
        registry.get("nope")

    with pytest.raises(UnknownSerializerError, match="nope"):
        registry.get("nope")


def test_registering_a_name_twice_is_refused():
    registry = SerializerRegistry()
    registry.register(Utf8Text())

    with pytest.raises(DuplicateSerializerError, match="utf8"):
        registry.register(Utf8Text())


def test_an_unavailable_backend_can_be_registered_but_not_used():
    registry = SerializerRegistry()

    registry.register(Missing())

    with pytest.raises(SerializerUnavailableError, match="missing"):
        registry.get("missing")


def test_the_public_register_adds_a_backend_to_the_process_registry(monkeypatch):
    monkeypatch.setattr(serializers_module, "_process_serializers", SerializerRegistry())

    kabudachi.register_serializer(Utf8Text())

    assert serializers_module.process_serializers().names() == ["utf8"]


@pytest.mark.parametrize("member", ["name", "available", "supports", "encode", "decode"])
def test_an_object_that_is_not_a_full_serializer_is_refused(member):
    backend = SimpleNamespace(
        name="incomplete",
        available=lambda: True,
        supports=lambda value_type: True,
        encode=lambda value, value_type: b"",
        decode=lambda payload, target_type: None,
    )
    delattr(backend, member)

    with pytest.raises(TypeError, match=member):
        SerializerRegistry().register(backend)


def duck(**overrides):
    members = {
        "name": "duck",
        "available": lambda: True,
        "supports": lambda value_type: True,
        "encode": lambda value, value_type: b"",
        "decode": lambda payload, target_type: None,
    }
    members.update(overrides)
    return SimpleNamespace(**members)


def test_every_missing_member_is_named():
    backend = duck()
    del backend.encode
    del backend.decode

    with pytest.raises(TypeError, match="encode, decode"):
        SerializerRegistry().register(backend)


@pytest.mark.parametrize("bad_name", ["", "   ", None, 3])
def test_a_serializer_needs_a_non_empty_string_name(bad_name):
    with pytest.raises(TypeError, match="name"):
        SerializerRegistry().register(duck(name=bad_name))


@pytest.mark.parametrize("member", ["available", "supports", "encode", "decode"])
def test_a_serializer_member_that_is_not_callable_is_refused(member):
    with pytest.raises(TypeError, match=f"{member} must be callable"):
        SerializerRegistry().register(duck(**{member: True}))
