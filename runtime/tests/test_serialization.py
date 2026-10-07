"""Serialization: the protobuf backend turns a message, a list of messages, or
nothing into bytes and back, refusing anything else and any bytes that are not
what was asked for; backends are found by name in the registry, and one that
cannot run here is reported when it is asked for, not when the package is
imported."""

import json
import subprocess
import sys
import textwrap
from types import SimpleNamespace

import pytest

import kabudachi
from kabudachi import serializers as serializers_module
from kabudachi.errors import (
    DuplicateSerializerError,
    SerializationError,
    SerializerUnavailableError,
    UnknownSerializerError,
)
from kabudachi.protobuf_serializer import ProtobufSerializer
from kabudachi.serializers import SerializerRegistry
from proto_messages import Greeting, Receipt, Strict


serializer = ProtobufSerializer()
NoneType = type(None)


def greeting(text="hi", times=1):
    return Greeting(text=text, times=times)


@pytest.mark.parametrize(
    ("original", "value_type"),
    [
        pytest.param(Greeting(), Greeting, id="empty message"),
        pytest.param([], list[Greeting], id="empty list"),
        pytest.param([Greeting(), Greeting(), Greeting()], list[Greeting], id="list of empty messages"),
        pytest.param([greeting("x" * 100_000, 1), greeting("y", 2)], list[Greeting], id="long message in a list"),
        pytest.param(Strict(must_be_set="ok"), Strict, id="message with required fields"),
    ],
)
def test_a_value_round_trips(original, value_type):
    assert serializer.decode(serializer.encode(original, value_type), value_type) == original


@pytest.mark.parametrize(
    ("value", "value_type", "match"),
    [
        pytest.param([greeting(), Receipt()], list[Greeting], None, id="list holding the wrong type"),
        pytest.param(greeting(), list[Greeting], None, id="non-list for a list type"),
        pytest.param(greeting(), NoneType, None, id="value for a none type"),
        pytest.param(Strict(), Strict, "Strict", id="message that cannot be serialized"),
        pytest.param([Strict(must_be_set="ok"), Strict()], list[Strict], "Strict", id="list item that cannot be serialized"),
    ],
)
def test_a_value_that_does_not_fit_its_type_is_refused_on_encode(value, value_type, match):
    with pytest.raises(SerializationError, match=match):
        serializer.encode(value, value_type)


@pytest.mark.parametrize("unsupported", [int, str, dict, list[int], list, list[list[Greeting]]])
def test_types_that_are_not_messages_are_refused(unsupported):
    with pytest.raises(SerializationError, match="supports"):
        serializer.encode(1, unsupported)
    with pytest.raises(SerializationError, match="supports"):
        serializer.decode(b"", unsupported)


def _framed_item_cut_short():
    complete = greeting("hi", 0).SerializeToString()
    return bytes([len(complete) + 5]) + complete


@pytest.mark.parametrize(
    ("payload", "value_type", "match"),
    [
        pytest.param(b"x", NoneType, None, id="bytes for a none type"),
        pytest.param(
            serializer.encode([greeting("a", 1), greeting("b", 2)], list[Greeting])[:-1],
            list[Greeting],
            None,
            id="truncated list",
        ),
        pytest.param(b"\x80", list[Greeting], None, id="broken length prefix"),
        pytest.param(b"\x05ab", list[Greeting], None, id="declared length runs past the end"),
        pytest.param(_framed_item_cut_short(), list[Greeting], "past the end", id="item cut short but valid"),
        pytest.param(serializer.encode(greeting("hello", 3), Greeting), list[Receipt], None, id="single message as a list"),
        pytest.param(b"\xff" * 10 + b"\x01", list[Greeting], "too long", id="length too long to be valid"),
        pytest.param(None, Greeting, None, id="None payload"),
        pytest.param(5, Greeting, None, id="int payload"),
        pytest.param("text", Greeting, None, id="str payload"),
    ],
)
def test_bytes_that_are_not_what_was_asked_for_are_refused_on_decode(payload, value_type, match):
    with pytest.raises(SerializationError, match=match):
        serializer.decode(payload, value_type)


@pytest.mark.parametrize("wrap", [bytearray, memoryview])
def test_bytes_like_payloads_are_accepted(wrap):
    original = greeting("hello", 3)
    encoded = serializer.encode(original, Greeting)

    assert serializer.decode(wrap(encoded), Greeting) == original


# --- the registry ----------------------------------------------------------

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


def without(*members):
    backend = duck()
    for member in members:
        delattr(backend, member)
    return backend


@pytest.mark.parametrize(
    ("backend", "match"),
    [
        *[
            pytest.param(without(member), member, id=f"missing {member}")
            for member in ["name", "available", "supports", "encode", "decode"]
        ],
        pytest.param(without("encode", "decode"), "encode, decode", id="every missing member is named"),
        *[pytest.param(duck(name=bad), "name", id=f"name {bad!r}") for bad in ["", "   ", None, 3]],
        *[
            pytest.param(duck(**{member: True}), f"{member} must be callable", id=f"{member} not callable")
            for member in ["available", "supports", "encode", "decode"]
        ],
    ],
)
def test_an_object_that_is_not_a_full_serializer_is_refused(backend, match):
    with pytest.raises(TypeError, match=match):
        SerializerRegistry().register(backend)


# --- without protobuf installed --------------------------------------------

PROGRAM = textwrap.dedent(
    """
    import importlib.abc
    import json
    import sys

    # The child imports kabudachi from wherever its parent does.
    sys.path[:0] = json.loads(sys.argv[1])

    class BlockProtobuf(importlib.abc.MetaPathFinder):
        def find_spec(self, name, path=None, target=None):
            if name == "google.protobuf" or name.startswith("google.protobuf."):
                raise ImportError("protobuf is not installed (blocked by the test)")

    sys.meta_path.insert(0, BlockProtobuf())

    import kabudachi
    from kabudachi.errors import SerializerUnavailableError
    from kabudachi.protobuf_serializer import ProtobufSerializer
    from kabudachi.serializers import SerializerRegistry

    assert ProtobufSerializer().available() is False

    try:
        SerializerRegistry.with_defaults().get("protobuf")
    except SerializerUnavailableError:
        pass
    else:
        raise SystemExit("asking for the protobuf serializer should have failed")

    try:
        ProtobufSerializer().encode(None, type(None))
    except SerializerUnavailableError:
        pass
    else:
        raise SystemExit("encoding without protobuf should have failed")

    print("ok")
    """
)


def test_the_package_imports_and_reports_a_missing_protobuf_on_use():
    result = subprocess.run(
        [sys.executable, "-c", PROGRAM, json.dumps(sys.path)],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "ok"
