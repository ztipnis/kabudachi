"""Serialization: the protobuf backend turns a message, a list of messages, or
nothing into bytes and back, refusing anything else and any bytes that are not
what was asked for; backends are found by name in the registry, and one that
cannot run here is reported when it is asked for, not when the package is
imported."""

import dataclasses
import json
import subprocess
import sys
import textwrap
from types import SimpleNamespace

import pydantic
import pytest

import kabudachi
from kabudachi import schemas
from kabudachi import serializers as serializers_module
from kabudachi.dataclass_serializer import DataclassSerializer
from kabudachi.errors import (
    DuplicateSerializerError,
    SerializationError,
    SerializerUnavailableError,
    UnknownSerializerError,
)
from kabudachi.protobuf_serializer import ProtobufSerializer
from kabudachi.pydantic_serializer import PydanticSerializer
from kabudachi.schemas import Incompatible, Newer, Runnable, SchemaRegistry
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

    BLOCKED = ("google.protobuf", "pydantic", "pydantic_core")

    class BlockOptionalPackages(importlib.abc.MetaPathFinder):
        def find_spec(self, name, path=None, target=None):
            if any(name == blocked or name.startswith(blocked + ".") for blocked in BLOCKED):
                raise ImportError(f"{name} is not installed (blocked by the test)")

    sys.meta_path.insert(0, BlockOptionalPackages())

    import kabudachi
    from kabudachi.errors import SerializerUnavailableError
    from kabudachi.protobuf_serializer import ProtobufSerializer
    from kabudachi.serializers import SerializerRegistry

    assert ProtobufSerializer().available() is False

    for missing in ("protobuf", "pydantic"):
        try:
            SerializerRegistry.with_defaults().get(missing)
        except SerializerUnavailableError:
            pass
        else:
            raise SystemExit(f"asking for the {missing} serializer should have failed")

    try:
        ProtobufSerializer().encode(None, type(None))
    except SerializerUnavailableError:
        pass
    else:
        raise SystemExit("encoding without protobuf should have failed")

    print("ok")
    """
)


def test_the_package_imports_and_reports_a_missing_optional_package_on_use():
    result = subprocess.run(
        [sys.executable, "-c", PROGRAM, json.dumps(sys.path)],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "ok"


# --- schema families and migrations ----------------------------------------

class Order(pydantic.BaseModel):
    text: str


class Shipment(pydantic.BaseModel):
    order: str
    items: list[str]
    notes: dict[str, int] = {}


def bump(name):
    def step(plain):
        return {**plain, "seen": [*plain.get("seen", []), name]}

    return step


INCOMPATIBLE = None


# Bumps by from_version: a name for an explicit step, INCOMPATIBLE, or absent for the default.
@pytest.mark.parametrize(
    "bumps, source, target, expected",
    [
        ({}, 2, 2, []),
        ({1: "1>2", 2: "2>3"}, 1, 3, ["1>2", "2>3"]),
        ({1: "1>2"}, 1, 3, ["1>2", "default"]),
        ({}, 0, 2, ["default", "default"]),
        (
            {2: INCOMPATIBLE},
            1,
            3,
            Incompatible("version 2 of tests.Order cannot be migrated to version 3: the bump is declared incompatible"),
        ),
        ({2: INCOMPATIBLE}, 3, 3, []),
        ({1: "1>2", 2: "2>3"}, 4, 3, Newer("version 4 of tests.Order is newer than this worker's 3")),
        ({}, 1, 0, Newer("version 1 of tests.Order is newer than this worker's 0")),
    ],
)
def test_a_migration_plan_takes_explicit_and_default_steps_and_stops_at_an_incompatible_bump(
    bumps, source, target, expected
):
    registry = SchemaRegistry()
    registry.declare(Order, version=target, family="tests.Order")
    for from_version, name in bumps.items():
        registry.add_migration("tests.Order", from_version, None if name is INCOMPATIBLE else bump(name))
    planned = registry.plan("tests.Order", source, target)
    if not isinstance(expected, list):
        assert planned == expected
        return
    assert isinstance(planned, Runnable) and planned.version == target
    plain = {}
    for step in planned.steps:
        plain = bump("default")(plain) if step is None else step(plain)
    assert plain.get("seen", []) == expected


def test_classes_are_versions_of_one_family_and_each_bump_is_declared_once():
    class OrderV1(pydantic.BaseModel):
        text: str

    class OrderV2(pydantic.BaseModel):
        text: str
        priority: int = 0

    registry = SchemaRegistry()
    upgrade = bump("1>2")
    registry.declare(OrderV1, version=1, family="shop.Order", version_path="$.v")
    registry.declare(OrderV2, version=2, family="shop.Order", version_path="$.v", migration=upgrade)
    # The same declaration again (a module imported under two names) is allowed.
    registry.declare(OrderV2, version=2, family="shop.Order", version_path="$.v", migration=upgrade)
    assert registry.family(list[OrderV1]) == "shop.Order"
    assert registry.schema_of(OrderV2).version_path == ("v",)
    assert registry.version_of(type(None)) == 0
    with pytest.raises(ValueError, match="already declared"):
        registry.declare(OrderV1, version=3, family="shop.Order", version_path="$.v")
    with pytest.raises(ValueError, match="version 2 of shop.Order"):
        registry.declare(Receipt, version=2, family="shop.Order", version_path="$.v")
    with pytest.raises(ValueError, match="version_path"):
        registry.declare(Shipment, version=3, family="shop.Order", version_path="$.other")
    with pytest.raises(ValueError, match="already has a migration from version 1"):
        registry.add_migration("shop.Order", 1, bump("again"))
    with pytest.raises(ValueError, match="non-negative"):
        registry.declare(Receipt, version=-1)
    with pytest.raises(TypeError, match="one argument"):
        registry.add_migration("shop.Order", 5, lambda a, b: a)
    with pytest.raises(ValueError, match="family"):
        schemas.migration(type(None), from_version=0)


def test_protobuf_names_a_message_family_validates_and_migrates_by_parsing_into_the_new_message():
    assert serializer.schema_family(Greeting) == Greeting.DESCRIPTOR.full_name
    assert serializer.schema_family(list[Greeting]) == Greeting.DESCRIPTOR.full_name
    assert serializer.extract_version(greeting("a").SerializeToString(), Greeting) is None
    serializer.validate(greeting("a"))
    assert serializer.type_compatibility(Greeting, Greeting) is True
    assert serializer.type_compatibility(Greeting, Receipt) is False
    assert serializer.type_compatibility(Greeting, dict) is None, "not a type it knows"
    # A Receipt's bytes read as a Greeting: fields the Greeting lacks are dropped, not carried.
    plain = serializer.load_plain(Receipt(ok=True).SerializeToString(), Greeting)
    assert serializer.dump_plain(plain, Greeting) == b""


def test_a_backend_without_the_schema_members_still_works_with_defaults():
    text = Utf8Text()
    registry = SchemaRegistry()
    assert schemas.schema_family(text, str, registry) == "builtins.str"
    assert schemas.extract_version(text, b"x", str) is None
    schemas.validate(text, "x")
    assert schemas.type_compatibility(text, str, str) is None, "the backend does not say, so unknown"
    assert not schemas.can_migrate(text)


# --- the pydantic and dataclass serializers --------------------------------

@dataclasses.dataclass
class Label:
    text: str


@dataclasses.dataclass
class Parcel:
    order: str
    weights: list[float]
    label: Label | None = None


@dataclasses.dataclass
class Reading:
    value: float | None
    by_day: dict[str, float]


class Crate(pydantic.BaseModel):
    order: str
    parts: list[Order]
    main: Order | None = None


JSON_CASES = [
    (PydanticSerializer(), Shipment(order="o1", items=["b", "a"], notes={"z": 1, "a": 2}), Shipment),
    (PydanticSerializer(), [Shipment(order="o1", items=[])], list[Shipment]),
    (PydanticSerializer(), None, type(None)),
    (DataclassSerializer(), Parcel(order="o1", weights=[1.5], label=Label("x")), Parcel),
    (DataclassSerializer(), [Parcel(order="o2", weights=[])], list[Parcel]),
]


@pytest.mark.parametrize("backend, value, value_type", JSON_CASES)
def test_a_json_serializer_round_trips_and_encodes_equal_values_to_the_same_bytes(backend, value, value_type):
    encoded = backend.encode(value, value_type)
    assert backend.decode(encoded, value_type) == value
    assert backend.encode(backend.decode(encoded, value_type), value_type) == encoded
    assert b" " not in encoded and b"\n" not in encoded, "compact separators"
    assert encoded == json.dumps(json.loads(encoded), sort_keys=True, separators=(",", ":")).encode(), "sorted keys"
    backend.validate(backend.decode(encoded, value_type))


@pytest.mark.parametrize(
    "value, value_type, expected",
    [
        pytest.param(Parcel("o", [1, 2.5]), Parcel, b'"weights":[1.0,2.5]', id="in a list"),
        pytest.param(Reading(3, {"mon": 4}), Reading, b'"by_day":{"mon":4.0},"value":3.0', id="in an optional and a dict"),
    ],
)
def test_an_int_in_a_float_field_encodes_to_the_same_bytes_as_the_float(value, value_type, expected):
    backend = DataclassSerializer()
    encoded = backend.encode(value, value_type)
    assert expected in encoded
    assert backend.encode(backend.decode(encoded, value_type), value_type) == encoded


def test_an_int_too_large_for_a_float_is_refused_not_overflowed():
    backend = DataclassSerializer()
    with pytest.raises(SerializationError):
        backend.encode(Parcel("o", [10**400]), Parcel)
    with pytest.raises(SerializationError):
        backend.decode(b'{"order":"o","weights":[1' + b"0" * 400 + b"]}", Parcel)


class Tagged(pydantic.BaseModel):
    tags: set[str]


class FrozenTagged(pydantic.BaseModel):
    tags: frozenset[str]


class Holder(pydantic.BaseModel):
    tagged: list[Tagged] | None = None


@pytest.mark.parametrize("value_type", [Tagged, FrozenTagged, Holder, list[Holder]])
def test_a_pydantic_model_with_a_set_is_refused_because_a_set_has_no_stable_order(value_type):
    backend = PydanticSerializer()
    assert not backend.supports(value_type)
    with pytest.raises(SerializationError, match="set"):
        backend.encode(None, value_type)


@dataclasses.dataclass
class Hidden:
    shown: str
    computed: str = dataclasses.field(init=False, default="x")


def test_a_dataclass_with_a_field_init_cannot_set_is_refused():
    assert not DataclassSerializer().supports(Hidden)


class Sealed(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    order: str


REFUSALS = [
    pytest.param(PydanticSerializer(), Shipment, b"{not json", "{not", id="pydantic not json"),
    pytest.param(DataclassSerializer(), Parcel, b"{not json", "{not", id="dataclass not json"),
    pytest.param(DataclassSerializer(), Parcel, b'{"order":"leaky-value"}', "leaky", id="dataclass missing required key"),
    pytest.param(
        DataclassSerializer(), Parcel, b'{"order":"o","weights":[],"leaky_key":1}', "leaky_key",
        id="dataclass unknown key",
    ),
    pytest.param(
        DataclassSerializer(), Parcel, b'{"order":7,"weights":["leaky-value"]}', "leaky",
        id="dataclass wrong scalar type",
    ),
    pytest.param(
        DataclassSerializer(), Reading, b'{"value":null,"by_day":{"leaky_key":"leaky-value"}}', "leaky",
        id="dataclass wrong scalar type under a dict key",
    ),
    pytest.param(
        PydanticSerializer(), Shipment, b'{"order":"o","items":"leaky-value"}', "leaky",
        id="pydantic validation failure",
    ),
    pytest.param(
        PydanticSerializer(), Shipment, b'{"order":"o","items":[],"notes":{"leaky_key":"x"}}', "leaky",
        id="pydantic bad value under a dict key",
    ),
    pytest.param(
        PydanticSerializer(), Sealed, b'{"order":"o","leaky_extra":1}', "leaky",
        id="pydantic extra key on a model that forbids them",
    ),
]


@pytest.mark.parametrize("backend, value_type, payload, leak", REFUSALS)
def test_a_json_serializer_refuses_bad_bytes_without_echoing_them(backend, value_type, payload, leak):
    with pytest.raises(SerializationError) as refused:
        backend.decode(payload, value_type)
    assert leak not in str(refused.value)


def test_a_json_serializer_refuses_types_it_cannot_carry():
    for backend in (PydanticSerializer(), DataclassSerializer()):
        assert not backend.supports(Greeting)
        assert not backend.supports(int)
        assert backend.type_compatibility(Greeting, Greeting) is None


@pytest.mark.parametrize(
    "backend, target, old, expected",
    [
        (PydanticSerializer(), Shipment, {"order": "o", "items": [], "gone": 1}, Shipment(order="o", items=[])),
        (
            DataclassSerializer(),
            Parcel,
            {"order": "o", "weights": [], "label": {"text": "t", "gone": 1}, "gone": 2},
            Parcel(order="o", weights=[], label=Label("t")),
        ),
    ],
)
def test_the_default_migration_drops_fields_the_new_version_lacks_and_defaults_new_ones(
    backend, target, old, expected
):
    migrated = backend.dump_plain(old, target)
    assert backend.decode(migrated, target) == expected


def test_the_default_migration_drops_fields_inside_nested_models_and_lists_of_them():
    migrated = PydanticSerializer().dump_plain(
        {
            "order": "o",
            "parts": [{"text": "t", "gone": 1}],
            "main": {"text": "m", "gone": 2},
            "gone": 3,
        },
        Crate,
    )
    assert json.loads(migrated) == {"order": "o", "parts": [{"text": "t"}], "main": {"text": "m"}}


VERSIONED = [
    pytest.param(PydanticSerializer(), Shipment, Shipment(order="o", items=[]), {"order": "o", "items": []}, id="pydantic"),
    pytest.param(DataclassSerializer(), Parcel, Parcel(order="o", weights=[]), {"order": "o", "weights": []}, id="dataclass"),
]


@pytest.mark.parametrize("backend, cls, value, bare", VERSIONED)
def test_a_family_with_a_version_path_carries_its_version_in_the_payload(
    monkeypatch, backend, cls, value, bare
):
    registry = SchemaRegistry()
    monkeypatch.setattr(schemas, "_process_schemas", registry)
    registry.declare(cls, version=3, version_path="$.meta.v")

    encoded = backend.encode(value, cls)
    assert json.loads(encoded)["meta"] == {"v": 3}
    assert backend.extract_version(encoded, cls) == 3
    assert backend.decode(encoded, cls) == value, "the version key is not a field"
    old = json.dumps({**bare, "meta": {"v": 1}}).encode()
    assert backend.extract_version(old, cls) == 1
    listed = backend.encode([value], list[cls])
    assert backend.extract_version(listed, list[cls]) is None
    kept = backend.dump_plain({**bare, "gone": 1, "meta": {"v": 2, "other": 1}}, cls)
    assert json.loads(kept) == {**bare, "meta": {"v": 2}}, "the default migration keeps the version only"
    with pytest.raises(SerializationError, match="version"):
        backend.extract_version(json.dumps(bare).encode(), cls)
