"""Dataclass serialization: a dataclass, a list of dataclasses, or nothing.

A dataclass may hold `str`, `int`, `float`, `bool`, `None`, other such
dataclasses, `list[...]`, `dict[str, ...]` and `X | None`. A value is
canonical JSON (sorted keys, no whitespace, UTF-8), so equal values are equal
bytes. Nothing (`None`) is JSON `null`. A dataclass whose family declares a
`version_path` carries its version there. Uses only the standard library.
"""

import dataclasses
import enum
import json
import types
import typing
from typing import Any

from kabudachi import schemas
from kabudachi.config import UNSET
from kabudachi.errors import SerializationError

_NONE_TYPE = type(None)
_SCALARS = (str, int, float, bool)


class _Shape(enum.Enum):
    NOTHING = "nothing"
    DATACLASS = "dataclass"
    LIST = "list"


def _is_dataclass_type(candidate: Any) -> bool:
    return isinstance(candidate, type) and dataclasses.is_dataclass(candidate)


def _hints(cls: Any) -> dict[str, Any]:
    return typing.get_type_hints(cls)


def _optional_arm(annotation: Any) -> Any:
    """The `X` of `X | None`, or `UNSET` if `annotation` is not that."""
    if typing.get_origin(annotation) in (typing.Union, types.UnionType):
        arms = [arm for arm in typing.get_args(annotation) if arm is not _NONE_TYPE]
        if len(arms) == 1 and len(typing.get_args(annotation)) == 2:
            return arms[0]
    return UNSET


def _carries(annotation: Any, seen: set[Any]) -> bool:
    """Whether a field of this type can be written as JSON and read back."""
    if annotation is _NONE_TYPE or annotation in _SCALARS:
        return True
    if _is_dataclass_type(annotation):
        if annotation in seen:
            return True
        seen.add(annotation)
        try:
            hints = _hints(annotation)
        except Exception:
            return False
        return all(_carries(hints[field.name], seen) for field in dataclasses.fields(annotation))
    arm = _optional_arm(annotation)
    if arm is not UNSET:
        return _carries(arm, seen)
    arguments = typing.get_args(annotation)
    if typing.get_origin(annotation) is list and len(arguments) == 1:
        return _carries(arguments[0], seen)
    if typing.get_origin(annotation) is dict and len(arguments) == 2 and arguments[0] is str:
        return _carries(arguments[1], seen)
    return False


def _classify(value_type: Any) -> tuple[_Shape, Any]:
    """What `value_type` is and the dataclass type inside it, or raises."""
    if value_type is _NONE_TYPE:
        return _Shape.NOTHING, None
    if _is_dataclass_type(value_type) and _carries(value_type, set()):
        return _Shape.DATACLASS, value_type
    if typing.get_origin(value_type) is list:
        (item_type,) = typing.get_args(value_type) or (None,)
        if _is_dataclass_type(item_type) and _carries(item_type, set()):
            return _Shape.LIST, item_type
    raise SerializationError(
        f"the dataclass serializer supports dataclasses of plain fields, lists of them and None, "
        f"not {value_type!r}"
    )


def _version_path(cls: Any) -> tuple[str, ...] | None:
    declared = schemas.process_schemas().schema_of(cls)
    return None if declared is None else declared.version_path


def _parse(payload: Any) -> Any:
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise SerializationError(f"the payload must be bytes, not {type(payload).__name__}")
    try:
        return json.loads(bytes(payload))
    except ValueError as error:
        raise SerializationError(f"not valid JSON: {error}") from error


def _build(plain: Any, annotation: Any, where: str) -> Any:
    """`plain`, parsed JSON, as an `annotation`; `where` names the field for errors."""
    if annotation is _NONE_TYPE:
        if plain is not None:
            raise SerializationError(f"{where} must be null")
        return None
    if annotation is bool:
        if not isinstance(plain, bool):
            raise SerializationError(f"{where} must be a boolean")
        return plain
    if annotation is int:
        if isinstance(plain, bool) or not isinstance(plain, int):
            raise SerializationError(f"{where} must be an integer")
        return plain
    if annotation is float:
        if isinstance(plain, bool) or not isinstance(plain, (int, float)):
            raise SerializationError(f"{where} must be a number")
        return float(plain)
    if annotation is str:
        if not isinstance(plain, str):
            raise SerializationError(f"{where} must be a string")
        return plain
    arm = _optional_arm(annotation)
    if arm is not UNSET:
        return None if plain is None else _build(plain, arm, where)
    if _is_dataclass_type(annotation):
        return _build_dataclass(plain, annotation, where)
    if typing.get_origin(annotation) is list:
        if not isinstance(plain, list):
            raise SerializationError(f"{where} must be an array")
        (item,) = typing.get_args(annotation)
        return [_build(entry, item, f"{where}[{index}]") for index, entry in enumerate(plain)]
    if not isinstance(plain, dict):
        raise SerializationError(f"{where} must be an object")
    value_type = typing.get_args(annotation)[1]
    return {key: _build(entry, value_type, f"{where}.{key}") for key, entry in plain.items()}


def _build_dataclass(plain: Any, cls: Any, where: str) -> Any:
    if not isinstance(plain, dict):
        raise SerializationError(f"{where} must be an object")
    hints = _hints(cls)
    fields = {field.name: field for field in dataclasses.fields(cls)}
    unknown = sorted(set(plain) - set(fields))
    if unknown:
        raise SerializationError(f"{where} has unknown fields: {', '.join(unknown)}")
    arguments = {}
    for name, field in fields.items():
        if name in plain:
            arguments[name] = _build(plain[name], hints[name], f"{where}.{name}")
        elif (
            field.default is dataclasses.MISSING and field.default_factory is dataclasses.MISSING
        ):
            raise SerializationError(f"{where} is missing the field {name}")
    return cls(**arguments)


def _conform(plain: Any, annotation: Any) -> Any:
    """`plain` with only the keys `annotation` has, down through nested dataclasses."""
    arm = _optional_arm(annotation)
    if arm is not UNSET:
        return _conform(plain, arm)
    if _is_dataclass_type(annotation):
        if not isinstance(plain, dict):
            return plain
        hints = _hints(annotation)
        return {
            field.name: _conform(plain[field.name], hints[field.name])
            for field in dataclasses.fields(annotation)
            if field.name in plain
        }
    if typing.get_origin(annotation) is list and isinstance(plain, list):
        (item,) = typing.get_args(annotation)
        return [_conform(entry, item) for entry in plain]
    if typing.get_origin(annotation) is dict and isinstance(plain, dict):
        return {key: _conform(entry, typing.get_args(annotation)[1]) for key, entry in plain.items()}
    return plain


class DataclassSerializer:
    """Serializes dataclasses of plain fields. Uses only the standard library,
    so it is always available."""

    name = "dataclass"

    def available(self) -> bool:
        return True

    def supports(self, value_type: Any) -> bool:
        """Whether `value_type` is a dataclass of supported fields, a `list`
        of one, or `None`."""
        try:
            _classify(value_type)
        except SerializationError:
            return False
        return True

    def encode(self, value: Any, value_type: Any) -> bytes:
        """The canonical JSON of `value`, a dataclass or a list of dataclasses
        of `value_type`, or `null` for `None`. Raises `SerializationError` if
        it is not one, or cannot be encoded."""
        kind, cls = _classify(value_type)
        if kind is _Shape.NOTHING:
            if value is not None:
                raise SerializationError(f"expected None, got {type(value).__name__}")
            return schemas.json_bytes(None)
        if kind is _Shape.DATACLASS:
            return schemas.json_bytes(self._plain(value, cls))
        if not isinstance(value, list):
            raise SerializationError(f"expected a list, got {type(value).__name__}")
        return schemas.json_bytes([self._plain(item, cls) for item in value])

    def _plain(self, value: Any, cls: Any) -> dict[str, Any]:
        if not isinstance(value, cls):
            raise SerializationError(f"expected a {cls.__name__}, got {type(value).__name__}")
        plain = dataclasses.asdict(value)
        path = _version_path(cls)
        if path is not None:
            schemas.set_path(plain, path, schemas.process_schemas().version_of(cls))
        return plain

    def decode(self, payload: bytes, target_type: Any) -> Any:
        """The dataclass, or list of dataclasses, of `target_type` that
        `payload` holds. Raises `SerializationError` if the bytes are not a
        valid one, naming the field and never its value."""
        kind, cls = _classify(target_type)
        plain = _parse(payload)
        if kind is _Shape.NOTHING:
            if plain is not None:
                raise SerializationError("expected null for None")
            return None
        if kind is _Shape.DATACLASS:
            return self._build(plain, cls, cls.__name__)
        if not isinstance(plain, list):
            raise SerializationError("expected a JSON array for a list")
        return [self._build(item, cls, f"{cls.__name__}[{index}]") for index, item in enumerate(plain)]

    def _build(self, plain: Any, cls: Any, where: str) -> Any:
        path = _version_path(cls)
        names = {field.name for field in dataclasses.fields(cls)}
        if path is not None and isinstance(plain, dict) and path[0] not in names:
            schemas.drop_path(plain, path)
        return _build_dataclass(plain, cls, where)

    def extract_version(self, payload: bytes, value_type: Any) -> int | None:
        """The version `payload` carries at its family's `version_path`, or
        `None` for a type whose family has none. Raises `SerializationError`
        if the payload should carry one and does not."""
        kind, cls = _classify(value_type)
        path = _version_path(cls) if kind is _Shape.DATACLASS else None
        if path is None:
            return None
        version = schemas.get_path(_parse(payload), path)
        if isinstance(version, bool) or not isinstance(version, int):
            raise SerializationError(f"the payload has no integer version at ${'.'.join(('', *path))}")
        return version

    def validate(self, value: Any) -> None:
        """Raises `SerializationError` if `value`, a dataclass or a list of
        dataclasses, is not one that survives being encoded and decoded."""
        for item in value if isinstance(value, list) else [value]:
            if item is None:
                continue
            if isinstance(item, type) or not dataclasses.is_dataclass(item):
                raise SerializationError(f"expected a dataclass, got {type(item).__name__}")
            cls = type(item)
            if self.decode(self.encode(item, cls), cls) != item:
                raise SerializationError(f"this {cls.__name__} does not survive being encoded")

    def schema_family(self, value_type: Any) -> str | None:
        """The family of `value_type`, or `None` for a type this does not support."""
        if not self.supports(value_type):
            return None
        return schemas.process_schemas().family(value_type)

    def type_compatibility(self, produced: Any, consumed: Any) -> bool | None:
        """Whether a `produced` value fits where `consumed` is taken, judged
        by family and version; `None` for a type this does not support."""
        return schemas.default_compatibility(self, produced, consumed, schemas.process_schemas())

    def load_plain(self, payload: bytes, value_type: Any) -> Any:
        """What a migration takes: the parsed JSON of `payload`."""
        return _parse(payload)

    def dump_plain(self, plain: Any, value_type: Any) -> bytes:
        """`plain`, conformed to `value_type` by dropping the keys it lacks,
        through nested dataclasses (the default migration), as canonical JSON.
        The version at the family's `version_path` is kept."""
        kind, cls = _classify(value_type)
        if kind is _Shape.NOTHING:
            return schemas.json_bytes(None)
        path = _version_path(cls)

        def conform(item: Any) -> Any:
            kept = _conform(item, cls)
            version = schemas.get_path(item, path) if path is not None else UNSET
            if isinstance(kept, dict) and version is not UNSET:
                schemas.set_path(kept, path, version)
            return kept

        if kind is _Shape.DATACLASS:
            return schemas.json_bytes(conform(plain))
        if not isinstance(plain, list):
            raise SerializationError(f"expected a list, got {type(plain).__name__}")
        return schemas.json_bytes([conform(item) for item in plain])
