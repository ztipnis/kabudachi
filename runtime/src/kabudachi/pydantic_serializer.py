"""Pydantic serialization: a model, a list of models, or nothing.

A value is canonical JSON (sorted keys, no whitespace, UTF-8), so equal values
are equal bytes. Nothing (`None`) is JSON `null`. A model whose family
declares a `version_path` carries its version there.
"""

import enum
import json
import types
import typing
from typing import Any

from kabudachi import schemas
from kabudachi.config import UNSET
from kabudachi.errors import SerializationError, SerializerUnavailableError

try:
    import pydantic as _pydantic
except ImportError:
    _pydantic = None

_NONE_TYPE = type(None)


class _Shape(enum.Enum):
    NOTHING = "nothing"
    MODEL = "model"
    LIST = "list"


def _is_model_type(candidate: Any) -> bool:
    return isinstance(candidate, type) and issubclass(candidate, _pydantic.BaseModel)


def _classify(value_type: Any) -> tuple[_Shape, Any]:
    """What `value_type` is and the model type inside it, or raises."""
    if _pydantic is None:
        raise SerializerUnavailableError("the pydantic serializer needs the 'pydantic' package")
    if value_type is _NONE_TYPE:
        return _Shape.NOTHING, None
    if _is_model_type(value_type):
        return _Shape.MODEL, value_type
    if typing.get_origin(value_type) is list:
        (item_type,) = typing.get_args(value_type) or (None,)
        if item_type is not None and _is_model_type(item_type):
            return _Shape.LIST, item_type
    raise SerializationError(
        f"the pydantic serializer supports BaseModel, list[BaseModel] and None, not {value_type!r}"
    )


def _version_path(model_type: Any) -> tuple[str, ...] | None:
    declared = schemas.process_schemas().schema_of(model_type)
    return None if declared is None else declared.version_path


def _parse(payload: Any) -> Any:
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise SerializationError(f"the payload must be bytes, not {type(payload).__name__}")
    try:
        return json.loads(bytes(payload))
    except ValueError as error:
        raise SerializationError(f"not valid JSON: {error}") from error


def _require_instance(value: Any, model_type: Any) -> None:
    if not isinstance(value, model_type):
        raise SerializationError(
            f"expected a {model_type.__name__}, got {type(value).__name__}"
        )


def _keys(model_type: Any) -> dict[str, Any]:
    """The keys a model's JSON form has, each with its field."""
    return {(field.alias or name): field for name, field in model_type.model_fields.items()}


def _nested_model(annotation: Any) -> tuple[Any, bool] | None:
    """The model and whether it is in a list, if `annotation` is a model, a
    list of models, or either or `None`."""
    if typing.get_origin(annotation) in (typing.Union, types.UnionType):
        arms = [arm for arm in typing.get_args(annotation) if arm is not _NONE_TYPE]
        return _nested_model(arms[0]) if len(arms) == 1 else None
    if _is_model_type(annotation):
        return annotation, False
    if typing.get_origin(annotation) is list:
        (item,) = typing.get_args(annotation) or (None,)
        if item is not None and _is_model_type(item):
            return item, True
    return None


def _conform(plain: Any, model_type: Any) -> Any:
    """`plain` with only the keys `model_type` has, down through its nested models."""
    if not isinstance(plain, dict):
        return plain
    kept = {}
    for key, field in _keys(model_type).items():
        if key not in plain:
            continue
        value = plain[key]
        nested = _nested_model(field.annotation)
        if nested is not None:
            inner, listed = nested
            if listed and isinstance(value, list):
                value = [_conform(item, inner) for item in value]
            elif not listed:
                value = _conform(value, inner)
        kept[key] = value
    return kept


def _first_failure(model_type: Any, error: Any) -> SerializationError:
    failures = error.errors()
    where = ".".join(str(part) for part in failures[0]["loc"]) if failures else ""
    kind = failures[0]["type"] if failures else "invalid"
    named = f"field {where}" if where else "the value"
    return SerializationError(f"not a valid {model_type.__name__}: {named} is {kind}")


class PydanticSerializer:
    """Serializes pydantic models. Needs the `pydantic` package, which is an
    optional extra: without it `available()` is false."""

    name = "pydantic"

    def available(self) -> bool:
        return _pydantic is not None

    def supports(self, value_type: Any) -> bool:
        """Whether `value_type` is a `BaseModel` class, a `list` of one, or `None`."""
        try:
            _classify(value_type)
        except (SerializationError, SerializerUnavailableError):
            return False
        return True

    def encode(self, value: Any, value_type: Any) -> bytes:
        """The canonical JSON of `value`, a model or a list of models of
        `value_type`, or `null` for `None`. Raises `SerializationError` if it is
        not one, or cannot be encoded."""
        kind, model_type = _classify(value_type)
        if kind is _Shape.NOTHING:
            if value is not None:
                raise SerializationError(f"expected None, got {type(value).__name__}")
            return schemas.json_bytes(None)
        if kind is _Shape.MODEL:
            return schemas.json_bytes(self._plain(value, model_type))
        if not isinstance(value, list):
            raise SerializationError(f"expected a list, got {type(value).__name__}")
        return schemas.json_bytes([self._plain(item, model_type) for item in value])

    def _plain(self, value: Any, model_type: Any) -> dict[str, Any]:
        _require_instance(value, model_type)
        try:
            plain = value.model_dump(mode="json", by_alias=True)
        except (ValueError, TypeError) as error:
            raise SerializationError(f"cannot serialize this {model_type.__name__}: {error}") from error
        path = _version_path(model_type)
        if path is not None:
            schemas.set_path(plain, path, schemas.process_schemas().version_of(model_type))
        return plain

    def decode(self, payload: bytes, target_type: Any) -> Any:
        """The model, or list of models, of `target_type` that `payload` holds.

        Raises `SerializationError` if the bytes are not a valid one; the
        message names a failing field, never its value.
        """
        kind, model_type = _classify(target_type)
        plain = _parse(payload)
        if kind is _Shape.NOTHING:
            if plain is not None:
                raise SerializationError("expected null for None")
            return None
        if kind is _Shape.MODEL:
            return self._build(plain, model_type)
        if not isinstance(plain, list):
            raise SerializationError("expected a JSON array for a list")
        return [self._build(item, model_type) for item in plain]

    def _build(self, plain: Any, model_type: Any) -> Any:
        path = _version_path(model_type)
        if path is not None and isinstance(plain, dict) and path[0] not in _keys(model_type):
            schemas.drop_path(plain, path)
        try:
            return model_type.model_validate(plain)
        except _pydantic.ValidationError as error:
            raise _first_failure(model_type, error) from None

    def extract_version(self, payload: bytes, value_type: Any) -> int | None:
        """The version `payload` carries at its family's `version_path`, or
        `None` for a type whose family has none. Raises `SerializationError`
        if the payload should carry one and does not."""
        kind, model_type = _classify(value_type)
        path = _version_path(model_type) if kind is _Shape.MODEL else None
        if path is None:
            return None
        version = schemas.get_path(_parse(payload), path)
        if isinstance(version, bool) or not isinstance(version, int):
            raise SerializationError(f"the payload has no integer version at ${'.'.join(('', *path))}")
        return version

    def validate(self, value: Any) -> None:
        """Raises `SerializationError` if `value`, a model or a list of
        models, does not pass its model's validation."""
        for model in value if isinstance(value, list) else [value]:
            if model is None:
                continue
            if not isinstance(model, _pydantic.BaseModel):
                raise SerializationError(f"expected a model, got {type(model).__name__}")
            try:
                type(model).model_validate(model.model_dump(by_alias=True))
            except _pydantic.ValidationError as error:
                raise _first_failure(type(model), error) from None

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
        through nested models (the default migration), as canonical JSON.
        The version at the family's `version_path` is kept."""
        kind, model_type = _classify(value_type)
        if kind is _Shape.NOTHING:
            return schemas.json_bytes(None)
        path = _version_path(model_type)

        def conform(item: Any) -> Any:
            kept = _conform(item, model_type)
            version = schemas.get_path(item, path) if path is not None else UNSET
            if isinstance(kept, dict) and version is not UNSET:
                schemas.set_path(kept, path, version)
            return kept

        if kind is _Shape.MODEL:
            return schemas.json_bytes(conform(plain))
        if not isinstance(plain, list):
            raise SerializationError(f"expected a list, got {type(plain).__name__}")
        return schemas.json_bytes([conform(item) for item in plain])
