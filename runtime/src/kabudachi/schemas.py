"""Schema families, their versions, and the migrations between versions.

A family is a data type's identity across versions: by default a protobuf
message's full name, or `module.qualname` for any other class; separate
classes can be declared versions of one family. Tasks have no versions of
their own: a task's input is at its type's version. A worker migrates a
task's input in memory when it claims the task, one version at a time; the
stored task never changes.
"""

import enum
import inspect
import json
import re
import threading
import types
import typing
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final, TypeVar

from kabudachi.config import UNSET, Unset
from kabudachi.errors import SerializationError, TaskFailure

F = TypeVar("F", bound=Callable[..., Any])

_PATH = re.compile(r"^\$(\.[A-Za-z_][A-Za-z0-9_]*)+$")
_DEFAULT = object()


def family_of(value_type: Any) -> str | None:
    """The default family of `value_type`; a list's is its item's. `None` for `NoneType`."""
    if typing.get_origin(value_type) is list:
        (item,) = typing.get_args(value_type) or (None,)
        return None if item is None else family_of(item)
    if value_type is type(None):
        return None
    full_name = getattr(getattr(value_type, "DESCRIPTOR", None), "full_name", None)
    if isinstance(full_name, str):
        return full_name
    module = getattr(value_type, "__module__", None)
    name = getattr(value_type, "__qualname__", None)
    if module is None or name is None:
        return repr(value_type)
    return f"{module}.{name}"


@dataclass(frozen=True)
class Schema:
    """A type's place in its family: which family, which version, and where in
    its encoded form the version is written (as the path's keys), if anywhere."""

    family: str
    version: int
    version_path: tuple[str, ...] | None


@dataclass(frozen=True)
class Runnable:
    """A payload can be run at `version`, after `steps` in order: a function
    for an explicit migration, `None` for the default step."""

    version: int
    steps: tuple[Callable[[Any], Any] | None, ...]


@dataclass(frozen=True)
class Newer:
    """The payload's version is newer than the one wanted."""

    reason: str


@dataclass(frozen=True)
class Incompatible:
    """A bump between the payload's version and the one wanted is declared
    incompatible."""

    reason: str


def _check_migration(function: Any) -> None:
    if function is None:
        return
    try:
        signature = inspect.signature(function)
        signature.bind(None)
    except (TypeError, ValueError) as error:
        raise TypeError(
            f"a migration must be a function taking one argument, the plain form: {function!r}"
        ) from error


class SchemaRegistry:
    """The versions declared for each family, and the migrations between them."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._by_type: dict[Any, Schema] = {}
        self._by_version: dict[tuple[str, int], Any] = {}
        self._paths: dict[str, tuple[str, ...] | None] = {}
        # An entry set to None is a bump declared incompatible.
        self._bumps: dict[tuple[str, int], Callable[[Any], Any] | None] = {}

    def declare(
        self,
        value_type: Any,
        version: int,
        version_path: str | None = None,
        family: str | None = None,
        migration: Callable[[Any], Any] | None | Unset = UNSET,
    ) -> None:
        """Declares `value_type` as `version` of `family` (by default its own
        family, see `family_of`).

        `migration` registers the bump from `version - 1`: a function for an
        explicit migration, `None` for one declared incompatible, left out for
        the default. Raises `ValueError` for a negative version, a bad
        `version_path`, a class declared twice differently, two classes as one
        version of a family, or a family whose declarations disagree on the
        path.
        """
        if isinstance(version, bool) or not isinstance(version, int) or version < 0:
            raise ValueError(f"a schema version must be a non-negative integer, not {version!r}")
        path = None
        if version_path is not None:
            if not isinstance(version_path, str) or not _PATH.match(version_path):
                raise ValueError(
                    f"version_path must look like '$.field' or '$.a.b', not {version_path!r}"
                )
            path = tuple(version_path.split(".")[1:])
        name = family if family is not None else family_of(value_type)
        if name is None:
            raise ValueError("None has no schema to declare")
        schema = Schema(name, version, path)
        if not isinstance(migration, Unset):
            if version == 0:
                raise ValueError("version 0 has no earlier version to migrate from")
            _check_migration(migration)
        with self._lock:
            declared = self._by_type.get(value_type)
            if declared is not None and declared != schema:
                raise ValueError(
                    f"{value_type!r} is already declared as version {declared.version} "
                    f"of {declared.family}"
                )
            holder = self._by_version.get((name, version))
            if holder is not None and holder is not value_type:
                raise ValueError(
                    f"version {version} of {name} is already declared by {holder!r}"
                )
            if name in self._paths and self._paths[name] != path:
                raise ValueError(
                    f"{name} declares a different version_path than its earlier versions"
                )
            if not isinstance(migration, Unset):
                self._check_bump(name, version - 1, migration)
            self._by_type[value_type] = schema
            self._by_version[(name, version)] = value_type
            self._paths[name] = path
            if not isinstance(migration, Unset):
                self._bumps[(name, version - 1)] = migration

    def schema_of(self, value_type: Any) -> Schema | None:
        """Where `value_type` stands in its family (a list's is its item's);
        version 0 of its default family if undeclared. `None` for `NoneType`."""
        if typing.get_origin(value_type) is list:
            (item,) = typing.get_args(value_type) or (None,)
            return None if item is None else self.schema_of(item)
        with self._lock:
            declared = self._by_type.get(value_type)
        if declared is not None:
            return declared
        name = family_of(value_type)
        return None if name is None else Schema(name, 0, None)

    def version_of(self, value_type: Any) -> int:
        """The version `value_type` is declared as; 0 when undeclared or `NoneType`."""
        schema = self.schema_of(value_type)
        return 0 if schema is None else schema.version

    def family(self, value_type: Any) -> str | None:
        """The family of `value_type`: the declared one, else its default."""
        schema = self.schema_of(value_type)
        return None if schema is None else schema.family

    def add_migration(
        self, family: str, from_version: int, function: Callable[[Any], Any] | None
    ) -> None:
        """Registers the bump `from_version` to `from_version + 1` of `family`:
        a function taking and returning the plain form, or `None` to declare it
        incompatible. Raises `TypeError` for a function that does not take one
        argument and `ValueError` for a second, different entry for one bump."""
        _check_migration(function)
        if isinstance(from_version, bool) or not isinstance(from_version, int) or from_version < 0:
            raise ValueError(f"from_version must be a non-negative integer, not {from_version!r}")
        with self._lock:
            self._check_bump(family, from_version, function)
            self._bumps[(family, from_version)] = function

    def _check_bump(
        self, family: str, from_version: int, function: Callable[[Any], Any] | None
    ) -> None:
        key = (family, from_version)
        if key in self._bumps and self._bumps[key] is not function:
            raise ValueError(
                f"{family} already has a migration from version {from_version}"
            )

    def plan(self, family: str, source: int, target: int) -> Runnable | Newer | Incompatible:
        """How a payload of `family` at version `source` becomes one at
        `target`: the steps to apply in order (`None` is the default step,
        which keeps only what the newer version has), or why it cannot."""
        if source > target:
            return Newer(f"version {source} of {family} is newer than this worker's {target}")
        steps = []
        with self._lock:
            for version in range(source, target):
                step = self._bumps.get((family, version), _DEFAULT)
                if step is None:
                    return Incompatible(
                        f"version {version} of {family} cannot be migrated to version "
                        f"{version + 1}: the bump is declared incompatible"
                    )
                steps.append(None if step is _DEFAULT else step)
        return Runnable(target, tuple(steps))


_process_schemas = SchemaRegistry()


def process_schemas() -> SchemaRegistry:
    """The schema registry of this process."""
    return _process_schemas


def schema(
    value_type: Any,
    *,
    version: int = 0,
    version_path: str | None = None,
    family: str | None = None,
    migration: Callable[[Any], Any] | None | Unset = UNSET,
) -> None:
    """Declares `value_type` as `version` of a family of data types.

    `family` defaults to the type's own (a protobuf message's full name, else
    `module.qualname`); give the same `family` to separate classes to make
    them versions of one. `version_path`, such as `"$.meta.v"`, is where a
    JSON-encoded form carries its version.

    The bump from `version - 1` is migrated by `migration`, a function taking
    and returning the plain form, by default (fields the new version lacks are
    dropped, new ones take their defaults), or not at all with `migration=None`,
    which declares the bump incompatible. Every worker of a shard must declare
    a family the same way. Raises `ValueError` for a declaration that
    contradicts an earlier one.
    """
    process_schemas().declare(
        value_type, version, version_path=version_path, family=family, migration=migration
    )


def migration(value_type: Any, *, from_version: int) -> Callable[[F], F]:
    """Registers the decorated function as the migration of `value_type`'s
    family from `from_version` to `from_version + 1`. It takes the plain form
    of the older version and returns that of the newer."""

    registry = process_schemas()
    family = registry.family(value_type)
    if family is None:
        raise ValueError(f"{value_type!r} has no family to migrate")

    def register(function: F) -> F:
        registry.add_migration(family, from_version, function)
        return function

    return register


# --- what a serializer says about types --------------------------------------
#
# Serializers may implement `extract_version`, `validate`, `schema_family`,
# `type_compatibility`, `load_plain` and `dump_plain`. These call them, and
# answer with a default for a backend that lacks one.


def _member(serializer: Any, name: str) -> Callable[..., Any] | None:
    member = getattr(serializer, name, None)
    if member is not None and not callable(member):
        raise TypeError(f"{serializer!r} is not a serializer: {name} must be callable")
    return member


def extract_version(serializer: Any, payload: bytes, value_type: Any) -> int | None:
    """The version `payload` says it is, or `None` when it does not say (or
    `serializer` cannot tell), in which case the task's own source version is
    the payload's."""
    member = _member(serializer, "extract_version")
    return None if member is None else member(payload, value_type)


def validate(serializer: Any, value: Any) -> None:
    """Checks a decoded `value` with `serializer`, which raises
    `SerializationError` for one that is not valid. A serializer without the
    check accepts it."""
    member = _member(serializer, "validate")
    if member is not None:
        member(value)


def schema_family(serializer: Any, value_type: Any, schemas: SchemaRegistry) -> str | None:
    """The family of `value_type` as `serializer` names it, else as `schemas` does."""
    member = _member(serializer, "schema_family")
    return schemas.family(value_type) if member is None else member(value_type)


def type_compatibility(serializer: Any, produced: Any, consumed: Any) -> bool | None:
    """Whether a `produced` value can be given where `consumed` is taken,
    judged by `serializer`; `None` when it does not say."""
    member = _member(serializer, "type_compatibility")
    return None if member is None else member(produced, consumed)


def default_compatibility(
    serializer: Any, produced: Any, consumed: Any, schemas: SchemaRegistry
) -> bool | None:
    """Whether a `produced` value can be given where `consumed` is taken,
    judged by family and version: the same family, and a migration plan
    from the producer's version up to the consumer's that crosses no bump
    declared incompatible. `None` when `serializer` does not handle one of
    the types."""
    if not (serializer.supports(produced) and serializer.supports(consumed)):
        return None
    if (typing.get_origin(produced) is list) != (typing.get_origin(consumed) is list):
        return False
    family = schema_family(serializer, produced, schemas)
    wanted = schema_family(serializer, consumed, schemas)
    if family is None or family != wanted:
        return family is None and wanted is None
    planned = schemas.plan(family, schemas.version_of(produced), schemas.version_of(consumed))
    return isinstance(planned, Runnable)


# --- what a step in a composition gives the next one -------------------------


@dataclass(frozen=True)
class Value:
    """One task's output, of `value_type` encoded by the serializer named `serializer`."""

    value_type: Any
    serializer: str


@dataclass(frozen=True)
class Items:
    """A list of results of different steps, one entry each, in order: a
    flow's or group's output."""

    entries: tuple["OutputShape", ...]


@dataclass(frozen=True)
class Each:
    """A list whose every item has the shape `item`: a map's output."""

    item: "OutputShape"


@dataclass(frozen=True)
class Either:
    """An entry that is one of `options`: a result, or the failure that took
    its place under `collect_all`."""

    options: tuple["OutputShape", ...]


class _Unknown:
    def __repr__(self) -> str:
        return "UNKNOWN"


UNKNOWN: Final = _Unknown()
"""A shape nothing is checked against, such as what a task that returns a step gives."""

OutputShape = Value | Items | Each | Either | _Unknown

_ANYTHING = (Any, object, list)


def fits(
    shape: OutputShape, wanted: Any, wanted_serializer: str, serializers: Any
) -> bool | None:
    """Whether a step whose output is `shape` can feed a task that takes
    `wanted`, which `wanted_serializer` carries. `serializers` is the registry
    its backends are found in. `None` when it cannot be told, which callers
    treat as a fit."""
    if shape is UNKNOWN or wanted is None or any(wanted is anything for anything in _ANYTHING):
        return None
    if isinstance(shape, Either):
        answers = [fits(option, wanted, wanted_serializer, serializers) for option in shape.options]
        return False if False in answers else (None if None in answers else True)
    if typing.get_origin(wanted) in (typing.Union, types.UnionType):
        answers = [fits(shape, arm, wanted_serializer, serializers) for arm in typing.get_args(wanted)]
        return True if True in answers else (None if None in answers else False)
    if isinstance(shape, (Items, Each)):
        if typing.get_origin(wanted) is not list:
            return False
        (item,) = typing.get_args(wanted) or (Any,)
        entries = shape.entries if isinstance(shape, Items) else (shape.item,)
        answers = [fits(entry, item, wanted_serializer, serializers) for entry in entries]
        return False if False in answers else (None if None in answers else True)
    return _value_fits(shape, wanted, wanted_serializer, serializers)


def _value_fits(produced: Value, wanted: Any, wanted_serializer: str, serializers: Any) -> bool | None:
    value_type = produced.value_type
    if value_type is TaskFailure:
        return isinstance(wanted, type) and issubclass(TaskFailure, wanted)
    if value_type is wanted:
        return True
    if value_type in _ANYTHING:
        return None
    if isinstance(value_type, type) and isinstance(wanted, type) and issubclass(value_type, wanted):
        return True
    backends = [serializers.find(wanted_serializer), serializers.find(produced.serializer)]
    for backend in backends:
        if backend is not None:
            answer = type_compatibility(backend, value_type, wanted)
            if answer is not None:
                return answer
    consumer = backends[0]
    if consumer is not None and produced.serializer != wanted_serializer and not consumer.supports(value_type):
        # Bytes in the producer's format, which the consumer's backend cannot even hold.
        return False
    return None


def can_migrate(serializer: Any) -> bool:
    """Whether `serializer` can take part in a migration: it has `load_plain`
    and `dump_plain`."""
    return _member(serializer, "load_plain") is not None and _member(
        serializer, "dump_plain"
    ) is not None


# --- helpers for serializers whose payload is JSON ----------------------------


def json_bytes(plain: Any) -> bytes:
    """`plain` as canonical JSON: sorted keys, no whitespace, UTF-8, so
    equal values always give equal bytes (results are certified by digest)."""
    try:
        return json.dumps(
            plain, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    except (TypeError, ValueError) as error:
        raise SerializationError(f"cannot encode as JSON: {error}") from error


def get_path(plain: Any, path: tuple[str, ...]) -> Any:
    """The value at `path` (the keys, outermost first) in the parsed JSON
    `plain`, or `UNSET` if some key on the way is missing."""
    for key in path:
        if not isinstance(plain, dict) or key not in plain:
            return UNSET
        plain = plain[key]
    return plain


def set_path(plain: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    """Sets `value` at `path` in `plain`, creating the objects on the way."""
    for key in path[:-1]:
        child = plain.setdefault(key, {})
        if not isinstance(child, dict):
            raise SerializationError(f"cannot write the version at {'.'.join(path)}: {key} is not an object")
        plain = child
    plain[path[-1]] = value


def drop_path(plain: dict[str, Any], path: tuple[str, ...]) -> None:
    """Removes the value at `path` from `plain`, and the objects on the way
    that this leaves empty."""
    trail = [plain]
    for key in path[:-1]:
        child = trail[-1].get(key)
        if not isinstance(child, dict):
            return
        trail.append(child)
    trail[-1].pop(path[-1], None)
    for key, parent, child in zip(reversed(path[:-1]), reversed(trail[:-1]), reversed(trail[1:])):
        if child:
            break
        del parent[key]


class Shape(enum.Enum):
    """What a JSON serializer's type is: nothing, one value, or a list of them."""

    NOTHING = "nothing"
    ONE = "one"
    MANY = "many"


def version_path_of(value_type: Any) -> tuple[str, ...] | None:
    """Where `value_type`'s family writes its version in JSON, if anywhere."""
    declared = process_schemas().schema_of(value_type)
    return None if declared is None else declared.version_path


def parse_json(payload: Any) -> Any:
    """The parsed JSON of `payload`; `SerializationError` if it is not bytes of JSON."""
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise SerializationError(f"the payload must be bytes, not {type(payload).__name__}")
    try:
        return json.loads(bytes(payload))
    except ValueError as error:
        raise SerializationError(f"not valid JSON: {error}") from error


def json_version(payload: Any, path: tuple[str, ...] | None) -> int | None:
    """The integer version at `path` in the JSON `payload`; `None` if there is
    no `path`. Raises `SerializationError` if the payload has none there."""
    if path is None:
        return None
    version = get_path(parse_json(payload), path)
    if isinstance(version, bool) or not isinstance(version, int):
        raise SerializationError(f"the payload has no integer version at ${'.'.join(('', *path))}")
    return version


def dump_conformed(
    plain: Any, shape: Shape, path: tuple[str, ...] | None, conform: Callable[[Any], Any]
) -> bytes:
    """`plain` as canonical JSON with each item (or the one value) passed
    through `conform`, which drops keys; the version at `path` is kept."""

    def one(item: Any) -> Any:
        kept = conform(item)
        version = get_path(item, path) if path is not None else UNSET
        if isinstance(kept, dict) and version is not UNSET:
            set_path(kept, path, version)
        return kept

    if shape is Shape.NOTHING:
        return json_bytes(None)
    if shape is Shape.ONE:
        return json_bytes(one(plain))
    if not isinstance(plain, list):
        raise SerializationError(f"expected a list, got {type(plain).__name__}")
    return json_bytes([one(item) for item in plain])


def supported_family(serializer: Any, value_type: Any) -> str | None:
    """The family of `value_type`, or `None` for a type `serializer` does not support."""
    if not serializer.supports(value_type):
        return None
    return process_schemas().family(value_type)


def compatibility(serializer: Any, produced: Any, consumed: Any) -> bool | None:
    """`default_compatibility` against this process's schemas."""
    return default_compatibility(serializer, produced, consumed, process_schemas())
