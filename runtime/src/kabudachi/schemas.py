"""Schema families, their versions, and the migrations between versions.

A family is a data type's identity across versions: by default a protobuf
message's full name, or `module.qualname` for any other class; separate
classes can be declared versions of one family. Tasks have no versions of
their own: a task's input is at its type's version. A worker migrates a
task's input in memory when it claims the task, one version at a time; the
stored task never changes.
"""

import inspect
import re
import threading
import typing
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from kabudachi.config import UNSET, Unset

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

    def register(function: F) -> F:
        registry = process_schemas()
        registry.add_migration(registry.family(value_type), from_version, function)
        return function

    return register
