"""Process-wide settings, and how a task's own choice takes precedence.

The order is always: framework default, then the process (`configure()` or
the environment), then the task's own explicit setting. A task setting of
`None` is a choice like any other; only `UNSET` means the task chose nothing.
"""

import os
from dataclasses import dataclass, fields
from datetime import timedelta
from typing import Any, get_type_hints

from kabudachi.errors import ConfigurationError


class Unset:
    """The type of `UNSET`: "no value was chosen", which is not the same as
    choosing `None`."""

    _instance: "Unset | None" = None

    def __new__(cls) -> "Unset":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "UNSET"


UNSET = Unset()


ENVIRONMENT_PREFIX = "KABUDACHI_"

# Bytes of serialized task input the scheduler may hold for tasks that have not
# finished. Past the soft limit bulk submission (group, map) pauses; past the
# hard limit a submission raises `BackpressureError`. Fixed
# numbers, not derived from the memory of the machine, until compaction exists.
DEFAULT_MEMORY_SOFT_LIMIT = 256 * 1024 * 1024
DEFAULT_MEMORY_HARD_LIMIT = 512 * 1024 * 1024


@dataclass(frozen=True, kw_only=True)
class Settings:
    """Every process-wide setting.

    To add one, add a field with its default, and refuse invalid values in
    `__post_init__`. `KABUDACHI_<NAME>` in the environment can set it; a field
    of type `int` is read from the environment as a number, and one of type
    `timedelta` as a number of seconds.
    """

    concurrency: int = 16
    """How many tasks a worker runs at once."""

    queue: str = "default"
    """The queue tasks are sent to unless they name their own."""

    result_ttl: int = 3600
    """How many seconds a finished task is kept, so its result and state can
    still be looked up, before it is forgotten."""

    memory_soft_limit: int = DEFAULT_MEMORY_SOFT_LIMIT
    """Bytes of pending task input past which bulk submission pauses."""

    memory_hard_limit: int = DEFAULT_MEMORY_HARD_LIMIT
    """Bytes of pending task input past which a submission is refused."""

    cancel_grace: timedelta = timedelta(seconds=30)
    """How long a task past its timeout has to stop once asked, before its
    run is failed and its body abandoned. `KABUDACHI_CANCEL_GRACE` gives it
    in seconds."""

    def __post_init__(self) -> None:
        # `True` is an int to Python, but never what someone meant by a count.
        if (
            not isinstance(self.concurrency, int)
            or isinstance(self.concurrency, bool)
            or self.concurrency <= 0
        ):
            raise ValueError(f"concurrency must be a positive integer, not {self.concurrency!r}")
        if (
            not isinstance(self.result_ttl, int)
            or isinstance(self.result_ttl, bool)
            or self.result_ttl <= 0
        ):
            raise ValueError(
                f"result_ttl must be a positive number of seconds, not {self.result_ttl!r}"
            )
        for name in ("memory_soft_limit", "memory_hard_limit"):
            limit = getattr(self, name)
            if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
                raise ValueError(f"{name} must be a positive number of bytes, not {limit!r}")
        if self.memory_soft_limit > self.memory_hard_limit:
            raise ValueError(
                f"memory_soft_limit ({self.memory_soft_limit}) must not be above "
                f"memory_hard_limit ({self.memory_hard_limit})"
            )
        if (
            not isinstance(self.cancel_grace, timedelta)
            or self.cancel_grace < timedelta(0)
        ):
            raise ValueError(
                f"cancel_grace must be a non-negative timedelta, not {self.cancel_grace!r}"
            )
        if not isinstance(self.queue, str) or not self.queue.strip():
            raise ValueError(f"queue must be a non-empty string, not {self.queue!r}")

    @classmethod
    def names(cls) -> tuple[str, ...]:
        """The name of every setting, in the order they are declared."""
        return tuple(field.name for field in fields(cls))

    @classmethod
    def from_environment(cls) -> dict[str, Any]:
        """The settings the environment sets, as keyword arguments."""
        found: dict[str, Any] = {}
        hints = get_type_hints(cls)  # resolved, so `from __future__ import annotations` works
        for field in fields(cls):
            raw = os.environ.get(ENVIRONMENT_PREFIX + field.name.upper())
            if raw is None:
                continue
            try:
                if hints[field.name] is int:
                    found[field.name] = int(raw)
                elif hints[field.name] is timedelta:
                    found[field.name] = timedelta(seconds=float(raw))
                else:
                    found[field.name] = raw
            except ValueError:
                raise ValueError(f"{field.name} must be a number, not {raw!r}") from None
        return found


class Configuration:
    """The settings of a process: what was configured, over the environment,
    over the framework defaults."""

    def __init__(self) -> None:
        self._configured: dict[str, Any] = {}
        self._settings: Settings | None = None

    def configure(self, **settings: Any) -> None:
        """Sets process-level settings, keeping any set by earlier calls.

        Raises `ConfigurationError` for an unknown setting or an invalid
        value, in which case none of the settings in this call are applied.
        """
        configured = {**self._configured, **settings}
        validated = self._validate(configured)
        self._configured, self._settings = configured, validated

    def resolve(self, name: str, task_value: Any = UNSET) -> Any:
        """The effective value of a setting: the task's own value if it chose
        one (`None` included), else the process setting, else the default.

        The environment is read the first time a setting is needed, so it has
        to be set before then.
        """
        if name not in Settings.names():
            known = ", ".join(sorted(Settings.names()))
            raise ConfigurationError(f"unknown setting {name!r}; the settings are: {known}")
        if task_value is not UNSET:
            return task_value
        if self._settings is None:
            self._settings = self._validate(self._configured)
        return getattr(self._settings, name)

    @staticmethod
    def _validate(configured: dict[str, Any]) -> Settings:
        unknown = sorted(set(configured) - set(Settings.names()))
        if unknown:
            known = ", ".join(sorted(Settings.names()))
            raise ConfigurationError(f"unknown setting {unknown[0]!r}; the settings are: {known}")
        try:
            return Settings(**{**Settings.from_environment(), **configured})
        except ValueError as error:
            raise ConfigurationError(str(error)) from error


_process_configuration = Configuration()


def process_configuration() -> Configuration:
    """The configuration of this process, which `configure()` changes."""
    return _process_configuration


def configure(**settings: Any) -> None:
    """Sets settings for this whole process, keeping any set by earlier calls.

    The settings are `concurrency`, the number of tasks a worker runs at
    once, `queue`, the queue tasks are sent to unless they name their own,
    `result_ttl`, the seconds a finished task is kept, `cancel_grace`, the
    timedelta a task past its timeout has to stop, and `memory_soft_limit` and
    `memory_hard_limit`, in bytes of pending task input (past the soft one
    `group` and `map` pause, past the hard one a submission raises
    `BackpressureError`); `KABUDACHI_<NAME>`
    in the environment sets each too, below what is configured here. A
    setting a task makes for itself always wins over these. `concurrency`, the
    memory limits and `result_ttl` are read when `run()` starts, so changing them during a run
    has no effect on that run.

    Raises `ConfigurationError` for an unknown setting or an invalid value,
    and then applies none of the settings in this call.
    """
    _process_configuration.configure(**settings)
