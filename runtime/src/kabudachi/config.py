"""Process-wide settings, and how a task's own choice takes precedence.

The order is always: framework default, then the process (`configure()` or
the environment), then the task's own explicit setting. A task setting of
`None` is a choice like any other; only `UNSET` means the task chose nothing.
"""

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
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
# numbers, not derived from the memory of the machine.
DEFAULT_MEMORY_SOFT_LIMIT = 256 * 1024 * 1024
DEFAULT_MEMORY_HARD_LIMIT = 512 * 1024 * 1024

# Above this many places per process, `concurrency_override=True` is needed:
# every place may hold a thread for a synchronous body.
MAX_CONCURRENCY = 32


def _one_per_cpu() -> int:
    # The CPUs this process may use, not the host's: a container or `taskset`
    # can allow fewer.
    if hasattr(os, "process_cpu_count"):
        return os.process_cpu_count() or 1
    if hasattr(os, "sched_getaffinity"):
        return len(os.sched_getaffinity(0)) or 1
    return os.cpu_count() or 1


@dataclass(frozen=True, kw_only=True)
class Settings:
    """Every process-wide setting.

    To add one, add a field with its default, and refuse invalid values in
    `__post_init__`. `KABUDACHI_<NAME>` in the environment can set it; a field
    of type `int` is read from the environment as a number, one of type
    `timedelta` as a number of seconds, a `bool` as true/false, yes/no, on/off
    or 1/0, `imports` as a comma-separated list, an `int | None` field as a
    number, and `reconnect_timeouts` as `queue=seconds` pairs separated by
    commas; an empty value leaves `imports` or an `int | None` field unset.
    """

    processes: int = field(default_factory=_one_per_cpu)
    """How many task processes run task bodies: one per CPU unless set. `0`
    runs bodies in this process instead, where a body that will not stop
    cannot be killed."""

    concurrency: int = 16
    """How many task bodies one process runs at once (in this process, with
    `processes=0`). Above 32 only with `concurrency_override=True`."""

    concurrency_override: bool = False
    """Allows `concurrency` above 32."""

    imports: tuple[str, ...] | None = None
    """The modules a task process imports to find its tasks. `None` imports
    every module that declared a task in this process."""

    max_runs_per_process: int | None = None
    """How many runs a task process takes before a fresh one replaces it,
    which bounds what bodies leak (memory, global state). The process takes
    no run past the limit, finishes the ones it has and exits; nothing it
    runs is cut short. `None` never replaces a process for this."""

    process_start_timeout: timedelta = timedelta(seconds=60)
    """How long a task process has, from its start, to import its modules
    and say it is ready, before it is killed. `KABUDACHI_PROCESS_START_TIMEOUT`
    gives it in seconds."""

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

    reconnect_timeouts: Mapping[str, float] = field(default_factory=dict)
    """Per queue, the seconds a run of a task sent there may go without its
    worker being heard, past the time it takes to suspect a silent worker,
    before the leader replays it, and so how long a worker cut off from its
    leader has to stop it. A task's own `reconnect_timeout` wins; a queue not
    named here uses the shard's. `KABUDACHI_RECONNECT_TIMEOUTS` gives it as
    `queue=seconds` pairs separated by commas."""

    def __post_init__(self) -> None:
        # `True` is an int to Python, but never what someone meant by a count.
        if (
            not isinstance(self.concurrency, int)
            or isinstance(self.concurrency, bool)
            or self.concurrency <= 0
        ):
            raise ValueError(f"concurrency must be a positive integer, not {self.concurrency!r}")
        if (
            not isinstance(self.processes, int)
            or isinstance(self.processes, bool)
            or self.processes < 0
        ):
            raise ValueError(f"processes must be a non-negative integer, not {self.processes!r}")
        if not isinstance(self.concurrency_override, bool):
            raise ValueError(
                f"concurrency_override must be True or False, not {self.concurrency_override!r}"
            )
        if self.concurrency > MAX_CONCURRENCY and not self.concurrency_override:
            raise ValueError(
                f"concurrency {self.concurrency} is above {MAX_CONCURRENCY}, which needs "
                "concurrency_override=True: each place may hold a thread"
            )
        if (
            not isinstance(self.process_start_timeout, timedelta)
            or self.process_start_timeout <= timedelta(0)
        ):
            raise ValueError(
                "process_start_timeout must be a positive timedelta, "
                f"not {self.process_start_timeout!r}"
            )
        if self.imports is not None:
            complaint = f"imports must be a list of module names, not {self.imports!r}"
            if isinstance(self.imports, str):
                raise ValueError(complaint)
            try:
                # Once: a generator would be used up by checking it.
                modules = tuple(self.imports)
            except TypeError:
                raise ValueError(complaint) from None
            if not all(isinstance(module, str) and module.strip() for module in modules):
                raise ValueError(complaint)
            # Frozen: a list given by the caller is kept as a tuple.
            object.__setattr__(self, "imports", modules)
        if self.max_runs_per_process is not None and (
            not isinstance(self.max_runs_per_process, int)
            or isinstance(self.max_runs_per_process, bool)
            or self.max_runs_per_process <= 0
        ):
            raise ValueError(
                "max_runs_per_process must be a positive integer or None, "
                f"not {self.max_runs_per_process!r}"
            )
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
        if not isinstance(self.reconnect_timeouts, Mapping) or not all(
            isinstance(queue, str) and queue.strip() and _positive_seconds(seconds)
            for queue, seconds in self.reconnect_timeouts.items()
        ):
            raise ValueError(
                "reconnect_timeouts must map queue names to positive numbers of seconds, "
                f"not {self.reconnect_timeouts!r}"
            )
        # A copy, as a plain dict: a mapping the caller changes later changes
        # nothing here, and the settings pickle into task processes.
        object.__setattr__(
            self,
            "reconnect_timeouts",
            {queue: float(seconds) for queue, seconds in self.reconnect_timeouts.items()},
        )

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
                found[field.name] = _parse(hints[field.name], raw)
            except ValueError:
                raise ValueError(f"{field.name} cannot be {raw!r} from the environment") from None
        return found


def _positive_seconds(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def _queue_seconds(raw: str) -> dict[str, float]:
    """`queue=seconds` pairs separated by commas, as a mapping. Raises
    `ValueError` for a pair without `=`, seconds that are not a number, or
    a queue named twice."""
    pairs: dict[str, float] = {}
    for item in raw.split(","):
        if not item.strip():
            continue
        queue, separator, seconds = item.partition("=")
        if not separator:
            raise ValueError(raw)
        queue = queue.strip()
        if queue in pairs:
            raise ValueError(raw)
        pairs[queue] = float(seconds)
    return pairs


def _parse(value_type: Any, raw: str) -> Any:
    """An environment variable's text as a setting of `value_type`."""
    if value_type is int:
        return int(raw)
    if value_type is timedelta:
        return timedelta(seconds=float(raw))
    if value_type is bool:
        lowered = raw.strip().lower()
        if lowered in ("1", "true", "yes", "on"):
            return True
        if lowered in ("0", "false", "no", "off"):
            return False
        raise ValueError(raw)
    if value_type == tuple[str, ...] | None:
        modules = tuple(part.strip() for part in raw.split(",") if part.strip())
        return modules or None  # an empty value is "not set", not "no modules"
    if value_type == int | None:
        return int(raw) if raw.strip() else None  # an empty value is "not set"
    if value_type == Mapping[str, float]:
        return _queue_seconds(raw)
    return raw


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
        if "reconnect_timeouts" in settings:
            # A copy: a mapping the caller changes later changes nothing here.
            configured["reconnect_timeouts"] = dict(validated.reconnect_timeouts)
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
        return getattr(self.settings(), name)

    def settings(self) -> Settings:
        """Every setting's effective value. The environment is read the first
        time any setting is needed."""
        if self._settings is None:
            self._settings = self._validate(self._configured)
        return self._settings

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

    The settings are `processes`, how many task processes run task bodies
    (one per CPU by default; `0` runs them in this process), `concurrency`,
    the number of tasks one process runs at once, `concurrency_override` to
    allow `concurrency` above 32, `imports`, the modules task processes import
    to find tasks (by default every module that declared one),
    `max_runs_per_process`, how many runs a task process takes before a fresh
    one replaces it (no limit by default), `process_start_timeout`, the
    timedelta a task process has to become ready, `queue`, the queue tasks
    are sent to unless they name their own, `result_ttl`, the seconds a
    finished task is kept, `cancel_grace`, the timedelta a task past its
    timeout has to stop, `reconnect_timeouts`, the seconds a run of a task in
    each named queue may go unheard past a suspicion before the leader
    replays it (a task's own `reconnect_timeout` wins), and
    `memory_soft_limit` and `memory_hard_limit`, in bytes of pending task
    input (past the soft one `group` and `map` pause, past the hard one a
    submission raises `BackpressureError`); `KABUDACHI_<NAME>` in the
    environment sets each too, below what is configured here. A setting a
    task makes for itself always wins over these. `concurrency`, `processes`,
    `imports`, `max_runs_per_process`, `process_start_timeout`, the memory
    limits and `result_ttl` are read when `run()` starts, so changing them
    during a run has no effect on that run.

    Raises `ConfigurationError` for an unknown setting or an invalid value,
    and then applies none of the settings in this call.
    """
    _process_configuration.configure(**settings)
