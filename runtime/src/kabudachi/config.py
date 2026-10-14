"""Process-wide settings, and how a task's own choice takes precedence.

The order is always: framework default, then the process (`configure()` or
the environment), then the task's own explicit setting. A task setting of
`None` is a choice like any other; only `UNSET` means the task chose nothing.
"""

import os
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from datetime import timedelta
from typing import Any, get_type_hints
from urllib.parse import SplitResult, parse_qs, urlsplit

from kabudachi._checks import (
    is_count,
    is_non_negative_timedelta,
    is_nonblank_string,
    is_positive_seconds,
    is_positive_timedelta,
    name_list,
)
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

# The key prefix of an `authority` URL that does not set its own.
DEFAULT_KEY_PREFIX = "kabudachi:"

# The highest database number an `authority` URL may name.
MAX_DATABASE = 65535

# The shortest authority TTL, in milliseconds, whose call timeout (a tenth
# of it) is a whole millisecond.
MIN_AUTHORITY_TTL_MS = 10

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
    the `_check_*` validator for its concern (or a new one, called from
    `__post_init__`). `KABUDACHI_<NAME>` in the environment can set it; a field
    of type `int` is read from the environment as a number, one of type
    `timedelta` as a number of seconds, a `bool` as true/false, yes/no, on/off
    or 1/0, `seeds` and `imports` as a comma-separated list, an `int | None`
    field as a number, and `reconnect_timeouts` as `queue=seconds` pairs
    separated by commas; an empty value leaves `seeds` empty and `imports`, an
    `int | None` field or a `str | None` field unset.
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

    listen: str | None = None
    """The address this worker listens on for the rest of its shard, such as
    `/ip4/0.0.0.0/tcp/4001`. Set, the worker is networked: it joins the shard
    `shard` and runs the tasks any worker of it submits. `None` runs a shard
    of one, inside this process."""

    seeds: tuple[str, ...] = ()
    """Addresses of workers to ask who leads the shard. A networked worker with
    no seeds and no authority founds the shard alone."""

    external_address: str | None = None
    """The address other workers reach this one at, when it is not `listen`
    (a wildcard bind, NAT, a container port mapping)."""

    authority: str | None = None
    """The shard's coordination authority, a Redis or Valkey server:
    `redis://` or `rediss://` (TLS), with user and password if it needs them,
    the database as the path (`/0` unless given), the key prefix as
    `?key_prefix=` (`kabudachi:` unless given), and as `?ttl=` the seconds a
    worker's registration outlives its last renewal (30 unless given; a
    cold worker waits this long before it founds the shard). `None`
    bootstraps from seeds alone."""

    shard: str = "default"
    """The name of the shard a networked worker serves."""

    heartbeat_interval: timedelta = timedelta(seconds=1)
    """How often a networked worker tells its leader it is alive."""

    heartbeat_timeout: timedelta = timedelta(seconds=10)
    """How long a networked worker goes without hearing its leader before it
    suspects it, and a leader without hearing a worker before it suspects
    that. Every worker of a shard must use the same value."""

    reconnect_timeout: timedelta = timedelta(seconds=30)
    """How long after a worker goes unheard its leader waits before it runs
    the worker's tasks again elsewhere; the worker stops them by then. Every
    worker of a shard must use the same value; tasks and queues may set their
    own."""

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
        # In this order, so a value with several bad settings names the same
        # one first as it always has.
        self._check_task_processes()
        self._check_network()
        self._check_shard_timings()
        self._check_run_limits()
        self._check_memory()
        self._check_stopping()
        self._check_queues()

    def _check_task_processes(self) -> None:
        _require_count("concurrency", self.concurrency, minimum=1, what="a positive integer")
        _require_count("processes", self.processes, minimum=0, what="a non-negative integer")
        if self.listen is not None and self.processes == 0:
            raise ValueError(
                "a networked worker (listen set) needs processes of at least 1: it must be "
                "able to kill a task body when its leader stops hearing it"
            )
        if not isinstance(self.concurrency_override, bool):
            raise ValueError(
                f"concurrency_override must be True or False, not {self.concurrency_override!r}"
            )
        if self.concurrency > MAX_CONCURRENCY and not self.concurrency_override:
            raise ValueError(
                f"concurrency {self.concurrency} is above {MAX_CONCURRENCY}, which needs "
                "concurrency_override=True: each place may hold a thread"
            )
        _require_positive_timedelta("process_start_timeout", self.process_start_timeout)
        if self.imports is not None:
            complaint = f"imports must be a list of module names, not {self.imports!r}"
            # Frozen: a list given by the caller is kept as a tuple.
            object.__setattr__(self, "imports", _name_tuple(self.imports, complaint))

    def _check_network(self) -> None:
        complaint = f"seeds must be a list of addresses, not {self.seeds!r}"
        object.__setattr__(self, "seeds", _name_tuple(self.seeds, complaint))
        for name in ("listen", "external_address"):
            value = getattr(self, name)
            if value is not None and not is_nonblank_string(value):
                raise ValueError(f"{name} must be an address, not {value!r}")
        if not is_nonblank_string(self.shard) or "/" in self.shard:
            raise ValueError(f"shard must be a name without '/', not {self.shard!r}")
        if self.authority is not None:
            authority_parts(self.authority)

    def _check_shard_timings(self) -> None:
        for name in ("heartbeat_interval", "heartbeat_timeout", "reconnect_timeout"):
            _require_positive_timedelta(name, getattr(self, name))
        if self.heartbeat_interval * 3 > self.heartbeat_timeout:
            raise ValueError(
                f"heartbeat_timeout ({self.heartbeat_timeout}) must be at least three "
                f"heartbeat_interval ({self.heartbeat_interval}): a leader whose followers are "
                "all alive would otherwise run out of lease"
            )

    def _check_run_limits(self) -> None:
        if self.max_runs_per_process is not None:
            _require_count(
                "max_runs_per_process",
                self.max_runs_per_process,
                minimum=1,
                what="a positive integer or None",
            )
        _require_count(
            "result_ttl", self.result_ttl, minimum=1, what="a positive number of seconds"
        )

    def _check_memory(self) -> None:
        for name in ("memory_soft_limit", "memory_hard_limit"):
            _require_count(name, getattr(self, name), minimum=1, what="a positive number of bytes")
        if self.memory_soft_limit > self.memory_hard_limit:
            raise ValueError(
                f"memory_soft_limit ({self.memory_soft_limit}) must not be above "
                f"memory_hard_limit ({self.memory_hard_limit})"
            )

    def _check_stopping(self) -> None:
        if not is_non_negative_timedelta(self.cancel_grace):
            raise ValueError(
                f"cancel_grace must be a non-negative timedelta, not {self.cancel_grace!r}"
            )

    def _check_queues(self) -> None:
        if not is_nonblank_string(self.queue):
            raise ValueError(f"queue must be a non-empty string, not {self.queue!r}")
        if not isinstance(self.reconnect_timeouts, Mapping) or not all(
            is_nonblank_string(queue) and is_positive_seconds(seconds)
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


def authority_parts(url: str) -> tuple[str, str, int, float | None]:
    """The server URL, key prefix, database and TTL in seconds an `authority`
    setting names; the TTL is `None` when the URL does not set one. Raises
    `ValueError` for a URL the authority cannot use."""
    if not isinstance(url, str):
        raise ValueError(f"authority must be a redis:// or rediss:// URL, not {url!r}")
    parts = urlsplit(url)
    _check_authority_server(parts, url)
    database = _authority_database(parts)
    query = _authority_query(parts, url)
    prefix = query.get("key_prefix", [DEFAULT_KEY_PREFIX])[0]
    if "{" in prefix or "}" in prefix:
        raise ValueError(f"the authority's key prefix may not contain braces: {prefix!r}")
    return f"{parts.scheme}://{parts.netloc}/", prefix, database, _authority_ttl(query)


def _check_authority_server(parts: SplitResult, url: str) -> None:
    try:
        parts.port  # a port that is not a number raises here
    except ValueError:
        raise ValueError(f"the authority URL's port must be a number: {url!r}") from None
    if parts.scheme not in ("redis", "rediss") or not parts.hostname:
        raise ValueError(f"authority must be a redis:// or rediss:// URL with a host, not {url!r}")


def _authority_database(parts: SplitResult) -> int:
    path = parts.path.strip("/")
    if path and not _is_database_number(path):
        raise ValueError(
            f"the authority URL's path must be a database number up to {MAX_DATABASE}, "
            f"not {parts.path!r}"
        )
    return int(path or 0)


def _is_database_number(path: str) -> bool:
    return path.isascii() and path.isdigit() and int(path) <= MAX_DATABASE


def _authority_query(parts: SplitResult, url: str) -> dict[str, list[str]]:
    query = parse_qs(parts.query, keep_blank_values=True, strict_parsing=bool(parts.query))
    unknown = sorted(set(query) - {"key_prefix", "ttl"})
    if unknown or any(len(values) != 1 for values in query.values()):
        raise ValueError(
            f"the authority URL may set key_prefix and ttl once each, and nothing else: {url!r}"
        )
    return query


def _authority_ttl(query: dict[str, list[str]]) -> float | None:
    if "ttl" not in query:
        return None
    raw = query["ttl"][0]
    try:
        ttl = float(raw)
    except ValueError:
        ttl = None
    if ttl is None or not is_positive_seconds(ttl):
        raise ValueError(f"the authority's ttl must be a positive number of seconds, not {raw!r}")
    # The authority bounds each call by a tenth of its TTL in whole
    # milliseconds, and refuses a TTL whose tenth rounds to nothing.
    if round(ttl * 1000) < MIN_AUTHORITY_TTL_MS:
        raise ValueError(
            f"the authority's ttl must be at least {MIN_AUTHORITY_TTL_MS / 1000} s, not {raw!r}"
        )
    return ttl


def _require_count(name: str, value: Any, *, minimum: int, what: str) -> None:
    if not is_count(value) or value < minimum:
        raise ValueError(f"{name} must be {what}, not {value!r}")


def _require_positive_timedelta(name: str, value: Any) -> None:
    if not is_positive_timedelta(value):
        raise ValueError(f"{name} must be a positive timedelta, not {value!r}")


def _name_tuple(value: Any, complaint: str) -> tuple[str, ...]:
    names = name_list(value)
    if names is None:
        raise ValueError(complaint)
    return names


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
    if value_type == tuple[str, ...]:
        return tuple(part.strip() for part in raw.split(",") if part.strip())
    if value_type == tuple[str, ...] | None:
        modules = tuple(part.strip() for part in raw.split(",") if part.strip())
        return modules or None  # an empty value is "not set", not "no modules"
    if value_type == str | None:
        return raw if raw.strip() else None  # an empty value is "not set"
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
    submission raises `BackpressureError`). `listen`, `seeds`,
    `external_address`, `authority` and `shard` make this a worker of a
    networked shard (see each setting; `authority` is a URL that may set
    `?key_prefix=` and `?ttl=`); `heartbeat_interval`,
    `heartbeat_timeout` and `reconnect_timeout` are its shard's timings.
    `KABUDACHI_<NAME>` in the environment sets each too, below what is
    configured here. A setting a task makes for itself always wins over
    these. The networked settings and timings, `concurrency`, `processes`,
    `imports`, `max_runs_per_process`, `process_start_timeout`, the memory
    limits and `result_ttl` are read when `run()` starts, so changing them
    during a run has no effect on that run.

    Raises `ConfigurationError` for an unknown setting or an invalid value,
    and then applies none of the settings in this call.
    """
    _process_configuration.configure(**settings)
