"""Process-wide settings, and how a task's own choice takes precedence.

The order is always: framework default, then the process (`configure()` or
the environment), then the task's own explicit setting. A task setting of
`None` is a choice like any other; only `UNSET` means the task chose nothing.
"""

import functools
import ipaddress
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, fields
from datetime import timedelta
from pathlib import Path
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


def _in_range(value: Any, low: int, high: int) -> bool:
    return is_count(value) and low <= value <= high


@dataclass(frozen=True, kw_only=True)
class QueueOptions:
    """How one queue's tasks are ordered, limited and kept.

    The leader orders and limits by `priority`, `weight`, `max_running` and
    `starvation_age`; every worker of a shard should give the same ones, and
    the leader's decide. `reconnect_timeout` and `result_ttl` are stamped on
    each task when it is submitted.
    """

    priority: int = 0
    """Higher goes first: a worker asking for work gets the highest priority
    that has any waiting."""

    weight: int = 1
    """Among queues of one priority, how many tasks this queue hands out for
    each one a queue of weight 1 does."""

    max_running: int | None = None
    """The most tasks of this queue claimed or running at once across the
    shard; `None` for no limit."""

    reconnect_timeout: timedelta | None = None
    """How long a run of a task sent here may go without its worker being
    heard, past the time it takes to suspect a silent worker, before the
    leader runs it again; a task's own `reconnect_timeout` wins. `None` uses
    the shard's."""

    result_ttl: timedelta | None | Unset = UNSET
    """How long a finished task sent here is kept; `None` keeps nothing, and
    `UNSET` uses the process's `result_ttl`. A task's own wins."""

    starvation_age: timedelta | None = None
    """How long a task may wait here before it goes ahead of every priority;
    `None` never."""

    def __post_init__(self) -> None:
        if not _in_range(self.priority, -(2**31), 2**31 - 1):
            raise ValueError(f"priority must be a 32-bit integer, not {self.priority!r}")
        if not _in_range(self.weight, 1, 2**32 - 1):
            raise ValueError(f"weight must be a positive 32-bit integer, not {self.weight!r}")
        if self.max_running is not None and not _in_range(self.max_running, 1, 2**32 - 1):
            raise ValueError(
                f"max_running must be a positive 32-bit integer or None, not {self.max_running!r}"
            )
        for name in ("reconnect_timeout", "starvation_age"):
            value = getattr(self, name)
            if value is not None and not is_positive_timedelta(value):
                raise ValueError(f"{name} must be a positive timedelta or None, not {value!r}")
        if not (
            self.result_ttl is None
            or self.result_ttl is UNSET
            or is_positive_timedelta(self.result_ttl)
        ):
            raise ValueError(
                f"result_ttl must be a positive timedelta, None or UNSET, not {self.result_ttl!r}"
            )


ENVIRONMENT_PREFIX = "KABUDACHI_"

# Bytes of task payload the process may hold. The scheduler counts every
# unfinished task's input; a networked process also counts the blob payloads
# crossing the network through it. Past the soft limit bulk submission (group,
# map) pauses; past the hard limit a submission raises `BackpressureError`,
# and a payload that alone is past it is refused with `PayloadTooLargeError`
# (an input) or fails its run with `ResultTooLargeError` (a result). Unless
# set, the limits are these shares of the memory this process may use, read
# once at start.
MEMORY_SOFT_SHARE = 4
MEMORY_HARD_SHARE = 2

# cgroup v1 reports "no limit" as a number near 2**63; anything from 2**60 up
# is no limit.
_CGROUP_V1_UNLIMITED = 2**60


def available_memory(
    cgroup_root: Path = Path("/sys/fs/cgroup"), proc_cgroup: Path = Path("/proc/self/cgroup")
) -> int:
    """The bytes of memory this process may use: its cgroup's limit (v2, then
    v1) when one is set and below physical memory, else physical memory."""
    physical = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    limit = _cgroup_limit(cgroup_root, proc_cgroup)
    return physical if limit is None else min(limit, physical)


def _cgroup_limit(root: Path, proc_cgroup: Path) -> int | None:
    try:
        lines = proc_cgroup.read_text().splitlines()
    except OSError:
        lines = []
    v2_path, v1_path = None, None
    for line in lines:
        hierarchy, _, rest = line.partition(":")
        controllers, _, path = rest.partition(":")
        if hierarchy == "0" and controllers == "":
            v2_path = path
        elif "memory" in controllers.split(","):
            v1_path = path
    candidates = []
    if v2_path is not None:
        candidates += [root / v2_path.lstrip("/") / "memory.max", root / "memory.max"]
    if v1_path is not None:
        v1 = root / "memory"
        candidates += [v1 / v1_path.lstrip("/") / "memory.limit_in_bytes", v1 / "memory.limit_in_bytes"]
    for candidate in candidates:
        try:
            text = candidate.read_text().strip()
        except OSError:
            continue
        if text == "max":
            return None
        try:
            value = int(text)
        except ValueError:
            continue
        return None if value >= _CGROUP_V1_UNLIMITED else value
    return None


@functools.cache
def _available_at_start() -> int:
    return available_memory()


def _memory_at_start() -> int:
    return _available_at_start()

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
    field as a number, a `timedelta | None` field as a number of seconds, and
    `queues` as a comma-separated list; an empty value leaves `seeds` empty and
    `imports`, `queues`, an `int | None` field, a `timedelta | None` field or a
    `str | None` field unset. `queue_options` and `router` cannot come from
    the environment.
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

    replication_factor: int = 3
    """How many voters of a networked shard store each task's record."""

    seed_rounds: int = 3
    """How many rounds a networked worker with seeds and no authority asks
    its silent seeds before it founds its shard alone."""

    routing_refresh: timedelta | None = None
    """How often a networked worker re-crawls its peers' routing when nothing
    else prompts it; `None` is ten heartbeat timeouts."""

    health_listen: str | None = None
    """`host:port` (an IP address, `[::]` for IPv6) on which the worker
    answers `/healthz` (200 while it runs) and `/readyz` (200 once it can
    take work); port 0 picks a free one. `None` serves neither."""

    heartbeat_interval: timedelta = timedelta(seconds=1)
    """How often a networked worker tells its leader it is alive."""

    heartbeat_timeout: timedelta = timedelta(seconds=10)
    """How long a networked worker goes without hearing its leader before it
    suspects it, and a leader without hearing a worker before it suspects
    that. Every worker of a shard must use the same value."""

    roll_call_deadline: timedelta | None = None
    """How long a networked worker's roll call runs before it decides on it;
    below `heartbeat_timeout`. `None` is a quarter second, or a quarter of
    `heartbeat_timeout` if that is shorter."""

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

    queues: tuple[str, ...] | None = None
    """The queues this worker takes tasks from. `None` takes `queue` alone on a
    networked worker, and every queue in a shard of one, where nothing else
    could run the rest. `KABUDACHI_QUEUES` gives them separated by commas."""

    queue_options: Mapping[str, QueueOptions] = field(default_factory=dict)
    """Per queue, its `QueueOptions`; a queue not named here has the defaults.
    Every worker of a shard should give the same; the leader's decide."""

    router: Callable[[str, Any], str | None] | None = None
    """Picks the queue of a task that names none, when it is submitted:
    called with the task's name and its argument, it returns a queue name, or
    `None` for `queue`. It runs in the submitting process only."""

    result_ttl: int = 3600
    """How many seconds a finished task is kept, so its result and state can
    still be looked up, before it is forgotten."""

    memory_soft_limit: int | None = None
    """Bytes of task payload past which bulk submission pauses: a quarter of
    the memory this process may use unless set."""

    memory_hard_limit: int | None = None
    """Bytes of task payload past which a submission is refused for now, and
    past which a single payload is refused for good: half of the memory this
    process may use unless set. A payload larger than one message (about
    1 MiB) travels as 256 KiB chunks stored in the shard."""

    cancel_grace: timedelta = timedelta(seconds=30)
    """How long a task past its timeout has to stop once asked, before its
    run is failed and its body abandoned. `KABUDACHI_CANCEL_GRACE` gives it
    in seconds."""

    drain_grace: timedelta | None = None
    """While `run` serves, how long tasks still running after the first SIGINT
    or SIGTERM have to finish before they are stopped as a second signal would
    stop them. `None` waits for them however long they take.
    `KABUDACHI_DRAIN_GRACE` gives it in seconds."""

    drain_wait_limit: timedelta | None = None
    """How long a networked worker that is leaving its shard waits at each step:
    a leader for its voters to be ready to take over (and, alone, for another
    voter to hand over to), a worker with no leader for one to appear, and every
    worker for the Task records it holds to be stored by others. Records still
    held then leave with it, and are logged. `None` is ten heartbeat timeouts.
    `KABUDACHI_DRAIN_WAIT_LIMIT` gives it in seconds."""

    def __post_init__(self) -> None:
        # In this order, so a value with several bad settings names the same
        # one first as it always has.
        self._check_task_processes()
        self._check_network()
        self._check_shard_timings()
        self._check_shard_tunables()
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
        if self.health_listen is not None:
            health_address_parts(self.health_listen)

    def _check_shard_timings(self) -> None:
        for name in ("heartbeat_interval", "heartbeat_timeout", "reconnect_timeout"):
            _require_positive_timedelta(name, getattr(self, name))
        if self.heartbeat_interval * 3 > self.heartbeat_timeout:
            raise ValueError(
                f"heartbeat_timeout ({self.heartbeat_timeout}) must be at least three "
                f"heartbeat_interval ({self.heartbeat_interval}): a leader whose followers are "
                "all alive would otherwise run out of lease"
            )
        if self.roll_call_deadline is not None and (
            not is_positive_timedelta(self.roll_call_deadline)
            # In whole milliseconds, as the native side reads them.
            or round(self.roll_call_deadline.total_seconds() * 1000)
            >= round(self.heartbeat_timeout.total_seconds() * 1000)
        ):
            raise ValueError(
                "roll_call_deadline must be a positive timedelta below heartbeat_timeout "
                f"({self.heartbeat_timeout}), not {self.roll_call_deadline!r}"
            )

    def _check_shard_tunables(self) -> None:
        _require_count(
            "replication_factor", self.replication_factor, minimum=1, what="a positive integer"
        )
        _require_count("seed_rounds", self.seed_rounds, minimum=1, what="a positive integer")
        if self.routing_refresh is not None:
            _require_positive_timedelta("routing_refresh", self.routing_refresh)

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
            limit = getattr(self, name)
            if limit is not None:
                _require_count(name, limit, minimum=1, what="a positive number of bytes")
        soft, hard = self._memory_limits_with_defaults()
        if soft > hard:
            raise ValueError(
                f"memory_soft_limit ({soft}) must not be above memory_hard_limit ({hard})"
            )
        object.__setattr__(self, "memory_soft_limit", soft)
        object.__setattr__(self, "memory_hard_limit", hard)

    def _memory_limits_with_defaults(self) -> tuple[int, int]:
        """The soft and hard limits, a missing one filled from the memory this
        process may use: half for the hard limit, and a quarter for the soft
        one unless the hard limit is lower."""
        hard = self.memory_hard_limit
        if hard is None:
            hard = _memory_at_start() // MEMORY_HARD_SHARE
        soft = self.memory_soft_limit
        if soft is None:
            soft = min(_memory_at_start() // MEMORY_SOFT_SHARE, hard)
        return soft, hard

    def _check_stopping(self) -> None:
        if not is_non_negative_timedelta(self.cancel_grace):
            raise ValueError(
                f"cancel_grace must be a non-negative timedelta, not {self.cancel_grace!r}"
            )
        if self.drain_grace is not None and not is_non_negative_timedelta(self.drain_grace):
            raise ValueError(
                f"drain_grace must be a non-negative timedelta or None, not {self.drain_grace!r}"
            )
        if self.drain_wait_limit is not None and not is_positive_timedelta(self.drain_wait_limit):
            raise ValueError(
                f"drain_wait_limit must be a positive timedelta or None, not "
                f"{self.drain_wait_limit!r}"
            )

    def _check_queues(self) -> None:
        if not is_nonblank_string(self.queue):
            raise ValueError(f"queue must be a non-empty string, not {self.queue!r}")
        if self.queues is not None:
            complaint = f"queues must be a non-empty list of queue names, not {self.queues!r}"
            queues = _name_tuple(self.queues, complaint)
            if not queues:
                raise ValueError(complaint)
            object.__setattr__(self, "queues", queues)
        if not isinstance(self.queue_options, Mapping) or not all(
            is_nonblank_string(queue) and isinstance(options, QueueOptions)
            for queue, options in self.queue_options.items()
        ):
            raise ValueError(
                f"queue_options must map queue names to QueueOptions, not {self.queue_options!r}"
            )
        # A copy, as a plain dict: a mapping the caller changes later changes
        # nothing here, and the settings pickle into task processes.
        object.__setattr__(self, "queue_options", dict(self.queue_options))
        if self.router is not None and not callable(self.router):
            raise ValueError(f"router must be callable or None, not {self.router!r}")

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


def health_address_parts(address: str) -> tuple[str, int]:
    """The IP address and port of a `health_listen` setting. Raises
    `ValueError` for anything else, a host name included."""
    complaint = (
        f"health_listen must be an IP address and port such as 0.0.0.0:8081, not {address!r}"
    )
    if not isinstance(address, str):
        raise ValueError(complaint)
    parts = urlsplit("//" + address)
    try:
        port = parts.port
        host = ipaddress.ip_address(parts.hostname or "")
    except ValueError:
        raise ValueError(complaint) from None
    if port is None or parts.path or parts.query:
        raise ValueError(complaint)
    return str(host), port


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
    if value_type == timedelta | None:
        return timedelta(seconds=float(raw)) if raw.strip() else None  # empty is "not set"
    if value_type == int | None:
        return int(raw) if raw.strip() else None  # an empty value is "not set"
    if value_type == timedelta | None:
        return timedelta(seconds=float(raw)) if raw.strip() else None  # an empty value is "not set"
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

        A networked worker reads all of its networked settings when `run()`
        starts, so a change made after that applies from the next `run()`.

        A failed task's error message is stored in the task's replicated
        record, so it can contain task data.
        """
        configured ={**self._configured, **settings}
        validated = self._validate(configured)
        if "queue_options" in settings:
            # A copy: a mapping the caller changes later changes nothing here.
            configured["queue_options"] = dict(validated.queue_options)
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
    timeout has to stop, `queues`, the queues this worker takes,
    `queue_options`, each queue's `QueueOptions` (priority, weight, running
    limit, reconnect timeout, result TTL, starvation age), `router`, which
    picks the queue of a task that names none, and `memory_soft_limit` and
    `memory_hard_limit`, in bytes of pending task payload (past the soft one
    `group` and `map` pause, past the hard one a submission raises
    `BackpressureError`; a quarter and half of the memory this process may use
    unless set). `listen`, `seeds`,
    `external_address`, `authority` and `shard` make this a worker of a
    networked shard (see each setting; `authority` is a URL that may set
    `?key_prefix=` and `?ttl=`); `heartbeat_interval`,
    `heartbeat_timeout` and `reconnect_timeout` are its shard's timings, and
    it also reads `roll_call_deadline`, `replication_factor`, `seed_rounds`
    and `routing_refresh` (see each setting). `health_listen` is the
    `host:port` that answers `/healthz` and `/readyz`.
    `KABUDACHI_<NAME>` in the environment sets each too, below what is
    configured here. A setting a task makes for itself always wins over
    these. The networked settings and timings, `concurrency`, `processes`,
    `imports`, `max_runs_per_process`, `process_start_timeout`, the memory
    limits, `queues`, `queue_options` and `result_ttl` are read when `run()`
    starts, so changing them during a run has no effect on that run.

    Raises `ConfigurationError` for an unknown setting or an invalid value,
    and then applies none of the settings in this call.
    """
    _process_configuration.configure(**settings)
