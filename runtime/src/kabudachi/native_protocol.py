"""What a session needs of the native runtime, and what the runtime hands it.

These are protocols, so the runtime's `NativeRuntime` and a test double both
satisfy them. They live apart from `session` so that `task_table` can name them
without importing `session`, which imports `task_table`.
"""

from dataclasses import dataclass
from typing import Any, Literal, Protocol, runtime_checkable

from kabudachi._native import CancelOutcome, EventKind
from kabudachi.options import SubmissionOptions, SubmissionSettings


class Claim(Protocol):
    """A task run handed to this worker to execute."""

    task_id: str
    task_run_id: str
    definition_id: str
    source_version: int
    queue: str
    attempt_number: int
    serialized_input: bytes
    chain: list[bytes]
    compaction: bool
    """A compaction run: `chain` holds the payloads to fold, oldest first;
    `serialized_input` is empty."""
    reconnect_timeout_ms: int
    """The run's reconnect timeout, resolved: its task's own, or the shard's."""
    client: str | None
    """The worker awaiting the result; `None` for a task nobody awaits across
    workers."""
    composite_id: str | None
    """The composite this task is a stage or member of; `None` for a task that is not."""
    composite_index: int
    """Which stage or member of its composite this task is; 0 when it is none."""
    expansion: bool
    """Whether this is an expansion run: its input is a list a map is to be
    split over, and it reports the map that results."""


class Event(Protocol):
    """Something the runtime decided on its own, such as that a task expired."""

    kind: EventKind
    task_id: str
    task_run_id: str
    was_running: bool
    superseded_by: str | None
    active: bool
    """For `EventKind.SLOW_DOWN`: whether it was raised (`True`) or cleared."""
    reason: str | None
    """For `EventKind.REFUSED`: why the leader refused the submission."""
    seconds_left: float
    """For `EventKind.ABORT`: seconds until the run may be run again elsewhere."""
    payload: bytes
    """For `EventKind.RESULT`: the run's result, not certified yet."""
    result_digest: bytes
    """For `EventKind.CERTIFIED`: the digest the leader certified."""
    state: Any
    """For `EventKind.OUTCOME`: the `RunState` the task's run ended in;
    `None` when the shard does not know the task. For
    `EventKind.COMPOSITE_FINISHED`: how the composite ended, `"succeeded"`,
    `"failed"` or `"cancelled"`."""
    failure_kind: str
    """For `EventKind.OUTCOME` and `EventKind.COMPOSITE_FINISHED`: the
    error's type name, when the run or composite failed."""
    failure_message: str
    """For `EventKind.COMPOSITE_FINISHED`: the error's message, when the
    composite failed."""
    failure_task_id: str | None
    """For `EventKind.COMPOSITE_FINISHED`: the task whose run raised the error
    that ended the composite, when a run did."""
    composite_id: str
    """For `EventKind.COMPOSITE_FINISHED`: the composite that is over."""
    parent_task_id: str | None
    """For `EventKind.COMPOSITE_FINISHED`: the task that returned the
    composite as its continuation; `None` for a composite submitted directly."""


class Certification(Protocol):
    """The leader's word that a run's result is the authoritative one."""

    task_id: str
    task_run_id: str
    result_digest: bytes


@dataclass(frozen=True)
class SubmitArguments:
    """One task for `Runtime.submit` to record.

    `kind` is the task's `TaskKind` value. `key` is the coalescing key
    resolved for that kind: a coalescing task always has one (`options.key`,
    or "" by default), and no other kind has one. A run that fails is
    replaced by a new attempt up to `retries` times. With `drop_oldest`, a
    coalescing task that would pass the hard memory limit loses its key's
    oldest retained payloads to fit instead of being refused. With
    `non_retriable`, the task is not safe to run twice: a run lost while
    running is orphaned.

    `options` is what the caller asked: with `delay_ms` the task is not
    claimed before that long has passed, and with `expires_in_ms` it
    expires instead of running if it is still unclaimed after that long.
    Its `key` is not read here; `key` above is. `settings` is what this
    process resolved: the task's queue, and, with `reconnect_timeout_ms`
    (which must be positive), how long after its worker goes a suspicion
    timeout unheard a run of the task may be replayed, instead of after the
    shard's reconnect timeout.
    """

    definition_id: str
    source_version: int
    serialized_input: bytes
    kind: Literal["task", "ephemeral", "coalescing"]
    key: str | None
    options: SubmissionOptions
    settings: SubmissionSettings
    retries: int = 0
    drop_oldest: bool = False
    non_retriable: bool = False


QueueOptionsArgument = tuple[str, int, int, int | None, int | None]
"""One queue's options: its name, priority, weight, running limit and
starvation age in milliseconds. The last two are `None` for none."""

_DEFAULT_WORKER_THREADS = 2
_DEFAULT_RESULT_TTL_MS = 60 * 60 * 1000


@dataclass(frozen=True)
class OneNodeArguments:
    """How a one-node runtime runs.

    `worker_threads` sizes the native thread pool. `suspect_timeout_ms` is
    how long the worker waits, with no leader to hear from, before it
    elects itself. A lone worker has no peer to wait for, so with the
    default of 0 it leads a millisecond after it starts, when its
    one-voter roll call closes. `result_ttl_ms` is how long a finished
    task is kept before it is forgotten. `memory_soft_limit` and
    `memory_hard_limit` are in bytes of serialized task input: past the
    soft one `next_events()` reports an `EventKind.SLOW_DOWN` event with
    `active` set, and past the hard one `submit` raises
    `BackpressureError`. Both or neither must be given, and the soft one
    must not be above the hard one, which `kabudachi.configure` checks. An
    input larger than one message is kept in this runtime's own store as
    blobs and handed back whole with its claim; it counts against the
    memory limits like any other input. With `health_listen` (`ip:port`;
    port 0 picks one), the runtime serves `/healthz` and `/readyz` there.
    `queues` names the queues this worker takes, every queue for `None`
    (an empty tuple is refused); `queue_options` gives queues their own
    priority, weight, running limit and starvation age.
    """

    worker_threads: int = _DEFAULT_WORKER_THREADS
    suspect_timeout_ms: int = 0
    result_ttl_ms: int = _DEFAULT_RESULT_TTL_MS
    memory_soft_limit: int | None = None
    memory_hard_limit: int | None = None
    health_listen: str | None = None
    queues: tuple[str, ...] | None = None
    queue_options: tuple[QueueOptionsArgument, ...] = ()


@dataclass(frozen=True)
class AuthorityArguments:
    """The shard's coordination authority: a `redis://` or `rediss://`
    server `url`, with its key prefix, database and TTL (the authority's
    own default unless given)."""

    url: str
    key_prefix: str | None = None
    database: int = 0
    ttl_ms: int | None = None


@dataclass(frozen=True)
class NetworkedArguments:
    """How a worker of the networked shard `shard` runs.

    It listens on `listen` and bootstraps into the shard through `seeds`,
    or, with none, founds the shard alone. The heartbeat interval,
    heartbeat timeout and reconnect timeout are the shard's election
    timings, the same on every worker of it. With an `authority`, the
    worker bootstraps through it instead, and its timings come from the
    authority's TTL. `memory_soft_limit` and `memory_hard_limit` bound
    pending task input while this worker leads, as for
    `OneNodeArguments`; `queues` and `queue_options` are as for
    `OneNodeArguments`. `roll_call_deadline_ms` overrides the default
    roll-call deadline; `replication_factor` voters store each record;
    `seed_rounds` silent rounds pass before a worker with seeds founds the
    shard alone; `routing_refresh_ms` is the period of the routing crawl
    nothing prompted. With `health_listen`, the worker serves `/healthz` and
    `/readyz` there, as for `OneNodeArguments`. `drain_wait_limit_ms` bounds
    each wait of a worker that is leaving its shard (ten heartbeat timeouts
    unless given).
    """

    shard: str
    listen: str
    heartbeat_interval_ms: int
    heartbeat_timeout_ms: int
    reconnect_timeout_ms: int
    seeds: tuple[str, ...] = ()
    external_address: str | None = None
    authority: AuthorityArguments | None = None
    result_ttl_ms: int = _DEFAULT_RESULT_TTL_MS
    memory_soft_limit: int | None = None
    memory_hard_limit: int | None = None
    worker_threads: int = _DEFAULT_WORKER_THREADS
    roll_call_deadline_ms: int | None = None
    replication_factor: int = 3
    seed_rounds: int = 3
    routing_refresh_ms: int | None = None
    health_listen: str | None = None
    queues: tuple[str, ...] | None = None
    queue_options: tuple[QueueOptionsArgument, ...] = ()
    drain_wait_limit_ms: int | None = None


@runtime_checkable
class Runtime(Protocol):
    """What a session needs of the native runtime."""

    certifies_locally: bool
    """Whether a report's call returns the leader's decision (a
    certification, a retry). `False` for a worker of a networked shard: its
    reports go to a leader that may be another process, the reports below
    then return `None`, and certifications and outcomes arrive as events."""

    payload_limit: int | None
    """The largest payload this process holds whole: a one-node runtime's
    hard memory limit, a networked worker's half of it for payloads in
    flight. `None` when no limit is set."""

    def shard_id(self) -> str:
        """The incarnation of the shard this runtime founded at start."""
        ...

    def worker_id(self) -> str:
        """This worker's id."""
        ...

    def shard_name(self) -> str:
        """The name of this worker's shard: `local` for a one-node runtime."""
        ...

    def health_address(self) -> str | None:
        """The `ip:port` the health endpoints are served on, or `None`
        unless `health_listen` was given."""
        ...

    def registry_loaded(self) -> None:
        """Says the worker is set up to take work, so `/readyz` can answer 200."""
        ...

    def submit(self, submission: SubmitArguments) -> str:
        """Records a new task. Raises `ValueError` if the submission's kind
        and key disagree, or its `reconnect_timeout_ms` is 0, and
        `BackpressureError` past the hard memory limit."""

    async def claim_pending(self, limit: int) -> list[Claim]:
        ...

    async def next_events(self) -> list[Event]:
        ...

    def report_started(self, task_run_id: str) -> None:
        ...

    def cancel(self, task_id: str) -> CancelOutcome | None:
        """Cancels a task, and says how that ended. `None`: the cancel was
        sent, and the leader decides."""

    def report_failure(
        self, task_run_id: str, failure_kind: str, failure_message: str = ""
    ) -> bool | None:
        """Reports that a claimed run failed, started or not, with the error's
        type and message; whether a retry is now queued. The message is stored
        in the task's replicated record, so it can contain task data. `None`
        when the leader decides elsewhere."""

    def report_lost(self, task_run_id: str) -> bool | None:
        """Reports that a claimed run was lost with the process that ran it;
        whether a new attempt now waits. Raises `RuntimeError` if the run is
        not this worker's to lose. `None` when the leader decides elsewhere."""

    def complete(
        self,
        task_run_id: str,
        result_digest: bytes | None = None,
        continuation: Any = None,
        *,
        result: bytes | None = None,
    ) -> Certification | None:
        """Reports that a running run succeeded; its certification. `None`
        when the leader decides elsewhere. Give the `result_digest`, or, for
        a task that returned a step, the `continuation` tree
        `_native.describe_step` encodes, not both. `result`, on a networked
        worker, is delivered to the task's client before the report."""

    def task_run_state(self, task_run_id: str) -> Any:
        """The state of a run this runtime decides, or None. Only a runtime
        that certifies locally (`certifies_locally`) has it, and a session
        calls it only on such a runtime. Raises `RuntimeError` once the
        runtime has shut down."""

    async def task_record(self, task_id: str) -> Any:
        """The newest record of the task the runtime reaches, or None."""

    def complete_compaction(self, task_run_id: str, folded: bytes) -> bool | None:
        """Reports the fold of a compaction run; whether the leader applied
        it. `None` when the leader decides elsewhere."""

    def submit_composite(self, tree: Any) -> str:
        """Records the composite `tree` describes, as `_native.describe_step`
        encodes it, and returns its id. Before this worker leads the
        composite is queued, as a task is. The runtime reports its end as an
        `EventKind.COMPOSITE_FINISHED` event. Raises `ValueError` for a step
        the leader would not accept or one that takes an input, and
        `BackpressureError` past the hard memory limit."""

    def cancel_composite(self, composite_id: str) -> CancelOutcome | None:
        """Cancels a composite and every member it has started; one still
        queued for want of a leader is dropped. Raises `RuntimeError` when this
        worker is not the leader and the composite is not queued. `None`: the
        cancel was sent, and the leader decides."""

    async def composite_status(
        self, composite_id: str
    ) -> tuple[str, int, int, int, int, int, int] | None:
        """A composite's state, the stage it is on, and the number of its
        members pending, running, succeeded, failed and cancelled; `None` when the
        runtime holds no such composite. Awaited, since a networked runtime
        asks its shard."""

    async def store_result(self, task_id: str, data: bytes) -> bytes | None:
        """Stores `data` as the blobs of a task's result and gives the digest
        to certify it by; `None` if a networked shard could not take it in time,
        in which case the run is to be reported lost. Raises
        `BackpressureError` past the hard memory limit."""

    async def store_composite_payload(self, composite_id: str, data: bytes) -> Any:
        """Stores `data` as blobs the composite owns and gives its opaque
        manifest, which goes into a tree as `("manifest", manifest)`. Raises
        `BackpressureError` past the hard memory limit."""

    async def composite_results(self, composite_id: str) -> Any:
        """A composite's results in order: `("list", [...])` of `("value",
        producer_definition_id, bytes)`, `("failure", kind, message, task_id)` and
        nested lists. Raises `RuntimeError` when a result is no longer held."""

    async def resolve_expansion(self, task_run_id: str) -> Any:
        """The list the expansion run `task_run_id` is to split, as
        `composite_results` has a list: `("value", producer_definition_id,
        bytes)` of the list one task returned, or a `("list", [...])`.
        Raises `RuntimeError` when the run is no expansion or the list is no
        longer held."""

    async def resolve_input(self, task_run_id: str) -> Any:
        """What a composite member's run is given besides its claim, as
        `(fields, input)`: the `(name, value)` pairs bound into its input, and
        what it takes from before it as `composite_results` has it,
        `("input", bytes)` for its own whole input stored as blobs, or `None`
        when the claim's `serialized_input` is the whole input."""
