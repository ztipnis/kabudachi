# Peer-to-Peer Python Task Queue: Project Handoff Brief

**Status:** Architecture proposal for peer review and implementation planning  
**Scope:** Python-first production task queue with a compiled native coordination core  
**Target audience:** distributed-systems engineers, Python runtime engineers, platform/SRE engineers, reviewers evaluating correctness and product fit  
**Document date:** September 2026

---

## 1. Executive summary

This project proposes a production-ready task queue for Python that preserves the ergonomics developers expect from Celery-class systems while replacing the broker-centric execution model with a peer-oriented worker cluster.

The central product thesis is straightforward:

> A Python task queue should let developers write normal typed functions, submit them with almost no ceremony, scale worker fleets aggressively, run tasks from milliseconds to hours, survive ordinary worker and leader churn, and understand exactly what happened to each execution attempt—without making Redis, RabbitMQ, or another broker the authoritative runtime substrate.

The system is intentionally not a workflow engine. A task is a unit of work, not a durable program. It may run for a long time, but the framework does not checkpoint arbitrary in-task progress or replay code from an event history. When a retriable task loses its worker, a new execution attempt starts from the beginning. When a non-retriable task may have performed an irreversible side effect and its worker disappears, the system explicitly reports uncertainty rather than pretending the task simply failed.

The system consists of two major layers:

1. **Python runtime and developer API**
   - task decorators and task registry;
   - serializer/type adapters;
   - local callable handles;
   - subprocess execution and asyncio integration;
   - `flow`, `group`, `map`, `reduce`, and binding;
   - callbacks and lifecycle hooks;
   - application-level configuration.

2. **Compiled native core**
   - peer networking and DHT;
   - shard membership and ring awareness;
   - worker-to-leader heartbeats;
   - leader election and reconciliation;
   - reliable lifecycle/control messaging;
   - task claim arbitration;
   - external coordination/fencing integration;
   - built-in metrics and tracing.

Rust is the leading implementation candidate for the native layer, but the architecture is deliberately language-agnostic at the protocol boundary. The specification requires an FFI-capable compiled implementation, not Rust specifically.

The DHT is used for peer registration/discovery inside the live cluster, immutable Task dissemination, TaskRun snapshots, and non-authoritative status reads. It is not the scheduler's source of truth. One worker per shard is elected leader and acts as the current scheduling and lifecycle authority. Workers discover pending work and request claims; the leader accepts or rejects those claims. This keeps task data decentralized while still serializing the small set of operations that genuinely require ordering.

An external `CoordinationAuthority` exists, with Redis as the default provider. This is intentionally not a broker. It serves as a cold-start directory, leader/shard discovery accelerator, task-to-shard cache, and catastrophic quorum-loss fencing authority. A healthy shard continues claiming, running, completing, and certifying work without Redis on the hot path. If Redis is briefly saturated, cleared, or unavailable, live shards continue. An extended authority outage eventually forces conservative self-fencing because safe automatic recovery from a network partition mathematically requires an external witness.

The initial scalability target is approximately **1,000 workers per elected leader/shard**. Larger fleets use worker-pool sharding. Sharding is expected to remain mostly invisible to normal task code: a TaskHandle remembers its shard, and clients maintain a live shard map. If catastrophic failure causes independent replacement shards to appear, they use new shard IDs and automatically converge when connectivity returns and the configured target shard count is exceeded.

Observability is built in rather than delegated to a separate monitoring ecosystem. Prometheus-compatible metrics, OpenTelemetry trace propagation, health/readiness endpoints, reference dashboards, lifecycle events, and an extension/plugin interface are all part of the baseline design. The framework should expose broad raw scheduler metrics sufficient for reliable autoscaling without requiring user instrumentation, while still exposing the built-in Prometheus registry so tasks can publish application-specific metrics.

The design deliberately occupies a niche between conventional job queues and workflow engines:

- simpler and more Python-native than a workflow engine;
- more explicit about execution attempts, worker loss, versioning, and long-running tasks than conventional queues;
- more internally complex than Redis-backed queues;
- less dependent on external infrastructure during normal execution.

---

## 2. Project goals

### 2.1 Primary goals

The implementation should achieve the following.

**Celery-class base ergonomics.** The normal case should be extremely small:

```python
from kabudachi import task

@task
async def transform(req: TransformRequest) -> TransformResult:
    ...
```

Distributed execution is the default meaning of calling a decorated task:

```python
result = await transform(req)
```

There is no `.delay()` requirement because a decorated task is no longer pretending to be an ordinary local function.

**Strong typed-data defaults.** Protocol Buffers are the preferred and default data model. The default task version is `0`. Pydantic and dataclass/JSON serializers should be optional backends, not mandatory dependencies. Third parties may register custom serializer backends.

**Explicit execution semantics.** A submitted logical Task and an execution attempt are different objects. Retries create new TaskRuns. Worker loss is represented explicitly. Non-retriable ambiguous operations become `ORPHANED`/unknown rather than silently replaying.

**Graceful Kubernetes/container churn.** Worker joins should be cheap and should not trigger re-election. Graceful shutdown should stop claims, self-remove the worker from the electorate, hand off DHT responsibility, and finish or cancel work according to task semantics. Abrupt worker loss should lead to well-defined TaskRun transitions.

**Long-running task support.** A task may run for hours without relying on a broker visibility timeout. Liveness is maintained through worker-to-leader heartbeats. Long-running tasks restart as units if retriable; checkpointing is deliberately outside scope.

**External-service resilience.** Redis or another coordination service should not be on the normal claim/execution/result path. Ordinary leader election should work peer-to-peer when quorum exists. External coordination is reserved for bootstrap/discovery and catastrophic fencing.

**Strong observability.** Prometheus and OpenTelemetry should be first-class. Built-in metrics must be broad enough to support reliable autoscaling without requiring every application to add custom instrumentation.

**Functional-style composition.** Retriable work should be encouraged toward small, typed, mostly side-effect-free units. `flow`, `group`, `task.map`, `task.reduce`, and `.bind()` should make that decomposition practical.

### 2.2 Secondary goals

- easy local development;
- easy packaging as a normal Python package;
- custom observability plugins without requiring plugins for simple lifecycle hooks;
- optional medium-term result retention;
- optional disaster recovery for unfinished durable Tasks;
- logical queue/routing support without an application-instance abstraction;
- compatibility with synchronous and asynchronous Python task functions.

### 2.3 Non-goals

The following are intentionally outside the initial product boundary.

**Not a durable workflow engine.** No deterministic workflow replay, no event-sourced Python program execution, and no generic intra-task checkpointing.

**No exactly-once external side effects.** The scheduler can guarantee one authoritative TaskRun result at a time, but it cannot undo an HTTP POST or database commit performed by an execution that later loses authority.

**No 100,000-worker single-leader promise.** The design should be efficient, but the initial operational target is around 1,000 workers per leader. Larger installations shard.

**No mandatory Kubernetes dependencies.** Kubernetes-native discovery/fencing can be an excellent optional provider, but the default system should be understandable and deployable by developers who know Python and Redis.

**No app-centric API.** There is no requirement to create arbitrary Celery-like application objects simply to register tasks.

---

## 3. Developer-facing programming model

### 3.1 Global/library configuration instead of application objects

Configuration belongs to the library/runtime process.

Conceptually:

```python
import kabudachi

kabudachi.configure(
    result_ttl="7d",
    worker_processes=4,
    concurrency=16,
)
```

Environment variables, ordinary Python configuration, or tools such as `python-decouple` can feed the same configuration layer. The framework does not need to own a large configuration-file subsystem.

The policy hierarchy is consistent everywhere:

```text
framework default
    <
application/process configuration
    <
explicit task-level override
```

An internal `UNSET` sentinel must be distinct from `None`. `None` can be an intentional override:

```python
@task(result_ttl=None)
def transient_result(...):
    ...
```

That task disables a process-wide result-retention policy.

### 3.2 Task decorators

There are three semantic decorators.

```python
@task
@ephemeral_task
@coalescing_task
```

`@task` is the normal default and should cover most usage.

Its baseline semantics are:

- durable/recoverable logical Task;
- Protobuf serializer/protocol;
- version `0`;
- automatic retry count `0`;
- execution is considered safe to restart after infrastructure loss;
- ordinary Python exception fails the task unless retry is explicitly requested/configured;
- result persistence follows inherited application policy.

`@ephemeral_task` is explicitly best effort. Complete cluster loss may lose it.

`@coalescing_task` represents continuously replaced work. A newer pending generation supersedes older pending generations with the same coalescing identity, but never cancels an already running generation. At most one running generation and one newest pending generation should generally exist per coalescing key.

The task class is a semantic choice and therefore requires an explicit decorator. Tunable policies remain inherited.

### 3.3 Calling tasks

Calling a decorated task queues distributed work:

```python
handle = transform(req)
result = await handle
```

The TaskHandle is awaitable and retains task/shard metadata needed for result delivery and later recovery.

There is no `.delay()`.

For explicit local execution:

```python
result = transform.local(req)
```

for a synchronous underlying callable, or:

```python
result = await transform.local(req)
```

for an async callable.

`.local()` performs the minimum type/schema/migration conversion needed to invoke the registered callable correctly, but bypasses DHT, leader scheduling, TaskRun persistence, retries, certification, result backend, callbacks, and worker heartbeat. Its sync/async behavior follows the underlying Python function rather than forcing a synthetic `.sync()` abstraction.

There is deliberately no eager-mode compatibility mechanism in the initial design.

### 3.4 Task binding and composition

A decorated task object is already the composable object. There is no separate "signature" abstraction.

Additional fixed arguments use `.bind()`:

```python
resize_512 = resize.bind(width=512)
```

`functools.partial` may also work where metadata preservation is practical, but `.bind()` is the framework-supported form because it can preserve serializer and typing information reliably.

Sequential composition uses `flow`:

```python
pipeline = flow(
    download,
    decode,
    resize.bind(width=512),
    upload,
)
```

Each stage receives the prior stage's output as its remaining unbound input. The framework should validate type compatibility when the active serializer/type backend exposes enough information to do so.

Parallel composition uses `group`.

Distributed functional operations use `task.map` and `task.reduce`.

```python
results = await transform.map(inputs)
result = await merge.reduce(results)
```

`map` creates independent distributed Tasks per input item; this is not one worker looping locally through the list. The result preserves ordering.

A reducer should normally have a binary shape similar to `(T, T) -> T`, and should be associative for robust distributed reduction. The implementation may construct a deterministic reduction tree.

A common pipeline becomes:

```python
flow(
    load_batch,        # Request -> list[Input]
    transform.map,     # list[Input] -> list[Output]
    merge.reduce,      # list[Output] -> Output
    persist,
)
```

### 3.5 List-valued protobuf input and output

Tasks must accept and return both:

```text
Message
list[Message]
```

for protobuf-backed APIs.

A list-valued result remains the result of one Task/TaskRun. The DHT and optional result backend should represent it as an ordered result manifest, potentially referencing separately content-addressed serialized blobs. This avoids forcing huge batches into one monolithic envelope and makes `flow`/`map`/`reduce` composition natural.

### 3.6 Functional-style guidance

The framework cannot enforce mathematical purity in Python, and purity is not the goal. Instead, it should encourage retriable tasks to approximate a function from typed serialized input to typed serialized output.

Recommended retriable work:

- parsing;
- deterministic transformation;
- aggregation;
- inference;
- compression;
- validation;
- formatting;
- other pure or near-pure computation.

Allowed but operationally dangerous retriable work:

- API GETs;
- database reads;
- object-store reads;
- cache reads.

These are logically repeatable but retries can amplify load or create thundering herds.

Allowed but explicitly high-risk retriable work:

- idempotent writes;
- UPSERTs by deterministic key;
- PUT to stable object IDs;
- other externally repeatable mutations.

Non-retriable tasks are recommended for:

- non-idempotent writes;
- payments/charges;
- one-shot commands;
- sensitive reads whose repetition itself has consequences;
- one-time secrets/tokens;
- metered or audited reads;
- operations where possible duplicate execution is unacceptable.

For complex read/compute/write operations, the framework should recommend decomposition:

```python
flow(
    load_customer,       # retriable read
    compute_result,      # retriable pure processing
    write_result,        # non-retriable side-effect boundary
)
```

This is an important part of the product philosophy. Instead of hiding failure boundaries inside a giant task, the API makes it easy to expose them.

### 3.7 Callbacks

Task callbacks are client-runtime observers, not distributed continuations.

```python
task(req).callback(on_complete)
```

A callback runs after the client runtime receives coordinator certification that the task's result is authoritative. It does not wait for user code to explicitly await or inspect the result.

Callback registrations are held strongly by the client runtime even if the returned TaskHandle is garbage-collected. If the client runtime itself dies, callbacks are lost. If the callback is business-critical across runtime loss, it belongs in a `flow` as a normal Task.

This yields a useful rule:

```text
critical continuation -> flow
non-critical reaction -> callback
```

Metrics emission, best-effort notifications, UI reactions, cache warming, or non-critical Kafka publication are appropriate callback uses.

---

## 4. Core data model

### 4.1 TaskDefinition

A `TaskDefinition` is immutable configuration and code identity created when Python task modules are imported.

It contains:

- stable task name/identity;
- serializer/protocol;
- current version;
- accepted versions and migrations;
- retry policy;
- task semantic class;
- input/output typing metadata;
- local in-process callable handle.

The callable handle is never serialized or transmitted. "Function pointer" in design discussions should be understood loosely: on CPython it may be a `PyObject` representing a function, bound method, callable object, wrapped coroutine function, etc.

### 4.2 Task

A `Task` is an immutable submitted logical invocation.

Representative fields:

```text
task_id
task_definition_id
source_version
serialized_input
not_before
deadline/expiration
logical queue/routing class
coalescing identity if any
trace context
durability metadata
```

Tasks are disseminated through the DHT and, for durable tasks, copied to the disaster-recovery backend according to policy.

A Task survives retries. Retry history belongs to TaskRuns.

### 4.3 TaskRun

A `TaskRun` is one execution attempt.

Immutable lineage fields include:

```text
task_run_id
task_id
attempt_number
parent_task_run_id
created_at
source_version
execution_version
```

Mutable fields include scheduling and worker-observed state such as:

```text
selected_worker
claimed_at
started_at
updated_at
state
failure
result metadata
```

Both worker and leader may mutate legitimate state fields. The implementation must not use naive whole-record "last writer wins" semantics. Mutations should be state-machine constrained, event/field scoped, and reconciliation must be deterministic.

Retries never move a failed TaskRun back to queued. A retry creates a new child TaskRun.

### 4.4 TaskRun states

The detailed implementation may add internal substates, but the externally meaningful lifecycle includes:

```text
SCHEDULED
QUEUED
CLAIMED
RUNNING
SUCCEEDED
FAILED
EXPIRED
SUPERSEDED
CANCELLED
LOST
ORPHANED
```

`LOST` means the prior execution lost authority and may safely be replaced.

`ORPHANED` means a non-retriable execution may have completed irreversible effects, but the scheduler cannot prove success or failure. It must not be automatically replayed.

---

## 5. Logical queues, routing, and shards

The design should distinguish **logical queues** from **shards**.

A logical queue is a developer-facing routing class such as:

```text
default
gpu
io
high_priority
emails
```

A TaskDefinition or application routing policy may assign work to a logical queue. Workers can advertise queue subscriptions and capabilities. The leader only accepts a claim if the requesting worker is eligible for that Task's queue and requirements.

A shard is an internal coordination/failure-domain unit containing a leader, a DHT peer set, and a worker electorate. A deployment may have one or many logical queues inside one shard. A deployment may also have multiple shards for scale.

This distinction matters because the project intentionally rejects the idea that "multiple apps" are necessary simply to create multiple work classes.

The initial target is approximately 1,000 workers per shard. Above that, the system should create or assign workers across multiple shards. A TaskHandle records `shard_id`, and the client maintains a shard directory.

At large scale, task-to-shard mappings can be cached externally. If that cache disappears, clients can rediscover live shards and query them directly.

---

## 6. Runtime architecture

### 6.1 Process shape

Python owns the application process.

A typical worker process starts approximately like this:

```text
Python process starts
    -> imports task modules
    -> decorators register TaskDefinitions
    -> initializes native FFI runtime
    -> Python main thread enters asyncio event loop
    -> native core runs peer/distributed responsibilities
    -> native core schedules execution work back into Python
```

The native runtime may use its own threads/event loop internally. It should not require the user to launch a separate broker process or scheduler daemon.

### 6.2 Python execution pool

Configuration exposes a process count and one bounded concurrency value per subprocess.

Conceptually:

```text
max_processes
max_coroutines_per_process
max_executor_threads_per_process
```

The last two should normally be equal. The initial implementation should impose a sane upper limit in the low tens (approximately 32 is a reasonable starting point) unless explicitly overridden.

For a task subprocess:

- async functions execute on the subprocess asyncio loop;
- synchronous functions execute via `run_in_executor`;
- both consume from the same bounded concurrency budget.

If configured process count is `0`, the main Python runtime may execute task functions itself.

The design does not encourage thousands of coroutines per worker process simply because asyncio technically permits it. Predictable bounded concurrency is more important.

### 6.3 Serializer backend registry

Serializer/data backends are runtime-resolved and extensible.

Potential package extras:

```text
package[protobuf]
package[pydantic]
package[dataclass]
```

The base package should not fail to import merely because an optional serializer dependency is absent.

A conceptual backend interface provides:

```text
available()
encode(value)
decode(payload, target_type)
extract_version(payload, task_definition)
validate(...)
type_compatibility(...)
```

Users may register backends such as `msgspec` or application-specific protocols.

---

## 7. Versioning and migrations

Versioning is explicit at the task-data boundary.

Default:

```text
version = 0
```

For protobuf, version semantics should use the authoritative protobuf contract/task metadata. For JSON-ish serializers, the task explicitly declares where the version lives:

```python
@task(
    protocol="json",
    serializer="pydantic",
    version_path="$.version",
)
```

Original Tasks are immutable. Migration is transient:

```text
stored Task v2
    -> worker supports v5
    -> migrate 2->3->4->5 in memory
    -> validate v5
    -> create/execute TaskRun with source_version=2, execution_version=5
```

The migrated Task is not written back over the original DHT record and is not persisted as a replacement Task.

If a worker cannot execute the current payload because no migration path exists, the Task remains queued/blocked rather than being destroyed immediately. A configurable TTL prevents permanent limbo. Expiry should alert and fail with an explicit version/migration reason.

---

## 8. Task submission, discovery, claim, execution, and result protocol

### 8.1 Submission

The client creates the immutable Task, chooses a shard if sharding is enabled, writes/disseminates the Task into the shard's DHT, and optionally writes the durable Task to the disaster-recovery backend.

The client receives a TaskHandle containing at least:

```text
task_id
shard_id
```

plus local callback and result-delivery state.

### 8.2 Worker-pull claim model

The leader does not push tasks blindly.

Workers inspect DHT-visible pending work and request claims:

```text
worker sees pending Task T
    -> worker -> leader REQUEST_CLAIM(T)
    -> leader examines authoritative state
```

Leader response may include:

```text
ACCEPT
REJECT_ALREADY_SELECTED
REJECT_NOT_READY
REJECT_TASK_UNKNOWN
REJECT_SUPERSEDED
REJECT_EXPIRED
REJECT_QUEUE_MISMATCH
```

Two workers racing for one Task are serialized at the leader:

```text
A -> REQUEST T
B -> REQUEST T

leader:
  A accepted, selected=A
  B rejected
```

The DHT handles data dissemination and discovery. The leader handles ownership serialization.

### 8.3 Heartbeats

Worker liveness is a dedicated worker-to-leader control path, separate from lifecycle pub/sub.

Heartbeats communicate:

- worker identity/incarnation;
- current term/recovery epoch observed;
- available capacity;
- active TaskRun digest/status;
- other local health.

Missing heartbeat eventually causes the leader to question execution ownership.

For retriable tasks:

```text
worker authority lost
    -> old TaskRun LOST
    -> new child TaskRun may be created
```

For non-retriable tasks:

```text
worker authority lost
    -> if completion cannot be proven: ORPHANED
    -> never automatic duplicate execution
```

### 8.4 Reliable lifecycle/control messaging

Control and lifecycle messages require explicit ACKs.

Control examples:

- cancellation;
- shutdown-related control;
- other client-to-worker actions.

Lifecycle examples:

- worker-to-leader TaskRun transitions;
- worker-to-client result payload;
- leader-to-client result certification.

Heartbeat traffic remains separate so event-bus backpressure cannot make healthy workers appear dead.

### 8.5 Result certification

Result transport and result authority are intentionally distinct.

Representative success flow:

```text
1. worker -> client: RESULT(task_run_id, payload)
2. client -> worker: ACK_RESULT
3. worker -> leader: COMPLETE(task_run_id, result_digest)
4. leader validates current authority and accepts/rejects completion
5. leader -> client: CERTIFY(task_run_id, digest)
6. client -> leader: ACK_CERTIFY
```

The client may physically possess result bytes before certification, but it must not treat them as authoritative until the leader certifies the TaskRun.

This solves stale-worker result races at the scheduler level. If an old retriable TaskRun loses authority and later reconnects with "success," the leader rejects completion and no certification arrives.

For a non-retriable TaskRun that survived leader replacement, the new leader may adopt the same TaskRun during reconciliation and certify its already-delivered result.


---

## 9. Disaster recovery and optional external persistence

The system has three different external persistence concerns. They must remain conceptually separate.

### 9.1 CoordinationAuthority

Default: Redis.

Purpose:

- cold shard/worker discovery;
- leader endpoint hints;
- task-to-shard mapping cache;
- recovery epoch/fencing for `NO_QUORUM`;
- initial bootstrap arbitration.

It is not the task queue and is not on the normal execution path.

### 9.2 Disaster-recovery Task store

Durable `@task` Tasks are backed up externally so complete live-cluster/DHT loss does not necessarily destroy unfinished logical work.

Only Tasks need to be backed up for baseline DR. Historical TaskRuns are intentionally not required.

After complete cluster loss:

```text
restore unfinished Task records
    -> start new shard/cluster
    -> create fresh TaskRuns
```

Non-retriable tasks require conservative handling because the old execution may have performed irreversible work.

### 9.3 Medium-term result backend

Result retention is optional.

If enabled, certified results are mirrored into a queryable backend for:

- inspection;
- later result lookup;
- debugging;
- recovery of completed result values;
- operational tooling.

A result-backend failure never changes `SUCCEEDED` back to `FAILED`.

Persistence is attempted only after leader certification. Suggested orthogonal persistence states are:

```text
DISABLED
PENDING
STORED
FAILED
```

`result_ttl` follows normal configuration inheritance and is not required on every task.

### 9.4 External callbacks/event sinks

Kafka, webhooks, custom event buses, and application callbacks may receive certified results or lifecycle events. They remain observers, not authorities.

The core rule is:

> External persistence accelerates discovery and extends retention; it does not become the broker or normal source of execution truth.

---

## 10. Worker and shard state machine

This section is intentionally prescriptive. Exact timer values and transport implementations are configurable, but the state transitions and safety properties should survive implementation language changes.

### 10.1 Worker states

A worker incarnation can occupy the following coordination states.

#### `BOOTSTRAPPING`

The process has loaded task definitions and initialized the native runtime but has not joined a shard.

Actions:

- contact the configured `CoordinationAuthority`;
- obtain worker/shard/leader hints;
- connect to known peers;
- if no shard exists, compete for atomic bootstrap ownership.

#### `JOINING`

The worker has identified a shard and is being incorporated into the live peer/ring view.

It may:

- exchange DHT/ring metadata;
- learn the current leader;
- prepare execution slots.

It should not become an election candidate until the cluster recognizes it as active.

#### `ACTIVE` / `FOLLOWER`

Normal worker state.

It may:

- discover pending Tasks;
- request claims;
- execute TaskRuns;
- send heartbeats to the leader;
- participate in ordinary elections;
- become leader.

#### `LEADER_SUSPECT`

The worker has missed leader control/heartbeat acknowledgements beyond the configured suspicion timeout.

It:

- stops requesting new claims;
- preserves currently running work during the short suspicion window;
- starts cooperative roll call.

#### `ROLL_CALL`

The worker participates in reachable-peer discovery for an ordinary election.

#### `CANDIDATE`

The worker is requesting votes for a new leader term.

#### `LEADER_RECONCILING`

The worker has won an election but cannot schedule new work yet. It reconstructs authoritative TaskRun state from live workers and DHT snapshots.

#### `LEADER`

The worker is the active coordinator.

#### `NO_QUORUM`

The worker cannot prove ordinary peer authority strongly enough to elect a leader safely.

The system continues recovery attempts rather than treating this as a permanent terminal state.

#### `DRAINING`

The worker is shutting down gracefully or migrating during shard convergence.

It:

- stops requesting new work;
- becomes ineligible for leadership;
- no longer casts ordinary votes;
- emits irreversible `SELF_REMOVE`;
- may finish existing work;
- hands off sole DHT replicas.

#### `FENCED`

The worker, including a former leader, has lost permission to make authoritative scheduling/lifecycle decisions.

#### `STOPPED`

Process terminated.

### 10.2 Worker state transition sketch

```text
BOOTSTRAPPING
      |
      v
   JOINING
      |
      v
    ACTIVE ------------------------------+
      |                                  |
      | leader timeout                   | SIGTERM / merge
      v                                  v
LEADER_SUSPECT                        DRAINING
      |                                  |
      v                                  v
  ROLL_CALL                           STOPPED
    /   \
   /     \
leader    quorum exists
returns       |
  |           v
ACTIVE    CANDIDATE
               |
               | wins
               v
      LEADER_RECONCILING
               |
               v
            LEADER
             /  \
     peer loss   SIGTERM
        |          |
        v          v
   NO_QUORUM    DRAINING
        |
        +--> ordinary quorum restored --> ROLL_CALL
        |
        +--> forced recovery -----------> ROLL_CALL
        |
        +--> authority continuity lost -> shard ABANDONED
```

A leader may also become `FENCED` if its low-frequency external recovery lease expires.

---

## 11. Ring-based peer awareness

At shard scale, every worker does not need to maintain heavyweight all-to-all failure-detection sessions.

Workers form a logical ring. Each node tracks several predecessors and successors—an initial default around three in each direction is reasonable.

Example:

```text
        A
    H       B
  G           C
    F       D
        E
```

Worker `D` may track:

```text
predecessors = [C, B, A]
successors   = [E, F, G]
```

The ring provides:

- cheap local liveness awareness;
- roll-call propagation;
- membership gossip;
- a deterministic route around failed neighbors.

It is not the source of election safety. Quorum/term/recovery semantics remain authoritative.

A neighbor heartbeat can include:

```text
worker_id
incarnation_id
shard_id
recovery_epoch
membership_generation
state
highest_term_seen
leader_id_seen
membership_digest
```

When a successor disappears:

```text
try successor[0]
if unavailable:
    try successor[1]
if unavailable:
    try successor[2]
```

If ring knowledge becomes badly fragmented, DHT and `CoordinationAuthority` discovery can repair it.

---

## 12. Leader liveness and ordinary election

### 12.1 Direct worker-to-leader control path

Every active worker maintains a logical heartbeat/control relationship with the leader.

Representative heartbeat:

```text
WORKER_HEARTBEAT {
    worker_id
    incarnation_id
    recovery_epoch_seen
    term_seen
    available_capacity
    active_task_runs_digest
}
```

Representative response:

```text
LEADER_HEARTBEAT_ACK {
    shard_id
    leader_id
    recovery_epoch
    term
    membership_generation
}
```

Use monotonic time for local timeout decisions. Distributed wall-clock synchronization should not be required for ordinary elections.

### 12.2 Detecting leader loss

Pseudocode:

```text
on_leader_ack(ack):
    if ack.recovery_epoch < local.recovery_epoch:
        ignore_stale_ack()
        return

    if ack.term < highest_term_seen:
        ignore_stale_ack()
        return

    highest_term_seen = max(highest_term_seen, ack.term)
    last_leader_contact = monotonic_now()

periodic_worker_tick():
    if state == ACTIVE:
        if monotonic_now() - last_leader_contact > LEADER_SUSPECT_TIMEOUT:
            state = LEADER_SUSPECT
            stop_requesting_new_tasks()
            begin_roll_call()
```

The suspicion timeout should tolerate ordinary jitter. One missed packet does not cause an election.

### 12.3 Draining workers and electorate reduction

A draining worker should not remain an ordinary voter because Kubernetes or an operator may kill it at any time.

On `ACTIVE -> DRAINING`, the worker emits:

```text
SELF_REMOVE {
    worker_id
    incarnation_id
    shard_id
    membership_generation
}
```

`SELF_REMOVE` is irrevocable for that worker incarnation. It means:

> This incarnation permanently withdraws from election participation.

Peers can reduce the effective electorate accordingly.

This is particularly important during scale-down:

```text
100 workers
80 receive SIGTERM
80 SELF_REMOVE
effective electorate becomes 20
quorum becomes 11
```

The remaining cluster does not depend on 51 soon-to-die pods.

A restarted pod has a new incarnation ID and joins from scratch.

### 12.4 Cooperative ring roll call

When the leader is suspected:

```text
ROLL_CALL {
    roll_call_id
    shard_id
    recovery_epoch
    membership_generation
    membership_digest
    highest_term_seen
    initiator_id
}
```

Each active worker:

1. validates shard and recovery epoch;
2. drops duplicate roll-call IDs;
3. appends its observation;
4. records whether it still sees a live leader;
5. forwards to its next reachable ring neighbor.

Pseudocode:

```text
handle_roll_call(call):
    if seen(call.roll_call_id):
        return

    mark_seen(call.roll_call_id)

    call.responses.add({
        worker_id,
        state,
        highest_term_seen,
        current_leader_seen,
        leader_contact_age
    })

    forward_to_next_reachable_ring_neighbor(call)
```

If the existing leader becomes verifiably reachable before the election begins, workers return to `ACTIVE`.

### 12.5 Choosing a candidate

When roll call demonstrates a quorum of the effective electorate, candidate selection should be deterministic so the same observation set naturally converges on the same preferred worker.

Example:

```text
next_term =
    max(term observed during roll call) + 1

candidate_priority =
    hash(shard_id, recovery_epoch, next_term, worker_id)

candidate =
    eligible active worker with highest candidate_priority
```

The hash function is a design detail. The important properties are deterministic selection and avoidance of a fixed preferred host.

### 12.6 Voting

The candidate broadcasts:

```text
VOTE_REQUEST {
    shard_id
    recovery_epoch
    term
    candidate_id
    membership_generation
    membership_digest
    roll_call_digest
}
```

A worker grants at most one vote per term.

Pseudocode:

```text
on_vote_request(req):
    if state != ACTIVE:
        reject("not voter")
        return

    if req.recovery_epoch != local.recovery_epoch:
        reject("wrong recovery epoch")
        return

    if req.term <= highest_term_seen:
        reject("stale term")
        return

    if voted_for(req.term) exists:
        reject("already voted")
        return

    if current_leader_still_valid():
        reject("leader still valid")
        return

    highest_term_seen = req.term
    voted_for[req.term] = req.candidate_id

    grant_vote(req)
```

The candidate wins when:

```text
votes >= floor(effective_electorate / 2) + 1
```

It publishes an election certificate and enters `LEADER_RECONCILING`.

Ordinary election does not need Redis if peer quorum exists.

---

## 13. Leader reconciliation

A newly elected leader must not immediately assign work.

It requests reconciliation reports from live workers:

```text
RECONCILE_REPORT {
    worker_id
    active_runs[]
    locally_completed_uncertified_runs[]
    locally_failed_runs[]
    last_leader_term_seen
    local_dht_generation
}
```

It combines:

```text
live worker reports
+
DHT Task records
+
DHT TaskRun snapshots
```

to rebuild authoritative state.

Representative decisions:

```text
DHT says Run 42 RUNNING on A
A says Run 42 RUNNING
=> adopt same assignment

DHT says Run 42 RUNNING on A
A alive but reports no Run 42
=> retriable: mark LOST and create child run
=> non-retriable: reconcile carefully; if execution uncertainty exists, ORPHANED

DHT says Run 42 RUNNING
A reports SUCCEEDED but uncertified
=> validate run still authoritative
=> accept completion and certify if valid
```

Only after reconciliation:

```text
LEADER_RECONCILING -> LEADER
```

and new claims resume.

---

## 14. `NO_QUORUM` and forced recovery

A peer-only protocol cannot safely allow an arbitrary surviving minority to redefine quorum after a partition. The minority cannot distinguish "other nodes crashed" from "other nodes are alive but unreachable."

Therefore `NO_QUORUM` is recoverable through multiple paths, but it cannot simply invent a smaller electorate.

While `NO_QUORUM`, workers continuously:

- probe ring peers;
- query DHT membership;
- apply valid `SELF_REMOVE` records;
- query the `CoordinationAuthority`;
- alert/emit metrics.

### 14.1 Exit path A: peers return

```text
NO_QUORUM
    -> enough members reachable
    -> ROLL_CALL
    -> ordinary election
```

### 14.2 Exit path B: graceful self-removals shrink electorate

```text
NO_QUORUM
    -> enough SELF_REMOVE records observed
    -> effective quorum shrinks
    -> ROLL_CALL
    -> ordinary election
```

### 14.3 Exit path C: forced reconfiguration through CoordinationAuthority

Redis or another configured authority stores a per-shard `recovery_epoch`.

Every authoritative message carries:

```text
shard_id
recovery_epoch
term
```

Forced recovery atomically advances the recovery epoch and establishes a replacement live membership.

Pseudocode:

```text
attempt_forced_recovery():
    if state != NO_QUORUM:
        return

    authority_view =
        authority.discover_workers(shard_id)

    reachable =
        intersect(
            authority_view,
            locally_reachable_workers()
        )

    if reachable is empty:
        fail_recovery()
        return

    old_epoch =
        authority.read_recovery_epoch(shard_id)

    new_epoch =
        authority.force_reconfigure(
            shard_id=shard_id,
            expected_recovery_epoch=old_epoch,
            replacement_members=reachable
        )

    if compare_and_swap_failed:
        reload_authority_state()
        return

    local.recovery_epoch = new_epoch
    rebuild_ring(reachable)
    establish_new_effective_electorate(reachable)

    state = ROLL_CALL
    elect_leader_normally()
```

A single surviving worker may recover if the forced reconfiguration makes it the only member.

### 14.4 Low-frequency recovery fencing

Safe minority recovery requires an old isolated leader to eventually stop acting authoritative.

Therefore the active leader periodically renews a recovery fence with the external authority.

This is intentionally low-frequency and not part of:

- task claims;
- TaskRun updates;
- normal worker heartbeat;
- result transport;
- ordinary elections.

A brief Redis outage has no effect on normal work.

If the external recovery fence cannot be renewed beyond its safety window:

```text
LEADER -> FENCED
```

A fenced leader stops:

- accepting claims;
- certifying new results;
- creating retries;
- making authoritative lifecycle transitions.

This availability tradeoff is unavoidable if the system also wants safe automatic minority recovery.

---

## 15. Redis failure semantics

Redis is the default coordination authority because it is widely understood and easy to deploy, not because the queue depends on Redis as a broker.

### 15.1 Redis CPU saturated or temporarily slow

Expected behavior:

- existing shards continue;
- workers continue to claim and run Tasks;
- leader heartbeats continue;
- results continue;
- discovery and metadata cache operations may degrade.

### 15.2 Redis temporarily unavailable

Expected behavior:

- live shards continue;
- ordinary peer elections continue if quorum exists;
- already-connected clients continue;
- new cold clients may have slower/failing bootstrap until peer hints are available.

### 15.3 Redis cleared while live shard has quorum

Missing external directory state is reconstructible.

The live leader/peers republish:

- shard membership hints;
- leader hint;
- current recovery metadata;
- task-to-shard cache as observed.

A `FLUSHALL` should not automatically destroy a healthy queue.

### 15.4 Redis unavailable beyond fencing lease

The leader eventually self-fences. This is the conservative price of using Redis as the catastrophic split-brain witness.

### 15.5 `NO_QUORUM` plus unusable/missing authority state

If both peer quorum and authoritative recovery continuity are gone, the system cannot prove continuity safely.

The correct model is **catastrophic shard loss**.

The old shard becomes:

```text
ABANDONED
```

and replacement workers create a new globally unique shard ID rather than pretending the old shard survived.

---

## 16. Catastrophic shard loss and `ShardLostError`

TaskHandles associated with the abandoned shard raise:

```python
ShardLostError
```

Suggested fields:

```text
task_id
old_shard_id
task_class
retriable
known_replacement_shards
```

Example:

```python
try:
    value = await handle
except ShardLostError as exc:
    new_handle = await handle.resubmit(strategy="balanced")
```

### 16.1 Retriable task

Eligible for explicit resubmission.

### 16.2 Non-retriable task

Becomes `ORPHANED`/unknown. Ordinary `.resubmit()` rejects because an irreversible operation may already have happened.

### 16.3 Coalescing task

Usually do not replay the stale generation. The next normal producer invocation should create the newest generation.

### 16.4 `resubmit()` strategies

`resubmit()` chooses placement only. It never manipulates cluster topology.

Supported strategies:

```text
balanced
specific
```

`balanced` chooses among viable replacement shards, preferably weighted by advertised available execution capacity rather than raw worker count.

`specific` accepts an explicit shard from an inspectable client-side shard collection:

```python
await handle.resubmit(
    strategy="specific",
    shard=kabudachi.shards["replacement-shard-id"],
)
```

---

## 17. Replacement shards and automatic convergence

During a severe partition, independent surviving groups may each determine that the old shard is abandoned and bootstrap replacement shards with distinct IDs.

This is acceptable.

They are not two leaders for one shard. They are two independent replacement shards.

For example:

```text
old shard: 7A -- ABANDONED

replacement: B1
replacement: C9
```

A resubmitted Task goes to exactly one replacement shard.

When connectivity returns and:

```text
current_shard_count > target_shard_count
```

the cluster automatically converges topology.

No task API requests a merge.

### 17.1 Convergence algorithm

Conceptually:

```text
detect related replacement shards
    -> deterministically select canonical survivor(s)
    -> losing shard enters MERGING/DRAINING
    -> stop new submissions/claims there
    -> redirect clients
    -> idle workers join survivor immediately
    -> existing running TaskRuns finish where they are
    -> transfer pending Tasks and DHT ownership
    -> retire losing shard after active runs reach terminal state
    -> update external shard/task maps
```

Canonical survivor selection should be stable and deterministic, not dependent on rapidly changing utilization.

Capacity can determine where workers move, but should not decide which shard identity survives.

### 17.2 Client redirect

Clients should understand:

```text
SHARD_REDIRECT(old_shard_id, new_shard_id)
```

A stale client contacting a retired shard gets:

```text
SHARD_MOVED(destination_shard_id)
```

instead of a generic connectivity failure.

---

## 18. Graceful shutdown semantics

### 18.1 Follower shutdown

```text
ACTIVE
    -> SIGTERM
DRAINING
    -> stop new claim requests
    -> emit SELF_REMOVE
    -> finish/cancel running TaskRuns according to semantics
    -> hand off sole DHT replicas
STOPPED
```

### 18.2 Leader shutdown

A leader should proactively hand off instead of waiting for timeout:

```text
LEADER
    -> announce LEADER_DRAINING
    -> stop new claims
    -> become ineligible for future leadership
    -> accelerate roll call/election
    -> remain reachable during reconciliation
    -> successor becomes authoritative
    -> self-remove
    -> hand off DHT responsibility
STOPPED
```

### 18.3 SIGKILL

No graceful guarantees apply.

Surviving peers recover through election/reconciliation. Complete live-cluster loss falls back to DR and/or catastrophic recovery semantics.

---

## 19. Observability and autoscaling

Observability is a core feature, not an integration afterthought.

### 19.1 Built-in Prometheus metrics

The framework exposes a Prometheus/OpenMetrics-compatible endpoint, normally `/metrics`.

Worker/process primitives should include:

```text
worker readiness
worker capacity
slots used / available
running/claimed/completed/failed/lost/orphaned counts
heartbeat latency
event-loop lag
executor utilization
worker churn
```

Shard/leader primitives should include:

```text
queued tasks
scheduled tasks
claimed tasks
running tasks
oldest queued age
queue wait histogram
runtime histogram
enqueue/completion/failure/retry rates
claim request/rejection/latency
total/available worker capacity
leader elections
reconciliation status/duration
```

Derived metrics may include:

```text
estimated queued work
estimated drain time
recommended capacity
```

but these are convenience signals, not canonical truth.

The design principle is:

> The framework exposes scheduler truth; the operator chooses the scaling policy.

No custom instrumentation should be required merely to scale the queue reliably.

### 19.2 Label cardinality

Prometheus labels should remain low-cardinality.

Good examples:

```text
task_definition
logical_queue
shard
state
failure_class
worker_pool
```

Do not expose by default as labels:

```text
task_id
task_run_id
worker_id
customer_id
```

High-cardinality IDs belong in traces/logs.

### 19.3 Shared Prometheus registry

Users can add application-specific metrics to the same built-in endpoint.

Conceptually:

```python
from kabudachi.metrics import Histogram

model_latency = Histogram(
    "myapp_model_latency_seconds",
    "Model execution latency",
)
```

The `kabudachi_` prefix should be reserved for framework metrics.

### 19.4 OpenTelemetry

Submission propagates standard trace context:

```text
traceparent
tracestate
baggage
```

Recommended trace representation:

```text
submit span
  -> TaskRun execution span #1
  -> TaskRun execution span #2 if retried
  -> ...
```

Useful high-cardinality attributes include:

```text
task_id
task_run_id
task_name
version
attempt
parent_run_id
shard
worker_id
retryability
```

`flow`, `group`, `map`, and `reduce` relationships should be represented with parent/child spans and/or links without constructing pathological trace trees for very large maps.

### 19.5 Logging context

Task execution should automatically expose context through Python `contextvars` or equivalent:

```text
task_id
task_run_id
task_name
attempt
shard
worker_id
trace_id
span_id
```

Application logs can then correlate naturally with scheduler traces.

### 19.6 Lifecycle hooks

Simple observability/customization should not require plugins.

Hooks are decorator-only:

```python
@kabudachi.events.task_failed
def observe_failure(event):
    ...

@kabudachi.events.leader_elected
async def observe_leader(event):
    ...
```

No imperative `.connect()`, `.register()`, or `.unregister()` API is necessary.

The hook registry holds weak references. If the final strong Python reference disappears:

```python
del observe_failure
```

the hook naturally unregisters.

Hooks receive immutable event DTOs.

Hook failures never affect Task success or cluster correctness.

### 19.7 Plugin architecture

Plugins are reserved for integrations requiring more than lifecycle callbacks or custom metrics:

- proprietary APM exporters;
- custom telemetry transports;
- persistent observer state;
- high-volume event consumers;
- custom collector behavior;
- explicit startup/shutdown ownership.

Plugins must be buffered, bounded, and failure-isolated.

### 19.8 Reference dashboards

The project should ship Grafana dashboards for:

- cluster/shard health;
- per-task-definition performance;
- autoscaling/capacity;
- reliability/elections/worker loss;
- DHT/coordination health.

### 19.9 Health endpoints

Baseline:

```text
/healthz
/readyz
/metrics
```

Worker readiness should mean the runtime is healthy, task registry loaded, shard membership usable, and the worker can participate in work.

Leader readiness should require reconciliation completion.

---

## 20. Autoscaling model

The system should support multiple valid scaling strategies because no single metric represents every workload.

Potential policies:

**Throughput-oriented jobs**
- queue depth;
- free slots;
- completion rate.

**Latency-sensitive jobs**
- oldest task age;
- p95/p99 queue wait.

**Variable-duration jobs**
- recent runtime distribution;
- optional estimated queued work.

**Long-running jobs**
- active TaskRun count;
- capacity utilization;
- pending count;
- queue age.

A KEDA/HPA/custom controller can consume Prometheus primitives without task-specific instrumentation.

The framework should not require users to adopt a fixed "target drain seconds" formula. Derived recommendations can help but remain advisory.

---

## 21. Comparison against the strongest alternative by category

The project should not be positioned as universally superior to every queue. Its value is the combination of properties.

### 21.1 Developer ergonomics: benchmark against Celery

Celery remains the most relevant Python ergonomics benchmark.

Current Celery uses an application object, `@app.task`, direct calls for local execution, `.delay()`/`.apply_async()` for asynchronous submission, and a signature/Canvas model for composition.

This project intentionally simplifies that surface:

```python
@task
async def x(...):
    ...

value = await x(...)
local = await x.local(...)
```

Composition uses `flow`, `group`, `.bind`, `.map`, and `.reduce` instead of a separate `signature` vocabulary.

Celery has enormous maturity and existing integrations. The project should not underestimate that advantage.

Where this design aims to improve:

- distributed execution is the obvious default call;
- local execution is explicitly named;
- serializer/version contracts are first-class;
- attempt history is explicit;
- worker-loss semantics are not hidden behind broker ACK options;
- composition can be type-aware.

### 21.2 Operational worker scaling: benchmark against Sidekiq Pro

Sidekiq Pro's `super_fetch` is a strong benchmark for boring autoscaling and long jobs. It keeps work represented in Redis while jobs execute and explicitly supports container environments and arbitrarily long jobs.

The proposed system aims for the same operational feeling:

```text
more pods -> more capacity
fewer pods -> drains/self-removals
pod disappears -> explicit execution-state transition
```

The difference is that Redis is not where executing work lives authoritatively. Worker heartbeats and TaskRun state make worker loss explicit.

Sidekiq remains substantially simpler internally. This project takes on more distributed-systems complexity in exchange for reduced hot-path infrastructure dependence and richer execution semantics.

### 21.3 Long-running/reliable execution: benchmark against Temporal Activities

Temporal is the strongest correctness benchmark for long-running work.

Temporal can heartbeat Activity progress and persist checkpoint details so retried Activities resume from a known point.

This project intentionally does not implement checkpointing.

Its contract is simpler:

```text
retriable long unit lost -> restart unit
non-retriable long unit ambiguous -> ORPHANED
```

The advantage is that developers keep a normal task-function programming model rather than adopting durable workflow execution.

Temporal is the better choice when checkpointed continuation, durable multi-step workflows, event history, or deterministic workflow replay are core requirements.

This project is aimed at teams that want stronger task execution semantics without moving to a workflow engine.

### 21.4 Minimal infrastructure: benchmark against RQ

RQ is intentionally simple: Redis-backed jobs and Python workers.

The proposed system cannot beat RQ on internal simplicity. It adds:

- DHT;
- election;
- heartbeats;
- reconciliation;
- fencing;
- sharding;
- result certification.

The intended win is deployment semantics: normal execution does not require every Task transition to traverse Redis, and a temporary Redis problem is not immediately a queue outage.

### 21.5 Typed/versioned deployment evolution: benchmark against Temporal

Temporal has mature worker/deployment versioning because durable workflows demand it.

This project takes a task-centric approach:

```text
immutable payload version
+
explicit migration path
+
source/execution version in TaskRun
```

That is particularly suitable for typed task payloads and rolling application deployments.

### 21.6 Observability: benchmark against Temporal

Temporal has strong task/workflow visibility and worker metrics.

This project should treat observability as equally foundational but more library-native:

- Prometheus built in;
- OTel built in;
- shared metrics registry;
- lifecycle decorators;
- custom plugins;
- raw scheduler metrics intended for Kubernetes autoscaling.

### 21.7 Overall niche

The strongest product fit is:

> Typed Python task workloads from milliseconds to hours, deployed into containerized/autoscaled environments where worker churn is expected, schema/version correctness matters, and teams want stronger execution semantics than conventional queues without adopting a durable workflow engine.

It is likely overkill for a tiny fire-and-forget queue where Redis + RQ is sufficient.

It is intentionally less capable than Temporal for durable process/workflow semantics.

Its differentiation is the middle ground.


---

## 22. Suggested implementation decomposition

The implementation should be split along stable semantic boundaries so the protocol is not accidentally coupled to a specific Rust networking crate or Python serializer.

A useful top-level decomposition is:

```text
python package
    task definitions / decorators
    config resolution
    serializer registry
    Task / TaskHandle API
    flow/group/map/reduce
    callbacks/lifecycle hooks
    Python subprocess execution
    Python logging/context integration
            |
            | FFI
            v
native core
    protocol types
    peer transport
    DHT adapter
    ring membership
    worker/leader state machines
    claim scheduler
    lifecycle/control transport
    result certification
    CoordinationAuthority adapter
    DR/result backend adapters
    Prometheus/OTel instrumentation
```

The protocol/domain layer should sit above concrete transports.

For example, election code should conceptually depend on:

```text
PeerMessenger
MembershipView
Clock
CoordinationAuthority
```

rather than directly on libp2p, Redis, or Tokio socket types.

That will materially improve testability.

### 22.1 Protocol/domain package

This layer should define:

- IDs and epochs;
- Task/TaskRun wire metadata;
- states;
- messages;
- transition validation;
- election certificates;
- roll-call messages;
- shard redirects;
- claim/rejection messages;
- result certification messages.

This is where protobuf schemas are likely most valuable.

It should be possible to test state transitions without networking.

### 22.2 Networking package

Responsibilities:

- peer connections;
- reliable addressed messaging;
- peer identity;
- DHT record/provider operations;
- ring-neighbor communication;
- reconnect/backoff behavior;
- protocol framing.

Do not bury election decisions inside transport callbacks.

### 22.3 Scheduler package

Responsibilities:

- worker-pull claim arbitration;
- queue/routing eligibility;
- active selection state;
- TaskRun creation;
- retry decisions;
- coalescing/supersession;
- expiration;
- leader-side reconciliation.

### 22.4 Worker runtime bridge

Responsibilities:

- map native claim acceptance to Python execution;
- start/cancel Python work;
- report started/failure/result;
- enforce subprocess/concurrency capacity;
- maintain local callable registry;
- translate Python exceptions into TaskRun outcomes.

### 22.5 External backend interfaces

Keep distinct interfaces for:

```text
CoordinationAuthority
DisasterRecoveryStore
ResultStore
```

One Redis implementation may satisfy all three, but the interfaces should not be merged merely because a common backend can implement them.

This prevents the hot coordination semantics from accidentally growing dependencies on result retention.

---

## 23. Candidate Rust ecosystem

The following are implementation candidates, not architectural commitments. Versions and ecosystem status should be re-evaluated when implementation begins.

### 23.1 Python FFI: PyO3

**Candidate:** PyO3  
**Purpose:** Python extension module, Python object/callable handles, invoking Python from Rust.

PyO3 is the obvious first candidate because it supports both exposing Rust modules/classes/functions to Python and calling Python from Rust. Its ownership/GIL-aware types are useful for safely retaining local callable handles.

Current documentation:
- https://pyo3.rs/
- https://pyo3.rs/main/python-from-rust

Design caution:

Do not leak PyO3 types into the protocol/domain model. Keep Python callable handles in the runtime bridge. The distributed system should refer only to stable TaskDefinition IDs.

### 23.2 Python/Rust asyncio bridge: pyo3-async-runtimes

**Candidate:** `pyo3-async-runtimes`

It explicitly bridges Rust futures and Python asyncio and handles the fact that the two languages maintain different runtime/event-loop models.

Current docs:
- https://docs.rs/pyo3-async-runtimes/latest/pyo3_async_runtimes/

This deserves a prototype early. Python's main-thread event-loop expectations, GIL behavior, cancellation propagation, and `contextvars` are likely to be among the trickiest FFI details.

The architecture should not require this crate specifically. A thinner custom bridge may ultimately be preferable if task scheduling semantics need tighter control.

### 23.3 Native async runtime: Tokio

**Candidate:** Tokio

Tokio provides the event loop, scheduler, timers, sockets, and broad Rust async ecosystem expected for this kind of network service.

Current docs:
- https://docs.rs/tokio/latest/tokio/

The native core should probably use a dedicated Tokio runtime rather than attempt to run native networking directly on Python asyncio.

That separation is consistent with the ASGI-inspired model: Python owns Python work; the native component owns distributed-system work; the FFI boundary schedules between them.

### 23.4 Peer-to-peer transport and DHT: rust-libp2p

**Candidate:** `libp2p`, especially its Kademlia behavior.

Current documentation:
- https://docs.rs/libp2p/latest/libp2p/
- https://docs.rs/libp2p/latest/libp2p/kad/

The appeal is not merely Kademlia. libp2p provides composable peer identity, transports, protocols, discovery components, and connection management.

Important caveat from the current Rust libp2p Kademlia documentation: Kademlia does not automatically infer all peer addresses; Identify or another discovery mechanism must be integrated deliberately. That aligns with this design, where Redis/CoordinationAuthority provides cold bootstrap and DHT/ring state provides hot peer awareness.

The project should prototype both:

1. using libp2p as the general peer transport plus Kademlia; and
2. using a narrower custom RPC transport with a DHT library/implementation.

libp2p is capable, but its abstractions may be heavier than necessary if our network protocol is tightly controlled.

### 23.5 Protocol Buffers: prost

**Candidate:** `prost` and `prost-build`

Current docs:
- https://docs.rs/prost/latest/prost/

`prost` generates idiomatic Rust types from proto2/proto3 schemas and integrates naturally with the rest of the Tokio ecosystem.

This is a strong match for:

- Task metadata;
- TaskRun metadata;
- election messages;
- lifecycle/control messages;
- backend manifests.

One caution: `prost` does not itself provide all runtime reflection behavior. If the implementation needs dynamic descriptors for arbitrary user protobuf classes, Python-side protobuf metadata or another descriptor mechanism may still be required.

### 23.6 RPC framing: tonic, custom libp2p protocol, or both

**Candidate:** `tonic`

Current docs:
- https://docs.rs/tonic/latest/tonic/

Tonic is a high-performance async gRPC implementation built around Tokio and protobuf tooling.

Potential uses:

- client-to-worker control APIs;
- worker-to-leader streaming heartbeat/control connections;
- admin/debug endpoints;
- CoordinationAuthority service adapter tests.

However, there is no requirement to use gRPC for peer traffic. If libp2p is chosen, a custom request-response/stream protocol may reduce duplication.

A reasonable experiment is:

```text
libp2p for worker<->worker peer plane
HTTP/gRPC for local/client/admin interfaces
```

but implementation should be decided by profiling and complexity rather than aesthetics.

### 23.7 Redis: redis-rs

**Candidate:** `redis` / redis-rs.

Current docs:
- https://docs.rs/redis/latest/redis/

The current crate supports async operation and low-level Redis command access, which is important because the CoordinationAuthority will likely need:

- atomic compare-and-set behavior;
- TTL records;
- Lua or transactions for fencing/bootstrap;
- Redis/Valkey compatibility.

The Redis adapter should make atomicity assumptions explicit and test them against supported server topologies, especially Redis Cluster if support is claimed.

### 23.8 IDs: uuid

**Candidate:** `uuid`

Current docs:
- https://docs.rs/uuid/latest/uuid/

Possible policy:

- UUIDv4 for opaque random incarnation IDs;
- UUIDv7 for Task/TaskRun IDs if roughly time-sortable IDs are operationally helpful.

Do not depend on ordering semantics unless explicitly standardized in the protocol.

### 23.9 Content hashing: BLAKE3

**Candidate:** `blake3`

Current docs:
- https://docs.rs/blake3/latest/

It is a reasonable candidate for:

- immutable DHT blob IDs;
- serialized payload digests;
- result certification digests;
- membership/roll-call digests.

The protocol should version its digest algorithm instead of assuming BLAKE3 can never change.

### 23.10 Concurrent maps and locking: DashMap / parking_lot

Candidates:

- `dashmap`: https://docs.rs/dashmap/latest/dashmap/
- `parking_lot`: https://docs.rs/parking_lot/latest/parking_lot/

These may help implement hot local registries:

- worker sessions;
- TaskRun views;
- callback-free native state;
- peer maps.

Do not select them prematurely. Actor/message-passing ownership can sometimes simplify correctness more than a highly concurrent shared map.

A good rule is to benchmark the state access pattern first, then choose between:

```text
single-owner task + channels
RwLock<HashMap>
DashMap
```

### 23.11 Errors: thiserror

**Candidate:** `thiserror`

Current docs:
- https://docs.rs/thiserror/latest/thiserror/

Useful for a stable internal error taxonomy and FFI mapping.

The public Python exception hierarchy should be intentional and should not simply stringify arbitrary Rust errors.

Examples likely include:

```text
TaskFailedError
TaskExpiredError
TaskOrphanedError
ShardLostError
UnsafeResubmitError
SerializationError
MigrationError
CoordinationUnavailableError
```

### 23.12 Metrics: prometheus-client

**Candidate:** `prometheus-client`

Current docs:
- https://docs.rs/prometheus-client/latest/prometheus_client/

It implements OpenMetrics-style metric instrumentation and a registry suitable for native framework metrics.

The Python-facing shared registry may require either:

- exposing native registration wrappers;
- bridging to Python `prometheus_client`;
- merging metric exposition from native and Python registries.

This is worth prototyping before freezing the metrics API. A single `/metrics` endpoint is a product requirement; one physical registry implementation is not.

### 23.13 Tracing and OpenTelemetry

Candidates:

- `tracing`
- `opentelemetry`
- `tracing-opentelemetry`

Current docs:
- https://docs.rs/opentelemetry/latest/opentelemetry/
- https://docs.rs/tracing-opentelemetry/latest/tracing_opentelemetry/

`tracing` is a natural fit for native structured spans/events. `tracing-opentelemetry` connects those spans to OTel context/exporters.

The implementation must carefully bridge trace context across:

```text
Python caller
-> native submission
-> wire Task metadata
-> worker native runtime
-> Python task context
```

The goal is one coherent trace, not parallel unrelated Python and Rust traces.

### 23.14 HTTP service endpoints: axum/hyper/tower

**Candidate:** `axum` with its underlying Hyper/Tower ecosystem.

Current docs:
- https://docs.rs/axum/latest/

Potential use:

```text
/healthz
/readyz
/metrics
/scaling (optional)
```

This is infrastructure convenience, not a protocol dependency.

### 23.15 Packaging: maturin

**Candidate:** Maturin

Current docs:
- https://www.maturin.rs/

Maturin supports packaging PyO3-based Rust extensions into Python wheels and is the most obvious first choice for a mixed Rust/Python package.

Packaging should be treated as part of the MVP, not something deferred until after the distributed system is written. If installation is painful, the project loses one of its primary ergonomic goals.

---

## 24. Candidate Python ecosystem

### 24.1 Core runtime

Prefer Python standard-library primitives where possible:

```text
asyncio
concurrent.futures
contextvars
weakref
functools
dataclasses
typing
importlib
```

The Python layer should stay relatively small. Distributed scheduling belongs in the native core.

### 24.2 Protobuf

Use Google's Python protobuf runtime as the default user message representation.

The task decorator should inspect enough message metadata to:

- validate protobuf input/output types;
- serialize deterministically where appropriate;
- identify schema/type names;
- support list-valued message results.

Do not require users to define duplicate Python dataclasses around protobuf messages.

### 24.3 Pydantic

Pydantic is an optional serializer/backend extra.

It should be dynamically imported only when selected.

The design should support Pydantic without making Pydantic semantics leak into the generic task protocol.

### 24.4 Dataclasses

The standard-library dataclass backend can provide a lightweight JSON-oriented alternative. It should still require explicit version extraction semantics if versioning is used.

### 24.5 OpenTelemetry Python

Current OpenTelemetry Python documentation describes traces and metrics as stable, with logs still developing:

- https://opentelemetry.io/docs/languages/python/

Use standard OTel context propagation rather than inventing kabudachi-specific trace headers.

### 24.6 Prometheus Python client

If the public custom-metric API delegates to the Python ecosystem, the Prometheus Python client is the natural candidate:

- https://prometheus.github.io/client_python/

The implementation must still provide one coherent metrics endpoint with native scheduler metrics.

### 24.7 Configuration helpers

No hard dependency is needed.

Applications should be able to call:

```python
kabudachi.configure(...)
```

with values loaded from any source. `python-decouple`, Pydantic settings, environment variables, or bespoke config systems are all valid caller concerns.

---

## 25. Protocol and implementation invariants

Peer review should focus heavily on invariants. If an implementation violates one of these, performance is irrelevant.

### 25.1 Execution invariants

1. A Task is immutable after submission.
2. A retry creates a new TaskRun.
3. A terminal TaskRun never returns to queued/running.
4. At most one TaskRun is currently authoritative for the logical retry lineage.
5. Result bytes are provisional until coordinator certification.
6. Stale TaskRuns cannot become authoritative after replacement.
7. Non-retriable ambiguous execution is never automatically replayed.
8. Callbacks cannot alter Task success/failure state.

### 25.2 Leadership invariants

1. Ordinary election requires quorum of the effective electorate.
2. Draining workers are removed through explicit self-withdrawal.
3. A worker votes at most once per term.
4. Terms only move forward within a recovery epoch.
5. Forced reconfiguration moves to a newer recovery epoch.
6. A stale recovery epoch cannot make authoritative changes.
7. A newly elected leader reconciles before accepting claims.
8. Loss of external recovery fencing eventually self-fences an old leader if forced recovery could otherwise occur.

### 25.3 Shard invariants

1. Catastrophic authority loss never silently reuses the old shard identity.
2. Independent catastrophic replacements use distinct shard IDs.
3. `resubmit()` selects a destination; it never performs topology mutation.
4. Shard convergence preserves running authoritative TaskRuns until completion when possible.
5. A retired shard redirects rather than silently accepting new work.

### 25.4 External-service invariants

1. Redis is not required for each claim.
2. Redis is not required for each TaskRun state change.
3. Redis is not required for primary result delivery.
4. Result backend failures cannot fail an already successful task.
5. DR restoration deals in logical Tasks, not assumptions about old TaskRun ownership.

---

## 26. Testing strategy

This system should not be validated primarily through happy-path integration tests.

### 26.1 Deterministic state-machine tests

The worker, TaskRun, and shard state machines should be modeled as pure state transitions wherever possible.

Generate sequences such as:

```text
leader heartbeat
worker heartbeat
leader timeout
roll call
vote
leader return
worker drain
forced recovery
late result
```

and assert invariants.

Property-based testing is strongly recommended.

### 26.2 Simulated network tests

Build an in-memory fake transport capable of:

- dropping messages;
- duplicating messages;
- reordering messages;
- delaying messages;
- partitioning selected peers;
- reconnecting partitions.

The same election/scheduler logic should run against this transport.

Scenarios should include:

```text
50/50 partition
51/49 partition
leader isolated alone
leader isolated with minority
rapid leader crash/restart
two candidates with crossing vote requests
stale election messages arriving after new epoch
ring fragmentation
SELF_REMOVE messages delayed or duplicated
```

### 26.3 Catastrophic authority tests

Test:

```text
peer quorum lost + Redis healthy
peer quorum lost + Redis slow
peer quorum lost + Redis empty
peer quorum lost + Redis partitioned differently from workers
Redis FLUSHALL with healthy shard
old leader reconnects after forced recovery
```

### 26.4 Task correctness tests

Test retriable and non-retriable tasks separately.

Examples:

```text
worker dies before task start
worker dies mid-retriable task
worker sends result then leader dies before certification
worker sends result after its run was replaced
non-retriable worker survives leader election
non-retriable worker disappears after external side effect
```

### 26.5 Kubernetes/chaos tests

Eventually run real deployment tests with:

- rolling restart;
- aggressive HPA scale up/down;
- node drain;
- pod SIGKILL;
- network policies isolating worker subsets;
- Redis outage;
- Redis flush;
- node/rack/zone-like partition simulation.

### 26.6 FFI stress tests

Test:

- Python interpreter shutdown;
- callback GC behavior;
- weak lifecycle hooks;
- task callable references;
- many concurrent Rust->Python scheduling events;
- cancellation races;
- sync functions in executor;
- async functions and contextvars;
- subprocess crashes.

---

## 27. Suggested implementation phases

### Phase 0: protocol model and simulator

Before a real DHT or Redis integration:

- define protobuf messages;
- implement Task/TaskRun states;
- implement worker election state machine;
- implement fake clock;
- implement fake network;
- implement fake CoordinationAuthority;
- prove key invariants under simulated partitions.

This phase should produce no production networking.

### Phase 1: single-process Python/native runtime

Implement:

- decorators;
- TaskDefinition registry;
- protobuf serialization;
- TaskHandle;
- `.local()`;
- native FFI initialization;
- local TaskRun execution;
- result certification in a one-node shard;
- callbacks/lifecycle hooks.

The one-worker cluster has quorum one and makes debugging the programming model easy.

### Phase 2: multi-worker ordinary election

Implement:

- peer identity;
- direct leader heartbeat;
- ring neighbors;
- roll call;
- voting;
- leader reconciliation;
- worker-pull claim arbitration.

Still avoid sharding.

### Phase 3: DHT task dissemination

Implement:

- immutable Task records;
- TaskRun snapshots;
- content hashes;
- replica/handoff behavior;
- worker discovery of pending Tasks.

### Phase 4: Redis CoordinationAuthority

Implement:

- cold worker bootstrap;
- leader/shard hints;
- task->shard cache;
- recovery epoch;
- low-frequency leader fence;
- forced reconfiguration;
- empty-authority catastrophic reset.

### Phase 5: Python subprocess execution

Implement:

- configured process limit;
- bounded asyncio concurrency;
- sync `run_in_executor`;
- cooperative cancellation;
- SIGTERM/SIGKILL behavior.

### Phase 6: flow/group/map/reduce

Add:

- typed compatibility validation;
- distributed map;
- reduction trees;
- group ordered result collection;
- durable flow continuations.

### Phase 7: observability

Prometheus and tracing should exist earlier for development, but this phase hardens:

- documented metrics;
- OTel propagation;
- dashboards;
- lifecycle event API;
- plugin interface;
- shared custom metric registry;
- autoscaling examples.

### Phase 8: sharding and convergence

Only after one shard is trustworthy:

- 1,000-worker-scale load testing;
- client shard map;
- task shard selection;
- replacement shards;
- `ShardLostError`;
- `.resubmit()`;
- automatic shard convergence.

Trying to implement sharding before single-shard elections are proven would multiply debugging complexity unnecessarily.

---

## 28. Key open design questions for peer review

The architecture is intentionally prescriptive about semantics, but several implementation questions should remain open until prototypes/tests provide evidence.

### 28.1 Exact peer transport

Does libp2p simplify the product enough to justify its abstraction cost, or would a custom TLS/QUIC/gRPC peer protocol be easier to operate?

### 28.2 DHT semantics

What exact DHT replication factor, expiration policy, provider-record behavior, and content manifest structure provide adequate durability without excessive chatter?

The DHT should not quietly become a database.

### 28.3 Vote durability

Ordinary per-term votes are described as ephemeral. Determine what local persistence, if any, is needed to protect against a process crash/restart inside one term. A restart has a new incarnation ID, which removes several classic persisted-vote requirements, but this needs formal review.

### 28.4 Recovery fence timing

The lease must balance:

```text
short lease -> quicker safe forced recovery, more sensitivity to authority outage
long lease  -> better Redis outage tolerance, slower catastrophic recovery
```

Defaults should probably be conservative and configurable.

### 28.5 Result-delivery failure matrix

The worker/client/leader certification protocol should be enumerated as a full failure matrix:

- client dies before ACK;
- leader dies after COMPLETE;
- certification retransmitted;
- worker dies after client receives payload;
- client reconnects through result backend;
- result payload too large for direct delivery.

### 28.6 DHT last-holder shutdown

The graceful "last holder does not exit until replica acknowledged" requirement needs an explicit scope:

- which records count as live;
- how acknowledgements are proven;
- maximum drain behavior;
- operator override.

### 28.7 Map cardinality and backpressure

A million-element `task.map()` cannot simply submit a million Tasks synchronously from one client without bounded fan-out/backpressure.

Map should likely stream task creation while preserving ordered result semantics.

### 28.8 Reduction failure behavior

Define how a reduction tree reacts when one branch fails and retries, and what metadata lets observability reconstruct the reduction topology.

### 28.9 Logical queue fairness

The leader should eventually define fairness/priority behavior across logical queues:

- strict priority;
- weighted fair scheduling;
- starvation protection;
- per-queue capacity restrictions.

This should remain separate from shard selection.

### 28.10 Security

Authentication, peer identity, TLS, authorization, and untrusted task submitters have not been fully specified in this brief.

Before production use, the protocol needs:

- authenticated peers;
- authenticated clients;
- authorization boundaries for queues/tasks;
- replay protection for signed/identified control messages;
- safe serializer policies;
- encrypted transport.

Do not use Python pickle as a production network serializer.

---

## 29. Why this project is worth pursuing

The architecture is complex, but the complexity is concentrated in the implementation rather than pushed onto every application team.

A conventional broker queue is easier to build by composition:

```text
Python task library
+
Redis/RabbitMQ
+
result backend
+
monitoring stack
```

but the user inherits failure semantics that cross those systems.

This project attempts to provide a stronger abstraction:

```text
install Python package
start worker processes
optionally provide Redis for discovery/fencing/DR
```

and make the worker fleet itself the task system.

The payoff is most meaningful when all of the following are true:

- workers scale aggressively;
- tasks are materially important;
- tasks may run for minutes or hours;
- schema evolution matters;
- operators want native metrics;
- teams dislike coupling task correctness to Redis/RabbitMQ availability;
- teams do not want the conceptual overhead of a full workflow engine.

The architecture should not be justified by saying Redis is bad. Redis is deliberately retained as the default coordination witness because it is useful and familiar.

The distinction is:

> Redis may help find and recover the queue; Redis does not *become* the queue.

That is the central operational bet.

---

## 30. Current ecosystem references

These links are included so an implementation team can re-check the suggested ecosystem rather than treating the recommendations above as frozen dependencies.

### Rust/Python implementation candidates

- PyO3: https://pyo3.rs/
- PyO3 calling Python: https://pyo3.rs/main/python-from-rust
- pyo3-async-runtimes: https://docs.rs/pyo3-async-runtimes/latest/pyo3_async_runtimes/
- Maturin: https://www.maturin.rs/
- Tokio: https://docs.rs/tokio/latest/tokio/
- rust-libp2p: https://docs.rs/libp2p/latest/libp2p/
- rust-libp2p Kademlia: https://docs.rs/libp2p/latest/libp2p/kad/
- Prost: https://docs.rs/prost/latest/prost/
- Tonic: https://docs.rs/tonic/latest/tonic/
- redis-rs: https://docs.rs/redis/latest/redis/
- UUID: https://docs.rs/uuid/latest/uuid/
- BLAKE3: https://docs.rs/blake3/latest/
- DashMap: https://docs.rs/dashmap/latest/dashmap/
- parking_lot: https://docs.rs/parking_lot/latest/parking_lot/
- thiserror: https://docs.rs/thiserror/latest/thiserror/
- prometheus-client (Rust): https://docs.rs/prometheus-client/latest/prometheus_client/
- OpenTelemetry Rust: https://docs.rs/opentelemetry/latest/opentelemetry/
- tracing-opentelemetry: https://docs.rs/tracing-opentelemetry/latest/tracing_opentelemetry/
- Axum: https://docs.rs/axum/latest/

### Python observability

- OpenTelemetry Python: https://opentelemetry.io/docs/languages/python/
- Prometheus Python client: https://prometheus.github.io/client_python/

### Comparison/reference systems

- Celery 5.6 user guide: https://docs.celeryq.dev/en/latest/userguide/
- Celery Canvas: https://docs.celeryq.dev/en/main/userguide/canvas.html
- Sidekiq Pro reliability/super_fetch: https://github.com/sidekiq/sidekiq/wiki/Pro-Reliability-Server
- Sidekiq reliability: https://github.com/sidekiq/sidekiq/wiki/Reliability
- Temporal Task Queues: https://docs.temporal.io/task-queue
- Temporal long-running Activity pattern: https://github.com/temporalio/documentation/blob/main/docs/design-patterns/long-running-activity.mdx
- Temporal Worker performance: https://docs.temporal.io/develop/worker-performance
- RQ workers: https://python-rq.org/docs/workers/

---

## 31. Final design summary

The proposed system is a Python-first distributed task runtime whose normal developer experience is intentionally simple:

```python
@task
async def work(req: Request) -> Result:
    ...

result = await work(req)
```

Underneath that small API, the system provides:

- immutable logical Tasks;
- explicit per-attempt TaskRuns;
- protobuf-first typing/versioning;
- transient migrations;
- worker-pull scheduling;
- a peer DHT for task dissemination;
- an elected shard leader for ownership/lifecycle authority;
- ring-based peer roll call;
- explicit worker heartbeat;
- conservative retry and `ORPHANED` semantics;
- certified result delivery;
- Redis-backed default cold discovery/fencing without broker dependence;
- catastrophic `ShardLostError` rather than invented continuity;
- automatic replacement-shard convergence;
- worker-pool sharding above the practical per-leader scale;
- `flow`, `group`, `map`, and `reduce`;
- callbacks for non-critical client-local reactions;
- Prometheus and OpenTelemetry by default;
- decorator lifecycle hooks;
- plugin extensibility;
- optional DR and result retention.

The project succeeds if developers can mostly ignore that complexity while operators gain clearer failure semantics than conventional queues.

The architecture should be challenged aggressively before implementation, especially around election safety, forced recovery, result certification races, and shard convergence. Those are the areas where distributed systems tend to fail in ways that are not visible in ordinary unit tests.

If those pieces hold under simulation and chaos testing, the design occupies a credible and useful niche: a task queue that feels like idiomatic Python but behaves more like purpose-built distributed infrastructure.
