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

**No exactly-once external side effects.** The scheduler can guarantee one authoritative TaskRun result at a time, but it cannot undo an HTTP POST or database commit performed by an execution that later loses authority. The design bounds how long a lost execution and its replacement can overlap (§8.3) but does not fence external writes; side-effecting work belongs in non-retriable tasks or must be made idempotent or transactional by the application.

**Not a task-submission deduplicator or inbox/outbox.** Submitting a task is not idempotent: every submission creates a distinct Task with its own `task_id`, and submitter semantics (deduplication, retry of the submit itself, transactional outbox patterns) belong entirely to the application. The single exception is `@coalescing_task`, where a newer generation supersedes an older pending one with the same key (§3.2.1). That is supersession, not deduplication: a submission made after the earlier generation was claimed still runs, and the application still owns idempotency of side effects.

**Not a lock service or control-loop runtime.** There is no general lease/lock API, and no facility for user-level long-running loops, runners, or singleton services. Locks, control loops, and the jobs they trigger remain application responsibilities; kabudachi executes the tasks those controllers submit.

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

`@ephemeral_task` is explicitly best effort. It is never replayed after worker loss (the caller observes `LOST`), and complete cluster loss may lose it.

`@coalescing_task` represents continuously replaced work. A newer pending generation supersedes older pending generations with the same coalescing key, but never cancels an already running generation. At most one running generation and one newest pending generation exist per coalescing key. The rules are in §3.2.1.

The task class is a semantic choice and therefore requires an explicit decorator. Tunable policies remain inherited.

#### 3.2.1 Coalescing semantics

**Key.** A coalescing key is a flat string. By default every submission of a coalescing task shares one key; a caller may supply a key per submission to partition the work (for example one key per tenant). Supersession requires an exact key match: keys are never hierarchical and there are no exclusion groups. Keys exist only on coalescing tasks; `@task` has no key.

```python
@coalescing_task(merge=merge_ids)
async def refresh_index(ids: IdSet) -> None: ...

refresh_index(ids)                         # default key
refresh_index.options(key=tenant)(ids)     # per-submission key (illustrative spelling)
```

**Superseded handles.** A superseded generation never runs; awaiting its handle raises `TaskSupersededError`, which names the generation that absorbed its payload (`superseded_by`). Only the newest generation's handle carries a result.

**Supersession cut-off.** Only pending (`SCHEDULED`/`QUEUED`) generations can be superseded. Once a generation is `CLAIMED` it is immune. Work that must never be superseded uses a different key.

**Reducer.** When a newer generation supersedes a pending one, an optional pure reducer `(a: T, b: T) -> T`, passed as `merge=`, combines the payloads. The default reducer returns the newest payload (`b`), which is last-wins. The reducer runs on the worker that claims the generation, before the task body, and never inside the leader, because the leader is a native-core role and must not execute user Python.

Until the claiming worker folds them, superseded payloads are retained as an ordered chain (§8.2). A left-fold of a prefix equals the plain left-fold of the whole chain, so compaction needs no associativity from the reducer, only that it is deterministic and side-effect free.

**Bounded retention.** The retained chain is bounded by a memory budget derived from available memory, or from a configured target memory limit. Past a soft threshold the leader schedules an internal compaction run on an eligible worker that folds the oldest payloads into one. Past a hard threshold submission is backpressured. A task may opt in to lossy `drop_oldest` behavior instead; it is never the default.

**One-node subset (Phase 1).** Before compaction exists, the two thresholds are absolute byte limits set with `configure(memory_soft_limit=..., memory_hard_limit=...)` and count the serialized bytes of every non-terminal Task payload, including retained chains. Past the soft limit the scheduler raises a `SlowDown` signal, cleared once usage falls a hysteresis margin below the limit; bulk submitters (`group`, `.map`) pause while it is raised, and a plain `task(x)` call proceeds. Past the hard limit `submit` raises `BackpressureError`, or, for a coalescing task that opted in to `drop_oldest`, drops that key's retained payloads oldest first until the new payload fits. If it still does not fit (the payload alone exceeds the limit, or the key has nothing left to drop), `submit` raises `BackpressureError` anyway, so `drop_oldest` never lets usage exceed the hard limit. Nothing is compacted, so a coalescing key that is never claimed can reach the hard limit from its own superseded payloads. `SlowDown` is a core-level event, so a later phase can carry it over the leader-to-client channel unchanged.

**Running generation and worker loss.** A running generation is never cancelled by a newer submission. If the worker running it is lost, the generation is replayed when it was the newest for its key (at-least-once for the latest state). If a newer pending generation already exists, that generation runs and the lost one stays `LOST` and is not replayed (`SUPERSEDED` is reserved for pending generations). A timeout is different: it is an ordinary task failure and is not requeued, so work does not grow through repeated requeue and timeout; the next submission creates the newest generation.

**Flow lifetime.** When a coalescing task returns a task-like value (§3.4), its running generation covers the entire resulting flow: the key is held until the flow is terminal.

#### 3.2.2 Delivery semantics

| Situation | `@task` retriable | `@task` non-retriable | `@ephemeral_task` | `@coalescing_task` |
|---|---|---|---|---|
| Worker lost while running (after the reconnect timeout, §8.3) | new TaskRun (at-least-once) | `ORPHANED` | `LOST`, not replayed | replayed if newest, else `LOST` and not replayed |
| Task subprocess dies, worker alive | as worker lost, within one heartbeat interval | `ORPHANED` | `LOST`, not replayed | as worker lost |
| Timeout | `FAILED`, retried per retry policy (in-process mode: retry deferred until the abandoned body exits, §6.2) | `FAILED`; hard-kill state is open (§28.13) | `FAILED` | `FAILED`, no special requeue |
| Leader change | run continues, adopted at reconciliation | adopted | adopted | adopted |
| Catastrophic shard loss | `ShardLostError`, may `resubmit()` | `ORPHANED` | lost | not replayed; next submission creates the newest (§16.3) |

### 3.3 Calling tasks

Calling a decorated task queues distributed work:

```python
handle = transform(req)
result = await handle
```

The TaskHandle is awaitable and retains task/shard metadata needed for result delivery and later recovery.

Submission is not idempotent: every call creates a distinct Task with its own `task_id` (§2.3). Applications that need deduplication implement it themselves; `@coalescing_task` is the only kind that supersedes earlier submissions, and it does so by key (§3.2.1).

There is no `.delay()`. Delayed submission, timeouts, and expiry are covered in §3.8.

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

Each stage receives the prior stage's output as its remaining unbound input. Awaiting a flow gives the list of every stage's result in stage order (the last entry is the final output). `.bind(value)` fixes a task's whole input, so the bound task is called with no argument and, as a flow stage, ignores the output before it; `.bind(field=value)` sets scalar fields on the protobuf message the task is called with, or the prior stage's output. Stages are submitted one at a time by the client runtime once their predecessor has finished; an implicit flow, whose continuation is committed atomically with certification, comes with the continuation rules below. The framework should validate type compatibility when the active serializer/type backend exposes enough information to do so.

Parallel composition uses `group`. A group has an explicit failure policy:

```python
group(a, b, c, on_error="fail_fast")     # default
group(a, b, c, on_error="collect_all")
```

The policies follow `asyncio.gather`:

Every member of a group gets the same input (a fully bound member ignores it), and awaiting the group gives the list of the members' results in member order. Cancelling a group cancels every member that has not finished. `fail_fast` follows `asyncio.gather` and leaves the other members running after the first failure; the following stage never starts.

- `fail_fast` (default): awaiting the group raises the first failure and the group is terminal `FAILED`. This matches the least-surprising behavior and Celery's chord semantics, where a failed header task does not run the chord body.
- `collect_all`: like `gather(return_exceptions=True)`, awaiting returns the full array in which each entry is either a step's value or its failure.

A continuation after a group is simply the next stage of a `flow`, for example `flow(group(a, b, c), cb)`; there is no separate `.then()` form. Under `fail_fast`, a failed group is terminal `FAILED`, the flow fails with it, and the following stage never starts. Under `collect_all` the following stage runs and receives the ordered array of per-member values and failures. The failures in that array reach the stage as input, so the stage's serializer has to be able to encode them; the protobuf serializer cannot, and a stage after a `collect_all` group then fails with a `SerializationError` unless it uses a serializer that can. That stage is an ordinary task and keeps what it was declared with: its own retry policy, timeout, queue and class (a `@task` stage is retried per its `retries`, an `@ephemeral_task` stage stays best-effort), and the group or flow imposes none of these on it (§3.4 "Independent tasks"). It runs as its own leader-certified TaskRun exactly once per group instance, across worker loss, leader change, and message reordering. "Exactly once" describes the certified completion run; side effects inside any TaskRun keep the at-least-once semantics of §3.6.

Distributed functional operations use `task.map` and `task.reduce`.

```python
results = await transform.map(inputs)
result = await merge.reduce(results)                          # seedless: (T, T) -> T
report = await add_to_report.reduce(results, initial=Report())  # seeded:   (T, P) -> P
```

`map` creates independent distributed Tasks per input item; this is not one worker looping locally through the list. The result preserves ordering. It is a `group` of one fully bound task per item (so it takes the group's `on_error` policy, `task.map(items, on_error=...)`), every item is checked against the task's serializer before any is submitted, and mapping an empty list returns `[]`. `task.map` is also a `flow` stage that takes the list the stage before it returns. Like any group, `map` submits its tasks one at a time and pauses while the scheduler's `SlowDown` signal is raised (memory use past `memory_soft_limit`, §3.2.1); a plain `task(x)` call is not paused, and past `memory_hard_limit` any submission raises `BackpressureError`.

`reduce` approximates the semantics of JavaScript's `Array.prototype.reduce`: a left fold over the items, in input order, with a reducer that takes the current item and the result so far.

```python
ThisType = TypeVar("ThisType")  # type of each item of the input list
PrevType = TypeVar("PrevType")  # type of the running result
ReducerFunctionType = Callable[[ThisType, PrevType], PrevType]

# The reducer is the task the method is called on. Seedless is (T, T) -> T, where
# PrevType is ThisType; PrevType differs from ThisType only when `initial` is given,
# and is then the type of `initial`.
def reduce(
    self: ReducerFunctionType,  # the reducer task the method is called on
    items: list[ThisType],
    initial: PrevType | Unset = UNSET,
) -> PrevType: ...
```

- **Seeded.** With `initial`, the running result starts as `initial` and every item is folded in: `previous = reducer(item, previous)`. `PrevType` is inferred from `initial` and may differ from `ThisType`. An empty list returns `initial` without calling the reducer.
- **Seedless.** Without `initial`, `PrevType` is `ThisType`: the first item is the starting result and the reducer is first called with the second item. A single item is returned as it is, without calling the reducer. An empty list with no `initial` fails the reduce with an error, as JavaScript throws a `TypeError`, rather than inventing a value.
- **`None` is a value.** As in JavaScript, passing an initial value counts even when that value is `None`; only leaving `initial` out selects the seedless form (`UNSET`, §3.1).
- **Order and determinism.** Items are folded strictly in input order and the reducer does not have to be associative. Given a deterministic reducer the result is deterministic.
- **Two arguments only.** JavaScript's callback also receives the index and the whole array. A distributed reducer never holds the whole list, so it gets just the item and the result so far.
- **A sequential chain.** Each step needs the previous step's result, so a fold runs as a chain: every step is an independent, retriable, leader-certified TaskRun whose input is the item and the serialized result so far. A step that fails after its retries fails the whole reduce and no later step starts, as an exception thrown by the callback ends a JavaScript reduce. Only a seedless reducer explicitly declared associative (illustrative spelling `associative=True` on its decorator) may be evaluated as a deterministic tree instead; that is an optimization that must give the same result as the chain.
- **Not the coalescing reducer.** The `merge=` reducer of a coalescing task (§3.2.1) is a simpler seedless fold over superseded payloads whose arguments are (older, newer), the opposite order to the (item, previous) order above.

A common pipeline becomes:

```python
flow(
    load_batch,        # Request -> list[Input]
    transform.map,     # list[Input] -> list[Output]
    merge.reduce,      # list[Output] -> Output
    persist,
)
```

#### Returning a task (implicit flow)

A task may return another task-like value: a bound task, a `flow`, or a `group`. Kabudachi then schedules it as a continuation of the returning task, exactly as if the caller had written an explicit `flow`. This is an *implicit flow* and reuses the explicit-flow machinery; it is not a separate model.

```python
@task
async def plan(req: Request) -> Flow:
    return flow(fetch.bind(req.id), transform.map, persist)
```

Rules:

- **Tasks are registered at package scope.** A returned value may only reference registered tasks (and bound forms of them). Lambdas and closures cannot be serialized across the FFI boundary, and workers can only execute tasks they know.
- **Atomic continuation.** The returned value is committed together with the returning task's certified completion (§8.5); the digest that certifies the run is the digest of the returned step's description, and the continuation starts only once the leader has certified it. The returning task is declared with the step type as its return annotation (`-> Flow`, `-> Group` or `-> BoundTask`), and the step must not need an input (bind its first stage). The task is not over until its continuation is, so its coalescing key stays held (§3.2.1) and its handle resolves only then, to the continuation's results (a returned bound task gives a list of one). A lost or uncertified TaskRun therefore leaves no continuation behind, and a failing continuation never re-runs the returning task. The flow's state becomes `FAILED` and the caller may resubmit.
- **Sequential stages.** A continuation stage starts only after its predecessor is terminal, so cancelling a flow targets the current stage; later stages never start. Cascading cancellation exists only for the members of a `group`.
- **Independent tasks.** Each stage is an independent task with its own timeout, retry policy, and queue; nothing is inherited from the returning task. There is no flow-level deadline. If a stage has no timeout or a very long one, it holds any coalescing key of the flow until the flow is terminal (§3.2.1); this is a documented risk, not something the framework works around.
- **Handle shape.** The handle of the original submission resolves to a flow handle whose result is an array with one entry per task or step, in step order. A step that itself returns a flow or group has its slot replaced by a nested array of that sub-flow's results, so nesting follows flow structure. Failures follow the group policy above.
- **Orphans.** A flow with a non-retriable stage that ends `ORPHANED` is itself terminal `ORPHANED`.

None of this adds workflow-engine semantics: there is no deterministic replay, no branching DSL, and no in-flow timers or signals (§2.3).

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

A callback runs after the client runtime receives coordinator certification that the task's result is authoritative, and is called with the result; it does not wait for user code to explicitly await or inspect the result. It runs only for a task that succeeded, may be async (a synchronous one runs off the event loop), and one that raises is logged by the error's type and changes nothing: not the task's outcome, not other callbacks (§25.1.8). `run()` waits for callbacks it has started before it returns.

Callback registrations are held strongly by the client runtime even if the returned TaskHandle is garbage-collected. If the client runtime itself dies, callbacks are lost. If the callback is business-critical across runtime loss, it belongs in a `flow` as a normal Task.

This yields a useful rule:

```text
critical continuation -> flow
non-critical reaction -> callback
```

Metrics emission, best-effort notifications, UI reactions, cache warming, or non-critical Kafka publication are appropriate callback uses.


### 3.8 Delayed submission, timeouts, expiry, and scheduling

**Delayed submission.** A submission may carry a relative `delay` or an absolute `eta` (spelled `transform.options(delay=timedelta(minutes=5))(req)`, or `eta=` with an aware datetime). This sets the Task's `not_before`; the Task is `SCHEDULED` until then and only then becomes `QUEUED`. Neither form guarantees a start time, only that the Task is not started earlier.

**Timeouts.** A task declares a `timeout` on its definition. Timeouts escalate softly to hard: at the soft limit the running task receives a cooperative cancellation signal; if it has not stopped by the hard limit, its subprocess is killed (in the in-process mode of §6.2, where nothing can be killed, the TaskRun is marked `FAILED` and abandoned). Timeouts are defined per task at package scope; child tasks of a flow are independent and never inherit them (§3.4). Coalescing tasks use the same knobs with no special requeue behavior (§3.2.1).

**Expiry.** A pending Task may declare `expires` (a duration or an aware datetime, in `.options(...)` like `delay`); if it has not started by then it becomes `EXPIRED` and its handle raises `TaskExpiredError` rather than running late.

**Periodic scheduling.** Cron-style scheduling is not part of the scheduler core but is in scope as a first-party plugin, with Celery beat as the parity baseline (§19.7):

- it runs as a dedicated scheduler process (a client that runs the plugin) and is assumed to be a singleton; running exactly one is the deployer's responsibility, with the same single-point-of-failure profile as Celery beat;
- schedules use standard cron expressions with IANA time zones and are registered in code;
- missed ticks are skipped (no backfill) and v1 keeps no durable schedule state;
- a firing is a plain submission. Because submission is not idempotent (§2.3), a second scheduler process will double-fire. A scheduled task may be declared `@coalescing_task` keyed by schedule so that a still-pending duplicate firing is superseded.

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
coalescing key if any (coalescing tasks only)
flow lineage if any (root task, parent step, step index)
trace context
durability metadata
```

Tasks are disseminated through the DHT and, for durable tasks, copied to the disaster-recovery backend according to policy.

A Task survives retries. Retry history belongs to TaskRuns.

A Task superseded by a newer coalescing generation stays immutable. Its serialized input is retained until the claiming worker folds it into the generation that superseded it (§3.2.1, §8.2).

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

`SUPERSEDED` applies only to a pending coalescing generation replaced by a newer generation with the same key. A generation that is `CLAIMED` or later is never superseded.

A flow (explicit or implicit, §3.4) has a terminal state derived from its stages: `SUCCEEDED` when every stage succeeded, `FAILED` under the group policy, `CANCELLED` when cancelled, and `ORPHANED` if a non-retriable stage ended `ORPHANED`.

### 4.5 Terms

These words recur below and in the runtime. Each keeps one meaning.

- **Task**: one submitted unit of work (§4.2). It has one `task_id` for its whole life, however many attempts it takes.
- **Task run**: one attempt to execute a task (§4.3), with its own `task_run_id` and attempt number. A retry is a new task run of the same task.
- **Task lifecycle**: everything that happens to one task from submission to a settled handle: claimed, running, cancelled, certified, continued or waiting for a retry, and finally settled. It spans every task run of the task, where the states of §4.4 describe a single task run. A worker tracks one lifecycle per task it submitted.
- **Claim**: a worker's request to execute a pending task run, which the leader accepts for one worker only (§8.2). While the claim stands, that task run is not handed to another worker; a retry is a new task run and can be claimed by any worker.
- **Certification**: the leader's word that a task run's result is the authoritative one, given as the digest of the result (§8.5). A result is authoritative only once certified.
- **Continuation**: what a task that returns another task (a step of a flow, §3.4) becomes once its own run is certified: the task is not over, and its coalescing key stays held, until the continuation ends.

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

A TaskDefinition or application routing policy may assign work to a logical queue. Workers advertise queue subscriptions, and the leader only accepts a claim if the requesting worker is subscribed to that Task's queue. There is no separate capability system: a scarce resource such as a GPU, a licensed library, or a heavyweight model is expressed as a queue, and only workers that have it subscribe.

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
- synchronous functions execute in a worker thread through `asgiref.sync_to_async(..., thread_sensitive=False)`, which propagates `contextvars`; the default `thread_sensitive=True` would serialize every sync task on one thread and is never used;
- both consume from the same bounded concurrency budget.

If configured process count is `0`, the main Python runtime may execute task functions itself. In that mode only cooperative (soft) cancellation is enforceable: a synchronous body that ignores cancellation cannot be killed in-process, so at the hard limit the TaskRun is marked `FAILED` and the runtime stops waiting for it. Nothing is killed, so in this mode the outcome is `FAILED`, as in the delivery table (§3.2.2); that is a provisional choice for the in-process mode, and the state after a hard-timeout kill in the subprocess pool stays open (§28.13). An abandoned body of a non-retriable task may still complete irreversible effects, which is the ambiguity `ORPHANED` names in §4.4. The abandoned body may still be running, so a retry, if the retry policy calls for one, is not started until that body has actually exited; a lineage never has two bodies executing at once. The failed TaskRun itself is terminal. When a retry is due, the lineage stays pending until the body exits and the handle does not resolve meanwhile; when none is due (no retries configured, or the last attempt), the lineage is terminal `FAILED` at once. A coalescing generation's key stays occupied until its abandoned body exits, so no second generation of that key starts while a body for it is still running (§25.4.1). Whatever the abandoned body later returns or raises is discarded and can never certify (§25.1.5, §25.1.6). It keeps its concurrency slot until it exits. A body that never exits therefore blocks that lineage's retry and holds its slot indefinitely, and enough of them exhaust the bounded pool: this is a documented limitation of the in-process mode, removed by the subprocess pool's hard kill (Phase 5). The rule limits duplicate side effects from retries but does not fence writes the abandoned body still makes (§2.3).

`kabudachi.run(main=None)` is the synchronous entry point, in the style of `asyncio.run`. It constructs and owns the event loop on the main thread, initializes the native runtime on it, runs `main()` if given (returning its result) and then drains and stops. Draining waits for every task that was called to finish, queued tasks and tasks started by other tasks included; if `main` raised or was interrupted, tasks already running are let finish, tasks that have not started fail with `RunStoppedError`, and the error propagates. A task that waits for another task does not occupy one of the `concurrency` places while it waits, so a task that waits for tasks it called cannot starve them. Before `main` starts, every registered task is checked against the serializers of the process, so a task that cannot work is reported at once; with no `main` it serves as a worker (on the main thread) until SIGINT/SIGTERM, then drains as above and returns; a second signal stops the waiting and raises `KeyboardInterrupt`, abandoning unfinished tasks (a synchronous task still in its thread keeps the interpreter from exiting until it returns). Submitting a task outside `run()` raises `RuntimeNotStartedError`; `.local()` (§3.3) needs no runtime.

Hard timeouts (§3.8) kill the task subprocess. Whether a subprocess is recycled after a configured number of runs, for tasks that hold native memory, is deferred to the Phase 5 design (§28.12).

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

Mixed code versions during a rolling deployment are handled by this same migration schema. A migration may explicitly refuse an incompatible boundary by raising `MigrateRejectError`; the Task then stays queued for a worker that can execute it. Rollout policy beyond this (batch sizes, minimum shard size, election budgets) is left to the application and deployment tooling.

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

Submission is not idempotent (§2.3): each submission creates a new Task.

A handle can report how durably its Task is held, and callers may await a stronger level before treating the submission as accepted:

```text
in_memory            held by the receiving peer
replicated           replicated to k DHT peers
dr_store_written     written to the disaster-recovery store (§9.2)
```

Kabudachi is not a transactional inbox/outbox. An application that must enqueue atomically with a database commit records the intent in its own store and submits after commit, handling repeat submission itself.

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

#### Supersession and payload folding

For coalescing tasks (§3.2.1), the leader also serializes supersession. A newer submission with the same key marks the older pending generation `SUPERSEDED` (a claim for it is answered `REJECT_SUPERSEDED`); a generation that is already `CLAIMED` cannot be superseded.

The leader never executes the reducer. It keeps the superseded Tasks' payloads linked in order; the worker that claims the newest generation folds the chain oldest to newest with the task's reducer before running the task body. If the retained chain exceeds its memory budget, the leader schedules an internal compaction run that a worker executes to fold the oldest payloads into one; past the hard threshold, new submissions are backpressured (or dropped-oldest if the task opted in).

After leader change, reconciliation (§13) rebuilds per-key occupancy, including the lifetime of any implicit flow, so a second running generation for the same key is never admitted.

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

#### Reconnect timeout and worker self-abort

A lost-but-alive worker could otherwise keep performing side effects while its replacement runs. Two timeouts bound this overlap:

```text
heartbeat_timeout     leader stops hearing from a worker
reconnect_timeout     grace period after which a lost TaskRun is considered dead
```

- The leader marks a TaskRun `LOST` when heartbeats stop, but creates a replacement TaskRun only after `reconnect_timeout` elapses.
- Workers monitor their connection to the leader (and client, where relevant) and abort the TaskRun, by cooperative cancellation and then subprocess kill, at or before `reconnect_timeout` if communication is not re-established. The worker's abort deadline is shorter than the leader's replacement deadline by a clock-skew margin, and both use monotonic time.
- If the worker reconnects in time, a non-retriable TaskRun's result is adopted and certified (§8.5); a competing replacement cannot supersede it.

The worst-case time from an abrupt kill (SIGKILL) to the replacement TaskRun starting is `heartbeat_timeout + reconnect_timeout`, plus an election if the leader was lost and the claim round trip. Defaults are on the order of tens of seconds and are configurable per queue and task. Heartbeats run in the native core, independent of the GIL and of any CPU-bound task subprocess, so a long native call cannot make a healthy worker look dead. If a task subprocess dies while its worker lives, the TaskRun is reported `LOST` within one heartbeat interval.

This bounds overlap; it does not fence external writes (§2.3).

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

When a task returns a task-like value (§3.4), the leader commits the continuation atomically with certification of the returning TaskRun. A TaskRun that is not certified therefore leaves no continuation behind.


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

Provider constraints, so that the default Redis provider coexists with other tenants of a shared instance:

- a configurable key prefix and, outside cluster mode, a database number; Redis Cluster supports only database `0`, so cluster mode isolates tenants by key prefix alone and rejects a non-zero database selection;
- never `SCAN` or `KEYS`; every key it reads is addressed by name;
- a documented minimal command set, stating whether Lua scripting or `WATCH`/`MULTI` are required, with a fallback that avoids Lua where feasible;
- tolerance of cluster mode through hash-tagged keys.

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

Usually do not replay the stale generation. The next normal producer invocation should create the newest generation. This concerns catastrophic shard loss only; within a live shard, worker loss replays the newest lost generation (§3.2.1).

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

Termination is soft and then hard, similar to a warm/cold shutdown in Celery. A soft terminate starts the drain above and lets running TaskRuns finish within a configured grace period; when the grace period elapses, running task subprocesses are cancelled cooperatively and then killed, and their TaskRuns follow the worker-loss semantics of §3.2.2.

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

Coalescing and scheduling metrics (superseded count, pending age, running duration, retained-payload memory, compaction runs, backpressure events) are labelled by `task_definition` and `logical_queue` only. There are no per-coalescing-key labels; applications needing per-key metrics emit their own through the shared registry (§19.3).

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

Separately from these observer hooks, the runtime offers execution lifecycle hooks that run inside the task subprocess: worker-process initialization, and per-run before/after. They exist so applications can prepare and clean up per-process state (for example returning database connections after each run). They are registered where tasks are registered, at package scope, so every subprocess that imports the task modules also has them; they are scoped to the queues the worker subscribes to. Framework-specific recipes, such as connection handling for a particular web framework, belong in documentation and cookbooks, not in the architecture.

### 19.7 Plugin architecture

Plugins are reserved for integrations requiring more than lifecycle callbacks or custom metrics:

- proprietary APM exporters;
- custom telemetry transports;
- persistent observer state;
- high-volume event consumers;
- custom collector behavior;
- explicit startup/shutdown ownership;
- task-submitting plugins and processes, such as the first-party cron scheduler (§3.8).

Plugins must be buffered, bounded, and failure-isolated. A task-submitting plugin acts as an ordinary client: it submits tasks through the public API and has no special authority over scheduling.

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
- coalescing/supersession, payload-chain retention, and compaction scheduling (the reducer itself runs on workers);
- reconnect-timeout handling and replacement TaskRun creation;
- flow continuation commit and lineage;
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

**Phase 2 design note:** Phase 2 chose between the two prototype paths above — libp2p as transport (TCP/Noise/Yamux, `identify`), but narrow: three `request_response` behaviours, for election, bootstrap join, and claim arbitration (`net/src/swarm.rs`'s `Behaviour`), with no Kademlia/DHT usage. The `net` crate implements this. This is an implementation decision, not a change to this section's candidate status.

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

**Phase 2 design note:** the peer plane uses the libp2p `request_response` custom-protocol path above, not tonic — no gRPC dependency is planned for peer traffic. The `net` crate implements it. This is an implementation decision, not a change to this section's candidate status.

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

Two packages are required dependencies, each chosen over writing the same thing by hand:

- `asgiref` for the sync/async boundary (`sync_to_async` runs synchronous task bodies on threads; `async_to_sync` backs blocking convenience forms);
- `wrapt` for the object a task decorator returns, which must behave like the function it wraps.

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

Pydantic is an optional serializer/backend extra. It is not a dependency of the core package.

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

Settings are one frozen dataclass, deliberately not a settings framework: there are few settings, and pulling in Pydantic for them would make it a required dependency. Adding a setting is adding a field with its default and a check in `__post_init__`. The framework default is the field's default, `KABUDACHI_<NAME>` in the environment can set it, and `kabudachi.configure(...)` overrides both (§3.1). Values can come from any source the caller likes (`python-decouple`, another settings library, environment variables) and be passed to `configure()`.

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
9. A worker aborts a TaskRun by the reconnect timeout when it cannot reach its leader, and no replacement TaskRun starts before the reconnect timeout has elapsed (§8.3).

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

### 25.4 Coalescing and flow invariants

1. At most one generation per coalescing key is `RUNNING` by authority; any overlap with a replacement is bounded by the reconnect timeout (§8.3).
2. Only pending generations are superseded; a `CLAIMED` generation never is.
3. A superseded pending payload is folded, never silently dropped (except under an explicit `drop_oldest` opt-in).
4. The reducer fold is order-preserving and its result does not depend on chain length or compaction points (except under an explicit `drop_oldest` opt-in).
5. A lost generation that was the newest for its key is replayed; a lost stale generation is not.
6. Per-key occupancy, including an implicit flow's lifetime, is rebuilt at reconciliation without admitting a second running generation.
7. A continuation is committed atomically with the returning TaskRun's certification, and a continuation failure never re-runs the returning task.
8. The stage after a group runs as one certified TaskRun per group instance under any single fault.

### 25.5 External-service invariants

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
- `.callback()` (§3.7);
- automatic retries, `handle.cancel()`, and `@ephemeral_task`;
- coalescing supersession and reducer folding in a one-node shard (§3.2.1);
- delayed submission, timeouts, and expiry (§3.8); only soft timeouts are enforceable in-process (§6.2);
- `.bind()`, `flow`, `group` with its failure policy, implicit flows, and `.map` (which hands off to `group`) in a one-node shard (§3.4);
- memory-budget backpressure: a soft-limit `SlowDown` signal that bulk submitters (`group`/`.map`) honor, and a hard-limit `BackpressureError` or opt-in `drop_oldest` (§3.2.1, one-node subset).

The one-worker cluster has quorum one, runs the real election state machine, and makes debugging the programming model easy.

Deferred out of Phase 1: compaction of retained coalescing chains (Phase 3), observer hooks `kabudachi.events.*` (Phase 7), `.reduce` and type-compatibility validation (Phase 6), migrations (§7), and queue-subscription enforcement (Phase 2).

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

Phase 1 left `NoPeers` and `NoAuthority` (`core/src/single_node.rs`) and the election tick loop in place as single-node placeholders, each with one adapter until this phase. Phase 2 gave peer messaging and the coordination authority their second real adapter and resolved that question, as follows.

`core/src/single_node.rs` is gone. `NoPeers` moved to `bindings/src/local_node.rs`, scoped to the one runtime that actually wants it: the single-process Python runtime, whose instant (zero suspicion timeout) self-election is a deliberate product choice for that runtime, not the generic behaviour of a one-member electorate. `NoAuthority` was removed outright rather than moved: `core::election::WorkerNode` only consults its authority from `attempt_forced_recovery`, which runs only from `WorkerState::NoQuorum`, and a one-member electorate driven by `NoPeers` can never reach `NoQuorum` — so the path is unreachable and `InMemoryAuthority` (`core/src/in_memory_authority.rs`) serves as the type there without ever being called. The generic multi-node case is `net/src/bootstrap.rs`'s `bootstrap_node` cascade (seeds, then the coordination authority, then self-election), where a lone node still waits out whatever suspicion timeout it was configured with.

There is no single election tick loop any more either: `bindings/src/election.rs`'s `run_election` drives the single-process node, and `net/src/driver.rs`'s `run_driver` drives a real-transport node, each on its own tick.

The bootstrap join is one-way in Phase 2. `bootstrap_node` adds the joining worker to its own electorate, but the members that answer its `JOIN_RESPONSE` keep their existing electorate, and nothing admits the joiner into it. After `c` joins `{a, b}`, `c` believes the electorate is `{a, b, c}` while `a` and `b` still believe `{a, b}`, so the nodes disagree about quorum and ring neighbours. This does not yet meet the `JOINING` state's requirement that the cluster recognize a new worker as active. Admitting a joiner needs leader-driven membership propagation (the `membership_generation` and `membership_digest` fields already on the wire are unread), which is new election behaviour and is not assigned to a phase yet. Until then, a shard is safe only at its bootstrap electorate.

The authority step of the bootstrap cascade does not yet produce a working node. `CoordinationAuthority::discover_workers` returns worker IDs without addresses, and `bootstrap_node` does not read the shard's recovery epoch, so a node that bootstraps this way is `Active` in an electorate it cannot reach, at recovery epoch 0. It fails safe (no quorum, so it never leads while other members exist) but does not recover on its own. Phase 4's real `CoordinationAuthority` must return addresses and the recovery epoch; until then, nodes join through seeds.

The one-node window between startup and the worker becoming leader is still not testable, and is still recorded as such in `docs/superpowers/follow-ups.md` ("Test for a signal arriving during startup, before leadership" — the one-node election takes about 20 ms, too short to hit without a flaky test; picked up when startup is slow enough to test, i.e. DHT election). Phase 2's real transport did not change that: it makes multi-node startup slower, but the *one-node* window is exactly the case that has no network to wait on.

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
- sync bodies via the §6.2 `asgiref` mechanism, now inside the task subprocess;
- cooperative cancellation and soft-to-hard timeout escalation;
- execution lifecycle hooks in the task subprocess;
- SIGTERM/SIGKILL behavior.

### Phase 6: flow/group/map/reduce

Phase 1 already delivers `flow`, `group`, implicit flows and `.map` in a one-node shard. Add:

- typed compatibility validation;
- multi-worker distributed map (one-node `.map` is Phase 1);
- `.reduce` (a sequential chain of certified steps; a reduction tree only for a seedless reducer declared associative);
- group ordered result collection across workers and leader changes;
- durable flow continuations.

### Phase 7: observability

Prometheus and tracing should exist earlier for development, but this phase hardens:

- documented metrics;
- OTel propagation;
- dashboards;
- lifecycle event API;
- plugin interface, including task-submitting plugins;
- first-party cron scheduler plugin (§3.8);
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

### 27.1 Acceptance criteria by phase

The coalescing, flow, and failure-detection invariants (§25.1 item 9 and §25.4) become named acceptance criteria. Those that exercise election and reconciliation extend the Phase 0 simulator; the rest are exit criteria of the phase that implements them.

| Phase | Criteria |
|---|---|
| 0 (simulator extension) | 25.1.9 (abort before replacement), 25.4.1 (single running generation by authority), 25.4.2 (pending-only supersession), 25.4.6 (occupancy rebuilt at reconciliation) |
| 1 | 25.4.3 and 25.4.4 (folding, order preservation), 25.4.7 (atomic continuation), 25.4.8 (stage after a group runs once), 25.4.5 in a one-node shard, and the one-node backpressure contract (`SlowDown` past the soft limit, `BackpressureError` past the hard limit, §3.2.1) |
| 2 | 25.4.5 under leader and worker loss, the measured time from worker unreachable (connection loss) to replacement claim (§8.3), and abort-before-replacement under a simulated (connection-loss) partition |
| 3 | Retained-payload chain across DHT replicas and compaction (the one-node soft/hard-limit backpressure contract is a Phase 1 exit criterion) |
| 5 | Hard-timeout subprocess kill; heartbeats unaffected by a CPU-bound task subprocess |

### 27.2 Production-readiness gate

No real workload should adopt kabudachi until the following are closed:

- peer and client authentication, and encrypted transport (§28.10);
- a minimal orchestrator requirements document. The implementation should minimize what it demands of the deployment environment (no mandatory Kubernetes, §2.3) and then define the small set that remains: how peers discover each other, behavior under address churn, reachability between peers, graceful-termination signals, and health endpoints;
- measured submit-to-start latency (§28.11).

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

A million-element `task.map()` cannot simply submit a million Tasks synchronously from one client without bounded fan-out/backpressure. The one-node contract (a `SlowDown` signal that bulk submitters honor, §3.2.1) is the Phase 1 answer; the distributed design remains open.

Map should likely stream task creation while preserving ordered result semantics.

### 28.8 Reduction failure behavior

Define how a reduction reacts when a step fails and retries, and what metadata lets observability reconstruct the chain of steps, or the tree when a declared-associative reducer is evaluated as one. §3.4 fixes the outcome once a step has exhausted its retries: the reduce fails and no later step starts.

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

### 28.11 Submit-to-start latency

No latency target is committed. The path is DHT dissemination, worker pull, and a leader claim round trip. Phase 2 prototype measurements should establish a realistic p50/p99 on a warm idle worker in one healthy shard before any target is stated; a leader push-offer to idle workers is a possible optimization if the pull path is too slow.

### 28.12 Subprocess recycling

Whether task subprocesses can be recycled after a number of runs (for tasks that accumulate native memory), and whether that is per task or per queue, is deferred to the Phase 5 execution-pool design.

### 28.13 State after a hard-timeout kill of a non-retriable task

A non-retriable TaskRun killed at its hard timeout may or may not have performed irreversible work. Whether it becomes `FAILED` or `ORPHANED` needs a decision consistent with §4.4.

### 28.14 Thresholds and margins

Soft and hard fractions of the retained-payload memory budget (§3.2.1; in Phase 1 they are absolute byte limits), and the clock-skew margin between a worker's abort deadline and the leader's replacement deadline (§8.3), are implementation details to fix with measurements.

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
