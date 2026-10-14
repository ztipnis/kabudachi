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
   - shard peer awareness (a gossipsub roll call, a peer book and routing refreshes);
   - worker-to-leader heartbeats;
   - leader election and reconciliation;
   - reliable lifecycle/control messaging;
   - task claim arbitration;
   - external coordination/fencing integration;
   - built-in metrics and tracing.

Rust is the leading implementation candidate for the native layer, but the architecture is deliberately language-agnostic at the protocol boundary. The specification requires an FFI-capable compiled implementation, not Rust specifically.

The DHT is used for peer registration/discovery inside the live cluster, versioned Task records (each holding a Task and its TaskRuns), and non-authoritative status reads. It is not the scheduler's source of truth. One worker per shard is elected leader and acts as the current scheduling and lifecycle authority. Workers discover pending work and request claims; the leader accepts or rejects those claims. This keeps task data decentralized while still serializing the small set of operations that genuinely require ordering.

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
    result_ttl=7 * 24 * 3600,   # seconds
    processes=4,
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

An internal `UNSET` sentinel is distinct from `None`. A task, queue or option that leaves a setting out holds `UNSET` and inherits. `None` is an intentional override:

```python
@task(result_ttl=None)
def transient_result(...):
    ...
```

That task disables a process-wide result-retention policy: it keeps no result, and nothing is mirrored to the result backend (§9.3).

`result_ttl` and `reconnect_timeout` resolve from the task, then its queue's `QueueOptions` (§5), then the process configuration. `retries`, `timeout` and `cancel_grace` resolve from the task, then the process configuration. A reducer (§3.4) takes no `result_ttl` of its own, since its steps are kept with the result of their reduce. The process-wide `result_ttl` is a whole number of seconds; the task and queue forms are a positive `timedelta`, `None` or left out.

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
- input schema version `0` unless the schema declares another (§7);
- automatic retry count `0`;
- execution is considered safe to restart after infrastructure loss;
- ordinary Python exception fails the task unless retry is explicitly requested/configured;
- result persistence follows inherited application policy.

`@ephemeral_task` is explicitly best effort. It is never replayed after worker loss (the caller observes `LOST`), and complete cluster loss may lose it.

`@coalescing_task` represents continuously replaced work. A newer pending generation supersedes older pending generations with the same coalescing key, but never cancels an already running generation. At most one running generation and one newest pending generation exist per coalescing key. The rules are in §3.2.1.

`@task(retriable=False)` declares a task that is not safe to run twice. A run lost while it was running is `ORPHANED`, and its handle raises `TaskOrphanedError`. It cannot be combined with `retries`, and only a plain `@task` can be declared this way.

Task names starting `kabudachi.` are reserved for kabudachi's own runs; declaring one raises `TaskDefinitionError`.

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

**Bounded retention.** The retained chain is bounded by a memory budget derived from available memory, or from a configured target memory limit. Past a soft threshold (per key, or memory-wide) the leader schedules an internal compaction run on a worker that runs compaction, which folds the oldest payloads that fit one claim into one (§8.2). Past a hard threshold submission is backpressured. A task may opt in to lossy `drop_oldest` behavior instead; it is never the default.

**Memory limits.** By default the soft and hard limits are 25% and 50% of the memory the process may use at start: its cgroup limit when one is set, otherwise physical memory. `configure(memory_soft_limit=..., memory_hard_limit=...)` sets byte limits instead. They count the serialized bytes of every non-terminal Task payload, including retained chains. A networked process also counts the blob payloads that move through it (§8.6) and splits its hard limit between those and the blobs it stores for its shard. Past the soft limit the scheduler raises a `SlowDown` signal, cleared once usage falls a hysteresis margin below the limit; bulk submitters (`group`, `.map`, `.reduce`) pause while it is raised, and a plain `task(x)` call proceeds. `SlowDown` reaches every submitter: the leader's answer to a submission carries it, and a networked submitter that sees it asks the leader again at the heartbeat interval until it clears. Past the hard limit `submit` raises `BackpressureError`, or, for a coalescing task that opted in to `drop_oldest`, drops that key's retained payloads oldest first until the new payload fits. If it still does not fit because the key has nothing left to drop, `submit` raises `BackpressureError` anyway, so `drop_oldest` never lets usage exceed the hard limit. An input that alone is past the hard limit can never fit, so `submit` raises `PayloadTooLargeError` for it, `drop_oldest` or not; a result that alone is past the hard limit of the worker that ran it fails the run with `ResultTooLargeError` (§8.5). Past the soft limit the scheduler compacts the key's chain on a free worker place, so a merge that shrinks its inputs keeps the key below the hard limit (a concatenating merge gets no such promise). `drop_oldest` does not relax the frame bound: a key whose claim would pass one message is refused with backpressure even with `drop_oldest`.

**Running generation and worker loss.** A running generation is never cancelled by a newer submission. If the worker running it is lost, the generation is replayed when it was the newest for its key (at-least-once for the latest state). If a newer pending generation already exists, that generation runs and the lost one stays `LOST` and is not replayed (`SUPERSEDED` is reserved for pending generations). A timeout is different: it is an ordinary task failure and is not requeued, so work does not grow through repeated requeue and timeout; the next submission creates the newest generation.

**Flow lifetime.** When a coalescing task returns a task-like value (§3.4), its running generation covers the entire resulting flow: the key is held until the flow is terminal. A coalescing task cannot itself be a stage, member or map of a composite; composition refuses it with `TypeError`, because its generations would replace one another inside the composite.

#### 3.2.2 Delivery semantics

| Situation | `@task` retriable | `@task` non-retriable | `@ephemeral_task` | `@coalescing_task` |
|---|---|---|---|---|
| Worker lost while running (after the reconnect timeout, §8.3) | new TaskRun (at-least-once) | `ORPHANED` | `LOST`, not replayed | replayed if newest, else `LOST` and not replayed |
| Task subprocess dies, worker alive | as worker lost, as soon as the worker sees the subprocess exit | `ORPHANED` | `LOST`, not replayed | as worker lost |
| Timeout | `FAILED`, retried per retry policy (in-process mode: retry deferred until the abandoned body exits, §6.2) | `FAILED`, killed at the hard limit or not (§28.13) | `FAILED` | `FAILED`, no special requeue |
| Leader change | run continues, adopted at reconciliation | adopted | adopted | adopted |
| Catastrophic shard loss | `ShardLostError`, may `resubmit()` | `ORPHANED` | lost | not replayed; next submission creates the newest (§16.3) |

### 3.3 Calling tasks

Calling a decorated task queues distributed work:

```python
handle = transform(req)
result = await handle
```

The TaskHandle is awaitable and retains task/shard metadata needed for result delivery and later recovery.

A handle reports how durably its task is held (`handle.durability`) and can wait for a stronger level (`await handle.durable(Durability.REPLICATED)`, §8.1). `await handle.status()` returns the task's state, its current run's state, the attempt, its submit and update times, whether its result reached the awaiting worker, and how mirroring the result to the result backend went. A flow or group handle returns a composite status instead: its kind, state, the count of its stages or members in each state and, for a flow, the stage it is at. Both read the record the shard holds, not the leader, so a change the leader has just made may not show yet. A process that restarts has lost its handles: `kabudachi.resume(id)`, given a task id or a composite id, reads the state from the record and the value from the result backend, or from the shard while it still holds it (§9.3). It raises `ResultUnavailableError` when neither has the value.

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

Each stage receives the prior stage's output as its remaining unbound input. Awaiting a flow gives the list of every stage's result in stage order (the last entry is the final output). `.bind(value)` fixes a task's whole input, so the bound task is called with no argument and, as a flow stage, ignores the output before it; `.bind(field=value)` sets scalar fields on the protobuf message the task is called with, or the prior stage's output. The shard's leader drives every flow and group. It starts each flow stage once the stage before is certified, and the stage's input names that stage's stored result (§8.6). A flow therefore keeps going if the process that submitted it dies; an implicit flow, whose continuation is committed atomically with certification, comes with the continuation rules below. Composition checks every adjacency when the step is built. An output fits an input when both share a schema family and the output's version is not newer than the input's, unless the chain between them crosses a bump declared `migration=None` (§7). Otherwise composition raises `TypeError`. A serializer that cannot tell answers "unknown", and that adjacency is not checked. A failed stage fails the flow; the failure carries the error's `kind` and `message`.

Parallel composition uses `group`. A group has an explicit failure policy:

```python
group(a, b, c, on_error="fail_fast")     # default
group(a, b, c, on_error="collect_all")
```

The policies follow `asyncio.gather`:

Every member of a group gets the same input (a fully bound member ignores it), and awaiting the group gives the list of the members' results in member order. Cancelling a group cancels every member that has not finished. `fail_fast` follows `asyncio.gather` and leaves the other members running after the first failure; the following stage never starts.

- `fail_fast` (default): awaiting the group raises the first failure and the group is terminal `FAILED`. This matches the least-surprising behavior and Celery's chord semantics, where a failed header task does not run the chord body.
- `collect_all`: like `gather(return_exceptions=True)`, awaiting returns the full array in which each entry is either a step's value or its failure.

A continuation after a group is simply the next stage of a `flow`, for example `flow(group(a, b, c), cb)`; there is no separate `.then()` form. Under `fail_fast`, a failed group is terminal `FAILED`, the flow fails with it, and the following stage never starts. Under `collect_all` the following stage runs and receives the ordered array of per-member values and failures. Each failure reaches the stage as a `kabudachi.TaskFailure` carrying the failure's `kind` and `message`, so a stage that follows a `collect_all` group must take `TaskFailure` entries in its input type; composition raises `TypeError` for one that cannot. That stage is an ordinary task and keeps what it was declared with: its own retry policy, timeout, queue and class (a `@task` stage is retried per its `retries`, an `@ephemeral_task` stage stays best-effort), and the group or flow imposes none of these on it (§3.4 "Independent tasks"). It runs as its own leader-certified TaskRun exactly once per group instance, across worker loss, leader change, and message reordering. "Exactly once" describes the certified completion run; side effects inside any TaskRun keep the at-least-once semantics of §3.6.

Distributed functional operations use `task.map` and `task.reduce`.

```python
results = await transform.map(inputs)
result = await merge.reduce(results)                          # seedless: (T, T) -> T
report = await add_to_report.reduce(results, initial=Report())  # seeded:   (T, P) -> P
```

`map` creates independent distributed Tasks per input item; this is not one worker looping locally through the list. The result preserves ordering. It is a `group` of one fully bound task per item (so it takes the group's `on_error` policy, `task.map(items, on_error=...)`), every item is checked against the task's serializer before any is submitted, and mapping an empty list returns `[]`. `task.map` is also a `flow` stage that takes the list the stage before it returns. A group or map streams its members to the leader in batches of up to 1,024 members or 512 KiB. A batch is accepted or refused whole, and one asked again creates nothing twice. Batches pause while `SlowDown` is raised (memory use past `memory_soft_limit`, §3.2.1); a plain `task(x)` call is not paused. Past `memory_hard_limit` the stream stops, the members already accepted are cancelled and the handle fails with `BackpressureError`. The leader seals a streamed composite when its last batch arrives, and the composite finishes once it is sealed and every member has settled. A leader that finds the composite already finished refuses a later batch, and the client stops streaming. Member outcomes live in pages of 1,024 entries (§8.6), so a million-item map never sits whole in one record or in the leader's memory. A `.map` after a stage that returned a list is split by a worker in an expansion run, never by the leader: the run streams the list's items into the map, as a client would. The reserved definition `kabudachi.expand` names that run. A streamed group or map whose submitter dies before its last batch keeps running the members the leader has until it is cancelled (Phase 8, remote client liveness).

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
    chunk_size: int = 64,       # items per chunk of a tree reduce
) -> PrevType: ...
```

- **Seeded.** With `initial`, the running result starts as `initial` and every item is folded in: `previous = reducer(item, previous)`. `PrevType` is inferred from `initial` and may differ from `ThisType`. An empty list returns `initial` without calling the reducer.
- **Seedless.** Without `initial`, `PrevType` is `ThisType`: the first item is the starting result and the reducer is first called with the second item. A single item is returned as it is, without calling the reducer. An empty list with no `initial` raises `ReduceEmptyError`, as JavaScript throws a `TypeError`, rather than inventing a value; `ReduceEmptyError` is also a `TypeError`.
- **`None` is a value.** As in JavaScript, passing an initial value counts even when that value is `None`; only leaving `initial` out selects the seedless form (`UNSET`, §3.1).
- **Order and determinism.** Items are folded strictly in input order and the reducer does not have to be associative. Given a deterministic reducer the result is deterministic.
- **Two arguments only.** JavaScript's callback also receives the index and the whole array. A distributed reducer never holds the whole list, so it gets just the item and the result so far.
- **A sequential chain.** Each step needs the previous step's result, so a fold runs as a chain: every step is an independent, retriable, leader-certified TaskRun whose input is the item and the serialized result so far. A reducer declared as a tree (below) is evaluated as a deterministic tree instead; that is an optimization that must give the same result as the chain.
- **Declaring a reducer.** A `@task` with exactly two parameters, `(item, previous)`, is a reducer, and `@reducer(associative=False, tree=None, merge=None, ...)` declares one with options. It takes the options of `@task` except `result_ttl`. It must be a plain durable task, so it cannot be an `@ephemeral_task` or a `@coalescing_task`; it must return the type of `previous` and cannot return a step. A direct call, `.bind`, `.map` and `.options` on a reducer raise `TypeError`; use `.reduce(items, initial=..., chunk_size=...)` or `.local(item, previous)`. As a flow stage, `.reduce` or `.reduce.options(initial=..., chunk_size=...)` takes the previous stage's list.
- **Input.** A reduce streams its items like a map: in batches that pause under `SlowDown` and fail the reduce with `BackpressureError` past the hard limit, into item pages the reduce owns (§8.6). No reduce is bounded by a record's size. Steps start once the last item has arrived.
- **A tree.** With `tree=True` the items are cut into chunks of `chunk_size` (64 by default). Each chunk folds its items in order as one certified step, from `initial` or from its first item. Adjacent chunk results then combine pairwise, in order, one certified step per combine, with `merge(left, right)` or, for an associative reducer with no `merge`, `reducer(item=right, previous=left)`; a result with no partner moves up a level unchanged. `tree` defaults to `associative`, and `tree=True` needs exactly one of `associative=True` or a `merge`. With a seed, `initial` must be the identity of the combiner, as in Spark's `treeAggregate`; this is documented, not checked.
- **Empty and single-item reduces.** A reduce of no items with an `initial` gives `initial`, and a reduce of one item without one gives that item, each without running a step. A seedless reduce of no items raises `ReduceEmptyError`: at submit when the list is known, otherwise when the leader finds it empty.
- **Failure.** A step is retried per the reducer's `retries`. A step that exhausts them fails the reduce with that step's error, and no later step starts, as an exception thrown by the callback ends a JavaScript reduce. Each step's task records its reduce and its place in the chain or tree, so the leader can rebuild the evaluation from the records alone (§13).
- **Determinism.** The leader plans a reduce from item counts and digests only, and never runs the reducer or `merge`. Every worker must import the same reducer and `merge` for the result to be deterministic; the leader cannot check this.
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
- **Atomic continuation.** The returned value is committed together with the returning task's certified completion (§8.5); the digest that certifies the run is the digest of the returned step's description, and the continuation starts only once the leader has certified it. The returned step is stored on the certifying run itself, so the one write that certifies the run commits the step. The returning task is declared with the step type as its return annotation (`-> Flow`, `-> Group` or `-> BoundTask`), and the step must not need an input (bind its first stage). The task is not over until its continuation is, so its coalescing key stays held (§3.2.1) and its handle resolves only then, to the continuation's results (a returned bound task gives a list of one). A lost or uncertified TaskRun therefore leaves no continuation behind, and a failing continuation never re-runs the returning task. The flow's state becomes `FAILED` and the caller may resubmit.
- **Sequential stages.** A continuation stage starts only after its predecessor is terminal, so cancelling a flow targets the current stage; later stages never start. Cascading cancellation exists only for the members of a `group`.
- **Independent tasks.** Each stage is an independent task with its own timeout, retry policy, and queue; nothing is inherited from the returning task. There is no flow-level deadline. If a stage has no timeout or a very long one, it holds any coalescing key of the flow until the flow is terminal (§3.2.1); this is a documented risk, not something the framework works around.
- **Handle shape.** The handle of the original submission resolves to a flow handle whose result is an array with one entry per task or step, in step order. A step that itself returns a flow or group has its slot replaced by a nested array of that sub-flow's results, so nesting follows flow structure. Failures follow the group policy above.
- **Orphans.** A flow whose stage ends `ORPHANED` fails. A composite has no `ORPHANED` state; a derived one needs remote client liveness (Phase 8).

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
client (the submitting worker, when it awaits the result)
composite id and index (a flow, group, map or reduce stage or member)
reduce node (level and position, on a step of a reduce)
input_manifest (a large input, by blob digest; set instead of serialized_input)
trace_parent, trace_state, baggage (carried unchanged; Phase 7 fills them)
result_ttl
```

A task has no durability field. Durability is a property of the handle (§8.1), and disaster-recovery storage is Phase 8.

Tasks are disseminated through the DHT and, for durable tasks, copied to the disaster-recovery backend according to policy.

A Task survives retries. Retry history belongs to TaskRuns.

A Task and all its TaskRuns travel as one Task record (§8.6): the submission is immutable after the record's first revision, and the runs are the part of the record the leader changes.

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
failure (kind, and message)
result metadata (result digest; result manifest, when a worker stored the result as blobs)
delivered
persistence
continuation
```

`delivered` says the result reached the task's client before the run was reported completed. `failure` carries the failing error's type name and its message. The message is bounded (1 KiB, cut further to the room the record has left) and never holds a traceback; it can hold task data. `persistence` is the result backend state of a certified run (§9.3): disabled, pending, stored or failed, and unset on a run not yet certified. `continuation` holds the step a succeeded run returned. It is written in the revision that certifies the run, so storing that revision commits the continuation with it.

Both worker and leader may mutate legitimate state fields. The implementation must not use naive whole-record "last writer wins" semantics. Mutations should be state-machine constrained, event/field scoped, and reconciliation must be deterministic.

A TaskRun is not stored on its own: it is one of the runs inside its Task's record (§8.6), so a Task and its runs share one key, one placement and one version. Only the leader writes the record, and each write carries a version ordered by recovery epoch, leader term and a per-term revision, so a stale or conflicting write is refused by the store rather than overwriting a newer one.

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

A flow, group, map or reduce (§3.4) has its own state in its composite record, which the shard's leader keeps and writes whole on every change, like a Task record. The states are:

```text
RUNNING
SUCCEEDED
FAILED
CANCELLED
```

A composite is `RUNNING` until it ends. It is `SUCCEEDED` when every stage or member succeeded, and for a reduce when every step it plans has. It is `FAILED` under the group policy, or when any step of a reduce fails, and its record keeps the first failure that ended it. It is `CANCELLED` when cancelled. A member that is itself a composite counts as succeeded when that composite did.

A stage or member whose task ends in any state but `SUCCEEDED` settles as a failure carrying that task's failure kind and message, so a stage that ends `ORPHANED` fails its flow. There is no `ORPHANED` composite state: a derived one needs remote client liveness (Phase 8). The record also keeps the stage that runs next (a flow), counts of members pending, running, succeeded, failed and cancelled, and where the outcomes are. The outcomes sit in pages of 1,024 entries, not in the composite record (§8.6).

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

A worker sets its subscription with the `queues` setting. Left unset (`None`), a networked worker subscribes to its `queue` alone, and a one-node worker takes every queue, since nothing else could run the rest. Python refuses an empty `queues`. The subscription travels in the worker's heartbeats (the `MemberReport`), so the leader knows it. On the wire an empty list of queues means every queue, so a worker the leader has not yet heard from is held to no restriction. The leader refuses a claim for a queue the worker is not subscribed to (`CLAIM_REJECT_NOT_SUBSCRIBED`, §8.2).

The `queue_options` setting maps a queue name to a `QueueOptions`. The leader orders and limits a queue by its `priority`, `weight`, `max_running` and `starvation_age`. The submitter stamps the other two, `reconnect_timeout` and `result_ttl`, on each task as it submits. Workers of one shard must give the same `queue_options`. The leader's options decide, and a worker that gives different ones is neither refused nor warned about: differing `queue_options` across workers is unsupported (mixed-version clusters are handled in release hardening, §27.2).

The `router` setting is a function `router(task_name, value) -> str | None`. When a call names no `queue=`, the submitter calls it with the registered task name and the argument, and it returns the queue, or `None` for the `queue` setting. The router runs on the submitting process only: it is not sent to the leader or to task processes. It runs after the payload-limit check.

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

If configured process count is `0`, the main Python runtime may execute task functions itself. In that mode only cooperative (soft) cancellation is enforceable: a synchronous body that ignores cancellation cannot be killed in-process, so at the hard limit the TaskRun is marked `FAILED` and the runtime stops waiting for it. Nothing is killed, so in this mode the outcome is `FAILED`, as in the delivery table (§3.2.2): the same outcome as a hard-timeout kill in the subprocess pool (§28.13). An abandoned body of a non-retriable task may still complete irreversible effects, which is the ambiguity `ORPHANED` names in §4.4. The abandoned body may still be running, so a retry, if the retry policy calls for one, is not started until that body has actually exited; a lineage never has two bodies executing at once. The failed TaskRun itself is terminal. When a retry is due, the lineage stays pending until the body exits and the handle does not resolve meanwhile; when none is due (no retries configured, or the last attempt), the lineage is terminal `FAILED` at once. A coalescing generation's key stays occupied until its abandoned body exits, so no second generation of that key starts while a body for it is still running (§25.4.1). Whatever the abandoned body later returns or raises is discarded and can never certify (§25.1.5, §25.1.6). It keeps its concurrency slot until it exits. A body that never exits therefore blocks that lineage's retry and holds its slot indefinitely, and enough of them exhaust the bounded pool: this is a documented limitation of the in-process mode, removed by the subprocess pool's hard kill. In this mode a body that raises `SystemExit` or `KeyboardInterrupt` fails its run with `TaskBodyError`, except an async body that raises `KeyboardInterrupt` on the main thread: that can be a real Ctrl-C, so it interrupts the whole worker. The rule limits duplicate side effects from retries but does not fence writes the abandoned body still makes (§2.3).

`kabudachi.run(main=None)` is the synchronous entry point, in the style of `asyncio.run`. It constructs and owns the event loop on the main thread, initializes the native runtime on it, runs `main()` if given (returning its result) and then drains and stops. Draining waits for every task that was called to finish, queued tasks and tasks started by other tasks included; if `main` raised or was interrupted, tasks already running are let finish, tasks that have not started fail with `RunStoppedError`, and the error propagates. A task that waits for another task does not occupy one of the `concurrency` places while it waits, so a task that waits for tasks it called cannot starve them. Before `main` starts, every registered task is checked against the serializers of the process, so a task that cannot work is reported at once; with no `main` it serves as a worker (on the main thread) until SIGINT/SIGTERM, then drains as above and returns; a second signal stops the waiting and raises `KeyboardInterrupt`, abandoning unfinished tasks (a synchronous task still in its thread keeps the interpreter from exiting until it returns). Submitting a task outside `run()` raises `RuntimeNotStartedError`; `.local()` (§3.3) needs no runtime.

By default the process count is the number of CPUs the worker may use (its CPU affinity, not the host's count). Task subprocesses start with the `spawn` method, never `fork`, because the worker holds a Tokio runtime and native threads. A spawned subprocess imports the main script again, so a script that calls `kabudachi.run()` must guard it with `if __name__ == "__main__":`. Each subprocess imports every module that declared a task or lifecycle hook in the worker (or the modules an explicit `imports` setting names instead), checks that it has every task at the same version, and reports ready within `process_start_timeout` (60 s by default); otherwise the worker's start fails. A task or hook declared in `__main__` is refused at start while the process count is above 0, since a subprocess cannot import it.

At the hard limit (§3.8) the run settles at once: a non-retriable run is `FAILED` with a timeout error, and a retriable one queues its retry, which starts once the body has exited. The subprocess hosting the body takes no new runs and is stopped (SIGTERM, then SIGKILL after a short grace) once its other runs have settled; a replacement starts at once, and neighbouring runs are never killed as collateral. A body that raises `SystemExit` or `KeyboardInterrupt` is contained the same way: its run fails with `TaskBodyError` naming the type, and its subprocess is stopped once its other runs settle.

A subprocess that dies (a crash, running out of memory, an external SIGKILL or SIGTERM) loses its runs, which are reported as soon as the worker sees it exit, and is replaced, after a wait that doubles while replacements keep dying quickly. A lost run follows the delivery table (§3.2.2) as if its worker had been lost: it is replayed without using up a retry, except an ephemeral task's or a coalescing generation's that a newer one waits behind (whose handles raise `TaskLostError`), and a non-retriable run that was running, which is `ORPHANED`. A compaction run lost with its subprocess is reported `FAILED`, not lost, so a merge that crashes its process is not run again on that key until its waiting generation changes.

A subprocess is recycled after `max_runs_per_process` runs (off by default), after a run of a task declared `recycle_process=True`, or after an `after_run` hook raises (§19.6): it takes no new runs, finishes the ones it has and exits, and its replacement starts then. Until it exits it still counts against the process count, so recycling never raises the number of processes (§28.12).

Task subprocesses ignore SIGINT, since a terminal's Ctrl-C reaches the whole process group and only the worker decides what it means, and they exit when the worker's end of their pipe closes, so none outlives its worker. The worker's first SIGINT or SIGTERM drains as described above; a running body past its timeout still escalates through its cancel grace to the hard limit. A second signal stops the task subprocesses at once (SIGTERM, then SIGKILL), and their unfinished runs are lost. A SIGTERM sent to a task subprocess itself cancels its bodies and ends it, and its runs are lost and replayed as for any subprocess that dies. Deployments should therefore signal only the worker process, not its whole process group. With systemd, set `KillMode=mixed`: systemd then sends SIGTERM to the worker process only, which drains and stops its task subprocesses itself, and SIGKILLs whatever remains only after the worker exits or `TimeoutStopSec` passes. The default, `KillMode=control-group`, sends SIGTERM to every process, which cancels the task-subprocess bodies during the drain (their runs are replayed).

The design does not encourage thousands of coroutines per worker process simply because asyncio technically permits it. Predictable bounded concurrency is more important.

### 6.3 Serializer backend registry

Serializer/data backends are runtime-resolved and extensible.

kabudachi ships `protobuf`, `pydantic` and `dataclass` serializers, installed as the package extras `kabudachi[protobuf]`, `kabudachi[pydantic]` and `kabudachi[dataclass]`. The `dataclass` extra adds no dependency. `protobuf` is the default. The JSON serializers encode deterministically (sorted keys, compact separators, UTF-8), so equal values give equal bytes and results can be certified by digest.

The base package does not fail to import when an optional serializer dependency is absent. A backend whose library is missing is still registered; asking for it raises `SerializerUnavailableError`, and asking for an unregistered name raises `UnknownSerializerError`.

A backend provides:

```text
name                                                     the name a task selects with serializer=
available() -> bool                                      whether the backend can run here
supports(value_type) -> bool                             whether it can hold this type
encode(value, value_type) -> bytes
decode(payload, target_type)
extract_version(payload, value_type) -> int | None       (None: the payload carries no version)
validate(value)                                          raises SerializationError
schema_family(value_type) -> str | None
type_compatibility(produced, consumed) -> bool | None    (None: unknown, not checked)
load_plain(payload, value_type) -> plain value           (what a migration takes)
dump_plain(plain, value_type) -> bytes                   (conformed to value_type, then encoded)
```

Every method after `decode` is optional for a third-party serializer, and kabudachi falls back to a default when one is missing: a payload with no version, every decoded value valid, the family declared with `kabudachi.schema` (else the type's own), a compatibility check skipped. A backend without `load_plain` and `dump_plain` cannot migrate, so a task that needs a migration is refused.

Users register backends such as `msgspec` or application-specific protocols with `kabudachi.register_serializer`. Every worker of a shard must register the same backend under the same name, since one worker encodes what another decodes.

---

## 7. Versioning and migrations

Versioning is explicit at the task-data boundary.

Tasks have no versions; data schemas do. `kabudachi.schema(Type, version=N, version_path=..., family=..., migration=...)` declares a type's current version. The version is 0 by default, so a type never declared is version 0 of its own family. `family=` makes separate classes versions of one family; by default a family is a protobuf message's full name, or `module.qualname` for any other class. A task's source and execution versions are its input schema's versions.

For the JSON serializers (`pydantic`, `dataclass`), `version_path` (for example `"$.version"`) says where the payload carries its version. For protobuf the payload carries none: a protobuf input is taken to be at the source version of the Task that holds it.

```python
kabudachi.schema(OrderV1, version=1, version_path="$.version", family="shop.Order")
kabudachi.schema(OrderV2, version=2, version_path="$.version", family="shop.Order")
```

A bump from `n` to `n + 1` with no explicit migration takes the default one: fields the new version lacks are dropped, and fields new in it take their defaults. `schema(Type, version=N, migration=f)` or `@kabudachi.migration(Type, from_version=N - 1)` declares an explicit step, a function from the older payload's plain form to the newer's. `migration=None` declares the bump incompatible. A family has one `version_path`, one class per version and at most one migration per bump; a contradicting declaration raises `ValueError`. Every worker of a shard declares a family's bumps the same way.

Original Tasks are immutable. Migration is transient:

```text
stored Task v2
    -> worker supports v5
    -> migrate 2->3->4->5 in memory
    -> validate v5
    -> create/execute TaskRun with source_version=2, execution_version=5
```

The claiming worker migrates in memory from the Task's `source_version` to its own version, one bump at a time, validates the result and records `execution_version` on the run. The migration runs before the run is reported started. The migrated Task is not written back over the original record and is not persisted as a replacement Task.

By default a migration that raises (the default one or an explicit one, `MigrateRejectError` included), or a bump declared `migration=None`, drops the Task. The worker logs a WARNING naming the Task, the family, both versions and the reason, never the input data. The Task ends failed with `MigrationError`, which names both versions. It is not retried.

`@task(raise_on_failed_migration=True)` keeps such a Task queued instead and reports it blocked to the leader, for a worker that can run it. `MigrateRejectError` is how a migration asks for that. A source version newer than the worker's always blocks, whatever the option. The leader records when and why the Task was first blocked (`TaskRecord.blocked_since` and `blocked_reason`), and a worker that blocked a Task is not handed it again by that leader. After `version_block_ttl` (`Settings.version_block_ttl`, one hour by default) a Task still waiting in the blocked state fails with `MigrationError`. Reporting a Task blocked releases a claimed run back to queued with no attempt spent.

A worker that cannot run a Task's source version does not claim it: discovery's own-records stage skips such a Task and reports it. Steal and oldest-first claims do not filter by version in advance, so a worker can be handed a newer Task and block it, which costs one round trip.

Mixed code versions during a rolling deployment are handled by this same migration schema. A migration may explicitly refuse an incompatible boundary by raising `MigrateRejectError`; with `raise_on_failed_migration=True` the Task then stays queued for a worker that can execute it. Rollout policy beyond this (batch sizes, minimum shard size, election budgets) is left to the application and deployment tooling.

Open: the mixed-version gaps in migration are listed with their owner (Release hardening) in the Phase 6 notes in §27.

Open: the leader's in-memory blocked set and the compaction refusal of old chains are listed with their owner (post-implementation testing) in the Phase 6 notes in §27.

---

## 8. Task submission, discovery, claim, execution, and result protocol

### 8.1 Submission

A worker mints the task id and the submission time and sends SUBMIT to the shard's leader (a client reaches it through a worker). The leader writes the Task's record at revision 0 (§8.6) and answers once the write is stored. If the answer is a retryable `NOT_LEADER`, the worker sends the same minted task to the new leader, and a task already recorded is never recorded twice. In the one-node runtime a submission made before the worker is granted leadership is queued and recorded when the grant arrives. Choosing a shard when sharding is enabled and writing the durable Task to the disaster-recovery backend are later phases.

The client receives a TaskHandle containing at least:

```text
task_id
shard_id
```

plus local callback and result-delivery state.

Submission is not idempotent (§2.3): each new mint is a new Task. Resubmitting a minted task after `NOT_LEADER` is a retry of that submission, not a second one.

A handle can report how durably its Task is held, and callers may await a stronger level before treating the submission as accepted:

```text
in_memory     held by the receiving peer (the one-node runtime)
replicated    stored at a majority of the record's placement (§8.6)
dr_stored     written to the disaster-recovery store (§9.2; Phase 8)
```

`handle.durable(level)` waits until the level holds. Asking for `replicated` on a shard of one, or for `dr_stored` anywhere, raises `DurabilityUnavailableError`.

Submission is acknowledged only once its record is `replicated` in this sense.

Kabudachi is not a transactional inbox/outbox. An application that must enqueue atomically with a database commit records the intent in its own store and submits after commit, handling repeat submission itself.

### 8.2 Worker-pull claim model

The leader does not push tasks blindly.

Workers inspect DHT-visible pending work and request claims:

```text
worker sees pending Task T
    -> worker -> leader REQUEST_CLAIM(T)
    -> leader examines authoritative state
```

A worker with room to run more finds work in stages, moving to the next until it has claimed as many tasks as it has room for (stopping early only when the leader refuses or does not answer):

1. the records it holds itself, nearest key first;
2. `STEAL` requests to shard peers, by distance class from the worker's own key, nearest first, widening one class at a time; a peer answers with the waiting tasks it holds, oldest first, up to a limit;
3. `CLAIM_OLDEST` at the leader, which hands back its oldest pending tasks.

Within each stage, candidates go by queue priority, then nearness (own records) or the answering peer's order, then age. `CLAIM_OLDEST` answers in strict priority tiers. Within a tier it takes queues by weighted round robin, and a task that has waited longer than its queue's `starvation_age` rises above every tier. On a claim the leader checks only the subscription and the queue's `max_running`, so a worker still claims nearby work first. The leader is a worker too: it claims from the records it holds first, by queue priority, then nearest key, then age, then the shard's oldest, and asks no peer.

A worker that finds nothing at every stage waits longer before it looks again, and finding work resets the wait. Every task found is claimed through `REQUEST_CLAIM`. A worker's view of a record can be stale, so the leader may refuse a claim for a task the worker saw waiting, and a refusal only moves discovery on. The leader answers a claim only for a worker that is a voter or a pending member of its shard.

Leader response may include:

```text
ACCEPT
CLAIM_REJECT_ALREADY_SELECTED
CLAIM_REJECT_NOT_READY
CLAIM_REJECT_TASK_UNKNOWN
CLAIM_REJECT_SUPERSEDED
CLAIM_REJECT_NOT_LEADER
CLAIM_REJECT_FINISHED
CLAIM_REJECT_KEY_BUSY
CLAIM_REJECT_NOT_MEMBER
CLAIM_REJECT_CANNOT_RUN
CLAIM_REJECT_NOT_SUBSCRIBED
CLAIM_REJECT_QUEUE_FULL
```

`NOT_SUBSCRIBED` means the claiming worker does not take the task's queue. `QUEUE_FULL` means the queue is already running its `max_running` tasks. Discovery counts `QUEUE_FULL` as a full queue and `NOT_SUBSCRIBED` as a stale view. A refused candidate spends no queue turn.

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

For coalescing tasks (§3.2.1), the leader also serializes supersession. A newer submission with the same key marks the older pending generation `SUPERSEDED` (a claim for it is answered `CLAIM_REJECT_SUPERSEDED`); a generation that is already `CLAIMED` cannot be superseded.

The leader never executes the reducer. It keeps the superseded Tasks' payloads linked in order, in the retained chain of the newest generation's record; the worker that claims the newest generation folds the chain oldest to newest with the task's reducer before running the task body.

Compaction keeps that chain short. When a waiting generation's chain holds more payload than the per-key soft threshold (half of what one claim may carry), or memory is past its soft limit, and some worker has said it runs compaction, the leader creates an internal compaction run naming the oldest entries that fit in one claim. The worker folds exactly those entries with the task's reducer, and the leader swaps them for one folded entry only while the chain still starts with them. A compaction run carries its key's queue, so only a worker subscribed to that queue folds it; `max_running` does not count it. Only a claimed compaction holds the newest generation back, and with no worker that runs compaction the newest generation folds its whole chain itself. No claim ever outgrows one message: a submission that would make its key's waiting claim too large is refused (backpressure), and a fold that grows past one message fails the newest generation with `CoalescedPayloadTooLarge`. Past the hard memory threshold, new submissions are backpressured (or dropped-oldest if the task opted in).

After leader change, reconciliation (§13) rebuilds per-key occupancy, including the lifetime of any implicit flow, so a second running generation for the same key is never admitted. The leader ends a continuation when its composite is terminal, on one node and over the network, so an implicit flow frees its key and memory across leader changes.

### 8.3 Heartbeats

Worker liveness is a dedicated worker-to-leader control path, separate from lifecycle pub/sub.

Heartbeats communicate:

- worker identity/incarnation;
- current term/recovery epoch observed;
- available capacity: free places across its task processes, which the leader records per member;
- the queues it subscribes to;
- active TaskRun digest/status.

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
- A worker starts no TaskRun until a leader has heard from it, so every run it holds has an abort deadline. Before the deadline it cancels each run cooperatively, the run's cancel grace ahead of it. At the deadline it kills the task subprocess of any body still running, which also ends the other runs that subprocess hosts; they share the same lost contact, so the same deadline. A run stopped this way is reported lost once its body has exited. If a leader hears the worker again before the deadline, the pending aborts are withdrawn. A leader runs its own claims under the same rule.
- The leader's replacement deadline is per TaskRun: the run's own `reconnect_timeout` after the leader suspects the silent worker. A worker heard again keeps every run whose deadline has not passed.

The worst-case time from an abrupt kill (SIGKILL) to the replacement TaskRun starting is `heartbeat_timeout + reconnect_timeout`, plus an election if the leader was lost and the claim round trip. Defaults are on the order of tens of seconds (`heartbeat_timeout` 10 s, `reconnect_timeout` 30 s). `reconnect_timeout` is configurable per shard, per queue (`QueueOptions.reconnect_timeout`) and per task: a task's own value wins, then its queue's, then the shard's. It is resolved when the task is submitted and stored in the Task record, so the leader's replacement deadline and the worker's abort deadline use the same value, and a successor leader reads it from the record. Heartbeats run in the native core, independent of the GIL and of any CPU-bound task subprocess, so a long native call cannot make a healthy worker look dead. If a task subprocess dies while its worker lives, the TaskRun is reported `LOST` as soon as the worker sees the subprocess exit, after waiting up to a second for the frames it sent last.

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

Result delivery and certification travel on their own request-response protocol, `/kabudachi/result/1`; each answer is the ACK, and a sender that hears none tries again within a bound (§8.5).

Heartbeat traffic remains separate so event-bus backpressure cannot make healthy workers appear dead.

### 8.5 Result certification

Result transport and result authority are intentionally distinct.

A successful run delivers and certifies its result like this:

```text
1. worker -> client: DeliverResult(task_id, run_id, digest, payload | manifest)
2. client -> worker: ResultAck            the client holds it, uncertified
3. worker -> leader: ReportCompleted(run, digest, delivered)
4. leader stores the certification at a quorum of the task's placement
5. leader -> client: ResultCertified(task_id, run_id, digest)
6. client -> leader: CertifiedAck         the client resolves the handle if the digests match
```

The client may physically possess result bytes before certification, but it must not treat them as authoritative until the leader certifies the TaskRun.

This solves stale-worker result races at the scheduler level. If an old retriable TaskRun loses authority and later reconnects with "success," the leader rejects completion and no certification arrives.

For a non-retriable TaskRun that survived leader replacement, the new leader may adopt the same TaskRun during reconciliation and certify its already-delivered result.

When a task returns a task-like value (§3.4), the leader commits the continuation atomically with certification of the returning TaskRun. A TaskRun that is not certified therefore leaves no continuation behind.

A task that ends without a result (failed, lost, orphaned, cancelled, expired or superseded) is pushed to its client as a conclusion, once the revision that ended it is stored. The leader pushes for 30 s. A client that missed it asks the next leader.

A run that returns a step delivers no result. Its handle resolves when the leader reports the continuation's composite finished (`CompositeFinished`), naming the results in its pages.

**Failure matrix.**

| Case | Behaviour |
|---|---|
| Client dies or cannot be reached before ACK | The worker tries again until the run's `reconnect_timeout` ends, then reports `delivered=false`. The task certifies; its side effects happened. The awaiting handle raises `ResultUnavailableError`, and the value is reachable only through a result backend (§9.3). |
| Leader dies after COMPLETE | The report is held as any report is. After a leader change, a client with uncertified results asks the new leader `QueryOutcome`; the answer is the same however often it is asked. |
| Certification sent twice | The client keeps payloads by run id and settles a handle once. |
| Worker dies after RESULT, before COMPLETE | A retriable task runs again after its reconnect timeout. The client drops the uncertified payload when a different run certifies. A non-retriable run is `ORPHANED`, and the handle raises `TaskOrphanedError`. |
| Client restarts | It has a new identity and no handles. `kabudachi.resume(id)` reads the record, then the result backend or, while the shard holds it, the shard's blobs, and checks the digest. When neither has the value it raises `ResultUnavailableError`. |
| Result too large for one message | It travels as a manifest of 256 KiB blobs (§8.6). A result alone past the hard memory limit of the worker that ran it fails the run with `ResultTooLargeError`, an ordinary body failure that is retried as the task's `retries` allow. One alone past the awaiting client's limit fails that handle only. |

### 8.6 Task records and placement

The shard's DHT holds one record per Task, written whole by the leader on every change. A record holds:

```text
version             recovery epoch (with its lineage), leader term, per-term revision
task                the submission, immutable after the first revision
runs                every TaskRun of the Task
retained chain      a coalescing generation's absorbed or folded payloads (§8.2)
input digest        BLAKE3 of the serialized input, the algorithm named
coalescing link     the generation that superseded it, and the ones it absorbed
placement           the voters that hold the record
publication time
finished            set once the Task is terminal
prior placements    where earlier revisions of a moved record were held (below)
```

A certified result carries its content digest the same way, so a reader can check a payload it receives. A Task and its runs share one key (the task id), one placement and one version.

**Version order.** Records compare by recovery epoch first, number and then lineage, so of two lineages at one number exactly one is newer, whatever the terms. Within one epoch the leader term decides, then the revision. The store refuses an older revision, and refuses a revision of the same version that is not identical, with two exceptions. A revision of the same version that differs only in its placement or prior placements (a leader re-place) is accepted and replaces a stub. A leader write that leaves the holder out of its placement is acknowledged by that holder as a stub. So an acknowledgement means the record was stored, or kept as a stub where the record moved away, and a writer that counts acknowledgements counts holders.

**Placement.** A record is written to the `r` placeable voters nearest its key by XOR distance (`r` defaults to 3 and is capped at the voters known), and a write counts once a majority of them has stored it. Any later read of `r - w + 1` of them then meets a holder of the newest revision. Use an odd `r`: a majority of an even `r` tolerates no more loss than the odd factor below it. Records travel on a per-shard records protocol that only the shard's workers speak, so a record never lands in another shard.

**Retention.** A finished record, composite or page is dropped after its `result_ttl`; an unfinished one never expires.

**Effects wait for storage.** Every answer that releases an effect (a submission acknowledgement, a claim, a certification, a failure answer, a cancel answer) is held until the writes that record it are stored and the leader's lease is still valid. If the writes miss their quorum, or the lease ended first, the answer is a retryable `NOT_LEADER`. A leader never acts on state that a successor could not find.

**Moving a record.** When the placeable voters change (a worker is admitted, leaves or is lost), the leader writes the records whose placement moves, a bounded number at a time, to their new holders. A write that moves a record is a joint write: it counts only once a quorum of the new placement and of each earlier placement the record may still be known by (counting only holders still in the configuration) has stored it. A reader that hears most of an earlier placement therefore meets the new revision. A holder a record moved away from keeps a key-only stub (task, version, new placement), which reconciliation reports (§13), and a later plain write that names no earlier placement clears the carried placements. A write the store refused is kept by repair and published again after a delay until it is stored; while the scheduler is not leading nothing is published, the refusal is kept, and it is published once the scheduler leads (it is forgotten when the leader leaves office). Only the reconciling leader's republish of a rebuild retries before the grant. Repair places writes that are waiting on the voters that remain when a holder leaves.

**One-node runtime.** The one-node runtime keeps its records in its own store with `r = 1`; the same writes happen, and each is stored at once.

**Composite records.** A flow, group, map or reduce is a composite record written by the leader under `composite/<id>`, with outcome pages under `composite/<id>/<page>` of up to 1,024 entries each. They are placed, replicated, moved, handed off and reconciled exactly as Task records are. Member and stage ids derive from the composite's id and the index, so a new leader that drives a composite again makes the same ids and never a second task for one stage.

**Blob records.** A payload of more than 956 KiB (a little under one message), and every stage's result, is stored as blobs under `blob/<blake3 hex>`, at a quorum of the owner's record placement, before the run is reported. The store checks each key against its value's digest. A blob expires with its owner's record. A blob whose owner the holder never sees expires after a grace (10 minutes). A manifest names a payload's blobs in order, with its total size; the leader reads only the size and the digests, never the bytes.

**Trust.** The record store trusts the peers of its shard: it checks a record's key, size and version, not who sent it, and it does not cap how many unfinished records a holder keeps. Stopping a forged or flooding writer needs peer authentication, which is part of the production-readiness gate (§27.2, §28.10).


---

## 9. Disaster recovery and optional external persistence

The system has three different external persistence concerns. They must remain conceptually separate.

### 9.1 CoordinationAuthority

Default: Redis.

Purpose:

- cold shard/worker discovery;
- leader endpoint hints;
- task-to-shard mapping cache (Phase 8);
- recovery epoch/fencing for `NO_QUORUM`;
- initial bootstrap arbitration.

It is not the task queue and is not on the normal execution path.

Provider constraints, so that the default Redis provider coexists with other tenants of a shared instance. The Redis provider (`kabudachi_redis_authority`):

- keeps every key of a shard name under `<prefix>{<name>}:` (one hash tag, so one cluster slot), with a configurable key prefix;
- takes a database number outside cluster mode and rejects a non-zero one in cluster mode, which supports only database `0` and so isolates tenants by key prefix alone;
- never sends `SCAN` or `KEYS`; every key it reads is addressed by name;
- needs no Lua, only `WATCH`/`MULTI`/`EXEC`; its crate documentation lists the exact command set (`COMMANDS`), which an ACL user limited to those commands and `~<prefix>*` suffices for;
- requires an eviction policy that never evicts its keys (`noeviction` or a `volatile-*` policy), because it sets no key TTLs.

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

### 9.3 Result backend

A result backend is optional. If enabled, certified results are mirrored into a queryable backend for:

- inspection;
- later result lookup;
- debugging;
- recovery of completed result values;
- operational tooling.

kabudachi defines the `ResultBackend` interface (`store`, `load`) and ships an in-memory backend and a Redis/Valkey backend. Nothing is forgotten by hand: each entry carries its own expiry, which every backend enforces. A Python object is a custom backend when it has `async store(key: str, entry: bytes, expires_at: datetime) -> None` and `async load(key: str) -> bytes | None`. It stores opaque bytes, and kabudachi decodes and checks them. `result_backend=` on `configure` selects one: a `redis://` URL or an object.

After its report is certified, the worker that ran the task mirrors the task id, run id, digest, value (as chunks when large) and expiry to the backend. A composite's client mirrors the composite's resolved results when it finishes: each member's value or failure, in order, as pages when large.

A result-backend failure is logged and never changes `SUCCEEDED` back to `FAILED`, or any other outcome.

Persistence is attempted only after leader certification. The orthogonal persistence states are:

```text
DISABLED
PENDING
STORED
FAILED
```

Python reads them as the strings `"disabled"`, `"pending"`, `"stored"` and `"failed"` in `TaskStatus.persistence` (`handle.status()`). The field is `None` when the state is unspecified, as for an adopted run. A composite has no persistence state, and a task that returned a step shows `"disabled"`.

`result_ttl` follows normal configuration inheritance and is not required on every task (§3.1).

`kabudachi.resume(id)` takes a task id or a composite id. It reads the state from the shard's record, waits while the task or composite runs, and reads the value from the backend, checked against the certified digest. While the shard still holds the result, `resume` falls back to the shard's blobs or pages when the backend has no matching entry, and mirrors them. Once the shard has dropped the result, the backend alone answers. With neither, `resume` raises `ResultUnavailableError`.

The Redis adapter shares the authority's connection settings type and ACL conventions, and nothing else. It serves a standalone server only and refuses `cluster = true`. The adapter ignores the `ttl` setting of its configuration, since each entry carries its own expiry. The Python `redis://` URL form refuses a `ttl` outright. The backend may point at another server than the authority.

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

The bootstrap cascade asks the configured seeds, then the workers the authority lists as registered, and then, with an authority, founds the shard only by winning a compare-and-swap of its recovery epoch. A worker with seeds and no authority founds its own shard only after the seeds have stayed silent for a configured number of rounds, which bounds the split such a founding can cause until the shards converge (§17).

#### `JOINING`

The worker has identified a shard and is being incorporated into the shard's peer view (§11), which the DHT routing crawl and the shard's gossip topic build.

It may:

- exchange routing metadata with its peers over the DHT;
- learn the current leader (a join asks a full pass of its peers and takes the newest pointer, by the epoch order of §12);
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
- starts a roll call (§12.4) once its jittered suspicion timeout ends, unless a roll call of another worker it answered has not yet resolved.

#### `ROLL_CALL`

The worker participates in reachable-peer discovery for an ordinary election.

#### `CANDIDATE`

The worker is requesting votes for a new leader term.

#### `LEADER_RECONCILING`

The worker has won an election but cannot schedule new work yet. It keeps every election duty of a leader (heartbeats, acks, its lease) but is granted no claims, and it reconstructs authoritative TaskRun state from live workers and Task records (§13). It leaves for `LEADER` when reconciliation finishes, and for `ACTIVE`, `LEADER_SUSPECT`, `NO_QUORUM`, `FENCED` or `DRAINING` when it loses office or is asked to drain.

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
- casts no ordinary votes, though a leader handing over still answers roll calls and votes (§18.2);
- emits irreversible `SELF_REMOVE`;
- may finish existing work;
- hands off the Task records it holds (§18.1).

A draining leader hands leadership to the voter that confirmed its ack most recently, the lowest id on a tie (§18.2). A node with no leader (`LEADER_SUSPECT`, `ROLL_CALL`, `NO_QUORUM`) drains too: it waits at most `drain_wait_limit` for a leader, then leaves and logs every record it could not hand off.

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
      |
      +--> the leader's ack arrives -----------------> ACTIVE
      |
      +--> returning voters are a quorum ------------> CANDIDATE
      |                                                   |
      |                                                   | wins
      |                                                   v
      |                                          LEADER_RECONCILING
      |                                                   |
      |                                                   v
      |                                                LEADER
      |                                                   |
      +--> short of a quorum                              | quorum-contact
      |                                                   | lease ends
      v                                                   v
  NO_QUORUM <---------------------------------------------+
      |
      +--> a leader's ack ---------------------------> ACTIVE
      |
      +--> peers return: its own roll call ----------> ROLL_CALL
      |
      +--> authority path: respondents are a majority
      |    of the live registrations; swaps the epoch
      |    ------------------------------------------> CANDIDATE (while it waits
      |                                                out the fence), then
      |                                                LEADER_RECONCILING
      |
      +--> authority holds an epoch it cannot recover
      |    from ------------------------------------> BOOTSTRAPPING (rejoins)
      |
      +--> authority holds no recovery epoch --------> STOPPED (shard ABANDONED)
      |
      +--> authority holds another incarnation
           of the shard ----------------------------> STOPPED (shard ABANDONED)
```

A candidate that loses its vote, or an initiator that abandoned its call for a better one or finds its term already holds a vote or a leader, returns to `LEADER_SUSPECT` and tries again at a later term after a fresh jittered suspicion timeout; a call short of quorum at its deadline goes to `NO_QUORUM`. A `CANDIDATE` waiting out the fence that the authority refuses for good goes back to `NO_QUORUM`. A leader asked to drain goes to `DRAINING`. With an authority configured, a `LEADER_SUSPECT` member first reads the authority's shard record and starts its roll call only if the read names its own epoch (or the authority holds none); if it names another epoch it rejoins at that epoch. A worker that finds the record naming another incarnation of its shard (another `ShardId` under its name) stops, its shard abandoned, from any state that reads the record: a rejoining node in `BOOTSTRAPPING` or `JOINING`, `LEADER_SUSPECT`, `NO_QUORUM`, `CANDIDATE`, `LEADER_RECONCILING`, `LEADER` and `FENCED`. A worker in any state from `ACTIVE` through `LEADER`, `NO_QUORUM` included, becomes `FENCED` if it fails to renew its authority registration. A leader's grant to schedule ends at the earlier of its recovery fence and its quorum-contact lease.

---

## 11. Shard peer awareness

A shard needs three things from its peers' connections, and none of them is all-to-all failure detection. A worker keeps one heartbeat relationship, with its leader (§12.1). It must be able to reach the other workers when the leader is lost, which is what a roll call does. And the leader must be able to tell how many workers answer (§12.4). No worker keeps a member list: a follower knows its shard's configuration only as a generation and a voter count, and only the leader holds the list of members (§12.1). The overlay that carries these three things is libp2p's, not a structure of this system's own.

### 11.1 Roles of the overlay

- **Gossipsub carries the roll call.** Each shard has one topic, `/kabudachi/<shard>/election/2`. A worker that suspects its leader publishes its roll call there, so one publish reaches the workers in the topic's mesh, whatever the shard's size. Delivery is best effort, like every election message: a call a worker misses is a worker the election does not count. Every message is signed with its author's key, and a message with no valid signature is dropped. A reply goes straight back to the initiator over a request-response connection, never over the topic.
- **Request-response carries everything addressed to one peer:** roll-call replies and refusals, vote requests and grants, worker heartbeats and leader acks, claims, JOIN, and the reconciliation and task exchanges.
- **Kademlia is for peer routing only.** It runs in server mode and `identify` feeds it each peer's listen addresses. A crawl asks the known peers for those closest to the node and connects to those it finds, and gossipsub then meshes with the connected peers that serve the same shard. The routing table is never read as membership: a node's table holds a fraction of the shard, and a count of live workers comes only from a roll call or from the authority's registrations (§12.4, §14.3). A second Kademlia behaviour stores Task records (§8.6).

### 11.2 The peer book

Each worker's swarm task keeps a peer book: which peers are connected, the address of record for each, its own address, which peers share its shard's gossip topic and mesh, and the traffic it has carried. A peer's address is taken, in order of preference, from its `identify` listen addresses, the address this worker successfully dialed, the address the peer stamped on a roll call or reply it sent (accepted only when the stamp names the peer that gossip or the connection vouches for), and, as a last resort, the source address of an inbound connection, which is never handed to a joiner or dialed by a send. A worker given an external address (`WorkerConfig::with_external_address`, for a wildcard bind, NAT or a container port mapping) gives only that one: in its registration, its leader hint, the JOIN pointers it hands out and its message stamps, and `identify` advertises it and no listen address.

### 11.3 Redial schedule

A dropped connection is redialed only when the peer was in the node's gossip mesh for its shard, because a peer cut off from the mesh never hears a roll call and gossipsub never dials. Any other connection reopens on its own when the next send dials it, or was not needed. The default schedule tries again after one second, backs off exponentially to 30 seconds over eight fast attempts, then retries once a minute for as long as the peer stays away. A blocked peer stays eligible and keeps failing and backing off. The first retry lands well within a suspicion timeout so that a follower whose mesh link dropped is back before the next election needs it.

### 11.4 Routing refresh

A worker's JOIN connects it to its seed and its leader only, so a burst of joiners would form a star that no roll call crosses once the leader is gone. The driver therefore crawls again once a change to a worker's view of its shard (its leader, the configuration it holds, and whether it is admitted) has held still for a quarter of a suspicion timeout, or a period after that change if the view never settles, and otherwise once a period; the period defaults to ten suspicion timeouts and is never under one second. A completed crawl is reported to the node. A draining leader waits for every other voter to report a crawl since its admission, up to `drain_wait_limit`, before it leaves, and a node in `ROLL_CALL` or `NO_QUORUM` that has reached no shard peer for a suspicion timeout searches for a leader again, through the authority's registrations and then the seeds (§12.3).

---

## 12. Leader liveness and ordinary election

### 12.1 Direct worker-to-leader control path

Every active worker maintains a logical heartbeat/control relationship with the leader.

A follower does not hold the member list. It knows its shard's current configuration only as a **generation** and a voter count (two counts, old and new, while a change is in flight), plus its own **admission generation**: the generation at which it became a voter. Only the leader holds the members, with each one's admission generation, and the workers waiting to be admitted. These are the terms (§4.5):

- A **`Generation`** is the triple (recovery epoch, term, counter), compared in that order. The term is that of the election or leader that announced the configuration, and the counter rises by one with every configuration change (each phase of an admission batch, each removal batch, each election).
- A **`Configuration`** is a generation, a base generation and either a single voter count or, for a joint configuration, the counts of its new and old sides (with the old side's base and generation, and the generation joiners are admitted at). Every change a leader makes re-bases the configuration at its new generation and re-admits there the members it counts, so the base is the generation of the latest change. A worker is a voter of a single configuration when its admission generation lies between the base and the generation, inclusive; on a joint one's old side it counts by the admission it held before (its prior admission). A worker with no admission generation, such as a joiner, is a pending member: it claims work and heartbeats, but it is no voter.

Representative heartbeat (the fields that matter here):

```text
WORKER_HEARTBEAT {
    worker_id
    incarnation_id
    shard_id
    recovery_epoch_seen
    recovery_epoch_lineage
    term_seen
    available_capacity
    active_task_runs_digest
    newest_accepted_ack        // term and send token of the newest leader ack it accepted
    configuration_generation   // the configuration it holds, if any
    admission_generation       // its own, if any
    send_token                 // its monotonic clock reading, echoed back by the ack
    routing_crawled            // whether it has finished a routing crawl since admission
}
```

Representative response:

```text
LEADER_HEARTBEAT_ACK {
    shard_id
    leader_id
    recovery_epoch
    recovery_epoch_lineage
    term
    configuration              // the leader's current configuration
    recipient_admission        // the recipient's admission generation in the leader's roster
    send_token
    heartbeat_token            // the send token of the heartbeat this ack answers
}
```

The ack is how a configuration change reaches a follower, and the heartbeat that echoes the newest ack is how the leader learns that a follower holds it: a leader counts a configuration change as committed once a majority of each of its sides has echoed exactly it. The same echoes keep the leader's **quorum-contact lease**: the leader may act only until the earlier of its recovery fence (§14.4) and the send time of the newest acks a majority of its configuration has confirmed, plus a suspicion timeout less a share for clock drift. A follower whose contact with its leader is fresh (it heard from its leader within a suspicion timeout) refuses to answer a roll call or grant a vote, so one worker with a flaky link cannot depose a healthy leader, and no majority can grant a vote while the old leader's lease runs.

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
    configuration_generation
    term_seen
    leader_term
}
```

`SELF_REMOVE` is sent to the draining worker's own leader alone. It is irrevocable for that worker incarnation. It means:

> This incarnation permanently withdraws from election participation.

Only the leader applies it, and only if it is addressed to the leader's own term and the sender has seen no later term than the leader's (a worker that has may have voted in a later election whose quorum counted it, and shrinking the electorate as well could let two quorums of one term miss each other). The leader applies every removal that has passed this check and arrived since its last announcement together, in the one next generation it announces, with no commit round; until it does, the departing workers still count, which is conservative for quorums. A removal that fails the check leaves the worker in the roster until the next election founds a configuration without it, or the authority path counts it out. A draining leader does not leave by announcement. It hands leadership over, then sends `SELF_REMOVE` to its successor like any follower (§18.2).

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

A draining leader keeps leading until every other voter has reported a routing crawl since its admission, bounded by `drain_wait_limit` (ten suspicion timeouts by default), and then leaves. Leaving earlier would strand a worker that knows no one but this leader: workers find one another only through their routing crawl. A voter that never reports cannot hold the shutdown up beyond the limit. The hand-over waits behind it.

A node in `ROLL_CALL` or `NO_QUORUM` that has reached no shard peer for one suspicion timeout searches for a leader again: it reads the authority's registrations and then asks the seeds, so a node stranded without a leader reconnects to whoever leads.

### 12.4 Roll call

When a follower's leader falls silent for a suspicion timeout, jittered per worker and per term so that workers do not all call at once, it starts a roll call: a census of its shard that the election then votes on. The **initiator** publishes the call on the shard's gossip topic (§11.1) and every worker that takes part answers it directly:

```text
ROLL_CALL {
    shard_id
    term                    // the term the initiator contests
    configuration           // the initiator's configuration
    timestamp_millis        // its wall clock, to break ties only
    initiator_id
    initiator_address
}

ROLL_CALL_REPLY {
    shard_id
    term
    initiator_id
    responder_id
    responder_address
    admission               // the responder's admission generation; absent for a pending member
    prior_admission         // the one it held before an election that founded a joint configuration admitted it
    configuration_generation
}
```

The initiator counts itself as a respondent. A respondent whose admission generations make it a voter of the call's configuration, on both sides of a joint one, is a **returning voter**; every other respondent, a pending member among them, is a **new voter**. The call collects replies until its deadline, which is configurable (and widens, up to a suspicion timeout, over consecutive calls that fell short of a quorum). At the deadline:

- if the returning voters among the respondents are a quorum of the call's configuration (of both sides, for a joint one), the initiator stands as the candidate (§12.5, §12.6);
- if they are not, the initiator goes to `NO_QUORUM` and tries again at a later term after a fresh jittered suspicion timeout. With an authority configured it also takes the authority path, counting the call's respondents (§14.3). The same happens when the authority holds a later recovery epoch of the initiator's own lineage: the call is then only a census and never stands the initiator as a candidate;
- an initiator that abandoned its call for a better one, or that finds the term it contested already holds a vote or a leader, returns to `LEADER_SUSPECT` instead.

A worker answers a call only from a state that takes part in elections (`ACTIVE`, `LEADER_SUSPECT`, `ROLL_CALL` or `NO_QUORUM`). Otherwise it refuses, naming why, in an `ELECTION_REJECT` that carries its highest term seen, its configuration, the leader it follows if it holds one, and its recovery epoch and lineage. The reasons are: a stale term, a stale generation (the call ran under an older configuration, or an older recovery epoch than the worker's), a leader still valid (the worker heard from its leader within a suspicion timeout), a worse call than one already answered, and a state that takes no part. A call from a later recovery epoch than the worker's own is ignored, not refused. The refusal is how an initiator learns that a leader is alive, or that it is behind.

**Competing calls.** A worker answers the first call it accepts for a term, and any later call for that term that ranks better; it refuses a worse one. Calls rank by the initiator's wall-clock timestamp, then by its `WorkerId`, lowest first, so a tie always has a winner. The timestamp only breaks ties: clocks that disagree bias who wins a tie but cannot break safety, so ordinary elections need no clock synchronization. A worker that has answered another's call does not start its own until that call resolves (the leader's ack for the answered term or a later one arrives), or, if the caller died, for a bounded wait.

If the existing leader becomes verifiably reachable before the election begins, workers return to `ACTIVE`. A node that accepts a leader's ack forgets the roll calls it made or answered for terms above the term it now follows (except where it granted a vote): those were made on a suspicion the live leader disproved, and kept they would make the node refuse the call that elects the leader's successor.

A roll call is answered only from sound state. A reply carries the configuration generation its sender holds, and a node refuses a call built on an older configuration, so a seed taken from a stale census is never counted. A promise of admission a worker holds, and any promise round for a configuration that has since removed a worker, are void once the removal commits.

**Epoch order across lineages.** A recovery epoch is a number within a lineage, and epochs are totally ordered by number and then lineage. Whenever a worker compares an epoch it hears of with its own (in election messages, join pointers and Task records alike), a higher number is newer, and at one number the higher lineage is; the lineage is a fixed tie-break, not an age. A worker whose own lineage has a later epoch than the one it followed rejoins it, and a refusal of an election message names the leader, so a node that missed a certificate learns who leads.

**Admission.** A joiner is admitted in two phases and at a pace. The leader first promises each joiner that has confirmed a recent ack its admission at a generation of the leader's term; once every one holds its promise, it starts a batch that admits exactly those workers. Nothing starts until every member of the just-committed configuration has echoed it, so a member left a generation behind cannot be needed for a quorum nobody can reach. A member whose heartbeats arrive but which confirms none of the leader's acks blocks admission only until a suspicion timeout and a reconnect timeout have passed since it last confirmed one. The leader then removes it, one voter per configuration change and only when the voters that hold the current configuration, the leader among them, are a majority of the voters of every configuration a muted or removed voter may still stand at. That bound keeps two successive removals from letting the removed voters, a majority of the old configuration, elect a second leader. A removed worker may rejoin as a pending member. A silent member is reported lost but not removed.

### 12.5 The initiator is the candidate

There is no separate candidate selection. The initiator whose roll call stands is the candidate for the term it contests, and the ranking above decides between initiators that call for the same term. The call's rank is deterministic, so choosing the candidate needs no hash and no extra round.

### 12.6 Voting

The candidate asks its respondents for their votes over the connections their replies opened:

```text
VOTE_REQUEST {
    shard_id
    recovery_epoch
    recovery_epoch_lineage
    term
    candidate_id
    roll_call_generation    // the generation of the configuration the call ran under
}
```

A voter grants at most one vote per term, only to the initiator of the best call it answered for that term, and never switches a vote it has granted. It refuses with a reason (the refusal shapes of §12.4) when the term is stale, the recovery epoch is not its own, it holds a newer configuration than the call's, the call is not the best it answered, it already voted in the term, or its leader contact is fresh. The candidate counts itself as a respondent and grants itself the first vote. A candidate that is asked for a later term, or that sees a later one in any message, steps down.

The candidate **wins** when both of these hold:

- the voters that granted it, new voters included, are a majority of all respondents so far, itself included;
- the returning voters among those that granted it are a quorum of the call's configuration, on both sides of a joint one.

A respondent that replies after the candidate stood joins the census, is asked for its vote, and raises the number of respondents a win needs a majority of.

**What a win does.** The respondents of the winning call found the next configuration. It is a joint configuration, so that a partitioned election under the old configuration cannot win beside it: its old side is the configuration the call ran under, where each respondent still counts by the admission it held before, and its new side has one voter per respondent, every one admitted at the new generation (the current recovery epoch, the election's term, the call configuration's counter plus one). The winner moves to `LEADER_RECONCILING`, leads the joint configuration, certifies it to every respondent:

```text
ELECTION_CERTIFICATE {
    shard_id
    recovery_epoch
    recovery_epoch_lineage
    term
    leader_id
    configuration
    recipient_admission
    recipient_prior_admission
}
```

and commits the new side alone once a majority of each side has echoed exactly the joint generation in a heartbeat confirming one of its acks. A worker that missed the call is no voter of the new side until the next election it answers. An election won under a joint configuration that is not yet committed founds nothing new: its winner re-stamps that configuration at a generation of its own term and commits it. A candidacy that is not won within as long again as the roll call's deadline returns to `LEADER_SUSPECT` and calls again at a later term.

Ordinary election does not need Redis if a quorum of returning voters exists.

---

## 13. Leader reconciliation

A newly elected leader must not immediately assign work.

Reconciliation rebuilds composites as it rebuilds tasks. A worker reports the composite records and pages it holds. A certified run that carries a returned step and has no composite yet gets one, derived from the run alone. A worker's report keeps `delivered` and any returned step for a run the new leader adopts. The new leader then drives each unfinished composite on, with the same derived ids.

It asks its voters and pending members for reconciliation reports. A worker answers a requester only if it is the leader the worker follows, or presents an election certificate for a term at least the worker's; only the exit rule below counts voters. A report is paged and holds:

```text
RECONCILE_REPORT {
    runs[]      every run the worker holds, each with the claim it was granted under
                and its state (claimed, running, succeeded, failed), result digest, failure kind
    keys[]      a summary per Task record held: version, input digest, latest run,
                placement, finished, task definition, coalescing key
    last        whether this is the final page
}
```

It combines:

```text
live worker reports
+
DHT Task records
```

to rebuild authoritative state.

**Per-key certainty.** The leader knows a task's newest record once `r - w + 1` of the holders of its latest reported placement have answered, so that a silent holder cannot hide a newer revision, or once every placement member still in the configuration has. A key short of that is uncertain: it is not scheduled, cancelled or republished, and requests for it are answered `NOT_READY`. A holder that rejoins keeps its store, and its stale copy counts only as the revision it holds. A stub left by a joint write (§8.6) names the placement the record moved to, so a revision on a placement the first reports did not name is still found.

**Exit rule.** The leader stops collecting once every voter has answered, or a quorum has and a suspicion timeout has passed since the first ask. Answers that come later are adopted after the leader leads, by the same rules, and workers reported lost meanwhile are applied as ordinary losses at the grant, except one that answered.

Representative decisions:

```text
Record says Run 42 RUNNING on A
A says Run 42 RUNNING
=> adopt same assignment

Record says Run 42 RUNNING on A
A alive but reports no Run 42
=> retriable: mark LOST and create child run
=> non-retriable: the run is ORPHANED; an ephemeral one is LOST with no replay


Record says Run 42 RUNNING
A reports SUCCEEDED but uncertified
=> validate run still authoritative
=> accept completion and certify if valid

A reports a run whose record the leader lacks
=> rebuild the run from the claim the worker holds

A holder never answers
=> an ordinary worker loss: the newest lost generation of a coalescing key is replayed,
   a superseded one is not
```

**Supersession.** A supersession writes the newer generation's record before it marks the older superseded. A leader lost between the two writes leaves a pair that the new leader finishes, writing the older generation last.

**Republish before the grant.** Before it is granted, the new leader writes every record of the rebuild again at its own term. Those revisions are newer than anything the old leader could still write, which fences the old leader's late writes, and any write a holder refused is repeated until it is stored.

**After the grant.** A worker's heartbeats carry a digest of the runs it holds. When the leader's view of a worker has disagreed with that digest for long enough, it asks the worker for its runs again.

Only after reconciliation:

```text
LEADER_RECONCILING -> LEADER
```

and new claims resume.

---

## 14. `NO_QUORUM` and forced recovery

A peer-only protocol cannot safely allow an arbitrary surviving minority to redefine quorum after a partition. The minority cannot distinguish "other nodes crashed" from "other nodes are alive but unreachable."

Therefore `NO_QUORUM` is recoverable through multiple paths, but it cannot simply invent a smaller electorate.

While `NO_QUORUM`, a worker:

- keeps answering other workers' roll calls and granting their votes, and starts a roll call of its own every jittered suspicion timeout;
- returns to `ACTIVE` as soon as a leader's ack reaches it;
- searches for a leader again, through the authority's registrations and then the seeds, once it has reached no shard peer for a suspicion timeout (§12.3);
- with an authority configured, runs the authority path after each roll call that falls short (§14.3);
- alerts/emits metrics.

A `NO_QUORUM` worker never reads the DHT's routing table as membership (§11.1) and applies no `SELF_REMOVE`: only a leader does (§14.2).

### 14.1 Exit path A: peers return

```text
NO_QUORUM
    -> enough members reachable
    -> ROLL_CALL
    -> ordinary election
```

### 14.2 Exit path B: graceful self-removals shrink the electorate

```text
leader applies SELF_REMOVE
    -> the next generation it announces has a smaller voter count
    -> a later election is counted against the smaller electorate
```

Only a leader applies a `SELF_REMOVE` (§12.3), so this path protects a shard that still has a leader: when most of its workers are scaled down, the removals shrink the electorate before the leader is lost. It does not rescue a leaderless `NO_QUORUM` shard. No worker in `NO_QUORUM` applies a removal, so with no authority such a shard leaves `NO_QUORUM` only once enough of its peers return (a product limit). With an authority, the departed workers' registrations lapse and the authority path (§14.3) counts them out.

### 14.3 Exit path C: forced reconfiguration through CoordinationAuthority

Redis or another configured authority stores, per shard **name**, a shard record `{shard_id, recovery_epoch}`: which incarnation of the shard lives under the name, and that incarnation's recovery epoch, which is a number and a lineage. Registrations, the fence and the leader hint are kept under the name too, and tagged with the `ShardId`. The lineage is drawn fresh whoever founds a shard, kept by every epoch recovered from it, and put back unchanged when a leader republishes its epoch after the authority lost its data; it tells two epochs that share a number apart. The authority also holds each worker's TTL registration and the leader's recovery fence (§14.4).

Every election message that is authoritative carries the recovery epoch and the term (leader acks, vote requests and grants, certificates, refusals). `ClaimResponse` deliberately carries neither: the claims an earlier leader grants are bounded by that leader's lease (§12.1), so a late claim response needs no epoch to reject it.

Forced recovery runs on a node in `NO_QUORUM` whose roll call has just fallen short, and it advances the recovery epoch on the strength of the roll call's respondents:

```text
attempt_forced_recovery(respondents):
    if state != NO_QUORUM:
        return

    live = authority.live_registrations(shard_name, shard_id)

    # The authority gives no authoritative count until one full TTL has
    # passed since it started or last lost its data: a few registrations
    # could pass for the whole shard.
    if live.authoritative_count() is None:
        give_up()
    # A node the authority does not list as live has no standing.
    if self not in live:
        give_up()

    counted = respondents intersect live
    if len(counted) < floor(live.authoritative_count() / 2) + 1:
        give_up()                    # other registered workers were not heard from

    held = authority.read_shard(shard_name)

    if held is missing:
        abandon_shard()              # state = STOPPED
        return
    if held.shard_id != shard_id:
        abandon_shard()              # another incarnation
        return
    if held is of another lineage, or lower in own lineage,
       or own lineage is unknown:
        rejoin_at(held)              # swapping from it could reuse a number
        return

    swapped = authority.compare_and_swap_shard(
        shard_name, expected=held, new=held with epoch held.epoch.next())

    if lost the race to another swap:
        rejoin_at(the epoch that won)
        return

    state = CANDIDATE                # while it waits out the fence
    acquire_fence(shard_name, held.next())  # FenceHeld: ask again after what remains
    on fence granted:
        lead a configuration founded at the new epoch
```

Respondents must be a majority of the live registrations, so a minority that can reach the authority cannot take over a live majority: the workers it did not hear from are still registered and still counted. Roll-call respondents are counted only if they are also live in the authority's view, and the node itself must be among them. A warm-up is never trusted. The shard's recovery epoch swap is what lets only one worker recover it from a given epoch, and the fence is what keeps a leader of the old epoch and the new leader from acting at the same time.

The configuration the recovery founds is a single configuration, at the generation (the new epoch, the roll call's term, the roll call configuration's counter plus one), with one voter per counted respondent, each admitted at that generation; every other respondent is a pending member. It needs no joint configuration, because every generation of the new epoch outranks every generation of the old one, so no worker of the new epoch counts a quorum of the old. If the fence is refused for good (the epoch moved on), the candidate goes back to `NO_QUORUM`, or rejoins when the epoch it finds is one it cannot recover from.

A single surviving worker may recover if it is a majority of the live registrations, which happens once the registrations of the others have lapsed.

The `ShardId` travels on election and JOIN messages only. Task records, claims and step records do not carry it, because records are placed, reconciled, claimed and stolen only among the voters of the configuration (by `WorkerId`) and task ids are UUIDv7, so a new incarnation never asks for or places an old one's records.

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

A restart or failover of the authority may have lost acknowledged writes (an older snapshot, a lagging replica), so the provider treats a new server run id like lost data for the fence: no fence and no authoritative count for one TTL. The shard keeps working, and its leader may fence itself until the window ends.

### 15.3 Redis cleared while live shard has quorum

Missing external directory state is reconstructible.

The live leader republishes, within a third of a TTL, its shard record (create-if-absent, unchanged id and epoch) and its leader hint; every worker re-registers on its renewal; the task-to-shard cache is Phase 8. The registrations are the shard membership hints. The warm-up keeps a bootstrapper from founding a second incarnation before the republish lands, and a bootstrapper asks the hinted leader first.

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

An empty authority is a new incarnation's: a founding against it mints a new `ShardId` (the name, `/`, a UUIDv7). A re-found (record present, no one registered, warm-up over) keeps the record's `ShardId` and draws a fresh lineage one epoch on. A shard that still has quorum after a flush keeps its id and republishes it (§15.3).

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
    -> hand off the Task records it holds
STOPPED
```

The drained worker asks the leader it followed where each of its records goes now, writes each copy to the holders it names (as the record is held, naming the worker as its publisher, so the holder keeps it whatever holders the record names), waits for the acknowledgements, and writes again what was refused, until the drain wait limit. A copy a holder refuses because it holds a newer revision needs no handing over.

Termination is soft and then hard, similar to a warm/cold shutdown in Celery. A soft terminate starts the drain above and lets running TaskRuns finish within a configured grace period; when the grace period elapses, running task subprocesses are cancelled cooperatively and then killed, and their TaskRuns follow the worker-loss semantics of §3.2.2. `Settings.drain_grace` (`KABUDACHI_DRAIN_GRACE`, off by default) gives running runs that long to finish after the first signal; then the worker stops them as a second signal does. It applies while `run()` serves with no `main`. A second SIGINT or SIGTERM still stops at once (§6.2).

### 18.2 Leader shutdown

A leader hands off proactively instead of waiting for a timeout:

```text
LEADER
    -> stop granting claims
    -> ack every follower with leader_draining
    -> TransferLeadership to the voter that confirmed its ack most recently (ties to the lowest id)
successor stands at once; voters that saw leader_draining vote for it
    -> old leader stays reachable for reconciliation and record hand-off
    -> once the successor is authoritative: SELF_REMOVE through it, hand off records
STOPPED
```

If no successor takes office within one suspicion timeout, the leader leaves as a follower would. A hand-over happens only under a committed single configuration. A transferring leader still answers roll calls and votes, so a shard of two can elect its successor.

**Sole voter.** A sole voter that drains admits any pending member and hands over to it. With no one to hand to, it waits for another voter, at most `drain_wait_limit`, then leaves and logs, at error level, every record that leaves with it. `drain_wait_limit` is the operator's override.

The one-node runtime drains with no wait.

A draining leader waits for the other voters' routing crawls before it leaves (§12.3), and places its records among its other placeable voters.

### 18.3 SIGKILL

No graceful guarantees apply.

Surviving peers recover through election/reconciliation: the leader reports the holder lost and repairs the records it held, writing them to a full placement without it. Complete live-cluster loss falls back to DR and/or catastrophic recovery semantics.

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

Task execution exposes context through Python `contextvars`, set where the body runs (in its asyncio task for an async body, in its thread for a synchronous one):

```text
task_id
run_id
task_name
attempt
shard
worker_id
trace_id
span_id
```

`kabudachi.current_context()` returns a `RunInfo` with these fields inside a task body, and `None` outside one. `kabudachi.logging.ContextFilter` adds them to log records. `trace_id` and `span_id` are empty strings until Phase 7. Native events reach Python `logging` under `kabudachi.native.<crate>.<module>`; `run()` reads their levels when it starts.

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

Separately from these observer hooks, the runtime offers execution lifecycle hooks that run where task bodies run, in the task subprocess (or in the worker's own process when the process count is 0): `@kabudachi.process_init` once when a subprocess starts, before it takes a run, and `@kabudachi.before_run(context)` and `@kabudachi.after_run(context, outcome)` around each run. The context (`kabudachi.RunContext`) names the task, the run and its attempt; the outcome is what the body returned or the exception it raised. Each hook may be sync or async. The two run hooks take an optional `queues=` that scopes them (by default, every queue), and run on the thread the body runs on, so per-thread state such as a database connection is the body's. They exist so applications can prepare and clean up per-process state, for example returning database connections after each run. They are declared at package scope, where tasks are, so every subprocess that imports the task modules has them.

- A `process_init` that raises fails the worker's start with `StartupError`, or delays a later replacement subprocess with the same growing wait as a crash.
- A `before_run` that raises fails the run under its retry policy; the body does not run, and the `after_run` hooks see the error.
- An `after_run` that raises is logged, the other `after_run` hooks still run, and the result stands; the subprocess is recycled (§6.2), since what the hook did not clean up may be left in it.

Framework-specific recipes, such as connection handling for a particular web framework, belong in documentation and cookbooks, not in the architecture.

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

`health_listen` (off by default) serves `/healthz` and `/readyz` from the native layer on their own listener. Each answers 503 when its condition does not hold. `/healthz` is 200 while the worker's loops are alive. `/readyz` is 200 once the task registry is loaded and the node will take work: a leader once it has reconciled and leads; a follower once a leader holding a grant has echoed one of its heartbeats, the same gate its claims wait behind. A follower of a leader still reconciling may read ready; its claims are refused as not ready until the leader finishes. The listener binds before the worker starts, and a port already in use fails `run()` at start. `/metrics` is Phase 7.

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
    shard peer awareness (roll call, peer book)
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
- gossipsub and routing-table communication;
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
ResultBackend
```

One Redis implementation may satisfy all three, but the interfaces are not merged merely because a common backend can implement them. The shipped result backend is the `ResultBackend` trait (`store` and `load` over opaque entries); the valkey adapter is a separate crate from the authority adapter.

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

Important caveat from the current Rust libp2p Kademlia documentation: Kademlia does not automatically infer all peer addresses; Identify or another discovery mechanism must be integrated deliberately. That aligns with this design, where Redis/CoordinationAuthority provides cold bootstrap and the shard's gossip roll call and peer book provide hot peer awareness.

The project should prototype both:

1. using libp2p as the general peer transport plus Kademlia; and
2. using a narrower custom RPC transport with a DHT library/implementation.

libp2p is capable, but its abstractions may be heavier than necessary if our network protocol is tightly controlled.

**Phase 2 design note:** Phase 2 chose between the two prototype paths above — libp2p as transport (TCP/Noise/Yamux, `identify`), but narrow: three `request_response` behaviours, for election, bootstrap join, and claim arbitration, plus `gossipsub` for the election roll call and `kad` for peer routing. Phase 3 added a second `kad` behaviour, apart from the routing one and on a protocol only the shard's workers speak, that stores Task records (§8.6; `net/src/swarm.rs`'s `Behaviour`). The `net` crate implements this. This is an implementation decision, not a change to this section's candidate status.

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
- result certification digests.

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

The health endpoints use `axum` with default features off (`http1`, `tokio`), on the bindings' Tokio runtime.

This is infrastructure convenience, not a protocol dependency.

### 23.15 Packaging

Bazel's `py_wheel` builds the wheel (`//runtime:kabudachi_wheel`): the pure-Python package and the `_native` extension, built for the stable ABI (`abi3`, CPython 3.12 and later), with metadata that repeats `runtime/pyproject.toml`, extras included. The Linux wheel is set to the platform tag `manylinux_2_34_x86_64` in `runtime/BUILD.bazel`, and the macOS arm64 wheel to `macosx_11_0_arm64` (the extension is linked with a minimum macOS version of 11.0). Other platforms build no wheel. The Linux tag follows the glibc of the machine that links the extension; a Bazel test (`//runtime/tests:test_wheel`) compares it with the tag `auditwheel` finds in the extension's symbols and fails if they differ. The same test checks the wheel's metadata against `pyproject.toml`, installs the wheel into a clean virtual environment, and runs a flow on one worker and across a networked shard of two. It is a Bazel test, not a CI job. Maturin is not used: Bazel already builds the extension.

Packaging is part of the MVP, not something deferred until after the distributed system is written. If installation is painful, the project loses one of its primary ergonomic goals.

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

It is imported only when selected (the `pydantic` extra, `kabudachi[pydantic]`).

The pydantic serializer handles a model, a list of models, or nothing, and reads a model's version through its family's `version_path` (§7). It encodes canonical JSON (sorted keys, no whitespace), so equal values are equal bytes; for that reason it refuses a model with a set field, which has no stable order. Pydantic semantics do not leak into the generic task protocol.

### 24.4 Dataclasses

The dataclass serializer encodes standard-library dataclasses as canonical JSON, a lightweight alternative that needs no extra package (the `dataclass` extra adds no dependencies). It handles a dataclass of plain fields (`str`, `int`, `float`, `bool`, `None`, other such dataclasses, `list`, `dict[str, ...]` and `X | None`), a list of them, or nothing. It reads a version through `version_path` the same way the pydantic serializer does.

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

The tests are organised as one Bazel `rust_test` target per crate (`//core:core_integration_test`, `//net:net_integration_test`, `//testkit:testkit_integration_test`), each an integration binary whose areas are modules, and a name filter runs one area. `core` has the areas `configuration`, `election`, `proptest`, `reconcile`, `records`, `scenario` and `scheduler` (`core/tests/<area>/`), and `net` has `bootstrap`, `claim`, `discovery`, `driver`, `election`, `join`, `lifecycle`, `reconcile`, `records` and `transport` (`net/tests/<area>/`), with no in-crate unit tests. The `testkit` crate is the shared test seam: the faulting authority (`FaultingAuthority`) and the step record with the invariants asserted over it, which the core simulator and the real-socket net tests both use. It also holds the simulated Task record store (`RecordSpace`) the core simulator writes to.

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
gossip mesh fragmentation
SELF_REMOVE messages delayed or duplicated
```

The 50/50 partition is `scenario_no_quorum::a_50_50_partition_elects_and_grants_on_neither_side_and_heals_to_one_leader`. The 51/49 partition and the leader isolated with a minority are `scenario_partition::a_51_49_partition_keeps_the_majoritys_leader_and_heals_without_deposing_it` and `scenario_partition::a_leader_cut_off_with_a_minority_loses_its_lease_and_both_follow_the_majoritys_new_leader`. All three are in `core/tests/scenario/`, run by `//core:core_integration_test`.

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

These are part of release hardening (§27).

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

Each bullet names the test that covers it:

- Python interpreter shutdown: `//runtime/tests:test_native::test_the_interpreter_exits_cleanly_while_run_and_its_native_threads_still_run`.
- Callback GC behavior: `//runtime/tests:test_session::test_a_callback_is_kept_alive_by_the_run_even_if_the_handle_is_dropped`.
- Weak lifecycle (observer) hooks arrive with the event API in Phase 7.
- Task callable references: tracked under post-implementation testing.
- Many concurrent Rust->Python scheduling events: `//runtime/tests:test_session::test_ten_thousand_expiries_decided_together_reach_every_waiting_handle` (one worker; the networked outcome queue under a burst is tracked under post-implementation testing).
- Cancellation races: `//runtime/tests:test_session::test_a_task_cancelled_after_it_is_claimed_but_before_it_is_reported_started_says_cancelled` and `test_a_cancel_this_process_asked_for_suppresses_the_runs_outcome_before_the_leaders_notice`.
- Sync functions in the executor: `//runtime/tests:test_session::test_synchronous_tasks_run_as_many_at_once_as_the_concurrency_allows`.
- Async functions and contextvars: `//runtime/tests:test_run::test_a_context_filter_puts_the_running_task_on_each_record_its_body_logs` (async and sync bodies) and `//runtime/tests:test_pool::test_a_body_in_a_task_process_sees_its_run_in_context_variables_async_or_sync`.
- Subprocess crashes: `//runtime/tests:test_pool::test_a_body_whose_process_dies_is_replayed_in_a_replacement_unless_its_task_is_ephemeral_or_not_retriable`.

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

Deferred out of Phase 1: compaction of retained coalescing chains (Phase 3), observer hooks `kabudachi.events.*` (Phase 7), `.reduce`, type-compatibility validation, migrations (§7) and queue-subscription enforcement (all Phase 6).

### Phase 2: multi-worker ordinary election

Implement:

- peer identity;
- direct leader heartbeat;
- roll call, published on a gossipsub topic;
- voting;
- worker-pull claim arbitration.

Still avoid sharding.

Phase 1 left a no-peers message sink, a no-authority coordination authority and the election tick loop in place as single-node placeholders, each with one adapter until this phase. Phase 2 gave peer messaging and the coordination authority their second real adapter and resolved that question, as follows.

The module that held them is gone. Its message-sink placeholder became `core::election::DropMessages` (`core/src/election/carry_out.rs`), which drops every message for a node with no peers, and the single-process Python runtime's wiring lives in `bindings/src/local_node.rs`. That runtime's instant (zero suspicion timeout) self-election is a deliberate product choice for it, not the generic behaviour of a one-member electorate. The Phase 1 `NoAuthority` coordination authority was removed rather than moved: the single-process runtime starts its node with no authority timings, so the node never asks for an authority call. The name now belongs to `core::election::NoAuthority`, the authority performer that runtime hands `core::election::carry_out`, which answers any call at once as unavailable. The generic multi-node case is `net/src/bootstrap.rs`'s `bootstrap` cascade (seeds, then the coordination authority's registered peers, then ownership of the shard, or, with no authority, founding it alone once the seeds have stayed silent for `seed_rounds` rounds), where a lone node still waits out whatever suspicion timeout it was configured with. A worker with seeds that all stay silent can therefore found its own shard; the split that causes is bounded by `seed_rounds` and ends when the shards converge (§17).

Both runtimes drive the node through `core::election::carry_out`: `bindings/src/election.rs`'s `run_election` on the single-process node, and `net/src/driver.rs`'s `run_driver` on a real-transport node, each on its own tick.

`kabudachi_net::worker::Worker` wires these together: its `run` calls `bootstrap`, starts the node with `WorkerNode::start` and hands it to `run_driver`.

The bootstrap join changes no one's configuration by itself. `bootstrap` only returns an `Entry`: for a join, `Entry::Joining` with the leader a seed or registered peer pointed at, found through the net `join` module (`net/src/join.rs`: `ask_for_leader` asks peers who leads, and `pointer_for` builds the pointer a node hands a joiner; the leader search in `net/src/leader_search.rs` decides whom to ask: bootstrap queries the configured seeds and then the workers the authority lists, while a rejoining node, which has no seeds, reads the live authority registrations first and then asks those workers). `WorkerNode::start` then makes the worker a pending member of that leader's shard, which no quorum counts; the members that answered it keep their configuration until the leader admits the joiner (§12.4). A join asks a full pass of its candidates and takes the newest pointer by the epoch order of §12.4, not the first reachable one.

The authority step of the bootstrap cascade reads `CoordinationAuthority::live_registrations`, which returns each registered worker's address, so a node asks the registered peers the way it asks seeds. A worker that finds no other worker registered registers itself and founds the shard only by winning a compare-and-swap of the shard's record (created at epoch 0, or one epoch on when re-founding), so two workers never found the same shard. The production implementations are the Redis adapter (`kabudachi_redis_authority::RedisAuthority`, Phase 4) and the in-memory one; tests also use `kabudachi_testkit::FaultingAuthority`.

The one-node window between startup and the worker becoming leader is tested by running the runtime with a non-zero suspicion timeout, which widens the window before leadership so a signal can arrive inside it.

### Phase 3: DHT task dissemination

Implement:

- leader reconciliation (§13), moved here from Phase 2 because it rebuilds state from DHT task records;
- versioned Task records: one record per Task, written whole by the leader on every change, holding the submission (immutable after the first revision) and every TaskRun of the Task; TaskRuns are not stored on their own, so a Task and its runs share one key, one placement and one version (§8.6);
- content hashes (BLAKE3, with the algorithm named) of inputs and certified results;
- placement and replication on the shard's kad record store, handoff of a draining worker's records, and repair when the placeable voters change;
- worker discovery of pending Tasks: own records, then shard peers outward by distance, then the leader's oldest pending tasks;
- compaction of retained coalescing chains.

Compaction runs on a worker, never in the leader's scheduler: a networked worker with an executor claims a compaction run and folds the chain it is handed (Phase 5), and the one-node runtime executes compaction on a free worker place.

### Phase 4: Redis CoordinationAuthority

Implemented:

- the Redis/Valkey adapter for `CoordinationAuthority` (`WATCH`/`MULTI`/`EXEC`, standalone and cluster, key prefix and database, bounded call timeout);
- the `CoordinationAuthority` contract suite run against valkey (standalone under a restricted ACL user, and a single-node cluster), with detection of a flush (a missing sentinel), a restart or failover (a new server run id) and an outage the client saw;
- the entry point `kabudachi_net::worker::Worker` with `AuthorityConfig::new(authority)`, whose timings come from the authority's TTL, and `WorkerConfig::with_external_address`;
- cold bootstrap, flush with a live leader, restart, and quorum loss plus flush, each against a real valkey server end to end (`//net:redis_end_to_end_test`);
- leader hints and their republish after a flush (§15.3);
- a minted `ShardId` per founding, kept by a re-founding, and abandonment of a shard whose name holds another incarnation's record (§15.5);
- the order of recovery epochs by the pair (number, lineage), model-checked, with the lineage on every authoritative election message;
- recovery fence timing (§28.4);
- the shared-instance provider constraints (§9.1).

The Python-hosted networked worker came in Phase 5; the task-to-shard cache, the client shard map and `ShardLostError` are Phase 8.

Open: two Redis test flakes were seen once each and never reproduced or explained. `//redis_authority:redis_authority_integration_test` hung after its tests had passed, before the port-ownership fixes; and a cluster test's first call once answered `Unavailable`. The port-ownership fixes may have removed both. If either recurs, find the cause before rerunning. Both are tracked under post-implementation testing.

### Phase 5: Python subprocess execution

Implemented:

- a configured process count (`processes`, by default the CPUs the worker may use; `0` runs bodies in the worker's process) and bounded concurrency per subprocess (`concurrency`, at most 32 unless overridden), shared by async bodies on the subprocess's event loop and sync bodies through `asgiref.sync_to_async(thread_sensitive=False)` (§6.2);
- task discovery in `spawn`ed subprocesses: each imports the modules that declared tasks and hooks (or `imports`), checks it has every task at the same version and reports ready within `process_start_timeout`; a task or hook declared in `__main__` is refused at start;
- cooperative cancellation and soft-to-hard escalation through the cancel grace: a hard limit settles the run at once and stops its subprocess once its other runs settle, with no neighbour killed as collateral (§28.13); a body raising `SystemExit` or `KeyboardInterrupt` is contained the same way, with `TaskBodyError`;
- a dead subprocess's runs reported lost, one run at a time while the worker lives, and replayed by the delivery table (§3.2.2); a lost compaction reported failed; the subprocess replaced with backoff;
- nested task, flow and group calls from a subprocess, proxied through the worker, and compaction's fold in a subprocess;
- execution lifecycle hooks (`process_init`, `before_run`, `after_run`, with `kabudachi.RunContext`) and recycling (`max_runs_per_process`, `recycle_process`, a failed `after_run`; §28.12);
- SIGTERM/SIGKILL behaviour: subprocesses ignore SIGINT and exit on SIGTERM or when their worker's pipe closes; the worker's first signal drains, a second stops its subprocesses at once (§6.2);
- the executor seam in `kabudachi_net` (`Work`, `Report`, `WorkerConfig::with_executor`, `ExecutorHandle::split`): a networked worker claims only once a leader has heard from it (`WorkerNode::has_contact_floor`), forwards each run's abort deadline (`Work::Abort`, withdrawn by `Work::AbortWithdrawn`), and runs the compaction runs it claims; a leader claims for itself from its own scheduler, submits and cancels through its own driver, and decides its own reports, each held like a remote worker's until a quorum of the task's placement has stored it;
- remote cancellation: a worker holding a run of a cancelled task learns it in its leader's next heartbeat ack, derived from the cancelled state stored in the Task record, so a successor leader tells holders an earlier one could not;
- per-run `reconnect_timeout` (§8.3): the task option, then the queue's (`reconnect_timeouts`), then the shard's, fixed at submission and used by the leader's per-run replay after a worker falls silent and by the worker's abort deadline;
- the networked Python worker: `kabudachi.run()` with `listen` set (and `seeds`, `external_address`, `authority` as a `redis://` or `rediss://` URL with `?key_prefix=` and `?ttl=`, `shard`, and the shard's heartbeat and reconnect timings) hosts `kabudachi_net::worker::Worker`, starts once it has joined and knows its leader, runs whatever that leader hands it, submits through the leader, and on stopping lets its held runs finish and waits until the leader has taken their reports; it needs `processes` of at least 1, so it can kill a body at its abort deadline;
- the multi-process SIGKILL harness (`//runtime/tests:test_networked`): a killed task subprocess, worker and leader, each run certified once, a CPU-bound body that leaves heartbeats and leadership alone, a networked compaction, a remote cancel, and a partitioned worker that stops its run by its abort deadline; and a Python worker cold-bootstrapped against valkey (`//runtime/tests:test_networked_valkey`).

Phase 6 delivers results across workers (§8.5).

Phase 5 had a defect that Phase 6 fixed: a script that ended while `run()` was live with one or more task processes hung at interpreter exit, because the task processes were joined with no timeout. `run()` now registers an exit hook that stops its own task processes and kills any that remain after a short grace.

### Phase 6: the production release

Implemented:

- result delivery from the worker that ran a task to the one awaiting it: certification, conclusions, `QueryOutcome` and the failure matrix (§8.5); durability levels and status reads (§3.3, §8.1); `TaskOrphanedError` and `retriable=False` (§3.2);
- blobs and manifests for payloads above one message, each counted against the memory limits of the processes that hold it, with no fixed cap (§8.6, §3.2.1);
- leader-driven composites: flows, groups, maps and implicit flows as composite records with outcome pages, holding across worker, leader and submitter loss; expansion runs; `TaskFailure` (§3.4, §8.6, §13);
- streaming `.map` with `SlowDown` reaching every submitter (§3.4, §3.2.1);
- `.reduce` as a chain or a chunked tree, and `reducer` (§3.4);
- schemas, migrations, version blocking and type compatibility, with the pydantic and dataclass serializers (§6.3, §7, §24);
- the result backend seam with Redis/Valkey and Python backends, `resume`, and policy inheritance (§3.1, §9.3);
- queue subscriptions, queue options, fair claiming and the router (§5, §8.2);
- leadership transfer (model-checked), drain grace, leaderless drain and the sole voter's wait (§18);
- health endpoints, derived memory limits, run context and native logs, capacity in heartbeats, run times and failure messages, reserved trace fields, and the networked tunables in Python (§19);
- an installable wheel (§23.15);
- DHT-locality load balancing, partition scenarios, submission immutability and FFI stress tests (§26).

Defects found and fixed during the phase:

- The Phase 5 exit hang (see Phase 5): fixed with the FFI stress tests.
- An executor place leak in `bindings/src/networked/handover.rs`: a run that was cancelled, aborted or failed before the executor took it never gave its place back, so later runs waited forever. A networked test that timed out waiting for all three runs to start exposed it. The seam now tracks runs the executor never took (`Seam::untaken`).
- Repair retry starvation: repair retried refused writes in key order, so a composite's write starved behind Task retries and its refusal stayed in the ledger. Retries now go oldest due first.
- An O(N^2) store sweep: expiry now comes from indexes (1,000 items took 1.5 s and now take 0.30 s; 4,000 items took 28 s and now take 1.22 s).
- Result pages stamped unfinished and never rewritten, so holders could not retire them. A composite's end now rewrites every page as finished.
- Test races: a results step in the first result-delivery test, a joiner race in the `led_shard` Redis end-to-end test, a wall-clock bound in the Phase 1 cancel-grace test (`//runtime/tests:test_session::test_cancel_grace_comes_from_the_process_unless_the_task_chose_its_own`) and a placement race in `test_networked`.

That cancel-grace test once measured 0.265 s against a 0.2 s wall-clock bound with 18 targets running in parallel. The bound was a harness timing, not a product limit. The test now compares a 50 ms process grace against a task's own one-second grace, so a loaded host has twenty times the margin; it is fixed, and no open flake remains for it.

The Open notes below each name an owner. Owners are post-implementation testing (load, engineered and flaky cases), Phase 8, or release hardening (the next section).

Open: a Task record read, and so `TaskHandle.status()`, waits out the 5 s kad query timeout while a shard peer is connected but unresponsive (stopped, say). The lookup should finish once the record's placement holders, or a quorum of them, have answered. Owner: post-implementation testing.

Open: large-input starvation. A claimant without room for a large blob input passes it over, and smaller inputs can keep taking that room, so a large input may wait indefinitely under steady load. Aging (raising a passed-over task's priority with its wait) is the known fix. For post-implementation load testing.

Open: a task whose input is larger than every worker's in-flight share waits until its expiry, or indefinitely with none, in a shard where every voter holds every record. In a larger shard, a non-holder can steal it and fail it with `PayloadTooLarge`, so it can fail after its retries. One explicit memory limit per networked shard avoids both. Owner: post-implementation testing.

Open: blob retention edges. (1) A holder that never saw an owner's record drops the owner's blobs only on a second answer of gone from the leader, a grace after the first, and a blob-carried submission is bounded to half a grace; what is left is a submission whose request is still in transit when the deadline cancels it: the leader may handle that request after answering the cancel with an unknown task, record the task, and run it though its handle was refused (no tombstone records the cancel), and the grace and the deadline are fixed constants, so no test can shorten them. (2) The tending task asks about due owners one at a time, each up to 5 s, so a dead named leader slows a tick and the blob re-puts in it; the asks should run concurrently. (3) An owner found finished is never asked about again, so its result blobs stay on a holder that never sees its record until the store drops them with the owner's retention elsewhere. (4) The store has no test seam for the orphan grace, so the leader-asked drop and the submission deadline are unproven by a test. Owner: post-implementation testing.

Open: `//runtime/tests:test_networked` also failed intermittently under load with a result reported unavailable; that symptom is not shown fixed. Owner: post-implementation testing, which diagnoses it.

Open: a drained worker's record hand-off can wait about 5 s before it returns when its copy of a record was already superseded (the hand-off reports stored 0, superseded 1); about a third of runs of the networked leader hand-over test showed it. The hand-off should finish as soon as the superseding copy is confirmed rather than waiting out a timeout. Owner: post-implementation testing.

Open: a node drained while fenced, bootstrapping or joining keeps the request until it leaves that state, so a networked run whose node is fenced at exit stops waiting after four drain waits plus a heartbeat timeout and shuts down with the records it held. A drain in those states should end after the drain wait limit like the other leaderless states. Owner: post-implementation testing.

Open: `runtime/tests:test_networked` failed in 4 of 80 runs in one batch (a cut-off worker, a killed task-process worker, a compaction timeout) while the machine was loaded and passed 100 of 100 in baselines; the cause is not found. Rerun on a quiet machine and trace before relying on it. Owner: post-implementation testing.

Open: an idle worker's claim backoff grows to 5 s (`IdleBackoff::MAX`) with no wake-up on new queued work, so work queued on an idle cluster can wait up to 5 s for a claimant. The fix is to clear the idle backoff when the leader announces pending work. `//runtime/tests:test_networked` failed intermittently under load because of it (a compaction run not claimed while the generation holding the key ran); its tests now hold the key until released and do not depend on the backoff. Owner: post-implementation testing.

Open: aging restarts at a leader change: a task's waiting time is counted from its installation in the new leader's waiting room. Owner: post-implementation testing.

Open: a compaction run for a queue that no compaction runner takes waits until its key's newest generation is claimed whole. Owner: post-implementation testing.

Open: a record that is re-placed off a killed voter stalls. The re-placement is a joint write that needs a majority of the prior placement, and a killed voter stays a member of the configuration until it leaves, so with replication 2 and a dead holder in the prior placement, every later write for that task (a claim included) cannot be acknowledged. The blobs of such a task do follow the new placement; the claim does not go through until the dead voter leaves the configuration. Owner: post-implementation testing.

Limit: with a replication factor of 2, writes stall while a record's holder is dead, until the record is placed again. A majority of two is both holders, so an even factor tolerates no more loss than the odd one below it. Use an odd factor; no fix is planned.

Open: `//runtime/tests:test_networked` is intermittent under load in the composite scenarios: a stopped leader's hand-over sometimes outlasts its timeout. It is not shown fixed. Owner: post-implementation testing.

Open: a leader killed between a pipeline's stages can replay the pipeline's body when the election outlasts the abort deadline of the follower running it: the follower aborts the body when it has been out of contact with a leader for too long, and the new leader replays it, so the body's first run is `LOST`. This is the at-least-once guarantee, not a defect, but the test must allow it. Also worth examining: leader failover sometimes needs a second term (the survivors log `no seed answered a round` and then rejoin), which can exceed that deadline. Owner: post-implementation testing.

Open: a leader's refused write to a record it no longer holds is forgotten (`Repair::check` drops the retry), while the write ledger's refusal of that record stands until the scheduler stops leading. Answers naming such a record keep getting `NotLeader`. Exposure: needs a refusal and the record's removal before the retry falls due; rare. Impact: severe for the clients concerned, which spin on `NotLeader` for the leader's tenure. The fix is to clear the ledger's refusal when the retry is dropped. Owner: post-implementation testing.

Open: no per-peer budget on a leader's record writes (`net/src/messenger/record_queries.rs`), so a burst of writes to one holder is refused wholesale when kad's 32 streams per connection are taken or its 5 s query timeout passes. Exposure: plausible, a leader serving many concurrent completions, or a slow or overloaded holder. Impact: moderate. `Repair` publishes refused writes again, oldest due first, so a refused composite does not starve behind refused Task retries; answers naming a refused record are `NotLeader` until its retry lands. A per-peer budget would shrink the refusal wave. Owner: post-implementation testing.

Open: no net-level burst test of a large map's completion. Wanted: 300 members completed in one `with()`, after which `submit_members` must return `CompositeFinished` within a deadline. Exposure: the burst needs a map of hundreds of members completing together, plausible for any large map. Impact: severe if it regressed, because a composite's final revision could stay unstored and its answers `NotLeader`, and nothing would catch it. Owner: post-implementation testing.

Open: `owners_to_check` (`core/src/task_record/store.rs`) still scans every blob owner on each call, and `tend_blobs` calls it once a second per worker. Fix: index the unseen, unfinished owners by `since`, so a call reads only those due. Exposure: every worker with many blob owners, which a large streamed map or many blob-carrying submissions produce. Impact: moderate, a per-second scan whose cost grows with the owners held. Owner: Phase 8 (scale).

Open: while a takeover has records not known yet, every member that settles in a composite re-checks its members for an uncertain record (`holds_composite_back`). The scan walks all the members (`0..members`) only while reconciliation still has unknown records, so in that worst case settling a large composite costs its member count each time until the records are known. Exposure: a leader change while a map of many thousand members runs, until reconciliation learns the late records. Impact: high at scale, not moderate: for a map of about a million members the quadratic work during a takeover or reconcile window could run long enough to stall the leader. Owner: release hardening, before any real workload runs maps of that size.

Open: a joiner whose leader dies before its first ack has no configuration yet, and with no other live member and an empty or flushed authority it stays `LeaderSuspect`: it neither abandons the shard nor runs a stranded search. An unconfigured node starts no roll call (`core/src/election/node/timers.rs`), and the empty authority read is not acted on for it (`core/src/election/authority_standing/replies.rs`); `StrandedWatch` (`net/src/leader_search.rs`) does not cover it. A restart recovers it. Exposure: rare, the join window plus loss of the leader plus a flushed authority. Impact: that node is stuck until restarted. Fix direction: let an unconfigured `LeaderSuspect` node act on an empty authority read, or extend `StrandedWatch` to cover it; either changes documented behaviour, so it needs a design ruling first. Owner: post-implementation testing, after that ruling.

Open: a released page cannot be re-placed. After the acknowledgement of its stored write, the leader releases a full page (a reduce's item pages, and the full results pages of a large map or group), so `LeaderRecords::repair` (`core/src/task_record/leader_records.rs`) has nothing held to publish again when the voters change. Fix: a driver read-back path that reads a released page from its old holders and writes it to the new placement, through `RecordPorts`. Exposure: a voter change within `result_ttl` of the composite's completion; plausible but rare. Impact: the page is unavailable once its holders are lost, so the composite's results surface as unavailable; resubmitting the work recovers them. Owner: post-implementation testing.

Open: no burst test of the networked runtime's outcome event queue. Wanted: 10 000 `RESULT`, `CERTIFIED` and `OUTCOME` events of a 10 000-task networked run reaching their waiting handles. The one-node burst is a regression test, but the networked queue is a separate path and nothing exercises it under that volume. Exposure: any large networked run. Impact: severe if an event were lost or misrouted, because a handle would wait for an outcome that was decided. For post-implementation load testing.

Open: mixed schema versions in one shard can strand a task. A record's submission digest (`ShardRecord::submission_digest`, `core/src/task_record/key.rs`) is computed over the re-encoded protobuf of its `Task` or `StepDescription`, and prost drops fields it does not know. A shard whose members run two schema versions can therefore compute different digests for one submission, and a holder that has the first revision's digest refuses a later revision with `SubmissionChanged`, so the task cannot be rewritten (a claim or a result included). Exposure: rare, a rolling upgrade that adds a field to `Task` or `StepDescription` while such a record is live. Impact: severe for the task, which stays stuck until it expires. A shard must run one schema version today. Fix: make the digest ignore unknown fields, or tag it with the schema version. Owner: release hardening (mixed-version clusters), with a rolling-upgrade test under post-implementation testing.

Open: the Redis result backend holds one connection behind one mutex (`redis_result_backend/src/lib.rs`), taken for every round trip and bounded per call by `call_timeout`. Exposure: every backend call from one process serialises on that connection: the mirror of each certified plain-task result, composite mirrors and resume reads. Impact: throughput and latency under a burst; and during a server outage, N queued calls each wait up to `call_timeout` in turn (about N x 2 s, on blocking-pool threads), though no task outcome changes. Benign for correctness, degraded for resume latency. Fix direction: a small connection pool, or a connection per call when the single one is busy. Owner: post-implementation testing.

Open: a result of an older class of a schema family that reaches a consumer taking a newer class is handed over as the producer's class, not converted through the migration chain. Owner: release hardening (mixed-version clusters).

Open: protobuf stage inputs carry no version, so a migration reads the member Task's `source_version`, not the producer's. JSON inputs, which carry a `version_path`, are exact. Owner: release hardening (mixed-version clusters).

Open: compaction refuses a chain whose payloads are below the worker's version and fails it with `MigrationError`; the leader skips that key until the waiting generation is replaced or the leader rebuilds. The blocked set lives only in the leader's memory, so a successor may hand a blocked task out again and its worker blocks it once more. Steal and `CLAIM_OLDEST` do not pre-filter versions, which costs one wasted round trip and no run. Owner: post-implementation testing.

Open: `Seam.refused_composites` gains one id for each composite refused for good and is never cleared. Exposure: a process that serves very many refused composites. Impact: slow memory growth. Owner: post-implementation testing.

Open: the result reader has head-of-line blocking, and it and a re-ask's answer can interleave on a multi-threaded runtime, so two remote results that arrive almost together can reach Python in swapped order. A client that misses the leader's push for 30 s and sees no leader change waits until one. Owner: post-implementation testing.

Open: the networked runtime's drain can wait out a delivery's reconnect timeout (`wait_until_reported`); a roll call that outruns the draining acks is refused once; and a leader that leaves through its successor while a third voter is dead leaves one live voter of two (§28.6). Owner: post-implementation testing.

Open: a run whose runner dies after certifying and before mirroring its result to the result backend stays `pending`; `resume` then waits one reconnect timeout and reads the shard, and a small result is lost if the client is gone too. A drain can cut mirrors short. A large composite costs one backend call per part. Owner: post-implementation testing.

### Release hardening

No real workload should adopt kabudachi until this phase is done. It holds the production-readiness gate (§27.2) and the rest of §28.10:

- peer and client authentication and authorization (§28.10). The transport is TCP with Noise and Yamux, so channels are encrypted, but the Noise handshake authenticates only the peer id: nothing decides who may join a shard or submit work. Authenticating peers also closes the trust the Task record store places in its shard (§8.6): it accepts a record write from any peer that passes its key, size and version checks, with no check of the writer's authority and no cap on how many unfinished records a holder keeps;
- authorization boundaries for queues and tasks, replay protection for identified control messages, and a safe serializer policy (§28.10);
- a minimal orchestrator requirements document. The implementation minimizes what it demands of the deployment environment (no mandatory Kubernetes, §2.3) and then defines the small set that remains: how peers discover each other, behavior under address churn, reachability between peers, graceful-termination signals and health endpoints;
- measured submit-to-start latency (§28.11), and the measured time from a worker becoming unreachable to its replacement claim (§8.3);
- mixed-version clusters: rolling upgrades with workers of two versions in one shard. This covers detecting workers whose `queue_options` differ, converting an older-class value for a newer-class consumer, giving protobuf stage inputs a version, and a submission digest that ignores fields an older build drops (`SubmissionChanged`);
- the leader's quadratic `holds_composite_back` scan during a reconcile window;
- Kubernetes and chaos tests (§26.5).

### Phase 7: observability

Prometheus and tracing should exist earlier for development, but this phase hardens:

- documented metrics, and the `/metrics` endpoint (§19.9);
- OTel propagation, filling the reserved trace fields (`trace_parent`, `trace_state`, `baggage`) and `RunInfo.trace_id` and `span_id`;
- dashboards;
- lifecycle event API, with hooks that watch a run's whole lifecycle (observer hooks);
- plugin interface, including task-submitting plugins;
- first-party cron scheduler plugin (§3.8);
- shared custom metric registry;
- autoscaling examples;
- metrics for a worker in `Bootstrapping`;
- `BackpressureError` attributes (`hard_limit`, `in_use`, `needed`).

### Phase 8: sharding and convergence

Only after one shard is trustworthy:

- 1,000-worker-scale load testing, including whether three replicas are enough at that scale (§28.2), blob lookups in shards above kad's bucket size (20), the leader's memory for settled outcomes of a million-member map, the quorum write each member Task costs and its throughput, the DHT reads a member's claim costs, reduce latency (steps start at the seal; a small item above 64 bytes costs a blob), and the per-second scan of blob owners (`owners_to_check`);
- client shard map;
- task shard selection;
- replacement shards;
- `ShardLostError`;
- `.resubmit()`;
- automatic shard convergence;
- remote client liveness and the derived `ORPHANED` flow state (the leader drives composites, so a dead client no longer strands a flow; but a streamed group, map or reduce whose submitter dies before its last batch keeps running until cancelled);
- the task-to-shard cache;
- the disaster-recovery Task store (§9.2).

Trying to implement sharding before single-shard elections are proven would multiply debugging complexity unnecessarily.

### 27.1 Acceptance criteria by phase

The coalescing, flow, and failure-detection invariants (§25.1 item 9 and §25.4) become named acceptance criteria. Those that exercise election and reconciliation extend the Phase 0 simulator; the rest are exit criteria of the phase that implements them.

| Phase | Criteria |
|---|---|
| 0 (simulator extension) | 25.1.9 (abort before replacement), 25.4.1 (single running generation by authority), 25.4.2 (pending-only supersession), 25.4.6 (occupancy rebuilt at reconciliation) |
| 1 | 25.4.3 and 25.4.4 (folding, order preservation), 25.4.7 (atomic continuation), 25.4.8 (stage after a group runs once), 25.4.5 in a one-node shard, and the one-node backpressure contract (`SlowDown` past the soft limit, `BackpressureError` past the hard limit, §3.2.1) |
| 2 | 25.4.5 under worker loss, and abort-before-replacement under a simulated (connection-loss) partition. The bound on the time from worker unreachable to replacement claim stays tested (`core/tests/scenario/scenario_partition.rs`); its measured value is a §27.2 gate item |
| 3 | 25.4.5 under leader loss (a new leader can replay only what reconciliation rebuilds), retained-payload chain across DHT replicas and compaction (the one-node soft/hard-limit backpressure contract is a Phase 1 exit criterion) |
| 4 | Cold bootstrap, flush with a live leader, restart, and quorum loss plus flush, each against a real Redis-compatible server, with one ShardId per incarnation |
| 5 | Hard-timeout subprocess kill; heartbeats unaffected by a CPU-bound task subprocess; the multi-process SIGKILL harness (a killed task subprocess, worker and leader, each run certified once); a partitioned worker stopping its runs by their abort deadline |
| 6 | 25.4.5-25.4.8 over the network under worker and leader loss (the `test_networked` kill scenarios); the §8.5 failure matrix, in the net and Python tests; a 100,000-item map in core, and 2,000 items one-node and 300 networked in Python, each member submitted once and results in input order; a reduce whose `join` step is replayed after its worker was killed; a killed client resuming its result from the result backend; leadership transfer model-checked at Bound A; partition scenarios (50/50, 51/49, a leader with a minority) and DHT-locality discovery in `net`; the installed wheel running a flow on one worker and across a shard of two (`//runtime/tests:test_wheel`) |

### 27.2 Production-readiness gate

The release-hardening phase (above) is this gate: no real workload should adopt kabudachi until it is done.

---

## 28. Key open design questions for peer review

The architecture is intentionally prescriptive about semantics, but several implementation questions should remain open until prototypes/tests provide evidence.

### 28.1 Exact peer transport

Closed. The shard runs on rust-libp2p: TCP with Noise and Yamux, Kademlia for records, gossipsub for roll calls and request-response for control and results. It has carried every phase so far; its cost is the dependency, not operations.

### 28.2 DHT semantics

Phase 3 settled the Task-record part of this question: three replicas by default, a majority write, no expiry for unfinished records, retention of finished ones for the result TTL, and no provider records (§8.6). Large payloads are closed: they travel as manifests of 256 KiB content-addressed blobs stored with their owner's record (§8.6). Whether three replicas give enough durability without too much chatter at 1,000 workers is measured in Phase 8's load testing.

The DHT should not quietly become a database.

### 28.3 Vote durability

Closed. Ordinary per-term votes are ephemeral, and nothing persists a vote. A restarted process takes a fresh `WorkerId` and joins as a new worker, so it cannot cast a second vote in a term its previous incarnation voted in (a `WorkerId` is the node's libp2p `PeerId`, generated with a fresh keypair on every start). Forced reconfiguration does not reopen the question, because the authority's compare-and-swap of the recovery epoch is durable.

### 28.4 Recovery fence timing

The lease must balance:

```text
short lease -> quicker safe forced recovery, more sensitivity to authority outage
long lease  -> better Redis outage tolerance, slower catastrophic recovery
```

Closed. One TTL governs registrations, the fence and the warm-up; it defaults to 30 s and is set on the authority, which is its only source. Workers renew every third of it and treat each grant as lapsing a tenth early for clock drift. After lost data or a server restart the fence is refused for one TTL, so a fence granted before it has lapsed before another can be (the delayed-restart rule). A shorter TTL recovers a lost shard sooner and fences leaders sooner in an outage.

### 28.5 Result-delivery failure matrix

Closed. §8.5 lists the protocol and its failure matrix.

### 28.6 DHT last-holder shutdown

Closed. A draining worker hands its Task records to the holders its leader names and waits for their acknowledgements, bounded by the drain wait limit (§18.1). Every record the worker holds counts, an acknowledgement is a holder's stored-record answer to the write, and a record a holder refuses as older needs no handing over. Blobs and composite records are handed off with the Task records. The sole voter of a shard admits a pending member and hands over to it, or waits up to `drain_wait_limit` and logs what leaves. The operator override of the bound is `drain_wait_limit`.

Leaving through a successor while a third voter is dead can leave a quorum-less pair. That edge is tracked under post-implementation testing.

### 28.7 Map cardinality and backpressure

Closed. `.map` streams batches under `SlowDown` and `BackpressureError`, and its results come back in input order from pages (§3.4).

### 28.8 Reduction failure behavior

Closed. A step is retried per the reducer's `retries`; one that exhausts them fails the reduce and starts nothing new. Each step's task records its reduce and its place (chain index, or tree level and position), which is what observability needs to rebuild the chain or tree (§3.4).

### 28.9 Logical queue fairness

Closed. Strict priority tiers, weighted round robin within a tier, starvation aging and per-queue `max_running` (§8.2). Fairness is separate from shard selection.

### 28.10 Security

Authentication, peer identity, authorization, and untrusted task submitters have not been fully specified in this brief.

Before production use, the protocol needs:

- authenticated peers;
- authenticated clients;
- authorization boundaries for queues/tasks;
- replay protection for signed/identified control messages;
- safe serializer policies.

The transport is encrypted already: Noise secures each connection between peers. It authenticates only the peer ids of the channel, so nothing yet decides who may join or what a peer may do.

Owner: release hardening (§27).

The Task record store checks a record's key, size and version, and trusts every peer of its shard to write it. It does not verify that a writer is the shard's leader or a member, and it does not cap how many unfinished records a holder keeps (the leader's memory budget bounds what an honest leader writes). A forged or flooding peer is stopped only by authenticated peers, so this is part of the gate in §27.2.

Do not use Python pickle as a production network serializer.

### 28.11 Submit-to-start latency

No latency target is committed. The path is DHT dissemination, worker pull, and a leader claim round trip. Phase 2 prototype measurements should establish a realistic p50/p99 on a warm idle worker in one healthy shard before any target is stated; a leader push-offer to idle workers is a possible optimization if the pull path is too slow.

Owner: release hardening (§27), which measures it.

### 28.12 Subprocess recycling

Closed. Recycling is per worker and per task. `max_runs_per_process` recycles a subprocess after that many runs (off by default). A task declared `recycle_process=True` recycles the subprocess that ran it once the run ends. An `after_run` hook that raises recycles its subprocess too (§19.6). A recycled subprocess takes no new runs, finishes the ones it has and exits; it counts against the process count until it exits, and its replacement starts then. No per-queue knob exists: a queue's tasks share their worker's subprocesses.

### 28.13 State after a hard-timeout kill of a non-retriable task

Closed. A non-retriable TaskRun killed at its hard limit is `FAILED` with a timeout error. It did not certify, and a body that raises midway is `FAILED` too, partial effects or not. `ORPHANED` stays the state for an unknown outcome: a run whose worker or subprocess was lost while it ran (§4.4). This matches the common practice of task queues and workflow engines, Celery and Temporal among them, of treating a timeout as a failure.

### 28.14 Thresholds and margins

Soft and hard fractions of the retained-payload memory budget (§3.2.1; by default 25% and 50% of available memory), and the clock-skew margin between a worker's abort deadline and the leader's replacement deadline (§8.3), are implementation details to fix with measurements. Measured in post-implementation testing.

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
- a gossipsub roll call that counts the shard's reachable workers;
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
