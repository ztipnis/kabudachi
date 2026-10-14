# kabudachi (Python runtime)

The Python package of kabudachi, a peer-to-peer task queue with a compiled native core. Run on its
own, a worker is a shard of one, its own leader, inside its process: the tasks you call are queued and
certified there. Given `listen`, it is a worker of a networked shard, and runs the tasks any worker of
the shard submits (see "Networked workers"). Either way, task bodies run in task processes it starts
(one per CPU it may use), or in the worker's own process with `processes=0`. Task processes are
started with `spawn` and re-import your script, so put the call to `kabudachi.run(...)` under
`if __name__ == "__main__":` whenever `processes` is above 0.

Tasks take one input and return one result, both protobuf messages (the `protobuf` extra, see "Installing"),
pydantic models or dataclasses (see "Serializers"), or other types if you register a serializer.

## Example

<!-- example -->
```python
import kabudachi
from myapp_pb2 import Order, Receipt  # any protobuf messages

kabudachi.configure(concurrency=8, processes=0)  # tasks declared in a script run in this process


@kabudachi.task
def price(order: Order) -> Order:  # a synchronous body runs in a thread
    return Order(item=order.item, quantity=order.quantity, cents=order.quantity * 250)


@kabudachi.task(retries=2)
async def charge(order: Order) -> Receipt:
    return Receipt(ok=True, total=order.cents)


checkout = kabudachi.flow(price, charge)  # price, then charge on its result


async def main():
    steps = await checkout(Order(item="tea", quantity=2))  # the result of every stage
    batch = await charge.map([Order(item="a", cents=100), Order(item="b", cents=250)])
    return steps[-1].total, [receipt.total for receipt in batch]


print(kabudachi.run(main))  # (500, [100, 250])
```

`kabudachi.run(main)` owns the event loop, waits for the worker to be leader, runs `main()` and
returns what it returns. Tasks can only be called while it is running.

## Installing

kabudachi installs from a wheel built with Bazel. Wheels are built for Linux x86_64 (glibc 2.34 or
newer) and macOS arm64 (11 or newer). One wheel serves every CPython from 3.12 on, since the native
extension uses Python's stable ABI.

```bash
bazel build //runtime:kabudachi_wheel
WHEEL="$(bazel cquery --output=files //runtime:kabudachi_wheel)"
pip install "$WHEEL"               # the runtime alone
pip install "$WHEEL[protobuf]"     # with protobuf messages as task input and output
pip install "$WHEEL[pydantic]"     # with pydantic models as task input and output
pip install "$WHEEL[dataclass]"    # with dataclasses; needs nothing beyond the runtime
```

The wheel holds the native extension, so nothing is compiled at install time. `pip install` pulls in
`asgiref` and `wrapt`. The `protobuf` extra adds `protobuf` and the `pydantic` extra adds `pydantic`
(2.7 or newer, below 3); the `dataclass` extra adds nothing.

## Running

- `kabudachi.run(main)` runs `main()` and, when it returns, waits for every task that was called
  (and every flow, group and callback) to finish. If `main` raises or is cancelled, running tasks
  are let finish, tasks that have not started fail with `RunStoppedError`, and the error is raised.
- `kabudachi.run()` with no `main` serves as a worker on the main thread until SIGINT or SIGTERM,
  then drains the same way. A second signal stops waiting, stops the task processes at once, and
  raises `KeyboardInterrupt`.
- It cannot be called from a running event loop, and only one can run per process.

## Task processes

With `processes` above 0, task bodies run in task processes the worker starts with `spawn`. Each
imports your script again, which is why the call to `kabudachi.run(...)` needs its
`if __name__ == "__main__":` guard.

- A task process imports every module that declared a task or a lifecycle hook, or the modules
  `imports` names instead, and must find every task at the same version. It has
  `process_start_timeout` to do so. Otherwise `kabudachi.run()` raises `StartupError`.
- Tasks and hooks must be declared in modules a task process can import, not in the script you run:
  `kabudachi.run()` refuses one declared there with `TaskDefinitionError`, unless `processes=0`.
- A body that ignores its timeout or a cancel for longer than its `cancel_grace` costs its task
  process: the run settles at once (failed, cancelled, or its retry queued to start once the old body
  exits), the process takes no new runs, a replacement starts at once, and the old process is stopped
  once its other bodies have finished, so they are never killed for it.
- A body that raises `SystemExit` or `KeyboardInterrupt` fails its run with `TaskBodyError` naming the
  type, and its task process is replaced the same way, without cutting short its other runs.
- A task process that dies (a crash, running out of memory, a kill) takes its runs with it and is
  replaced, after a growing wait while replacements keep dying quickly. Each run it held is tried
  again without using up a retry, except an `@ephemeral_task`'s, and a `@coalescing_task`
  generation's that a newer generation of its key is waiting to supersede; their handles raise
  `TaskLostError`. A run of a `retriable=False` task is not tried again either: its handle raises
  `TaskOrphanedError`. A compaction it was folding counts as failed, and that key is not compacted again
  until its waiting generation changes.
- A task process is replaced by a fresh one after `max_runs_per_process` runs, after a run of a task
  declared `recycle_process=True`, or after an `after_run` hook raises. It takes no new runs,
  finishes the ones it has and exits, and its replacement starts then; until it exits it counts
  against `processes`.
- Task processes ignore Ctrl-C: the worker decides what a signal means. The first SIGINT or SIGTERM
  to the worker drains, and the second stops the task processes at once. A SIGTERM sent to a task
  process itself ends it, and its runs are lost and tried again as above. So signal only the worker
  process, not its whole process group. With systemd, set `KillMode=mixed`: systemd then sends
  SIGTERM to the worker process only, which drains and stops its task processes itself, and SIGKILLs
  whatever remains only after the worker exits or `TimeoutStopSec` passes. The default,
  `KillMode=control-group`, sends SIGTERM to every process, which cancels task-process bodies during
  the drain (their runs are tried again).

## Lifecycle hooks

Hooks run where task bodies run: in each task process, or in the worker's own process with
`processes=0`. Declare them at package scope, like tasks, so every task process has them. Each may be
sync or async.

```python
@kabudachi.process_init
def connect() -> None: ...  # once per task process, before it takes a run


@kabudachi.before_run(queues=["billing"])
def before(context: kabudachi.RunContext) -> None: ...


@kabudachi.after_run
async def after(context: kabudachi.RunContext, outcome: object) -> None: ...
```

- `process_init` takes no arguments and no `queues=`. If it raises when the task processes first
  start, `kabudachi.run()` raises `StartupError` naming it; a replacement process whose hook raises is
  started again after a growing wait.
- `before_run` and `after_run` take an optional `queues=` (by default, every queue), and run on the
  thread the body runs on. `kabudachi.RunContext` has the task name, the run id and the attempt (1 for
  the first run). `after_run` also gets the outcome: what the body returned, or the exception it or a
  `before_run` hook raised.
- A `before_run` that raises fails the run with its error, under the task's retries; the body does
  not run.
- An `after_run` that raises is logged by its error's type, the other `after_run` hooks still run,
  and the run's result stands. The task process is then replaced once its runs finish, since what the
  hook did not clean up may be left in it.

## Networked workers

Set `listen` and the worker joins a networked shard instead of running one of its own:

| setting | meaning |
|---|---|
| `listen` | the address this worker listens on, such as `/ip4/0.0.0.0/tcp/4001`; set, the worker is networked |
| `seeds` | addresses of workers to ask who leads; with neither seeds nor `authority`, the worker founds the shard alone |
| `external_address` | the address other workers reach this one at, if not `listen` |
| `authority` | the shard's Redis or Valkey, as a `redis://` or `rediss://` (TLS) URL: the database as the path, `?key_prefix=` (default `kabudachi:`), and `?ttl=`, the seconds a registration outlives its last renewal (default 30) |
| `shard` | the shard's name (default `"default"`) |
| `heartbeat_interval`, `heartbeat_timeout`, `reconnect_timeout` | the shard's timings (1 s, 10 s, 30 s), the same on every worker; tasks and queues may set their own `reconnect_timeout` |

- `kabudachi.run()` starts once the worker has joined and knows its leader. It runs whatever the
  leader hands it, whoever submitted it, and submits through the leader.
- A networked worker needs `processes` of at least 1: a run it can no longer show its leader it holds
  must stop before another worker may run it again. It asks the body to stop `cancel_grace` before
  that deadline, and kills its task process at the deadline.
- Awaiting a task's result works as on one worker, inside task bodies too: the worker that runs the task
  delivers its result to the one that submitted it, and the handle settles once the leader certifies it.
  If the result cannot reach the submitter, the await raises `ResultUnavailableError`. Flows, groups,
  maps and tasks that return a flow, group or bound task run on a networked shard as on one worker:
  the leader drives them, and the worker that submitted one hears when it is over (and re-reads its
  record when the leader changes). A result larger than the worker
  can hold (`memory_hard_limit`, or half of it on a networked worker) fails the run with
  `ResultTooLargeError`; it is retried per `retries`.
- `handle.cancel()` sends the cancel to the leader without waiting and returns `True`; the worker
  running the task hears of it from the leader and stops it.
- When `main` returns, or at the first signal when serving without `main`, `run()` waits until every
  task called here has settled; then the worker lets the runs it holds finish and waits until the
  leader has taken their reports.
- The memory limits apply while this worker leads.

## Declaring tasks

`@task`, `@ephemeral_task` and `@coalescing_task` take:

| option | meaning |
|---|---|
| `name` | how workers find the task; defaults to its module and name, which then must be at package scope |
| `queue`, `serializer` | the queue, and the registered serializer name |
| `raise_on_failed_migration` | `True` to leave a task whose input this worker cannot migrate for another worker instead of dropping it (see "Schemas and migrations") |
| `retries` | how many more times a run that raises or times out is tried again (not for `@ephemeral_task`) |
| `retriable` | `False` for a `@task` that is not safe to run twice: a run that was running when its worker or task process was lost ends with `TaskOrphanedError` instead of being run again. Takes no `retries` |
| `timeout`, `cancel_grace` | ask the body to stop after `timeout`; fail the run if it has not stopped `cancel_grace` later |
| `reconnect_timeout` | on a networked shard, how long a run may go unheard, once its worker is suspected, before the leader runs it again elsewhere; by default its queue's, else the shard's |
| `recycle_process` | replace the task process that ran it once the run ends |
| `merge` | `@coalescing_task` only: a pure `(older, newer)` reducer for superseded payloads |
| `drop_oldest` | `@coalescing_task` only: past `memory_hard_limit`, drop the key's oldest retained payloads when that frees enough bytes; otherwise the submission raises `BackpressureError` |

The function takes exactly one argument, and both it and the return value must be annotated.
Declaring a task refuses anything that could never work, where it is written.

`task.local(x)` runs the function right here with no runtime, passing input and result through the
serializer as a worker would.

## Calling tasks

Calling a task returns a handle to await. Awaiting it gives the result once the leader has
certified it, or raises what the task raised.

- `handle.cancel()` cancels the task whatever it is doing and says whether it did; awaiting then
  raises `TaskCancelledError`. An awaiter giving up never cancels the task.
- `handle.callback(fn)` calls `fn(result)` after certification, for a task that succeeded. It is a
  best-effort reaction: a callback that raises is logged and changes nothing.
- `task.options(delay=, eta=, expires=, key=)(x)` delays the start, or makes the task fail with
  `TaskExpiredError` if it has not started by `expires`. `key` is a coalescing task's key.
- `handle.durability` says where the task is kept, a `Durability`: `IN_MEMORY` (only this process holds
  it: always so on one worker, and on a networked one until the leader has stored it), `REPLICATED` (a
  networked leader stored it on a quorum of its shard) or `DR_STORED` (a disaster-recovery copy, which
  no runtime offers yet).
- `await handle.durable(Durability.REPLICATED)` returns once the task is kept at least that durably. It
  raises `DurabilityUnavailableError` on one worker and for `DR_STORED`, `BackpressureError` if the
  leader refused the task, and the handle's own error if the task ended before this process heard it
  was stored. Cancelling the wait does not affect the task.
- `await handle.status()` gives a `TaskStatus`: `state` (`"unknown"`, `"pending"`, `"running"`, or how
  the newest run ended: `"succeeded"`, `"failed"`, `"expired"`, `"superseded"`, `"cancelled"`,
  `"lost"` or `"orphaned"`), `run_state`, `attempt`, `submitted_at`, `updated_at` and `delivered`
  (whether the result reached the worker awaiting it). It is read from the record the shard holds, not
  from the leader, so a change the leader has just made may not show yet. The handle of a flow or group
  gives a `CompositeStatus` instead: `kind`, `state` (`"running"`, `"succeeded"`, `"failed"` or
  `"cancelled"`), `counts` of its stages or members by state, and, for a flow, `stage`.
- A task whose run ended and whose record had no room for another attempt raises
  `TaskRecordFullError`.

## Composition

- `task.bind(value)` fixes the whole input; `task.bind(field=value)` sets fields on the message it is
  called with.
- `flow(a, b, c)` runs stages one after another, each on the output of the one before. Awaiting it
  gives the list of every stage's result. A failed stage fails the flow and later stages never start.
- `group(a, b, on_error="fail_fast" | "collect_all")` runs members side by side on the same input and
  gives the list of their results in member order.
- `task.map(items)` is a group with one task per item, in the order of the items.
- Flows and groups nest, and the results nest the same way.
- A task declared `-> Flow`, `-> Group` or `-> BoundTask` may return one, which then runs as its
  continuation: the run is certified first, and the task's handle resolves to the continuation's
  results.

Flows, groups, `.map` and returned steps run on a networked worker as on one node. A `collect_all`
failure is a `TaskFailure` with its `kind` and `message`. A coalescing task cannot be a stage, and
`kabudachi.` is a reserved task-name prefix.

A stage that consumes the array of a `collect_all` group receives failures in it, so it needs a
serializer that can encode them; the protobuf serializer cannot.

Building a `flow()`, `group()`, `.map` or `.reduce` checks, where it is written, that what each step
gives fits what the step after it takes, and raises `TypeError` if it cannot. See "Schemas and
migrations" for what fits.

### Large groups and maps

A group or map sends its members to the leader in batches, so how many it can hold is bounded by
memory, not by the size of one record. A `.map` placed after a stage is fed the same way: the
worker that splits the prior stage's list sends the items in batches.

- Each batch waits while the leader says SlowDown (memory in use is past `memory_soft_limit`). A
  networked worker hears SlowDown from the leader and checks again every heartbeat interval. Past
  `memory_hard_limit` the stream stops, the members already sent are cancelled and the handle fails
  with `BackpressureError`. A stop of the run, or a cancel of the handle, ends the stream the same
  way, failing the handle with `RunStoppedError` or `TaskCancelledError`.
- Because the first batch can go before the rest are sent, a group or map that hits the hard limit
  midway fails through its handle: `.start()` and the call have already returned, and the composite
  exists, cancelled. What stays synchronous is everything that can be checked before anything is
  sent: the arguments (`TypeError`), a member the serializer cannot encode, a run that is stopping
  (`RunStoppedError`) or not serving (`RuntimeNotStartedError`), and `BackpressureError` when even
  the group's first record is refused. A group's members are all described when it starts; a map's
  items are encoded as they are sent.
- A group or map whose submitter dies before sending every member keeps running the members the
  leader has until it is cancelled; it never finishes on its own.
- A value bound into a step, a group's member or a map item that is too large to travel inline is
  stored in the shard instead. Only the structure of a flow or a returned step has to fit one record
  (about 1 MiB). A flow whose stages themselves pass that, roughly 25,000 bound stages, is refused
  with a `BackpressureError` that names the record limit, and a task that returns such a step fails
  with `ContinuationTooLarge` and is not run again. A single value past this process's memory limit
  raises `PayloadTooLargeError` instead. To run that many tasks, start a group or map, which sends
  its members in batches, instead of a flow or a returned step.
- Bound bytes fields of a message (set with `task.bind(field=value)`) are not stored in the shard:
  they count as part of the structure, as do names and settings. A single group member whose own
  structure passes one message to the leader raises `ValueError`.

## Reducing

`@reducer` declares a durable task of two parameters, `(item, previous) -> previous`: it folds one
item into the result so far. A plain `@task` of two parameters is the same as a bare `@reducer`.
Both parameters and the return value are annotated, and the return type must be the type of
`previous`. It takes the options of `@task` except `version`, plus `associative`, `tree` and `merge`
below. It must be durable, so it cannot be an `@ephemeral_task` or a `@coalescing_task`, and it cannot
return a step. Calling it, or using `.bind`, `.map` or `task.options(...)` on it, raises `TypeError`;
`reducer.local(item, previous)` runs the function here, like `task.local`.

```python
@kabudachi.reducer
def total(item: int, previous: int) -> int:
    return previous + item

await total.reduce([1, 2, 3, 4], initial=0)   # 10
```

`await reducer.reduce(items, initial=..., chunk_size=...)` folds a list, left to right, and gives the
folded value. Each step is a certified task of its own, so retries, timeouts and the queue apply per
step, and a step that fails after its retries fails the reduce, with the step's error, and no later
step runs. `initial` may be any value, `None` included. Without it, the first item starts the fold
and is not passed to the reducer, so a list of `n` items runs `n - 1` steps. `reduce` also takes
`items` as a tuple, and anything else raises `TypeError`.

A reduce is a chain unless the reducer says otherwise: step `i` folds item `i` into step `i - 1`'s
result. A tree folds chunks of `chunk_size` items (64 by default, a positive integer) side by side,
each with the reducer, then combines neighbouring results, in order, until one is left. It keeps the
chain's result while spreading the work across workers, if the combine of two results gives what
folding them in order would. Declare that with exactly one of:

- `@reducer(associative=True)`: the reducer is its own combine, so `(item, previous)` take and return
  one type, and combining two results is `reducer(item=right, previous=left)`. A sum, a
  concatenation or a maximum are of this kind.
- `@reducer(tree=True, merge=combine)`: for a reducer whose items are not its result, such as one
  that counts items into a dict. `combine(left, right)` is a synchronous function defined at module
  level (or a lambda), so that every worker that imports the reducer has the same one. An
  associative reducer takes no `merge`, and `merge` needs `tree=True`.

`tree` defaults to `associative`, so `associative=True, tree=False` declares a reducer that is
associative but always runs as a chain. A tree with an `initial` folds every chunk from `initial`,
so `initial` must be the combine's identity (`0` for a sum, `[]` or `""` for a concatenation). This
is not checked: a seed that is not an identity is counted once per chunk.

`reducer.reduce` is also a `flow` stage that folds the list the stage before it returns, and so is
`reducer.reduce.options(initial=..., chunk_size=...)`. The stage before it is a task that returns
`list[X]` where the reducer's item is `X` (a different type raises `TypeError` when the
flow is built), or a `map`, in which case the reduce folds the map's results. A reduce cannot be the
first stage of a flow, since it has no list to fold: call it with `reducer.reduce(items)` instead.
The flow's results list holds the folded value for the reduce's stage.

```python
flow(split_words, upper.map, join.reduce.options(initial=""))
```

A reduce of no items with an `initial` gives `initial`, and a reduce of one item without one gives
that item, each without running a step. A reduce of no items without an `initial` has no result: for
a list that is empty when `reduce` is called it raises `ReduceEmptyError` at once, and for a list that
a flow's earlier stage returns empty it fails the reduce, so awaiting it raises `ReduceEmptyError`.
`ReduceEmptyError` is also a `TypeError`, as in `functools.reduce`.

A list of any size is sent to the shard in batches, as a map's items are, so it may be larger than one
record or one message: an item or an `initial` too long to be stored inline is stored as blobs that
the root of the reduce's tree owns, and the reducer reads it back whole. Memory limits apply as
they do to maps (see "Large groups and maps"). A step must return a value: a reducer that returns a
step fails the reduce with `ReducerContinuedError`.

## Schemas and migrations

Tasks have no versions of their own. A task's input type has one, and a worker brings a task's input to
its own version of that type when it claims the task. The stored task never changes; only the worker's
copy is migrated, one version at a time. A worker whose version already matches does nothing.

```python
kabudachi.schema(Type, version=N, version_path=None, family=None, migration=...)
```

declares `Type` as version `N` of a family. A family is a data type's identity across versions: by
default a protobuf message's full name, or `module.qualname` of any other class. Give the same `family`
to separate classes to make them versions of one, such as `OrderV1` and `OrderV2`. A type never declared
is version 0 of its own family, and `version` is 0 by default. `version_path` (`"$.version"`, or
`"$.meta.v"`) is where a JSON-encoded payload carries its version; without one a payload is taken to be at
the version of the type it was submitted as. A family has one `version_path`, one class per version, and
at most one migration per bump; declaring a contradiction raises `ValueError`.

`migration` is the bump from `version - 1`:

- left out: the default step, which drops the fields the new version lacks and gives new fields their
  defaults;
- a function: an explicit step, taking the older payload's plain form and returning the newer's. The plain
  form is the JSON object for the `pydantic` and `dataclass` serializers and the message for `protobuf`.
  The framework writes the new version into the result, so a step need not;
- `None`: the bump cannot be migrated. A payload that would cross it is refused.

`@kabudachi.migration(Type, from_version=N)` registers the explicit step from `N` to `N + 1` of `Type`'s
family on a function, as an alternative to passing `migration=`.

```python
from pydantic import BaseModel

import kabudachi


class OrderV1(BaseModel):
    item: str
    quantity: int
    unit_price: float


class OrderV2(BaseModel):
    item: str
    quantity: int
    unit_cents: int


class Shipment(BaseModel):
    summary: str


kabudachi.schema(OrderV1, version=1, version_path="$.version", family="shop.Order")
kabudachi.schema(OrderV2, version=2, version_path="$.version", family="shop.Order")


@kabudachi.migration(OrderV1, from_version=1)
def cents_not_dollars(order: dict) -> dict:
    return {
        "item": order["item"],
        "quantity": order["quantity"],
        "unit_cents": round(order["unit_price"] * 100),
    }


@kabudachi.task(serializer="pydantic")
def ship(order: OrderV2) -> Shipment:
    return Shipment(summary=f"{order.quantity} x {order.item} at {order.unit_cents}c")
```

A worker running this code takes an order submitted as `OrderV1` by an older worker, runs
`cents_not_dollars` on its JSON and runs `ship` on the result. Every worker of a shard must declare a
family's bumps the same way; during a rollout their versions differ, which
is the point.

### A task that cannot be migrated

A worker that claims a task whose input it cannot bring to its version, because a migration raised, a bump
is declared incompatible or the serializer cannot migrate, drops the task by default: it logs a warning
naming the reason and the task fails with `MigrationError`. A migration can raise `MigrateRejectError` to
say the payload cannot be migrated here; that is treated the same way as any failed migration.

With `raise_on_failed_migration=True` on the task, the worker instead leaves the task for another worker,
whose version may run it. If no worker can, it fails with `MigrationError` once it has waited
`Settings.version_block_ttl` (`KABUDACHI_VERSION_BLOCK_TTL`, in seconds; one hour by default), which every
worker of a shard should set alike.

A task whose input is newer than the worker's version is always left for another worker, whatever the
option, and fails the same way after `version_block_ttl`.

### What composes

A flow, group, `.map` or `.reduce` checks each adjacency where it is built: what a step gives must fit what
the step after it takes, or `TypeError` is raised. A returned step is checked in the same way when the
run of the task that returns it completes. What fits:

- the same type, or a subclass;
- a different class of the same family, if the producer's version is not newer than the consumer's and no
  bump between them is declared incompatible (so a downgrade is refused);
- a group's list of results, or a map's, where a list of the one type is taken, entry by entry;
- under `on_error="collect_all"`, each entry is `T | TaskFailure`: a stage taking the list must accept
  failures as well as `T`;
- when a type cannot be judged, such as `Any` or what a task that returns a step gives, the check passes.

Types of different families do not compose, even across serializers.

## Serializers

A task's `serializer` names the backend that encodes its input and output. `protobuf` is the default.

- `pydantic` (extra `pydantic`): a `BaseModel`, a `list` of one, or `None`. A model is written as canonical
  JSON (sorted keys, no whitespace, UTF-8, field aliases used), so equal values are equal bytes. A model
  holding a `set` is refused, since a set has no stable order: use a list. A payload that fails validation
  raises `SerializationError` naming the field, never its value.
- `dataclass` (extra `dataclass`, which adds no dependency): a dataclass, a `list` of one, or `None`. Fields
  may be `str`, `int`, `float`, `bool`, `None`, other such dataclasses, `list[...]`, `dict[str, ...]` and
  `X | None`. The encoding is canonical JSON as above.

Either serializer finds a payload's version at the family's `version_path`, and writes it there when
encoding.

A custom serializer needs `name`, `available`, `supports`, `encode` and `decode`. It may add the members
below; one it lacks takes its default, so existing backends keep working:

| member | default |
|---|---|
| `extract_version(payload, value_type)` | `None`: the payload carries no version, and the source type's is used |
| `validate(value)` | every decoded value is valid; raise `SerializationError` otherwise |
| `schema_family(value_type)` | the family declared with `kabudachi.schema`, else the type's own |
| `type_compatibility(produced, consumed)` | `None`: cannot say, so the family and version rules above decide |
| `load_plain(payload, value_type)` and `dump_plain(plain, value_type)` | absent: the backend cannot migrate, and a task needing a migration is refused. `dump_plain` conforms the migrated form to `value_type` (dropping what it lacks) and encodes it |

## Coalescing and backpressure

A newer pending submission of a `@coalescing_task` with the same key supersedes the older one, whose
handle raises `TaskSupersededError`. A running generation is never cancelled, and only one generation
of a key runs at a time.

The scheduler counts the bytes of task input that has not finished. Past `memory_soft_limit`, `group`
and `map` pause; past `memory_hard_limit`, submitting raises `BackpressureError`. A `@coalescing_task` declared with `drop_oldest=True` instead drops the key's oldest retained payloads until the submission fits, but still raises `BackpressureError` when dropping every retained payload for the key would not free enough bytes (for example an oversized payload). Independent of the limits, a task whose input, queue and key together exceed about 1 MiB (the size of one message to a worker) also raises `BackpressureError`, because no worker could ever be sent it. `drop_oldest` does not relax this bound: a key whose claim would pass one message raises `BackpressureError` even with `drop_oldest`.

A coalescing key's superseded payloads are kept as a chain until the newest generation runs. When a
key's chain holds more than about half of one message to a worker (or memory is past
`memory_soft_limit`), the scheduler creates a compaction run. It runs on a free worker place with the
task's `merge` function, which folds the oldest payloads that fit one claim into one. The newest
generation still sees every payload folded in order, so `merge` must be deterministic and free of side
effects but need not be associative. While a key's chain is as large as one message to a worker,
submitting to that key raises `BackpressureError`. If a fold grows past one message, the newest
generation fails with `CoalescedPayloadTooLargeError`.

## Errors and native types

`KabudachiError`, the base of the custom errors kabudachi exposes, and `BackpressureError` are
defined by the compiled `kabudachi._native` module, which raises them itself; `kabudachi.errors`
re-exports them, so catch them as `kabudachi.errors.*` like the rest. Some native calls (`submit`,
`report_failure`, `cancel`) can also raise a plain `RuntimeError` for shutdown, ownership and state
errors, which `except KabudachiError` does not catch.

The native module reports what happened as typed values rather than strings, all in
`kabudachi._native`: `EventKind` (`EXPIRED`, `SUPERSEDED`, `SLOW_DOWN`, `CANCELLED`, `RECORD_FULL`, `COALESCED_PAYLOAD_TOO_LARGE`, and from a
networked worker `ACCEPTED`, `REFUSED`, `ABORT`, `ABORT_WITHDRAWN`, `RESULT`, `CERTIFIED`, `OUTCOME`) for scheduler
events, `CancelOutcome` (`CANCELLED`, `ALREADY_FINISHED`, `UNKNOWN_TASK`) for how a cancel ended, and
`RunState` (`SCHEDULED` through `ORPHANED`) for where a run is. The runtime turns these into the
handle behaviour described above; you meet them only if you call the native module directly.
`COALESCED_PAYLOAD_TOO_LARGE` surfaces as `CoalescedPayloadTooLargeError` on the handle.

Task processes and networked workers add `StartupError` (task processes could not start),
`TaskBodyError` (a body raised what could not be sent back as it was, such as `SystemExit`; `kind`
names its type), `TaskOutcomeError` (a task ended without a result for a reason outside its body; the
base of the next two), `TaskLostError` (a task's process died and the task is not run again),
`TaskOrphanedError` (a run of a task not safe to run twice was running when its worker was lost),
`ResultUnavailableError` (the outcome is known but the result cannot be had here; `task_id` names the
task), `ResultTooLargeError` (a result larger than the hard memory limit of the worker that ran it or of its awaiter) and
`DurabilityUnavailableError` (the durability asked for cannot be reached), all in `kabudachi.errors`.

## Configuration

`kabudachi.configure(...)`, or `KABUDACHI_<NAME>` in the environment:

`processes` (task processes; by default one per CPU this process may use, `0` runs bodies in this
process), `concurrency` (places per process, 16 by default, at most 32 without
`concurrency_override=True`), `imports` (modules task processes import; by default every module that
declared a task or hook), `process_start_timeout` (how long a task process has to import its modules
and report ready, default 60 s), `max_runs_per_process` (runs a task process takes before it is
replaced; no limit by default; the fresh one starts once it has drained and exited, and until then it
counts against `processes`), `queue`, `result_ttl` (seconds a finished task is kept), `cancel_grace`,
`queues`, `queue_options` and `router` (see "Queues" below), `memory_soft_limit` and
`memory_hard_limit` (bytes), `version_block_ttl` (how long a task left for another worker waits before
it fails with `MigrationError`, one hour by default; see "Schemas and migrations"), and the networked settings `listen`, `seeds`, `external_address`,
`authority`, `shard`, `heartbeat_interval`, `heartbeat_timeout` and `reconnect_timeout` (see
"Networked workers"). In the environment a duration is a number of seconds, and `seeds`, `imports`
and `queues` are comma-separated lists.

### Queues

`queues` is the queues a worker takes tasks from, a non-empty tuple of names. A worker in a shard of
one takes every queue by default, since nothing else could run the rest; a worker on a network takes
`queue` alone, so a queue is served only by the workers that name it.

`queue_options` gives a queue its own `kabudachi.QueueOptions`, and a queue it does not name has the
defaults:

```python
kabudachi.configure(
    queues=("default", "gpu"),
    queue_options={"gpu": kabudachi.QueueOptions(priority=10, max_running=4)},
)
```

- `priority` (an integer, `0` by default): a worker asking for work gets the highest priority that
  has any waiting.
- `weight` (a positive integer, `1` by default): among queues of one priority, how many tasks this
  queue hands out for each one a queue of weight 1 does.
- `max_running` (a positive integer, or `None` for no limit, the default): the most tasks of the
  queue claimed or running at once across the shard.
- `reconnect_timeout` (a positive `timedelta`, or `None` for the shard's, the default): how long a run
  of a task sent here may go without its worker being heard, past the time it takes to suspect a
  silent worker, before the leader runs it again. A task's own `reconnect_timeout` wins.
- `result_ttl` (a positive `timedelta`, `None` to keep nothing; by default the process's
  `result_ttl`): how long a finished task sent here is kept. A task's own wins.
- `starvation_age` (a positive `timedelta`, or `None` for never, the default): how long a task may
  wait before it goes ahead of every priority.

An invalid option raises `ValueError` when the `QueueOptions` is built. Every worker of a shard
should give the same queue options; the leader's decide.

`router(name, value) -> str | None` picks the queue of a task that names none, when it is
submitted: it is called with the task's name and its argument, and returns a queue name, or `None`
for `queue`. It runs in the submitting process only.

## Logging

A task that fails is logged at WARNING by the type of its error only, never its message. At DEBUG the
full traceback and message are logged, which can include task input, so leave DEBUG off where that
matters.

## Limits

- A worker running a shard of one keeps everything in memory, lost when it ends.
- With `processes=0` a body that ignores a timeout or a cancel cannot be killed: its run settles as
  with task processes, and the body is abandoned to finish on its own, holding its concurrency place
  until it does. An async body that raises `KeyboardInterrupt` there is taken as a Ctrl-C and stops
  the whole run.
- A flow's later stage or a group's member that the scheduler refuses when it starts (the queue is
  full, or memory is past its limit) is not queued to try again: it fails, and so does the flow if
  its policy fails on a failed member. The failure is a `BackpressureError`, which can pass once the
  shard has room.
