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
  `imports` names instead, and must find the same tasks as the worker. It has
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
| `roll_call_deadline` | how long a roll call runs before the worker decides on it; a positive `timedelta` below `heartbeat_timeout`; by default a quarter second, or a quarter of `heartbeat_timeout` if that is shorter |
| `replication_factor` | how many voters store each task's record (default 3); an even factor tolerates no more lost holders than the odd one below it (with 2, writes stall while a holder is dead), so use odd ones |
| `seed_rounds` | how many rounds a worker with seeds and no `authority` asks its silent seeds before it founds the shard alone (default 3) |
| `routing_refresh` | how often a worker re-crawls its peers' routing when nothing else prompts it; by default ten heartbeat timeouts |
| `health_listen` | an `ip:port` to serve `/healthz` and `/readyz` on (see "Health") |
| `queues`, `queue_options`, `router` | which queues the worker takes and how they are ordered (see "Queues") |
| `drain_grace`, `drain_wait_limit` | how long a leaving worker waits (see "Shutdown") |

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
  record when the leader changes). The flows, groups and maps survive a lost worker, a lost leader and a
  lost submitter (see "Large groups and maps" for a submitter that dies mid-stream). A result larger than the
  worker can hold (`memory_hard_limit`, or half of it on a networked worker) fails the run with
  `ResultTooLargeError`; it is retried per `retries`.
- A worker passes over a task whose input it cannot hold or whose input version it cannot run, and
  another worker may take it.
- Workers of one shard must give the same `queue_options`; the leader's decide, and a difference is not
  detected.
- `handle.cancel()` sends the cancel to the leader without waiting and returns `True`; the worker
  running the task hears of it from the leader and stops it.
- When `main` returns, or at the first signal when serving without `main`, `run()` waits until every
  task called here has settled; then the worker lets the runs it holds finish and waits until the
  leader has taken their reports. See "Shutdown" for how it then leaves its shard.
- The memory limits apply while this worker leads.

## Shutdown

The first SIGINT or SIGTERM to a serving worker, or the return of `main`, starts a drain. `drain_grace`
bounds how long tasks still running have to finish, after which they are stopped as a second signal
would stop them; the default `None` waits however long they take.

A networked worker then leaves its shard in order, and each wait is bounded by `drain_wait_limit` (by
default ten `heartbeat_timeout`s):

- A leader waits for its voters to be ready to take over, then hands leadership to one. A worker with no
  leader waits for one to appear.
- A leader that is the shard's only voter waits for another voter to hand over to.
- Every worker waits for the task records it holds to be stored by others. Records still held when the
  wait ends leave with it, and are logged.

The worker gives up on leaving after four such waits plus one `heartbeat_timeout`, then shuts down.
A worker that is fenced, still bootstrapping or joining holds its drain until it leaves that state.
A shard of one leaves at once. Both settings can also be given as `KABUDACHI_DRAIN_GRACE` and
`KABUDACHI_DRAIN_WAIT_LIMIT`, in seconds.

## Health

Set `health_listen` to an IP address and port, such as `0.0.0.0:8081` (port 0 picks a free one), and the
worker serves two endpoints from its start. A port already in use fails `kabudachi.run()` at start.

- `/healthz` answers 200 while the worker's native loops run, and 503 otherwise.
- `/readyz` answers 200 once the worker is alive, its task registry is loaded and it can take work, and 503
  otherwise. A leader is ready once it has
  finished reconciling its records after winning. A follower is ready once a leader holding a grant has
  answered one of its heartbeats.

## Declaring tasks

`@task`, `@ephemeral_task` and `@coalescing_task` take:

| option | meaning |
|---|---|
| `name` | how workers find the task; defaults to its module and name, which then must be at package scope |
| `queue`, `serializer` | the queue, and the registered serializer name |
| `raise_on_failed_migration` | `True` to leave a task whose input this worker cannot migrate for another worker instead of dropping it (see "Schemas and migrations") |
| `retries` | how many more times a run that raises or times out is tried again (not for `@ephemeral_task`); by default the process's `retries` |
| `retriable` | `False` for a `@task` that is not safe to run twice: a run that was running when its worker or task process was lost ends with `TaskOrphanedError` instead of being run again. Takes no `retries`, and never inherits the process's |
| `timeout`, `cancel_grace` | ask the body to stop after `timeout`; fail the run if it has not stopped `cancel_grace` later. By default the process's `timeout` and `cancel_grace`; `timeout=None` is no limit even when the process sets one |
| `result_ttl` | a positive `timedelta`, or `None`: how long the finished task is kept; by default its queue's, else the process's (see "Configuration"). `None` keeps no result |
| `reconnect_timeout` | on a networked shard, how long a run may go unheard, once its worker is suspected, before the leader runs it again elsewhere; by default its queue's, else the shard's |
| `recycle_process` | replace the task process that ran it once the run ends |
| `merge` | `@coalescing_task` only: a pure `(older, newer)` reducer for superseded payloads |
| `drop_oldest` | `@coalescing_task` only: past `memory_hard_limit`, drop the key's oldest retained payloads when that frees enough bytes; otherwise the submission raises `BackpressureError` |

The function takes exactly one argument, and both it and the return value must be annotated.
Declaring a task refuses anything that could never work, where it is written.

`task.local(x)` runs the function right here with no runtime, passing input and result through the
serializer as a worker would.

## Calling tasks

Calling a task returns a `TaskHandle` to await. Awaiting it gives the result once the leader has
certified it, or raises what the task raised. A flow gives a `FlowHandle` and a group or map a
`GroupHandle`, which are awaited the same way.

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
  (whether the result reached the worker awaiting it) and `persistence` (how mirroring its result to
  the result backend went: see "Results after a restart"). It is read from the record the shard holds, not
  from the leader, so a change the leader has just made may not show yet. The handle of a flow or group
  gives a `CompositeStatus` instead: `kind`, `state` (`"running"`, `"succeeded"`, `"failed"` or
  `"cancelled"`), `counts` of its stages or members by state, and, for a flow, `stage`.
- A task whose run ended and whose record had no room for another attempt raises
  `TaskRecordFullError`.
- `await kabudachi.resume(id)` gets the result of a task, flow or group by its id in a later process
  (see "Results after a restart").

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
`previous`. It takes the options of `@task`, but its `result_ttl` is not used and is ignored if given (a reduce takes
its retention from its queue, then the process), plus `associative`, `tree` and `merge` below. It must be durable, so it cannot
be an `@ephemeral_task` or a `@coalescing_task`, and it cannot return a step. Calling it, or using
`.bind`, `.map` or `.options(...)` on it, raises `TypeError`;
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
step fails the reduce with `ReducerContinuedError`. A reduce whose stored items fall short of the count
its record names fails with `ReduceItemsLostError` rather than fold fewer items. Both are in
`kabudachi.errors`.

The leader plans a reduce from counts and digests alone and never runs the reducer or `merge`. So a
tree gives one answer only if every worker that runs a step imports the same reducer and `merge`; the
leader cannot check that. A reduce fails on its first failed step whatever a group's `on_error` would
say, and its steps start only once all its items are sent.

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

Limits while workers run different versions:

- A value of an older class of a family that reaches a stage taking a newer class is handed over as
  the producer's class, not converted.
- The protobuf serializer carries no version in its payload, so a protobuf stage's input is migrated
  from the version of the member task that takes it, not the producer's. A JSON payload with a
  `version_path` is exact.
- A steal or an oldest-first claim can hand a worker a task newer than itself. The worker reports the
  task blocked and runs nothing, which costs one round trip.
- The leader remembers which tasks are blocked only in memory. After a leader change the new leader
  may hand a blocked task to the same worker once more, and the worker blocks it again.

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

`kabudachi.register_serializer(backend)` adds a custom serializer to this process, to be named with
`serializer=`. It raises `DuplicateSerializerError` if the name is taken. Asking for a name nobody
registered raises `UnknownSerializerError`, and for a registered backend that cannot run here (its
library is missing) `SerializerUnavailableError`. Every worker of a shard must register the same
backend under the same name, since one worker encodes what another decodes.

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

The scheduler counts the bytes of task input that has not finished, and a networked process also counts
the large payloads moving through it. Past `memory_soft_limit`, `group` and `map` pause; past
`memory_hard_limit`, submitting raises `BackpressureError`. Unless set, the soft limit is a quarter and
the hard limit half of the memory this process may use (its cgroup limit when that is below physical
memory), read once at start.

There is no fixed cap on a payload. An input or result too large for one message (a little under 1 MiB,
956 KiB) is cut into 256 KiB blobs that the shard stores; the task's record and every claim carry only a
list of them. The one permanent refusal is a payload that alone cannot fit under the hard limit of the
process that must hold it whole: an input raises `PayloadTooLargeError` (it has `size` and `limit` in
bytes; `limit` is `None` where the leader refused), and a result fails its run with `ResultTooLargeError`,
which is retried per `retries` like any failure, though it normally fails the same way. Awaiting a result
past your own hard limit raises `ResultTooLargeError` on that handle only. A result that the shard cannot
store in time is treated as lost, so a task that may run twice is run again.

A networked process splits its hard limit in half: one half for blob payloads in flight, the other for the
blobs it stores for its shard. Inputs fill at most half of the store, so results always have room. A worker
claims a blob input only when it has room for it.

Derived limits differ by host, so give every worker of a networked shard the same explicit
`memory_soft_limit` and `memory_hard_limit`. Otherwise a task too large for some workers is passed over
until one that can hold it asks; it waits until it expires, or fails after its retries, when no worker can.
A very large input can also starve while smaller work keeps arriving.

A `@coalescing_task` declared with `drop_oldest=True` drops the key's oldest retained payloads until the
submission fits under the limits, but still raises `BackpressureError` when dropping every retained
payload for the key would not free enough bytes, and `PayloadTooLargeError` when its payload alone is
past the hard limit. A key's waiting claim carries its retained payloads, and must fit one message:
`drop_oldest` does not relax that, so a submission that would make it larger raises
`BackpressureError` even with `drop_oldest`.

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

The rest of `kabudachi.errors`, for what is asked wrongly or cannot run:

- `ConfigurationError`: a setting is unknown or its value is unacceptable.
- `TaskDefinitionError`, and its subclass `DuplicateTaskError` (two tasks of one name): a function cannot
  be a task, or a decorator option is invalid.
- `UnknownTaskError`: a worker was given a task that no task in its process is named for.
- `CertificationError`: the leader certified a result that is not the one this process holds, so the
  result is not delivered.
- `TaskInterruptedError`: a task, flow or group was interrupted (a cancellation or an interrupt) before
  it finished. Only the type of the interrupting error is repeated, never its message.
- `TaskTimeoutError`: a run went past its timeout, so it failed. A body that does not stop within
  `cancel_grace` of being asked is abandoned.
- `UnknownSerializerError`, `DuplicateSerializerError` and `SerializerUnavailableError`: see
  "Serializers".
- `ReducerContinuedError` and `ReduceItemsLostError`: see "Reducing".

## Results after a restart

Set `result_backend` and a result outlives the process that asked for it: a new process can get it back by
its id with `kabudachi.resume(id)`.

```python
kabudachi.configure(result_backend="redis://cache.internal:6379/0?key_prefix=app:")
handle = add(1)
result_id = handle.task_id  # a flow or group handle has composite_id; keep it somewhere that survives
...
value = await kabudachi.resume(result_id)  # in any process of the run, later
```

`result_backend` is a Redis or Valkey URL (`redis://` or `rediss://`, the database as the path, and
`?key_prefix=`, as for `authority`; a `ttl` is refused, since each entry carries its own expiry), or an
object with async `store` and `load` (a custom backend, below).

- After the leader certifies a run, the worker that ran it mirrors the result to the backend. A client
  mirrors the resolved results of a flow, group or map when they finish, in bounded pages and chunks.
- `handle.status().persistence` says how that went: `"disabled"` (nothing is mirrored: no backend, a
  task that keeps no result, or a member of a flow, group or map), `"pending"`, `"stored"` or `"failed"`.
  It is `None` before the task succeeds, and for a run whose worker has not said yet.
- A backend failure (an error, or a call past its limit) is logged by the logger `kabudachi.result_backend`,
  never with a payload, and changes no task's outcome: the handle resolves as it would have.
- An entry expires with the task's retention (see "Configuration"), counted from the moment it is
  mirrored. A task that keeps no result is not mirrored.

`await kabudachi.resume(id)` gives the result of the task or composite `id`, in any process of the run,
waiting while it runs (and, for a task, while its result is being mirrored, for as long as a
`reconnect_timeout`, since a worker that died before mirroring never will). Bound the wait with
`asyncio.wait_for`. The process must declare the task, since the value is decoded with the serializer it
was stored with.

- A task that did not succeed raises an error of the kind a task run in another process would raise
  (`TaskCancelledError`, `TaskExpiredError`, `TaskLostError`, `TaskOrphanedError`, a failure as
  kabudachi's own error of its kind or else `TaskBodyError` naming the kind, with the message the leader
  recorded; for a flow or group, that of the member that ended it). The exception object the task raised
  in its own process is not kept.
- It raises `ResultUnavailableError` when the shard has forgotten the id, when no backend is configured
  or it holds no entry (the task kept nothing, the entry expired, or its worker died before mirroring), or
  when the entry does not match the record: a result is never returned unless it matches what the leader
  certified.
- A backend that lost a plain task's result is not always fatal. On the networked runtime, if the
  result was large and was delivered to an awaiting client, its worker stored it in the shard as blobs,
  and `resume` reads it from there and mirrors it back to the backend (detached, never over an entry the
  backend holds whole; with a fresh expiry from now). A task nobody awaited, and any one-node run, has
  no shard copy, and a small result never does, so without the backend entry `resume` raises
  `ResultUnavailableError`. A task that kept no result is not mirrored back.
- A flow, group or map resumes from the backend alone, even after the shard has forgotten it. If the
  backend has lost it, `resume` falls back to the pages the shard still holds, mirroring them again; once
  the shard has dropped them too, it raises `ResultUnavailableError`.
- `resume` runs only in the process that runs `kabudachi.run()`: elsewhere, and in a task process, it
  raises `RuntimeNotStartedError`.
- A worker that dies after the leader certified a run but before it mirrored the result leaves the run
  `pending` for good. `resume` waits one `reconnect_timeout`, then reads what the shard holds. A small
  result is then lost if the client that received it is gone too.
- A draining worker waits a bounded time for its mirrors to finish, so a slow backend can cut one short.
- A mirror of a large flow, group or map costs one backend call per part. A call that fails is logged
  and a mirror written in part is not repaired.
- An entry's expiry is a time set by the writer, so clock skew between hosts moves it.

### A custom backend

An object with these coroutines is a backend (`kabudachi.ResultBackend` is the protocol):

```python
class MyBackend:
    async def store(self, key: str, entry: bytes, expires_at: datetime) -> None: ...
    async def load(self, key: str) -> bytes | None: ...
```

`store` keeps `entry` under `key` until `expires_at` (UTC), replacing what was there, and may drop it
after; `load` returns what `key` holds, or `None`. An entry is opaque bytes: kabudachi encodes, checks and
expires it, so the backend is a plain key-value store. Both calls
run on the run's event loop, so they must not block it. A call that raises, or takes longer than 10
seconds, is a failure, logged and never an outcome. Task processes are never given the backend: it is
used only by the process running `kabudachi.run()`.

### The Redis and Valkey backend

It serves a standalone server only, not a cluster. It connects at its first use, so a server that is
down at start is not an error. Its keys are `<key_prefix>result:` followed by the entry's key, and an
entry expires on the server (`SET ... PX`), so a prefix may be shared with `authority`. Every call,
connecting included, is bounded by a call timeout; past it the call fails and the connection is dropped.

- An ACL user needs the commands `get`, `set` and `select`, and the keys `~<key_prefix>*`.
- Use an eviction policy that evicts only keys with a TTL (`volatile-*`) or none (`noeviction`): every
  key here has one.

## Configuration

`kabudachi.configure(...)`, or `KABUDACHI_<NAME>` in the environment:

`processes` (task processes; by default one per CPU this process may use, `0` runs bodies in this
process), `concurrency` (places per process, 16 by default, at most 32 without
`concurrency_override=True`), `imports` (modules task processes import; by default every module that
declared a task or hook), `process_start_timeout` (how long a task process has to import its modules
and report ready, default 60 s), `max_runs_per_process` (runs a task process takes before it is
replaced; no limit by default; the fresh one starts once it has drained and exited, and until then it
counts against `processes`), `queue`, `result_ttl` (seconds a finished task is kept), `retries` (how
many more times a failed run is tried, 0 by default) and `timeout` (how long a run may take; none by
default), both for the tasks that set no `retries` or `timeout` of their own, `result_backend` (where
results are mirrored; see "Results after a restart"), `cancel_grace`, `queues`, `queue_options` and `router` (see "Queues" below), `memory_soft_limit` and
`memory_hard_limit` (bytes), `version_block_ttl` (how long a task left for another worker waits before
it fails with `MigrationError`, one hour by default; see "Schemas and migrations"), `drain_grace` and
`drain_wait_limit` (see "Shutdown"), `health_listen` (see "Health"), and the networked settings `listen`,
`seeds`, `external_address`, `authority`, `shard`, `replication_factor`, `seed_rounds`, `routing_refresh`,
`heartbeat_interval`, `heartbeat_timeout`, `roll_call_deadline` and `reconnect_timeout` (see
"Networked workers"). `memory_soft_limit` and `memory_hard_limit` default to a quarter and a half of the
memory this process may use (see "Coalescing and backpressure"). In the environment a duration is a number of seconds, and `seeds`, `imports`
and `queues` are comma-separated lists. `KABUDACHI_RESULT_BACKEND` can give a backend URL, and
`KABUDACHI_TIMEOUT` is seconds (empty is none); an object `result_backend` can only be configured in code.

`result_ttl` is the process's retention. A task keeps its result for its own `result_ttl`, else its
queue's (`QueueOptions.result_ttl`), else the process's; a task or queue that leaves it out
inherits, and one that sets `None` keeps no result: nothing is mirrored to the result backend, and its
record stays in the shard only the minute the shard needs. The shard keeps a finished record at least a
minute whatever the value. Every worker of a shard should give the same `result_ttl`, as the leader's
keeps the records and the worker that ran a task sets its mirrored entry's expiry.

`retries` and `timeout` are the defaults of a task that sets none of its own; a task's own `timeout=None`
means no limit. A task declared `retriable=False` never inherits `retries`, and an `@ephemeral_task` is
never retried.

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

A failed run's message (not its traceback) is also stored in the task's record, cut to 1 KiB and to the
room the record has left, with the error's type cut to 128 bytes. Every holder of the record can read
it, so keep secrets out of exception messages. A failure message mirrored to a result backend is cut to
a quarter of its page.

Inside a task body, `kabudachi.current_context()` gives the `RunInfo` of the run, or `None` outside
one: `task_id`, `run_id`, `task_name`, `attempt` (1 for the first run), `shard` (`local` for a shard of
one), `worker_id`, and `trace_id` and `span_id`, which are empty because no trace is recorded yet. To
put them on log records, add `kabudachi.logging.ContextFilter()` to a handler; each becomes an
attribute for a formatter (`%(task_id)s`), an empty string outside a body and `0` for `attempt`.

The native core logs through Python's `logging` under `kabudachi.native.<crate>.<module>`; set the
level of `kabudachi.native` to see it. The native side never waits for Python: it queues up to 4,096
records, drops the rest and logs how many it dropped, and stops forwarding when the interpreter exits.
A backend failure is logged on `kabudachi.result_backend` (see "Results after a restart").

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
- A networked client reading a task's record, which `status()` does, waits out a five-second lookup
  timeout when a shard peer is connected but not answering.
- A group or map whose submitter dies keeps running until cancelled (see "Large groups and maps").
- A worker that is killed after a run is certified but before its result is mirrored leaves that run
  `pending` in the result backend (see "Results after a restart").
- Workers of a shard must give the same `queue_options`, `result_ttl` and memory limits; nothing checks
  that they do.
