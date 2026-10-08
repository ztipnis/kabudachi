# kabudachi (Python runtime)

The Python package of kabudachi, a peer-to-peer task queue with a compiled native core. This package
runs one worker, which is its own leader, with no network: the tasks you call are queued and certified
inside the process that calls them, and their bodies run in task processes it starts (one per CPU), or
in the process itself with `processes=0`.

Tasks take one input and return one result, both protobuf messages (`pip install kabudachi[protobuf]`),
or other types if you register a serializer.

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

## Running

- `kabudachi.run(main)` runs `main()` and, when it returns, waits for every task that was called
  (and every flow, group and callback) to finish. If `main` raises or is cancelled, running tasks
  are let finish, tasks that have not started fail with `RunStoppedError`, and the error is raised.
- `kabudachi.run()` with no `main` serves as a worker on the main thread until SIGINT or SIGTERM,
  then drains the same way. A second signal stops waiting and raises `KeyboardInterrupt`.
- It cannot be called from a running event loop, and only one can run per process.

## Declaring tasks

`@task`, `@ephemeral_task` and `@coalescing_task` take:

| option | meaning |
|---|---|
| `name` | how workers find the task; defaults to its module and name, which then must be at package scope |
| `version`, `queue`, `serializer` | the data format version, the queue, and the registered serializer name |
| `retries` | how many more times a run that raises or times out is tried again (not for `@ephemeral_task`) |
| `timeout`, `cancel_grace` | ask the body to stop after `timeout`; fail the run if it has not stopped `cancel_grace` later |
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

A stage that consumes the array of a `collect_all` group receives failures in it, so it needs a
serializer that can encode them; the protobuf serializer cannot.

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
`kabudachi._native`: `EventKind` (`EXPIRED`, `SUPERSEDED`, `SLOW_DOWN`, `CANCELLED`, `RECORD_FULL`, `COALESCED_PAYLOAD_TOO_LARGE`) for scheduler
events, `CancelOutcome` (`CANCELLED`, `ALREADY_FINISHED`, `UNKNOWN_TASK`) for how a cancel ended, and
`RunState` (`SCHEDULED` through `ORPHANED`) for where a run is. The runtime turns these into the
handle behaviour described above; you meet them only if you call the native module directly.
`COALESCED_PAYLOAD_TOO_LARGE` surfaces as `CoalescedPayloadTooLargeError` on the handle.

## Configuration

`kabudachi.configure(...)`, or `KABUDACHI_<NAME>` in the environment:

`processes` (task processes; one per CPU by default, `0` runs bodies in this process), `concurrency`
(places per process, at most 32 without `concurrency_override=True`), `imports` (modules task processes
import; by default every module that declared a task), `queue`, `result_ttl` (seconds a finished task
is kept), `cancel_grace`, `memory_soft_limit` and `memory_hard_limit` (bytes).

## Logging

A task that fails is logged at WARNING by the type of its error only, never its message. At DEBUG the
full traceback and message are logged, which can include task input, so leave DEBUG off where that
matters.

## Limits of this phase

- Everything the worker holds is in memory, and lost when it ends.
- A body that ignores a timeout or a cancel for longer than its `cancel_grace` costs its task process:
  the run settles at once (failed, cancelled, or its retry queued to start once the old body exits), the
  process takes no new runs and is replaced at once, and it is stopped once its other bodies have
  finished, so they are never killed for it. With `processes=0` such a body cannot be killed: its run
  settles the same way and the body is abandoned to finish on its own, holding its concurrency place
  until it does.
- A task process that dies (a crash, running out of memory, a kill) takes its runs with it and is
  replaced, after a growing wait while replacements keep dying quickly. Each run it held is tried again
  without using up a retry, except an `@ephemeral_task`'s, and a `@coalescing_task` generation's that a
  newer generation of its key is waiting to supersede; their handles raise `TaskLostError`. With
  `processes=0` there is no such loss, and `@ephemeral_task` behaves like `@task`.
