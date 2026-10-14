# Getting started

`kabudachi.run()` starts a worker node with no required arguments. Callers submit tasks and await results; workers execute tasks. These are separate roles, and a caller must not start a worker to submit work.

The current runtime does not yet meet this core model: it combines submission with worker startup, and networked handles cannot deliver results. Its `run(main)` form also exposes test-only behavior through the production API. Accepting a function is a defect; the production worker entry point should be `kabudachi.run()` with no arguments.

## Build from source

Use Python 3.12 or later, uv, and Bazel through Bazelisk. Run these commands from the repository root on macOS or Linux. Bazel uses the version pinned by the repository and builds the native extension.

```sh
bazel build //bindings:_native
uv sync --project runtime --extra protobuf
export PYTHONPATH="$PWD/runtime/src:$PWD/bazel-bin/bindings${PYTHONPATH:+:$PYTHONPATH}"
```

The Python package needs both its Python sources and the compiled extension. Installing the Python project alone does not build that extension. The protobuf extra supplies the default serializer's dependency.

Verify that the Python API and native extension load:

```sh
runtime/.venv/bin/python -c 'import kabudachi; print(kabudachi.__version__)'
```

Then save your task definitions in an importable module, such as `my_tasks.py`, using the [task declaration example](../README.md#define-a-task).

```mermaid
sequenceDiagram
    actor Caller
    participant Queue as Distributed task queue
    participant Worker
    Caller->>Queue: Submit typed task input
    Queue->>Worker: Assign task
    Worker->>Queue: Return task result
    Queue-->>Caller: Resolve task handle
    Note over Caller,Worker: Intended production workflow; caller separation and remote results remain incomplete
```

There is currently no complete production caller walkthrough to recommend. The function-accepting `run(main)` test API must not substitute for an independent caller.

## Declare tasks

A task accepts exactly one argument. Annotate both the input and return value with types the selected serializer supports. The default protobuf serializer supports protobuf messages, lists of messages, and `None`; plain strings and dictionaries need another registered serializer.

Task functions may be synchronous or asynchronous. Calling a decorated function should submit work from a caller. Currently, submission requires an active worker runtime, which conflicts with that role separation. Use `task.local(input)` to exercise its body and serialization without starting a worker. For an async body, await the value returned by `.local()`.

All workers must import compatible task definitions under the same names and use matching versions and serializers.

## Use task processes

By default, Kabudachi starts one task process per available CPU. Put task declarations in an importable module, such as `my_tasks.py`, and import them from a separate entry script. Start the worker with no-argument `kabudachi.run()` under an `if __name__ == "__main__":` guard.

Configure the process count and concurrency before starting the runtime:

```python
kabudachi.configure(processes=2, concurrency=8, imports=("my_tasks",))
```

`concurrency` is the number of task bodies each task process may run at once. Use lifecycle hooks to initialize process resources and prepare or clean up each run. Process recycling can limit accumulated memory or application state.

## Control timing and failure

Set task defaults on the decorator:

```python
from datetime import timedelta

@kabudachi.task(retries=2, timeout=timedelta(seconds=30))
async def greet(request: StringValue) -> StringValue:
    return StringValue(value=f"Hello, {request.value}!")
```

`retries=2` permits two further attempts after a body raises or times out. This differs from replay after losing a worker. Keep effects safe to repeat in either case.

Set timing for a single submission while a runtime is active:

```python
handle = greet.options(
    delay=timedelta(seconds=5),
    expires=timedelta(minutes=1),
)(StringValue(value="world"))
```

A delay sets the earliest start, not a guaranteed start time. Expiry fails work that has not started before its deadline. Use either `delay` or a timezone-aware `eta` for the start constraint.

## Start networked workers

Use an importable task module on every worker. A worker entry script can be as small as:

```python
import kabudachi
import my_tasks

if __name__ == "__main__":
    kabudachi.run()
```

With Redis or Valkey available, configure each worker through its environment:

```sh
export KABUDACHI_SHARD=reports
export KABUDACHI_LISTEN=/ip4/0.0.0.0/tcp/4001
export KABUDACHI_AUTHORITY=redis://127.0.0.1:6379/0
runtime/.venv/bin/python worker.py
```

Use a distinct port for each worker sharing a host. The authority URL must reach the same coordination service from every worker. Set `KABUDACHI_EXTERNAL_ADDRESS` to a reachable worker address when its listening address is a wildcard or differs from the address peers use. Workers can also discover the shard through configured seed addresses.

Task submission should work from an application without a top-level `kabudachi.run(...)`. Currently it raises `RuntimeNotStartedError` unless that process has an active worker runtime. This exposes an architectural gap: the current API combines the caller and worker roles instead of providing an independent caller.

The existing `kabudachi.run(main)` API executes a supplied function while also joining as a worker. This is test-only behavior exposed at the production boundary and is a defect, not a caller integration to recommend. Remote-result delivery is a separate gap: awaiting a networked handle raises `RemoteResultUnavailableError`, including when a task awaits a child task. Writing outputs to an application store can work around that gap temporarily. Local flow, group, and map examples rely on local result delivery.

A worker should start only through no-argument `kabudachi.run()`.

The first SIGINT or SIGTERM asks a serving worker to drain. A second signal forces it to stop waiting.

## Settings and common problems

`kabudachi.configure(...)` overrides matching `KABUDACHI_<NAME>` environment settings. An explicit task setting takes precedence where that setting is supported. Set worker configuration before `run()` starts.

| Symptom | Check |
| --- | --- |
| Cannot import `kabudachi._native` | Build the native target and include `bazel-bin/bindings` in `PYTHONPATH` |
| Serializer rejects a type | Use supported protobuf types or register a suitable serializer on every worker |
| Task declared in the entry script is refused | Move it to an importable module, or use `processes=0` for a local experiment |
| `run()` reports an existing event loop | Call it from a synchronous entry point; it owns its event loop |
| `RemoteResultUnavailableError` | Known remote-result delivery gap; local mode or external output storage is a temporary workaround |
| `RuntimeNotStartedError` when submitting from an application | The independent caller role is missing from the current runtime |
| `kabudachi.run` accepts a function | Known production API defect: test-only behavior is exposed at the worker entry point |
| `BackpressureError` | Reduce submission volume or task input size, or review the configured memory limits |
