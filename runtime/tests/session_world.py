"""A session wired to a real native runtime, for tests of what happens between
a task being called and its handle being settled."""

import asyncio
import contextlib
import pickle
import weakref

from kabudachi import _native

from kabudachi.config import Configuration
from kabudachi.registry import TaskRegistry
from kabudachi.serializers import SerializerRegistry
from kabudachi.session import Session, activate, deactivate
from kabudachi.tasks import Task
from faulting_runtime import FaultingRuntime

WAIT = 5


class World:
    def __init__(
        self,
        *functions,
        concurrency=None,
        memory_soft_limit=None,
        memory_hard_limit=None,
        **task_options,
    ):
        if (memory_soft_limit is None) != (memory_hard_limit is None):
            raise ValueError("give both memory limits or neither")
        limits = {}
        if memory_soft_limit is not None:
            limits = {
                "memory_soft_limit": memory_soft_limit,
                "memory_hard_limit": memory_hard_limit,
            }
        self.native = _native.NativeRuntime("worker-world", "incarnation-world", **limits)
        weakref.finalize(self, self.native.shutdown)
        self.runtime = FaultingRuntime(self.native)
        self.registry = TaskRegistry()
        self.serializers = SerializerRegistry.with_defaults()
        self.configuration = Configuration()
        if concurrency is not None:
            self.configuration.configure(concurrency=concurrency, concurrency_override=True)
        self.tasks = {
            function.__name__: Task(
                function,
                registry=self.registry,
                serializers=self.serializers,
                name=f"tests.{function.__name__}",
                **task_options,
            )
            for function in functions
        }
        self.session = Session(
            self.runtime, self.registry, self.serializers, self.configuration
        )

    def call(self, name, argument):
        return self.session.submit(self.tasks[name].definition, argument)

    async def working(self, body):
        """Runs `body` while the session's worker loop is claiming tasks."""
        await asyncio.wait_for(self.native.wait_until_leader(), WAIT)
        worker = asyncio.ensure_future(self.session.work())
        try:
            return await asyncio.wait_for(body(), WAIT)
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)


async def until(condition, what="the condition", limit=WAIT):
    """Waits for `condition()` to be true, polling, and fails once `limit` has passed."""
    deadline = asyncio.get_running_loop().time() + limit
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError(f"{what} never became true")
        await asyncio.sleep(0.001)


def run(coroutine):
    return asyncio.run(coroutine)


async def with_events(world, body):
    """Runs `body` with both the worker loop and the event watcher going."""
    events = asyncio.ensure_future(world.session.watch_events())
    try:
        return await world.working(body)
    finally:
        events.cancel()
        await asyncio.gather(events, return_exceptions=True)


@contextlib.contextmanager
def activated(world):
    """Makes the world's session the one tasks are called on, as `run()` does."""
    activate(world.session)
    try:
        yield
    finally:
        deactivate(world.session)


class Pickle:
    """A backend that can encode anything, failures included, which protobuf cannot."""

    name = "pickle"

    def available(self):
        return True

    def supports(self, value_type):
        return True

    def encode(self, value, value_type):
        return pickle.dumps(value)

    def decode(self, payload, target_type):
        return pickle.loads(payload)
