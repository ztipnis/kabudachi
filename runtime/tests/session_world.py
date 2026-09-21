"""A session wired to a fake runtime, for tests of what happens between a task
being called and its handle being settled."""

import asyncio
import contextlib
import pickle

from kabudachi.config import Configuration
from kabudachi.registry import TaskRegistry
from kabudachi.serializers import SerializerRegistry
from kabudachi.session import Session, activate, deactivate
from kabudachi.tasks import Task
from fake_runtime import FakeRuntime

WAIT = 5


class World:
    def __init__(self, *functions, concurrency=None, **task_options):
        self.runtime = FakeRuntime()
        self.registry = TaskRegistry()
        self.serializers = SerializerRegistry.with_defaults()
        self.configuration = Configuration()
        if concurrency is not None:
            self.configuration.configure(concurrency=concurrency)
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
        worker = asyncio.ensure_future(self.session.work())
        try:
            return await asyncio.wait_for(body(), WAIT)
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)


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
