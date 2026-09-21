""".local(): run a task's function in this process, with no runtime, checked
and converted the way a worker would."""

import asyncio
import inspect

import pytest

from kabudachi.errors import RuntimeNotStartedError, SerializationError
from kabudachi.registry import TaskRegistry
from kabudachi.serializers import SerializerRegistry
from kabudachi.tasks import Task
from proto_messages import Greeting, Receipt


def declare(function):
    return Task(
        function,
        registry=TaskRegistry(),
        serializers=SerializerRegistry.with_defaults(),
        name=f"tests.{function.__name__}",
    )


def shout(request: Greeting) -> Greeting:
    return Greeting(text=request.text.upper(), times=request.times)


async def shout_later(request: Greeting) -> Greeting:
    await asyncio.sleep(0)
    return Greeting(text=request.text.upper(), times=request.times)


def wrong_result(request: Greeting) -> Receipt:
    return Greeting(text="not a receipt")


def explode(request: Greeting) -> Greeting:
    raise ValueError("body failed")


def test_a_synchronous_task_runs_in_place_and_returns_its_result():
    assert declare(shout).local(Greeting(text="hi", times=2)) == Greeting(text="HI", times=2)


def test_an_async_task_gives_an_awaitable_for_its_result():
    outcome = declare(shout_later).local(Greeting(text="hi"))

    assert inspect.isawaitable(outcome)
    assert asyncio.run(outcome).text == "HI"


def test_local_needs_no_running_runtime():
    # No run() anywhere in this module: calling the task itself would refuse.
    task = declare(shout)
    with pytest.raises(RuntimeNotStartedError, match=r"kabudachi\.run"):
        task(Greeting())
    assert task.local(Greeting(text="a")).text == "A"


def test_the_body_sees_a_copy_of_the_argument_as_a_worker_would():
    def mutate(request: Greeting) -> Greeting:
        request.text = "changed"
        return request

    argument = Greeting(text="original")
    declare(mutate).local(argument)

    assert argument.text == "original"


def test_an_argument_of_the_wrong_type_is_refused_before_the_body_runs():
    ran = []

    def record(request: Greeting) -> Greeting:
        ran.append(True)
        return request

    with pytest.raises(SerializationError):
        declare(record).local("not a message")

    assert ran == []


def test_a_result_of_the_wrong_type_is_refused():
    with pytest.raises(SerializationError):
        declare(wrong_result).local(Greeting())


def test_an_error_in_the_body_is_raised_as_it_is():
    with pytest.raises(ValueError, match="body failed"):
        declare(explode).local(Greeting())


def test_an_async_result_of_the_wrong_type_is_refused_when_awaited():
    async def wrong(request: Greeting) -> Receipt:
        return Greeting()

    outcome = declare(wrong).local(Greeting())

    with pytest.raises(SerializationError):
        asyncio.run(outcome)
