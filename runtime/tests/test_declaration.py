"""Declaring a task and calling it: `@task` and `@ephemeral_task` turn a
function into a registered task and refuse what cannot work as one, `.local()`
runs it in this process checked and converted the way a worker would, `.options()`
checks what is asked of a submission, configuration settles what a task did not
choose, and the handle a call returns is awaitable and settled once from any
thread, on the awaiting loop."""

import asyncio
import inspect
import os
import sys
import threading
import types
from datetime import datetime, timedelta, timezone

import pytest

import proto_messages
from kabudachi import ephemeral_task, task
from kabudachi import config as config_module
from kabudachi import registry as registry_module
from kabudachi.config import UNSET, Configuration
from kabudachi.errors import (
    ConfigurationError,
    DuplicateTaskError,
    SerializationError,
    TaskDefinitionError,
)
from kabudachi.handle import TaskHandle
from kabudachi.options import SubmissionOptions, submission_options
from kabudachi.registry import TaskKind, TaskRegistry
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


def charge(request: Greeting) -> Receipt:
    """Charge a customer."""
    return Receipt(ok=True)


async def charge_later(request: Greeting) -> Receipt:
    return Receipt(ok=True)


def unannotated_input(request) -> Receipt:
    return Receipt()


def unannotated_output(request: Greeting):
    return None


def no_parameters() -> Receipt:
    return Receipt()


def two_parameters(request: Greeting, extra: Greeting) -> Receipt:
    return Receipt()


def variadic(*requests: Greeting) -> Receipt:
    return Receipt()


def keyword_variadic(**requests: Greeting) -> Receipt:
    return Receipt()


def bad_attribute(request: "proto_messages.Nope") -> Receipt:
    return Receipt()


def bad_syntax(request: "list[") -> Receipt:  # noqa: F722  (deliberately not valid)
    return Receipt()


def bad_expression(request: "1/0") -> Receipt:
    return Receipt()


def wrong_input_type(request: int) -> Receipt:
    return Receipt()


def wrong_return_type(request: Greeting) -> str:
    return ""


def text_echo(text: str) -> str:
    return text


def with_attribute(request: Greeting) -> Receipt:
    return Receipt()


with_attribute.definition = "an attribute of the function that must not become the definition"

MODULE_LAMBDA = lambda request: request  # noqa: E731 - a lambda at package scope is the case


class Charger:
    def __call__(self, request: Greeting) -> Receipt:
        return Receipt()


class AsyncCharger:
    async def __call__(self, request: Greeting) -> Receipt:
        return Receipt()


class Utf8Text:
    name = "utf8"

    def available(self):
        return True

    def supports(self, value_type):
        return value_type is str

    def encode(self, value, value_type):
        return value.encode()

    def decode(self, payload, target_type):
        return payload.decode()


class Unavailable(Utf8Text):
    name = "unavailable"

    def available(self):
        return False

    def supports(self, value_type):
        return False


def define(func, **options):
    return Task(func, registry=TaskRegistry(), **options)


def test_a_task_is_registered_under_its_module_and_name():
    registry = TaskRegistry()

    Task(charge, registry=registry)

    name = f"{charge.__module__}.charge"
    assert name in registry
    assert registry.get(name).func is charge


def test_a_task_keeps_the_name_and_docs_of_its_function():
    wrapped = define(charge)

    assert wrapped.__name__ == "charge"
    assert wrapped.__doc__ == "Charge a customer."
    assert wrapped.__wrapped__ is charge
    assert inspect.signature(wrapped) == inspect.signature(charge)


def test_the_same_name_cannot_be_registered_twice():
    registry = TaskRegistry()
    Task(charge, registry=registry)

    name = f"{charge.__module__}.charge"

    with pytest.raises(DuplicateTaskError, match=name.replace(".", r"\.")):
        Task(charge_later, registry=registry, name=name)

    assert registry.get(name).func is charge


BAD_OPTIONS = (
    [("name", bad, "name") for bad in ["", "   ", 3, None]]
    + [("version", bad, "version") for bad in [-1, 1.5, "1", True, None]]
    + [("queue", bad, "queue") for bad in ["", 3, None]]
    + [("serializer", bad, "serializer") for bad in ["", "  ", 3, None]]
    + [("retries", bad, "retries") for bad in [-1, True, 1.5, "2", None]]
    + [("timeout", bad, "timeout") for bad in [timedelta(0), timedelta(seconds=-1), 5, "5s", True]]
    + [("cancel_grace", bad, "cancel_grace") for bad in [timedelta(seconds=-1), 5, "5s", None, True]]
)


@pytest.mark.parametrize(
    ("option", "bad", "match"), BAD_OPTIONS, ids=[f"{o}={b!r}" for o, b, _ in BAD_OPTIONS]
)
def test_an_option_of_the_wrong_kind_or_range_is_refused(option, bad, match):
    options = {option: bad}
    if option == "cancel_grace":
        options["timeout"] = timedelta(seconds=1)

    with pytest.raises(TaskDefinitionError, match=match):
        define(charge, **options)


def a_nested_function():
    def nested(request: Greeting) -> Receipt:
        return Receipt()

    return nested


@pytest.mark.parametrize(
    "func",
    [lambda request: request, a_nested_function(), MODULE_LAMBDA],
    ids=["lambda", "nested function", "lambda at package scope"],
)
def test_a_lambda_or_nested_function_cannot_be_a_task_unless_named(func):
    with pytest.raises(TaskDefinitionError, match="package scope"):
        define(func)


def test_a_nested_function_can_be_a_task_if_given_a_name():
    def nested(request: Greeting) -> Receipt:
        return Receipt()

    assert define(nested, name="tests.nested").definition.name == "tests.nested"


def test_something_that_is_not_callable_is_refused():
    with pytest.raises(TaskDefinitionError, match="callable"):
        define("not a function")


def test_missing_annotations_are_refused():
    with pytest.raises(TaskDefinitionError, match="input"):
        define(unannotated_input)
    with pytest.raises(TaskDefinitionError, match="return"):
        define(unannotated_output)


@pytest.mark.parametrize("func", [no_parameters, two_parameters, variadic, keyword_variadic])
def test_a_task_takes_exactly_one_argument(func):
    with pytest.raises(TaskDefinitionError, match="one"):
        define(func)


def test_a_failed_definition_registers_nothing():
    registry = TaskRegistry()

    with pytest.raises(TaskDefinitionError):
        Task(no_parameters, registry=registry)

    assert registry.definitions() == []


@pytest.mark.parametrize(
    ("decorate", "name", "queue", "version"),
    [
        pytest.param(lambda: task(charge), f"{charge.__module__}.charge", UNSET, 0, id="bare"),
        pytest.param(
            lambda: task(name="billing.charge", queue="gpu", version=4)(charge),
            "billing.charge",
            "gpu",
            4,
            id="with options",
        ),
    ],
)
def test_the_public_decorator_registers_in_the_default_registry(
    monkeypatch, decorate, name, queue, version
):
    monkeypatch.setattr(registry_module, "_default_registry", TaskRegistry())

    decorated = decorate()

    assert isinstance(decorated, Task)
    assert decorated.definition.name == name
    assert decorated.definition.name in registry_module.default_registry()
    assert decorated.definition.queue == queue
    assert decorated.definition.version == version


def test_the_public_decorator_refuses_a_positional_name():
    with pytest.raises(TypeError):
        task("billing.charge")


class Uninspectable:
    """A callable whose signature is not something `inspect` can read."""

    __signature__ = "not a signature"

    def __call__(self, request):
        return request


@pytest.mark.parametrize(
    ("func", "name"),
    [(max, None), (Uninspectable(), "tests.uninspectable")],
    ids=["builtin without a signature", "callable object with a broken signature"],
)
def test_a_callable_without_a_usable_signature_is_refused(func, name):
    options = {} if name is None else {"name": name}
    with pytest.raises(TaskDefinitionError, match="signature"):
        define(func, **options)


@pytest.mark.parametrize("func", [bad_attribute, bad_syntax, bad_expression])
def test_an_annotation_that_cannot_be_resolved_is_a_task_definition_error(func):
    with pytest.raises(TaskDefinitionError, match="cannot be resolved"):
        define(func)


def test_a_callable_object_can_be_a_task_if_it_is_named():
    charger = define(Charger(), name="tests.charger").definition

    assert charger.input_type is Greeting
    assert charger.output_type is Receipt


def test_a_class_cannot_be_a_task():
    with pytest.raises(TaskDefinitionError, match="class"):
        define(Charger, name="tests.charger")


def test_an_attribute_of_the_function_cannot_replace_the_definition():
    wrapped = define(with_attribute)

    assert wrapped.definition.func is with_attribute


@pytest.mark.parametrize(
    ("func", "role"), [(wrong_input_type, "input"), (wrong_return_type, "return")]
)
def test_types_the_serializer_cannot_handle_are_refused_where_declared(func, role):
    with pytest.raises(TaskDefinitionError, match=f"protobuf.*{role} type"):
        define(func)


def test_types_a_custom_serializer_handles_are_accepted():
    serializers = SerializerRegistry()
    serializers.register(Utf8Text())

    accepted = Task(
        text_echo, registry=TaskRegistry(), serializer="utf8", serializers=serializers
    )

    assert accepted.definition.serializer == "utf8"


def test_types_a_custom_serializer_cannot_handle_are_refused():
    serializers = SerializerRegistry()
    serializers.register(Utf8Text())

    with pytest.raises(TaskDefinitionError, match=r"utf8.*input type"):
        Task(charge, registry=TaskRegistry(), serializer="utf8", serializers=serializers)


@pytest.mark.parametrize("serializer", ["registered-later", "unavailable"])
def test_a_serializer_that_is_not_registered_or_cannot_run_here_is_not_checked(serializer):
    serializers = SerializerRegistry()
    serializers.register(Unavailable())

    accepted = Task(
        charge, registry=TaskRegistry(), serializer=serializer, serializers=serializers
    )

    assert accepted.definition.serializer == serializer


def test_a_callable_object_with_an_async_call_is_an_async_task():
    assert define(AsyncCharger(), name="tests.async_charger").definition.is_async is True
    assert define(Charger(), name="tests.charger").definition.is_async is False


def test_a_task_gives_access_to_the_functions_own_attributes():
    def marked(request: Greeting) -> Receipt:
        return Receipt()

    marked.custom_note = "kept"

    assert define(marked, name="tests.marked").custom_note == "kept"


def test_a_task_that_wraps_an_async_function_is_recognised_as_a_coroutine_function():
    wrapped = define(charge_later)

    assert inspect.iscoroutinefunction(wrapped)


@pytest.fixture
def fresh_registry(monkeypatch):
    monkeypatch.setattr(registry_module, "_default_registry", TaskRegistry())


def test_an_ephemeral_task_is_never_retried_so_it_takes_no_retries(fresh_registry):
    with pytest.raises(TypeError):
        ephemeral_task(name="retry.ephemeral", retries=1)(charge)


def test_a_zero_cancel_grace_and_an_ephemeral_timeout_are_allowed(fresh_registry):
    zero = task(
        name="timing.zero", timeout=timedelta(seconds=1), cancel_grace=timedelta(0)
    )(charge).definition
    ephemeral = ephemeral_task(name="timing.ephemeral", timeout=timedelta(seconds=2))(charge)

    assert zero.cancel_grace == timedelta(0)
    assert ephemeral.definition.kind is TaskKind.EPHEMERAL
    assert ephemeral.definition.timeout == timedelta(seconds=2)


def add_greetings(older: Greeting, newer: Greeting) -> Greeting:
    return Greeting(text=older.text + newer.text)


def test_a_coalescing_task_is_registered_with_its_kind_and_reducer(fresh_registry):
    from kabudachi import coalescing_task

    defined = coalescing_task(
        name="coalesce.merged", merge=add_greetings, drop_oldest=True
    )(charge_greeting).definition
    plain = coalescing_task(name="coalesce.plain")(charge_greeting).definition

    assert defined.kind is TaskKind.COALESCING
    assert defined.merge is add_greetings
    assert defined.drop_oldest is True
    assert plain.kind is TaskKind.COALESCING
    assert plain.merge is None
    assert plain.drop_oldest is False


def one_argument(only: Greeting) -> Greeting:
    return only


def three_arguments(a: Greeting, b: Greeting, c: Greeting) -> Greeting:
    return a


@pytest.mark.parametrize("bad", ["not callable", 3, one_argument, three_arguments])
def test_a_reducer_must_be_a_function_of_two_arguments(fresh_registry, bad):
    from kabudachi import coalescing_task

    with pytest.raises(TaskDefinitionError, match="merge"):
        coalescing_task(name="coalesce.bad", merge=bad)(charge_greeting)


@pytest.mark.parametrize(
    ("option", "match"),
    [
        pytest.param({"merge": add_greetings}, None, id="reducer"),
        # Matched on the message: `drop_oldest` was once an unknown keyword,
        # which is also a TypeError, so a bare `raises(TypeError)` would pass unbuilt.
        pytest.param({"drop_oldest": True}, "only a coalescing task takes drop_oldest", id="drop_oldest"),
    ],
)
def test_only_a_coalescing_task_takes_a_reducer_or_drop_oldest(option, match):
    with pytest.raises(TypeError, match=match):
        Task(charge_greeting, registry=TaskRegistry(), name="only.coalescing", **option)


def charge_greeting(request: Greeting) -> Greeting:
    return request


# --- .local() --------------------------------------------------------------

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


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_a_result_of_the_wrong_type_is_refused(is_async):
    if is_async:

        async def wrong(request: Greeting) -> Receipt:
            return Greeting()

        outcome = declare(wrong).local(Greeting())
        with pytest.raises(SerializationError):
            asyncio.run(outcome)
    else:
        with pytest.raises(SerializationError):
            declare(wrong_result).local(Greeting())


def test_an_error_in_the_body_is_raised_as_it_is():
    with pytest.raises(ValueError, match="body failed"):
        declare(explode).local(Greeting())


# --- .options() ------------------------------------------------------------

NOW = datetime(2030, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def options(**arguments):
    return submission_options(now=NOW, **arguments)


@pytest.mark.parametrize(
    ("arguments", "field", "expected"),
    [
        pytest.param({"delay": timedelta(seconds=2, milliseconds=500)}, "delay_ms", 2500, id="delay in milliseconds"),
        pytest.param({"delay": timedelta(microseconds=1)}, "delay_ms", 1, id="fraction of a millisecond rounds up"),
        pytest.param({"eta": NOW + timedelta(minutes=5)}, "delay_ms", 300_000, id="eta is time from now"),
        pytest.param({"eta": NOW - timedelta(hours=1)}, "delay_ms", 0, id="eta in the past is no delay"),
        pytest.param({"expires": timedelta(seconds=10)}, "expires_in_ms", 10_000, id="expires as a duration"),
        pytest.param({"expires": NOW + timedelta(seconds=10)}, "expires_in_ms", 10_000, id="expires as a time"),
        pytest.param({"expires": NOW - timedelta(seconds=1)}, "expires_in_ms", 0, id="expiry already past"),
    ],
)
def test_what_is_asked_becomes_the_milliseconds_the_runtime_takes(arguments, field, expected):
    assert getattr(options(**arguments), field) == expected


def test_a_delay_and_an_eta_cannot_both_be_given():
    with pytest.raises(ValueError, match=r"delay.*eta"):
        options(delay=timedelta(seconds=1), eta=NOW + timedelta(seconds=1))


@pytest.mark.parametrize("bad", [timedelta(seconds=-1), -1, 1.5, "1s"])
def test_a_delay_must_be_a_non_negative_timedelta(bad):
    with pytest.raises((ValueError, TypeError)):
        options(delay=bad)


def test_a_time_needs_a_time_zone():
    naive = datetime(2030, 1, 1, 12, 5)
    with pytest.raises(ValueError, match="time zone"):
        options(eta=naive)
    with pytest.raises(ValueError, match="time zone"):
        options(expires=naive)


def test_a_task_that_would_expire_before_it_can_start_is_refused():
    with pytest.raises(ValueError, match="expire"):
        options(delay=timedelta(seconds=10), expires=timedelta(seconds=5))
    with pytest.raises(ValueError, match="expire"):
        options(delay=timedelta(seconds=10), expires=timedelta(seconds=10))


def test_a_task_that_expires_after_it_can_start_is_accepted():
    assert options(delay=timedelta(seconds=5), expires=timedelta(seconds=10)) == SubmissionOptions(
        delay_ms=5000, expires_in_ms=10_000
    )


def passthrough(request: Greeting) -> Greeting:
    return request


def test_options_are_checked_when_they_are_given_not_when_the_task_is_called():
    with pytest.raises(ValueError):
        declare(passthrough).options(delay=timedelta(seconds=-1))


def coalescing_task_for_options():
    from kabudachi.registry import TaskKind

    def refresh(request: Greeting) -> Greeting:
        return request

    return Task(
        refresh,
        registry=TaskRegistry(),
        serializers=SerializerRegistry.with_defaults(),
        name="tests.refresh",
        kind=TaskKind.COALESCING,
    )


def test_only_a_coalescing_task_takes_a_key():
    with pytest.raises(ValueError, match="coalescing"):
        declare(passthrough).options(key="tenant-1")


@pytest.mark.parametrize("bad", [1, 2.5, b"x"])
def test_a_key_must_be_a_string(bad):
    with pytest.raises(TypeError):
        coalescing_task_for_options().options(key=bad)


@pytest.mark.parametrize("argument", ["eta", "expires"])
def test_a_time_that_is_not_a_datetime_is_a_type_error_not_an_attribute_error(argument):
    with pytest.raises(TypeError):
        options(**{argument: 0})


# --- configuration ---------------------------------------------------------

def test_configuring_again_keeps_earlier_settings_and_replaces_the_one_given():
    configuration = Configuration()
    configuration.configure(queue="emails")

    configuration.configure(concurrency=2)

    assert configuration.resolve("queue") == "emails"
    assert configuration.resolve("concurrency") == 2

    configuration.configure(concurrency=8)

    assert configuration.resolve("queue") == "emails"
    assert configuration.resolve("concurrency") == 8


def test_an_unknown_setting_is_refused():
    with pytest.raises(ConfigurationError, match="no_such_setting"):
        Configuration().configure(no_such_setting=1)


@pytest.mark.parametrize(
    "settings",
    [
        {"concurrency": 0},
        {"concurrency": -1},
        {"concurrency": 1.5},
        {"concurrency": "many"},
        {"concurrency": True},
        {"concurrency": None},
        {"queue": ""},
        {"queue": "   "},
        {"queue": 3},
        {"queue": None},
        {"result_ttl": 0},
        {"result_ttl": -1},
        {"result_ttl": 1.5},
        {"result_ttl": True},
        {"result_ttl": "1h"},
        {"cancel_grace": timedelta(seconds=-1)},
        {"cancel_grace": 5},
        {"cancel_grace": "5"},
        {"cancel_grace": True},
    ],
)
def test_an_invalid_value_is_refused_and_changes_nothing(settings):
    configuration = Configuration()
    configuration.configure(queue="emails")

    with pytest.raises(ConfigurationError):
        configuration.configure(**settings)

    assert configuration.resolve("queue") == "emails"


def test_a_call_with_one_bad_setting_applies_none_of_them():
    configuration = Configuration()

    with pytest.raises(ConfigurationError):
        configuration.configure(queue="emails", concurrency=0)

    assert configuration.resolve("queue") == "default"


def test_a_task_level_none_overrides_a_process_level_choice():
    configuration = Configuration()
    configuration.configure(queue="emails")

    assert configuration.resolve("queue", None) is None


def test_the_environment_sets_a_default(monkeypatch):
    monkeypatch.setenv("KABUDACHI_CONCURRENCY", "3")
    monkeypatch.setenv("KABUDACHI_QUEUE", "from-env")

    configuration = Configuration()

    assert configuration.resolve("concurrency") == 3
    assert configuration.resolve("queue") == "from-env"


def test_configure_wins_over_the_environment(monkeypatch):
    monkeypatch.setenv("KABUDACHI_CONCURRENCY", "3")
    configuration = Configuration()
    configuration.configure(concurrency=9)

    assert configuration.resolve("concurrency") == 9


def test_an_invalid_environment_value_is_a_configuration_error(monkeypatch):
    monkeypatch.setenv("KABUDACHI_CONCURRENCY", "many")

    with pytest.raises(ConfigurationError, match="concurrency"):
        Configuration().resolve("concurrency")


def test_the_environment_is_read_when_a_setting_is_first_needed(monkeypatch):
    configuration = Configuration()
    monkeypatch.setenv("KABUDACHI_CONCURRENCY", "5")

    assert configuration.resolve("concurrency") == 5


def test_result_ttl_defaults_to_an_hour():
    assert Configuration().resolve("result_ttl") == 3600


def test_cancel_grace_defaults_to_thirty_seconds_and_may_be_set_to_zero():
    configuration = Configuration()
    assert configuration.resolve("cancel_grace") == timedelta(seconds=30)

    configuration.configure(cancel_grace=timedelta(0))
    assert configuration.resolve("cancel_grace") == timedelta(0)


def test_cancel_grace_can_come_from_the_environment_in_seconds(monkeypatch):
    monkeypatch.setenv("KABUDACHI_CANCEL_GRACE", "2.5")

    assert Configuration().resolve("cancel_grace") == timedelta(seconds=2.5)


def test_configuring_only_the_soft_limit_above_the_default_hard_limit_is_refused():
    hard = config_module.DEFAULT_MEMORY_HARD_LIMIT
    with pytest.raises(ConfigurationError, match="soft"):
        Configuration().configure(memory_soft_limit=hard + 1)


@pytest.mark.parametrize("bad", [0, -1, 1.5, True, "big"])
def test_memory_limits_must_be_positive_integers(bad):
    with pytest.raises(ConfigurationError):
        Configuration().configure(memory_soft_limit=bad, memory_hard_limit=10**12)
    with pytest.raises(ConfigurationError):
        Configuration().configure(memory_hard_limit=bad)


def test_the_soft_limit_may_not_be_above_the_hard_limit():
    with pytest.raises(ConfigurationError, match="soft"):
        Configuration().configure(memory_soft_limit=2000, memory_hard_limit=1000)
    Configuration().configure(memory_soft_limit=1000, memory_hard_limit=1000)


def test_task_processes_default_to_one_per_cpu():
    assert Configuration().resolve("processes") == (os.cpu_count() or 1)


@pytest.mark.parametrize(
    "settings, refused",
    [
        ({"processes": -1}, "processes"),
        ({"processes": True}, "processes"),
        ({"concurrency": 33}, "concurrency_override"),
        ({"imports": "app.tasks"}, "imports"),
        ({"process_start_timeout": timedelta(0)}, "process_start_timeout"),
    ],
)
def test_worker_settings_that_cannot_work_are_refused(settings, refused):
    with pytest.raises(ConfigurationError, match=refused):
        Configuration().configure(**settings)


def test_worker_settings_are_read_from_the_environment(monkeypatch):
    monkeypatch.setenv("KABUDACHI_PROCESSES", "2")
    monkeypatch.setenv("KABUDACHI_CONCURRENCY", "40")
    monkeypatch.setenv("KABUDACHI_CONCURRENCY_OVERRIDE", "true")
    monkeypatch.setenv("KABUDACHI_IMPORTS", "app.tasks, app.more_tasks")
    monkeypatch.setenv("KABUDACHI_PROCESS_START_TIMEOUT", "2.5")
    configuration = Configuration()

    assert [
        configuration.resolve(name)
        for name in ("processes", "concurrency", "imports", "process_start_timeout")
    ] == [2, 40, ("app.tasks", "app.more_tasks"), timedelta(seconds=2.5)]

    monkeypatch.setenv("KABUDACHI_IMPORTS", "")
    assert Configuration().resolve("imports") is None


def test_environment_values_are_parsed_by_the_resolved_type_even_with_deferred_annotations(
    monkeypatch,
):
    module = types.ModuleType("deferred_annotations")
    module.Settings = config_module.Settings
    monkeypatch.setitem(sys.modules, module.__name__, module)
    exec(  # noqa: S102  (builds a class whose annotations are strings, as `from __future__` does)
        "from __future__ import annotations\n"
        "from dataclasses import dataclass\n"
        "from datetime import timedelta\n"
        "@dataclass(frozen=True, kw_only=True)\n"
        "class Extra(Settings):\n"
        "    burst: int = 1\n"
        "    pause: timedelta = timedelta(seconds=1)\n",
        module.__dict__,
    )
    monkeypatch.setenv("KABUDACHI_BURST", "7")
    monkeypatch.setenv("KABUDACHI_PAUSE", "2.5")

    found = module.Extra.from_environment()

    assert found["burst"] == 7 and isinstance(found["burst"], int)
    assert found["pause"].total_seconds() == 2.5


# --- the handle ------------------------------------------------------------

def test_a_handle_settled_from_another_thread_wakes_the_waiter():
    handle = TaskHandle("task-1")

    async def main():
        waiting = asyncio.ensure_future(_await(handle))
        await asyncio.sleep(0.02)
        threading.Thread(target=lambda: handle._resolve("from a thread")).start()
        return await asyncio.wait_for(waiting, 5)

    assert asyncio.run(main()) == "from a thread"


def test_only_the_first_settlement_counts():
    handle = TaskHandle("task-1")

    handle._resolve("first")
    handle._resolve("second")
    handle._fail(RuntimeError("late"))

    assert asyncio.run(_await(handle)) == "first"

    failed_first = TaskHandle("task-2")
    failed_first._fail(RuntimeError("first"))
    failed_first._resolve("late")

    with pytest.raises(RuntimeError, match="first"):
        asyncio.run(_await(failed_first))


def test_a_handle_that_is_never_awaited_leaves_no_warning(recwarn):
    handle = TaskHandle("task-1")
    handle._fail(RuntimeError("nobody looked"))
    del handle

    assert [w for w in recwarn if "never retrieved" in str(w.message)] == []


async def _await(handle):
    return await handle


def test_one_awaiter_timing_out_or_being_cancelled_does_not_settle_the_handle_for_the_others():
    handle = TaskHandle("task-1")

    async def main():
        cancelled = asyncio.ensure_future(_await(handle))
        survivor = asyncio.ensure_future(_await(handle))
        await asyncio.sleep(0.02)
        cancelled.cancel()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(handle, 0.05)
        assert not handle.done()
        assert not survivor.done()
        handle._resolve("value")
        return await asyncio.wait_for(survivor, 5)

    assert asyncio.run(main()) == "value"


def test_callbacks_are_called_in_the_order_they_were_added_and_one_added_late_is_called_at_once():
    seen = []
    handle = TaskHandle("task-1")

    assert handle.callback(lambda v: seen.append(("a", v))).callback(
        lambda v: seen.append(("b", v))
    ) is handle
    handle._resolve(1)
    handle.callback(lambda v: seen.append(("c", v)))  # added after settling: called at once

    assert seen == [("a", 1), ("b", 1), ("c", 1)]


def test_a_callback_must_be_callable():
    with pytest.raises(TypeError):
        TaskHandle("task-1").callback("not callable")


def test_an_async_callback_on_a_bare_handle_settled_outside_a_loop_runs_to_completion(recwarn):
    seen = []

    async def callback(value):
        await asyncio.sleep(0)
        seen.append(value)

    handle = TaskHandle("task-1")
    handle.callback(callback)
    handle._resolve("the value")

    assert seen == ["the value"]
    assert not [w for w in recwarn if "never awaited" in str(w.message)]


def test_cancelling_a_flow_after_a_stage_that_failed_does_not_claim_to_have_cancelled_it():
    from kabudachi.composites import FlowHandle

    failed, succeeded = TaskHandle("stage-1"), TaskHandle("stage-2")
    failed._fail(ValueError("stage failed"))
    succeeded._resolve("done")

    flow_after_failure, flow_after_success = FlowHandle("flow-1"), FlowHandle("flow-2")
    flow_after_failure._current = (failed, False)
    flow_after_success._current = (succeeded, False)

    # The first flow is about to fail with the stage's own error; the second
    # has a stage still to start, which the cancel stops.
    assert flow_after_failure.cancel() is False
    assert flow_after_success.cancel() is True
