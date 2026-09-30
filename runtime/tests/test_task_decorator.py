"""`@task` and `@ephemeral_task`: turn a function into a registered task,
refusing what cannot work as one, at the place it is written."""

import inspect
from datetime import timedelta

import pytest

import proto_messages
from kabudachi import ephemeral_task, task
from kabudachi import registry as registry_module
from kabudachi.errors import DuplicateTaskError, TaskDefinitionError
from kabudachi.registry import TaskKind, TaskRegistry
from kabudachi.serializers import SerializerRegistry
from kabudachi.tasks import Task
from proto_messages import Greeting, Receipt


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


@pytest.mark.parametrize("bad_name", ["", "   ", 3, None])
def test_a_name_must_be_a_non_empty_string(bad_name):
    with pytest.raises(TaskDefinitionError, match="name"):
        define(charge, name=bad_name)


@pytest.mark.parametrize("bad_version", [-1, 1.5, "1", True, None])
def test_a_version_must_be_a_non_negative_integer(bad_version):
    with pytest.raises(TaskDefinitionError, match="version"):
        define(charge, version=bad_version)


@pytest.mark.parametrize("bad_queue", ["", 3, None])
def test_a_queue_must_be_a_non_empty_string(bad_queue):
    with pytest.raises(TaskDefinitionError, match="queue"):
        define(charge, queue=bad_queue)


def test_a_lambda_cannot_be_a_task():
    with pytest.raises(TaskDefinitionError, match="package scope"):
        define(lambda request: request)


def test_a_nested_function_cannot_be_a_task():
    def nested(request: Greeting) -> Receipt:
        return Receipt()

    with pytest.raises(TaskDefinitionError, match="package scope"):
        define(nested)


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


def test_the_public_decorator_works_bare_and_registers_in_the_default_registry(monkeypatch):
    monkeypatch.setattr(registry_module, "_default_registry", TaskRegistry())

    decorated = task(charge)

    assert isinstance(decorated, Task)
    assert decorated.definition.name in registry_module.default_registry()


def test_the_public_decorator_takes_options(monkeypatch):
    monkeypatch.setattr(registry_module, "_default_registry", TaskRegistry())

    decorated = task(name="billing.charge", queue="gpu", version=4)(charge)

    assert decorated.definition.name == "billing.charge"
    assert decorated.definition.queue == "gpu"
    assert decorated.definition.version == 4


def test_the_public_ephemeral_decorator_marks_the_kind(monkeypatch):
    monkeypatch.setattr(registry_module, "_default_registry", TaskRegistry())

    decorated = ephemeral_task(charge)

    assert decorated.definition.kind is TaskKind.EPHEMERAL


def test_the_public_decorator_refuses_a_positional_name():
    with pytest.raises(TypeError):
        task("billing.charge")


class Uninspectable:
    """A callable whose signature is not something `inspect` can read."""

    __signature__ = "not a signature"

    def __call__(self, request):
        return request


def test_a_callable_without_a_usable_signature_is_a_task_definition_error():
    # `inspect.signature` raises ValueError for this C builtin.
    with pytest.raises(TaskDefinitionError, match="signature"):
        define(max)


def test_a_callable_object_with_a_broken_signature_is_refused_by_name():
    with pytest.raises(TaskDefinitionError, match="signature"):
        define(Uninspectable(), name="tests.uninspectable")


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


def test_a_lambda_at_package_scope_cannot_be_a_task():
    with pytest.raises(TaskDefinitionError, match="package scope"):
        define(MODULE_LAMBDA)


@pytest.mark.parametrize("bad_serializer", ["", "  ", 3, None])
def test_a_serializer_must_be_a_non_empty_string(bad_serializer):
    with pytest.raises(TaskDefinitionError, match="serializer"):
        define(charge, serializer=bad_serializer)


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


def test_a_serializer_not_registered_yet_is_not_checked():
    accepted = Task(
        text_echo,
        registry=TaskRegistry(),
        serializer="registered-later",
        serializers=SerializerRegistry(),
    )

    assert accepted.definition.serializer == "registered-later"


def test_a_serializer_that_cannot_run_here_is_not_checked():
    serializers = SerializerRegistry()
    serializers.register(Unavailable())

    accepted = Task(
        charge, registry=TaskRegistry(), serializer="unavailable", serializers=serializers
    )

    assert accepted.definition.serializer == "unavailable"


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


@pytest.mark.parametrize("bad", [-1, True, 1.5, "2", None])
def test_retries_must_be_a_non_negative_integer(fresh_registry, bad):
    with pytest.raises(TaskDefinitionError, match="retries"):
        task(name="retry.bad", retries=bad)(charge)


def test_an_ephemeral_task_is_never_retried_so_it_takes_no_retries(fresh_registry):
    with pytest.raises(TypeError):
        ephemeral_task(name="retry.ephemeral", retries=1)(charge)


@pytest.mark.parametrize("bad", [timedelta(0), timedelta(seconds=-1), 5, "5s", True])
def test_a_timeout_must_be_a_positive_timedelta(fresh_registry, bad):
    with pytest.raises(TaskDefinitionError, match="timeout"):
        task(name="timing.bad", timeout=bad)(charge)


@pytest.mark.parametrize("bad", [timedelta(seconds=-1), 5, "5s", None, True])
def test_a_cancel_grace_must_be_a_non_negative_timedelta(fresh_registry, bad):
    with pytest.raises(TaskDefinitionError, match="cancel_grace"):
        task(name="timing.bad", timeout=timedelta(seconds=1), cancel_grace=bad)(charge)


def test_a_zero_cancel_grace_is_allowed(fresh_registry):
    defined = task(
        name="timing.zero", timeout=timedelta(seconds=1), cancel_grace=timedelta(0)
    )(charge).definition
    assert defined.cancel_grace == timedelta(0)


def test_an_ephemeral_task_can_have_a_timeout_too(fresh_registry):
    defined = ephemeral_task(name="timing.ephemeral", timeout=timedelta(seconds=2))(charge)
    assert defined.definition.timeout == timedelta(seconds=2)


def add_greetings(older: Greeting, newer: Greeting) -> Greeting:
    return Greeting(text=older.text + newer.text)


def test_a_coalescing_task_is_registered_with_its_kind_and_reducer(fresh_registry):
    from kabudachi import coalescing_task

    defined = coalescing_task(
        name="coalesce.merged", merge=add_greetings, drop_oldest=True
    )(charge_greeting).definition

    assert defined.kind is TaskKind.COALESCING
    assert defined.merge is add_greetings
    assert defined.drop_oldest is True


def test_a_coalescing_task_without_a_reducer_keeps_the_newest_payload(fresh_registry):
    from kabudachi import coalescing_task

    defined = coalescing_task(name="coalesce.plain")(charge_greeting).definition

    assert defined.kind is TaskKind.COALESCING
    assert defined.merge is None
    assert defined.drop_oldest is False


def test_only_a_coalescing_task_takes_drop_oldest():
    # The message is matched: today `drop_oldest` is an unknown keyword, which
    # is also a TypeError, so a bare `raises(TypeError)` would pass unbuilt.
    with pytest.raises(TypeError, match="only a coalescing task takes drop_oldest"):
        Task(charge_greeting, registry=TaskRegistry(), name="drop.no", drop_oldest=True)


@pytest.mark.parametrize("bad", ["not callable", 3])
def test_a_reducer_must_be_callable(fresh_registry, bad):
    from kabudachi import coalescing_task

    with pytest.raises(TaskDefinitionError, match="merge"):
        coalescing_task(name="coalesce.bad", merge=bad)(charge_greeting)


def test_a_reducer_must_take_two_arguments(fresh_registry):
    from kabudachi import coalescing_task

    def one(only: Greeting) -> Greeting:
        return only

    def three(a: Greeting, b: Greeting, c: Greeting) -> Greeting:
        return a

    for bad in (one, three):
        with pytest.raises(TaskDefinitionError, match="merge"):
            coalescing_task(name="coalesce.arity", merge=bad)(charge_greeting)


def test_only_a_coalescing_task_takes_a_reducer(fresh_registry):
    with pytest.raises(TypeError):
        task(name="coalesce.no", merge=add_greetings)(charge_greeting)


def charge_greeting(request: Greeting) -> Greeting:
    return request
