""".options(delay=, eta=, expires=): what is asked of a submission, checked
and turned into the milliseconds the runtime takes."""

from datetime import datetime, timedelta, timezone

import pytest

from kabudachi.options import SubmissionOptions, submission_options
from kabudachi.registry import TaskRegistry
from kabudachi.serializers import SerializerRegistry
from kabudachi.tasks import Task
from proto_messages import Greeting

NOW = datetime(2030, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def options(**arguments):
    return submission_options(now=NOW, **arguments)


def test_a_delay_is_taken_in_milliseconds():
    assert options(delay=timedelta(seconds=2, milliseconds=500)).delay_ms == 2500


def test_a_fraction_of_a_millisecond_rounds_up_so_the_task_is_never_early():
    assert options(delay=timedelta(microseconds=1)).delay_ms == 1


def test_an_eta_is_the_time_from_now_until_then():
    assert options(eta=NOW + timedelta(minutes=5)).delay_ms == 300_000


def test_an_eta_in_the_past_means_no_delay():
    assert options(eta=NOW - timedelta(hours=1)).delay_ms == 0


def test_expires_takes_a_duration_from_now_or_a_time():
    assert options(expires=timedelta(seconds=10)).expires_in_ms == 10_000
    assert options(expires=NOW + timedelta(seconds=10)).expires_in_ms == 10_000


def test_an_expiry_already_past_expires_at_once():
    assert options(expires=NOW - timedelta(seconds=1)).expires_in_ms == 0


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


def declare(function):
    return Task(
        function,
        registry=TaskRegistry(),
        serializers=SerializerRegistry.with_defaults(),
        name=f"tests.{function.__name__}",
    )


def shout(request: Greeting) -> Greeting:
    return request


def test_options_are_checked_when_they_are_given_not_when_the_task_is_called():
    with pytest.raises(ValueError):
        declare(shout).options(delay=timedelta(seconds=-1))


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
        declare(shout).options(key="tenant-1")


@pytest.mark.parametrize("bad", [1, 2.5, b"x"])
def test_a_key_must_be_a_string(bad):
    with pytest.raises(TypeError):
        coalescing_task_for_options().options(key=bad)


@pytest.mark.parametrize("argument", ["eta", "expires"])
def test_a_time_that_is_not_a_datetime_is_a_type_error_not_an_attribute_error(argument):
    with pytest.raises(TypeError):
        options(**{argument: 0})
