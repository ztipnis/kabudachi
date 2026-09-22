"""Both adapters of the `Runtime` seam, the native runtime and the faulting
wrapper, must offer what `Session` calls."""

import inspect

import pytest

from kabudachi import _native
from kabudachi.session import Runtime
from faulting_runtime import FaultingRuntime

SUBMIT_PARAMETERS = [
    "definition_id",
    "source_version",
    "serialized_input",
    "queue",
    "retries",
    "delay_ms",
    "expires_in_ms",
    "coalescing_key",
    "drop_oldest",
]


@pytest.fixture
def native():
    runtime = _native.NativeRuntime("worker-1", "incarnation-1")
    yield runtime
    runtime.shutdown()


def test_the_native_runtime_is_a_runtime(native):
    assert isinstance(native, Runtime)


def test_the_faulting_wrapper_is_a_runtime(native):
    assert isinstance(FaultingRuntime(native), Runtime)


def test_the_protocol_submit_names_the_parameters_the_native_submit_takes(native):
    protocol = list(inspect.signature(Runtime.submit).parameters)[1:]

    assert protocol == list(inspect.signature(native.submit).parameters) == SUBMIT_PARAMETERS


def test_the_faulting_wrapper_submit_takes_the_same_parameters(native):
    wrapper = FaultingRuntime(native)

    assert list(inspect.signature(wrapper.submit).parameters) == SUBMIT_PARAMETERS
