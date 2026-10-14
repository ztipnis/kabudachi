# Merges the compiled //bindings:_native extension (a separate Bazel
# package/sys.path root) into this package as `kabudachi._native`.
# Bazel dev/test-layout glue only: it exists because Bazel puts the extension
# in a separate package root, and a normally built wheel may not need it.
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

from kabudachi.composites import FlowHandle, GroupHandle
from kabudachi.config import QueueOptions, configure
from kabudachi.errors import (
    DurabilityUnavailableError,
    MigrateRejectError,
    MigrationError,
    PayloadTooLargeError,
    ReduceEmptyError,
    ResultTooLargeError,
    ResultUnavailableError,
    TaskFailure,
    TaskOrphanedError,
    TaskOutcomeError,
)
from kabudachi.flow import BoundTask, Flow, Group, flow, group
from kabudachi.handle import TaskHandle
from kabudachi.lifecycle import RunContext, after_run, before_run, process_init
from kabudachi.result_backend import ResultBackend
from kabudachi.run_info import RunInfo, current_context
from kabudachi.runner import run
from kabudachi.schemas import migration, schema
from kabudachi.serializers import register_serializer
from kabudachi.status import CompositeStatus, Durability, TaskStatus
from kabudachi.tasks import coalescing_task, ephemeral_task, reducer, task

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "after_run",
    "before_run",
    "coalescing_task",
    "CompositeStatus",
    "configure",
    "current_context",
    "BoundTask",
    "Durability",
    "DurabilityUnavailableError",
    "ephemeral_task",
    "Flow",
    "flow",
    "FlowHandle",
    "group",
    "Group",
    "GroupHandle",
    "MigrateRejectError",
    "migration",
    "MigrationError",
    "PayloadTooLargeError",
    "process_init",
    "QueueOptions",
    "ReduceEmptyError",
    "reducer",
    "register_serializer",
    "ResultBackend",
    "ResultTooLargeError",
    "ResultUnavailableError",
    "run",
    "RunContext",
    "RunInfo",
    "schema",
    "task",
    "TaskFailure",
    "TaskHandle",
    "TaskOrphanedError",
    "TaskOutcomeError",
    "TaskStatus",
]
