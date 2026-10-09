# Merges the compiled //bindings:_native extension (a separate Bazel
# package/sys.path root) into this package as `kabudachi._native`.
# Bazel dev/test-layout glue only: it exists because Bazel puts the extension
# in a separate package root, and a normally built wheel may not need it.
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

from kabudachi.composites import FlowHandle, GroupHandle
from kabudachi.config import configure
from kabudachi.flow import BoundTask, Flow, Group, flow, group
from kabudachi.handle import TaskHandle
from kabudachi.lifecycle import RunContext, after_run, before_run, process_init
from kabudachi.runner import run
from kabudachi.serializers import register_serializer
from kabudachi.tasks import coalescing_task, ephemeral_task, task

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "after_run",
    "before_run",
    "coalescing_task",
    "configure",
    "BoundTask",
    "ephemeral_task",
    "Flow",
    "flow",
    "FlowHandle",
    "group",
    "Group",
    "GroupHandle",
    "process_init",
    "register_serializer",
    "run",
    "RunContext",
    "task",
    "TaskHandle",
]
