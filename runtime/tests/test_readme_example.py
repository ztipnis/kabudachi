"""The example in runtime/README.md runs, and does what its comment says."""

import re
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

from kabudachi import config as config_module
from kabudachi import registry as registry_module
from kabudachi import runner as runner_module
from kabudachi.config import Configuration
from kabudachi.registry import TaskRegistry
from faulting_runtime import FaultingNative
from proto_messages import make_message_class

README = Path(__file__).resolve().parents[1] / "README.md"


def example_source():
    text = README.read_text()
    match = re.search(r"<!-- example -->\n```python\n(.*?)```", text, re.S)
    assert match, "README.md has no example block"
    return match.group(1)


@pytest.fixture(autouse=True)
def fresh_process(monkeypatch):
    FaultingNative.instances = []
    monkeypatch.setattr(registry_module, "_default_registry", TaskRegistry())
    monkeypatch.setattr(config_module, "_process_configuration", Configuration())
    monkeypatch.setattr(runner_module, "_native", SimpleNamespace(NativeRuntime=FaultingNative))


def test_the_readme_example_runs_and_prints_what_it_says(monkeypatch, capsys):
    messages = types.ModuleType("myapp_pb2")
    messages.Order = make_message_class(
        "ExampleOrder", [("item", "string"), ("quantity", "int64"), ("cents", "int64")]
    )
    messages.Receipt = make_message_class("ExampleReceipt", [("ok", "bool"), ("total", "int64")])
    monkeypatch.setitem(sys.modules, "myapp_pb2", messages)
    source = example_source()

    exec(compile(source, str(README), "exec"), {"__name__": "readme_example"})

    assert capsys.readouterr().out.strip() == "(500, [100, 250])"
