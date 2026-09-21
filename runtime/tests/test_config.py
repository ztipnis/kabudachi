"""Configuration: process-wide settings, and how a task's own choice overrides
them. `None` is a real choice, so "not set" is a separate sentinel."""

import sys
import types

import pytest

from kabudachi import configure
from kabudachi import config as config_module
from kabudachi.config import UNSET, Configuration
from kabudachi.errors import ConfigurationError


def test_defaults_apply_when_nothing_is_configured():
    configuration = Configuration()

    assert configuration.resolve("queue") == "default"
    assert configuration.resolve("concurrency") == 16


def test_a_process_setting_overrides_the_default():
    configuration = Configuration()

    configuration.configure(queue="emails", concurrency=4)

    assert configuration.resolve("queue") == "emails"
    assert configuration.resolve("concurrency") == 4


def test_a_task_setting_overrides_the_process_setting():
    configuration = Configuration()
    configuration.configure(queue="emails")

    assert configuration.resolve("queue", "gpu") == "gpu"


def test_an_unset_task_setting_falls_back_to_the_process_setting():
    configuration = Configuration()
    configuration.configure(queue="emails")

    assert configuration.resolve("queue", UNSET) == "emails"


def test_configuring_twice_keeps_settings_from_the_first_call():
    configuration = Configuration()
    configuration.configure(queue="emails")

    configuration.configure(concurrency=2)

    assert configuration.resolve("queue") == "emails"
    assert configuration.resolve("concurrency") == 2


def test_a_later_call_replaces_a_setting():
    configuration = Configuration()
    configuration.configure(concurrency=2)

    configuration.configure(concurrency=8)

    assert configuration.resolve("concurrency") == 8


def test_unset_is_not_none():
    assert UNSET is not None
    assert UNSET != None  # noqa: E711 - the point is the comparison itself
    assert repr(UNSET) == "UNSET"


def test_unset_is_a_single_value():
    assert type(UNSET)() is UNSET


def test_an_unknown_setting_is_refused():
    with pytest.raises(ConfigurationError, match="no_such_setting"):
        Configuration().configure(no_such_setting=1)


def test_an_unknown_setting_is_refused_when_resolving():
    with pytest.raises(ConfigurationError, match="no_such_setting"):
        Configuration().resolve("no_such_setting")


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


def test_the_public_configure_sets_the_process_configuration(monkeypatch):
    monkeypatch.setattr(config_module, "_process_configuration", Configuration())

    configure(queue="from-public-api")

    assert config_module.process_configuration().resolve("queue") == "from-public-api"


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


def test_a_task_setting_wins_over_the_environment(monkeypatch):
    monkeypatch.setenv("KABUDACHI_QUEUE", "from-env")

    assert Configuration().resolve("queue", "task-queue") == "task-queue"


def test_an_invalid_environment_value_is_a_configuration_error(monkeypatch):
    monkeypatch.setenv("KABUDACHI_CONCURRENCY", "many")

    with pytest.raises(ConfigurationError, match="concurrency"):
        Configuration().resolve("concurrency")


def test_the_environment_is_read_when_a_setting_is_first_needed(monkeypatch):
    configuration = Configuration()
    monkeypatch.setenv("KABUDACHI_CONCURRENCY", "5")

    assert configuration.resolve("concurrency") == 5


def test_result_ttl_defaults_to_an_hour_and_must_be_a_positive_number_of_seconds():
    configuration = Configuration()
    assert configuration.resolve("result_ttl") == 3600

    for bad in (0, -1, 1.5, True, "1h"):
        with pytest.raises(ConfigurationError):
            Configuration().configure(result_ttl=bad)


def test_cancel_grace_defaults_to_thirty_seconds_and_is_a_non_negative_timedelta():
    from datetime import timedelta

    configuration = Configuration()
    assert configuration.resolve("cancel_grace") == timedelta(seconds=30)

    configuration.configure(cancel_grace=timedelta(0))
    assert configuration.resolve("cancel_grace") == timedelta(0)
    for bad in (timedelta(seconds=-1), 5, "5", True):
        with pytest.raises(ConfigurationError):
            Configuration().configure(cancel_grace=bad)


def test_cancel_grace_can_come_from_the_environment_in_seconds(monkeypatch):
    from datetime import timedelta

    monkeypatch.setenv("KABUDACHI_CANCEL_GRACE", "2.5")

    assert Configuration().resolve("cancel_grace") == timedelta(seconds=2.5)


def test_memory_limits_default_to_named_constants_soft_below_hard():
    configuration = Configuration()

    soft = configuration.resolve("memory_soft_limit")
    hard = configuration.resolve("memory_hard_limit")

    assert 0 < soft < hard
    assert soft == config_module.DEFAULT_MEMORY_SOFT_LIMIT
    assert hard == config_module.DEFAULT_MEMORY_HARD_LIMIT


def test_memory_limits_can_be_configured_in_bytes():
    configuration = Configuration()
    configuration.configure(memory_soft_limit=1000, memory_hard_limit=2000)

    assert configuration.resolve("memory_soft_limit") == 1000
    assert configuration.resolve("memory_hard_limit") == 2000


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
