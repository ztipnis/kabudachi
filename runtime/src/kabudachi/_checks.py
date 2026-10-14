"""Predicates that settings and task options are checked with."""

import math
from datetime import timedelta
from typing import Any


def is_count(value: Any) -> bool:
    """Whether `value` is an integer. `True` is an int to Python, but never
    what someone meant by a count."""
    return isinstance(value, int) and not isinstance(value, bool)


def is_nonblank_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def name_list(value: Any) -> tuple[str, ...] | None:
    """`value` as a tuple of non-blank strings, or `None` if it is not a list
    of them. A string is not a list of names, though it iterates as one."""
    if isinstance(value, str):
        return None
    try:
        # Once: a generator would be used up by checking it.
        names = tuple(value)
    except TypeError:
        return None
    if not all(is_nonblank_string(name) for name in names):
        return None
    return names


def is_positive_timedelta(value: Any) -> bool:
    return isinstance(value, timedelta) and value > timedelta(0)


def is_non_negative_timedelta(value: Any) -> bool:
    return isinstance(value, timedelta) and value >= timedelta(0)


def is_positive_seconds(value: Any) -> bool:
    """Whether `value` is a finite, positive number of seconds that a
    `timedelta` can hold."""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(value) and 0 < value <= timedelta.max.total_seconds()
    except OverflowError:
        return False
