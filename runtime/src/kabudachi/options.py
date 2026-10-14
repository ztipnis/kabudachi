"""What a single submission asks for beyond the task's own settings: to start
later, or to expire if it has not started by some time."""

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True)
class SubmissionOptions:
    """A submission's timing, as milliseconds from the moment it is submitted.
    `None` asks for nothing."""

    delay_ms: int | None = None
    expires_in_ms: int | None = None
    key: str | None = None
    """A coalescing task's key for this submission; `None` is the default key."""


@dataclass(frozen=True)
class SubmissionSettings:
    """What a submission carries that the submitting process resolved from
    the task's own settings and its own, as opposed to what the caller asked
    for in `SubmissionOptions`."""

    queue: str
    reconnect_timeout_ms: int | None = None
    """The reconnect timeout its runs carry; `None` leaves it to the shard."""


def submission_options(
    *,
    delay: timedelta | None = None,
    eta: datetime | None = None,
    expires: timedelta | datetime | None = None,
    key: str | None = None,
    now: datetime | None = None,
) -> SubmissionOptions:
    """Checks what a submission asks for and turns it into milliseconds from
    now, rounded up so a task is never early.

    `delay` is a duration to wait and `eta` a time to wait until; give at most
    one. `expires` is either a duration or a time. A time must have a time
    zone, and one already past means no delay, or expiry at once. `key` is a
    coalescing task's flat key, a string. Raises
    `ValueError` for a contradiction, and `TypeError` for the wrong type.
    """
    now = now if now is not None else datetime.now(timezone.utc)
    if key is not None and not isinstance(key, str):
        raise TypeError(f"key must be a string, not {key!r}")
    delay_ms = _delay_ms(delay, eta, now)
    expires_in_ms = _expires_in_ms(expires, now)
    if delay_ms and expires_in_ms is not None and expires_in_ms <= delay_ms:
        raise ValueError(
            "the task would expire before it could start: expires must be later than the delay"
        )
    return SubmissionOptions(delay_ms=delay_ms, expires_in_ms=expires_in_ms, key=key)


def _delay_ms(delay: timedelta | None, eta: datetime | None, now: datetime) -> int | None:
    if delay is not None and eta is not None:
        raise ValueError("give a delay or an eta, not both")
    if delay is not None:
        return _milliseconds(_duration("delay", delay))
    if eta is not None:
        return _milliseconds(max(_moment("eta", eta) - now, timedelta(0)))
    return None


def _expires_in_ms(expires: timedelta | datetime | None, now: datetime) -> int | None:
    if expires is None:
        return None
    if isinstance(expires, datetime):
        return _milliseconds(max(_moment("expires", expires) - now, timedelta(0)))
    return _milliseconds(_duration("expires", expires))


def _duration(name: str, value: object) -> timedelta:
    if not isinstance(value, timedelta):
        raise TypeError(f"{name} must be a timedelta, not {value!r}")
    if value < timedelta(0):
        raise ValueError(f"{name} must not be negative, not {value!r}")
    return value


def _moment(name: str, value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime, not {value!r}")
    if value.tzinfo is None:
        raise ValueError(f"{name} needs a time zone; use an aware datetime")
    return value


def _milliseconds(span: timedelta) -> int:
    return math.ceil(span / timedelta(milliseconds=1))
