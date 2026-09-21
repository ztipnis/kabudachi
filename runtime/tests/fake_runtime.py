"""Test doubles for the native runtime: the same calls, with each step able
to misbehave, so guarantees can be provoked without a real Tokio runtime."""

import asyncio
import threading
import time
from types import SimpleNamespace


class FakeRuntime:
    """Stands in for the native runtime: tasks submitted here are claimed in
    order, and each step can be made to misbehave."""

    def __init__(self):
        self.events = []
        self.submitted = []
        self._queue = asyncio.Queue()
        self._numbers = iter(range(1, 10_000))
        self.refuse_completion = False
        self.altered_digest = None
        self.on_complete = None
        self.after_certification = None
        self.fail_start = False
        self.refuse_failure = False
        self.retries = {}
        self.refuse_after = None
        self.slow_down_after = None
        self.chains = {}
        self.claimed = set()
        self.finished = set()
        self.continuing = set()
        self.cancelled = set()
        self.submit_options = []
        self._events = asyncio.Queue()
        self.event_error = None
        self.attempts = {}
        self._task_of_run = {}
        self.submit_lingers_for = 0
        self.claim_error = None
        self.hold_claims = False
        self._claims_given = 0
        self._loop = None

    def submit(
        self,
        definition_id,
        source_version,
        payload,
        queue,
        retries=0,
        delay_ms=None,
        expires_in_ms=None,
        coalescing_key=None,
    ):
        if self.refuse_after is not None and len(self.submitted) >= self.refuse_after:
            from kabudachi.errors import BackpressureError

            raise BackpressureError("submitting would pass the hard memory limit")
        self.submit_options.append(
            {"delay_ms": delay_ms, "expires_in_ms": expires_in_ms, "coalescing_key": coalescing_key}
        )
        task_id = f"task-{next(self._numbers)}"
        self.submitted.append((task_id, definition_id, source_version, payload, queue))
        if self.slow_down_after == len(self.submitted):
            self.slow_down(True)
        self.retries[task_id] = retries
        self._task_of_run[f"run-{task_id}"] = task_id
        claim = self._claim(task_id, f"run-{task_id}", 1, definition_id, source_version, payload, queue)
        self._enqueue(claim)
        # The claim is already available to a worker while this call has not
        # returned yet, as on a real runtime.
        time.sleep(self.submit_lingers_for)
        return task_id

    def _claim(self, task_id, run_id, attempt, definition_id, source_version, payload, queue):
        return SimpleNamespace(
            task_id=task_id,
            task_run_id=run_id,
            attempt_number=attempt,
            definition_id=definition_id,
            source_version=source_version,
            serialized_input=payload,
            queue=queue,
            chain=list(self.chains.get(task_id, [])),
        )

    def _enqueue(self, claim):
        if self._loop is None or threading.current_thread() is threading.main_thread():
            self._queue.put_nowait(claim)
        else:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, claim)

    async def claim_pending(self, limit):
        self._loop = asyncio.get_running_loop()
        if self.claim_error is not None:
            raise self.claim_error
        if self.hold_claims and self._claims_given >= 1:
            # After the first claim, nothing more is ever handed out.
            await asyncio.Event().wait()
        while True:
            claims = [await self._queue.get()]
            while len(claims) < limit and not self._queue.empty():
                claims.append(self._queue.get_nowait())
            claims = [claim for claim in claims if claim.task_id not in self.cancelled]
            if claims:
                break
        self._claims_given += len(claims)
        self.claimed.update(claim.task_id for claim in claims)
        return claims

    def end_continuation(self, task_id):
        self.events.append(("end_continuation", task_id))
        if task_id in self.continuing:
            self.continuing.discard(task_id)
            self.finished.add(task_id)
            return True
        return False

    def cancel(self, task_id):
        if task_id not in self.retries:
            return "unknown"
        if task_id in self.finished or task_id in self.cancelled or task_id in self.continuing:
            return "finished"
        self.cancelled.add(task_id)
        self._events.put_nowait(
            SimpleNamespace(
                kind="cancelled",
                task_id=task_id,
                task_run_id=f"run-{task_id}",
                was_running=task_id in self.claimed,
            )
        )
        return "cancelled"

    def expire(self, task_id):
        """The runtime decides the task ran out of time."""
        event = SimpleNamespace(
            kind="expired", task_id=task_id, task_run_id=f"run-{task_id}", was_running=False
        )
        self._events.put_nowait(event)

    def slow_down(self, active=True):
        """The runtime says memory use crossed the soft limit, or fell back."""
        kind = "slow_down" if active else "slow_down_cleared"
        self._events.put_nowait(
            SimpleNamespace(
                kind=kind, task_id="", task_run_id="", was_running=False, superseded_by=None
            )
        )

    def supersede(self, task_id, by):
        """The runtime decides a newer generation replaced this one."""
        self._events.put_nowait(
            SimpleNamespace(
                kind="superseded",
                task_id=task_id,
                task_run_id=f"run-{task_id}",
                was_running=False,
                superseded_by=by,
            )
        )

    async def next_events(self):
        if self.event_error is not None:
            raise self.event_error
        events = [await self._events.get()]
        while not self._events.empty():
            events.append(self._events.get_nowait())
        return events

    def report_started(self, run_id):
        self.events.append(("started", run_id))
        if self.fail_start:
            raise RuntimeError("not the leader")

    def fail(self, run_id, failure_kind):
        self.events.append(("fail", run_id, failure_kind))
        if self.refuse_failure:
            raise RuntimeError("the run does not belong to this worker")
        task_id = self._task_of_run[run_id]
        if task_id in self.cancelled:
            raise RuntimeError("the run was cancelled")
        used = self.attempts.get(task_id, 1)
        if used > self.retries[task_id]:
            self.finished.add(task_id)
            return False
        self.attempts[task_id] = used + 1
        original = next(entry for entry in self.submitted if entry[0] == task_id)
        _, definition_id, source_version, payload, queue = original
        retry_id = f"run-{task_id}-{used + 1}"
        self._task_of_run[retry_id] = task_id
        self._enqueue(
            self._claim(task_id, retry_id, used + 1, definition_id, source_version, payload, queue)
        )
        return True

    def complete(self, run_id, digest, continues=False):
        self.events.append(("complete", run_id))
        if self.on_complete is not None:
            self.on_complete(run_id)
        if self.refuse_completion:
            raise RuntimeError("the run does not belong to this worker")
        task_id = self._task_of_run[run_id]
        if task_id in self.cancelled:
            raise RuntimeError("the run was cancelled")
        if continues:
            self.continuing.add(task_id)
        else:
            self.finished.add(task_id)
        if self.after_certification is not None:
            # The leader has accepted the completion; the caller has not heard yet.
            self.after_certification(run_id)
        return SimpleNamespace(
            task_id=task_id,
            task_run_id=run_id,
            result_digest=self.altered_digest or digest,
        )


class FakeNative(FakeRuntime):
    """Stands in for `kabudachi._native.NativeRuntime` as `run()` uses it."""

    instances = []

    def __init__(self, worker_id="worker", incarnation_id="incarnation", **options):
        super().__init__()
        self.options = options
        self.leader_error = None
        self.shutdowns = 0
        FakeNative.instances.append(self)

    async def wait_until_leader(self):
        if self.leader_error is not None:
            raise self.leader_error

    def shutdown(self):
        self.shutdowns += 1
