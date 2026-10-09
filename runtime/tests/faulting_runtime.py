"""A real native runtime with a recorder and a few ways to misbehave, so the
session's guarantees can be provoked without re-implementing what the
scheduler decides. Every call is forwarded to the real runtime."""

import asyncio
import time
from types import SimpleNamespace

from kabudachi import _native


class FaultingRuntime:
    """Wraps a `_native.NativeRuntime`: records what the session asks of it, and
    can make chosen calls fail, be refused or be held back. `before_started`,
    `on_complete` and `after_certification` let a test act at a chosen point of a
    call, such as between a claim and its started report.

    An instance is bound to the first event loop that awaits it: the injected
    queue and the pending native-events future attach to that loop. Use a fresh
    instance for each `asyncio.run(...)`, and call `inject_event` from the loop's
    thread."""

    def __init__(self, native):
        self.native = native
        self.events = []
        self.submitted = []
        self.submit_options = []
        self.refuse_completion = False
        self.altered_digest = None
        self.on_complete = None
        self.after_certification = None
        self.before_started = None
        self.fail_start = False
        self.refuse_failure = False
        self.claim_error = None
        self.event_error = None
        self.hold_claims = False
        self.submit_lingers_for = 0
        self._claims_given = 0
        self._injected = asyncio.Queue()
        self._native_events = None

    def __getattr__(self, name):
        return getattr(self.native, name)

    def submit(self, definition_id, source_version, serialized_input, queue, kind, key, **options):
        task_id = self.native.submit(
            definition_id, source_version, serialized_input, queue, kind, key, **options
        )
        self.submitted.append((task_id, definition_id, source_version, serialized_input, queue))
        self.submit_options.append({"kind": kind, "key": key, **options})
        # Holds this call open after the task exists. `time.sleep` blocks the
        # event loop, so no worker on it can claim meanwhile: this only widens
        # the window for a cancel arriving from another thread.
        time.sleep(self.submit_lingers_for)
        return task_id

    async def claim_pending(self, limit):
        if self.claim_error is not None:
            raise self.claim_error
        if self.hold_claims and self._claims_given >= 1:
            await asyncio.Event().wait()
        claims = await self.native.claim_pending(limit)
        self._claims_given += len(claims)
        return claims

    def inject_event(self, kind, task_id=""):
        """Makes `next_events()` also return an event the real runtime did not
        produce, for one it could never cause (such as one for another session's task)."""
        self._injected.put_nowait(
            SimpleNamespace(
                kind=kind,
                task_id=task_id,
                task_run_id="",
                was_running=False,
                superseded_by=None,
                active=False,
            )
        )

    def inject_abort(self, task_run_id, seconds_left):
        """Makes `next_events()` also return the abort deadline a shard's
        leader would send a worker that lost contact with it."""
        self._injected.put_nowait(
            SimpleNamespace(
                kind=_native.EventKind.ABORT,
                task_id="",
                task_run_id=task_run_id,
                seconds_left=seconds_left,
            )
        )

    def inject_cancel(self, task_id, task_run_id):
        """Makes `next_events()` also return the cancellation a shard's leader
        would send the worker holding the task's run."""
        self._injected.put_nowait(
            SimpleNamespace(
                kind=_native.EventKind.CANCELLED,
                task_id=task_id,
                task_run_id=task_run_id,
                was_running=True,
            )
        )

    async def next_events(self):
        if self.event_error is not None:
            raise self.event_error
        # Events the native call has taken are lost if it is cancelled, so it
        # is kept running across calls and never cancelled here.
        if self._native_events is None:
            self._native_events = asyncio.ensure_future(self.native.next_events())
        injected = asyncio.ensure_future(self._injected.get())
        try:
            await asyncio.wait({self._native_events, injected}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            if not injected.done():
                injected.cancel()
        events = []
        if injected.done() and not injected.cancelled():
            events.append(injected.result())
            while not self._injected.empty():
                events.append(self._injected.get_nowait())
        if self._native_events.done():
            events.extend(self._native_events.result())
            self._native_events = None
        return events

    def report_started(self, task_run_id):
        self.events.append(("started", task_run_id))
        if self.before_started is not None:
            self.before_started(task_run_id)
        if self.fail_start:
            raise RuntimeError("not the leader")
        return self.native.report_started(task_run_id)

    def report_failure(self, task_run_id, failure_kind):
        self.events.append(("fail", task_run_id, failure_kind))
        if self.refuse_failure:
            raise RuntimeError("the run does not belong to this worker")
        return self.native.report_failure(task_run_id, failure_kind)

    def report_lost(self, task_run_id):
        self.events.append(("lost", task_run_id))
        return self.native.report_lost(task_run_id)

    def complete(self, task_run_id, result_digest, continues=False):
        self.events.append(("complete", task_run_id))
        if self.on_complete is not None:
            self.on_complete(task_run_id)
        if self.refuse_completion:
            raise RuntimeError("the run does not belong to this worker")
        certification = self.native.complete(task_run_id, result_digest, continues)
        if self.after_certification is not None:
            # The leader has accepted the completion; the caller has not heard yet.
            self.after_certification(task_run_id)
        if self.altered_digest is not None:
            return SimpleNamespace(
                task_id=certification.task_id,
                task_run_id=certification.task_run_id,
                result_digest=self.altered_digest,
            )
        return certification

    def end_continuation(self, task_id):
        self.events.append(("end_continuation", task_id))
        return self.native.end_continuation(task_id)

    def cancel(self, task_id):
        # Forwarded unchanged, but named here: `isinstance` against a runtime
        # protocol looks only at the class, so `__getattr__` alone is not enough.
        return self.native.cancel(task_id)


class FaultingNative(FaultingRuntime):
    """Stands in for the `NativeRuntime` class as `kabudachi.run()` uses it:
    builds the real runtime and records how it was made, waited on and shut down."""

    instances = []

    def __init__(self, worker_id="worker", incarnation_id="incarnation", **options):
        super().__init__(_native.NativeRuntime(worker_id, incarnation_id, **options))
        self.options = options
        self.leader_error = None
        self.shutdowns = 0
        FaultingNative.instances.append(self)

    async def wait_until_leader(self):
        if self.leader_error is not None:
            raise self.leader_error
        await self.native.wait_until_leader()

    def shutdown(self):
        self.shutdowns += 1
        self.native.shutdown()
