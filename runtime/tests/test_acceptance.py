"""Phase 1 acceptance criteria (README §27.1) that are Python behaviour, as
properties: 25.4.3 and 25.4.4 (payloads are folded in order, whatever the
compaction points) and 25.4.8 (the stage after a group runs exactly once). The
scheduler-side coalescing invariants are `core/tests/proptest_flow_invariants.rs`
and 25.4.7 is `test_continuation.py` plus that file's continuation checks."""

import functools
from types import SimpleNamespace

from hypothesis import given, settings
from hypothesis import strategies as st

from kabudachi.flow import flow, group
from kabudachi.registry import TaskKind, TaskRegistry
from kabudachi.serializers import SerializerRegistry
from kabudachi.session import Session
from kabudachi.tasks import Task
from proto_messages import Greeting
from session_world import Pickle, World, run, with_events

SERIALIZERS = SerializerRegistry.with_defaults()
PROTOBUF = SERIALIZERS.get("protobuf")


def fold_left(older: Greeting, newer: Greeting) -> Greeting:
    """Not associative, so an order or grouping mistake changes the result."""
    return Greeting(text=f"({older.text}>{newer.text})")


def coalescing_definition():
    def refresh(request: Greeting) -> Greeting:
        return request

    coalescing_task = Task(
        refresh,
        registry=TaskRegistry(),
        serializers=SERIALIZERS,
        name="tests.refresh",
        kind=TaskKind.COALESCING,
        merge=fold_left,
    )
    return coalescing_task.definition


def claim_of(texts):
    encode = lambda text: PROTOBUF.encode(Greeting(text=text), Greeting)  # noqa: E731
    return SimpleNamespace(chain=[encode(t) for t in texts[:-1]], serialized_input=encode(texts[-1]))


texts = st.lists(st.text(alphabet="abcdef", min_size=1, max_size=3), min_size=1, max_size=12)


@given(texts)
def test_25_4_3_the_claiming_worker_folds_every_absorbed_payload_in_order(payloads):
    """A superseded payload is folded, never dropped, oldest first.

    Criterion 25.4.4 (the fold does not depend on where a prefix was compacted)
    follows from this: a left fold gives the same result however its prefix was
    grouped.
    """
    definition = coalescing_definition()

    folded = Session._fold(definition, PROTOBUF, claim_of(payloads))

    expected = functools.reduce(fold_left, [Greeting(text=t) for t in payloads])
    assert folded == expected


# --- 25.4.8 -------------------------------------------------------------

RETRIES = 2


def run_group_then_stage(failures, on_error):
    """A group whose member `i` fails its first `failures[i]` attempts (a
    member that is retried `RETRIES` times still fails if that is not enough),
    then a stage that records how it was called. Returns what the stage saw
    and how the flow ended."""
    attempts = [0] * len(failures)
    seen = []

    def member(index):
        def body(request: Greeting) -> Greeting:
            attempts[index] += 1
            if attempts[index] <= failures[index]:
                raise ValueError(f"member {index} failed")
            return Greeting(text=f"m{index}")

        body.__name__ = f"member_{index}"
        return body

    def stage(request: list) -> str:
        seen.append(request)
        return "stage"

    world = World(*[member(i) for i in range(len(failures))], retries=RETRIES)
    world.serializers.register(Pickle())
    stage_task = Task(
        stage,
        registry=world.registry,
        serializers=world.serializers,
        name="tests.stage",
        serializer="pickle",
    )
    members = group(
        *[world.tasks[f"member_{i}"] for i in range(len(failures))], on_error=on_error
    )
    pipeline = flow(members, stage_task)

    async def body():
        try:
            return await pipeline.start(world.session, Greeting())
        except ValueError as error:
            return error

    outcome = run(with_events(world, body))
    return seen, outcome


failure_counts = st.lists(st.integers(min_value=0, max_value=RETRIES + 1), min_size=1, max_size=5)


@settings(max_examples=15, deadline=None)
@given(failure_counts)
def test_25_4_8_under_collect_all_the_stage_after_a_group_runs_once_with_every_outcome(failures):
    seen, outcome = run_group_then_stage(failures, "collect_all")

    assert len(seen) == 1, "the stage after the group must run exactly once"
    [array] = seen
    assert len(array) == len(failures)
    for index, entry in enumerate(array):
        if failures[index] <= RETRIES:
            assert entry == Greeting(text=f"m{index}")
        else:
            assert isinstance(entry, ValueError)
    assert outcome[-1] == "stage"


@settings(max_examples=15, deadline=None)
@given(failure_counts)
def test_25_4_8_under_fail_fast_the_stage_runs_once_only_if_every_member_succeeds(failures):
    seen, outcome = run_group_then_stage(failures, "fail_fast")

    if all(count <= RETRIES for count in failures):
        assert len(seen) == 1
        assert outcome[-1] == "stage"
    else:
        assert seen == [], "a failed group must not start the stage after it"
        assert isinstance(outcome, ValueError)
