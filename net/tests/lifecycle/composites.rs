//! Composites over real sockets: submitted by a client that is not the
//! leader, run by whichever workers claim their members, and finished to
//! the client, or durably with no client at all.

use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::blob::{BlobKey, BlobOwner};
use kabudachi_core::protocol::generated::CompositeState as WireState;
use kabudachi_core::protocol::ids::{CompositeId, IdGenerator, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::messages::{CancelOutcome, TaskRejectReason, task_response};
use kabudachi_core::result_backend::PersistenceState;
use kabudachi_core::scheduler::MemoryLimits;
use kabudachi_core::scheduler::composite::{
    CompositeKind, CompositeState, Resolved, Stage, SubmittedComposite, Unresolved, member_input, results,
};
use kabudachi_core::task_record::RecordKey;
use kabudachi_core::time::{RealClock, WallTime};
use kabudachi_net::blobs::BlobRole;
use kabudachi_net::composites::{CountingSource, NetSource};
use kabudachi_net::executor::Report;

use crate::support::deadline::within_deadline;
use crate::support::election::wait_until;
use crate::support::executor::FakeExecutor;
use crate::support::records::{
    MIB, ThreeVoters, Voters, claimed_member_run_through, composite_submitted_as, composite_submitted_through,
    finished_heard_by, flow_of_bound, group_of_chunked, member_claimed_through, member_run_through,
    member_started_through, plain_with, retry, submitted_through, two_holders, varied,
};
use crate::support::worker::poll_until;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flow_submitted_by_a_client_runs_stage_after_stage_and_its_client_hears_it_finished() {
    within_deadline(async {
        let (mut shard, client) = Voters::start(3).await;
        let leader = shard.drive_until_a_leader().await;
        shard.join_as_pending(&client, leader).await;
        let (nets, leader_id) = (shard.nets.clone(), shard.id(leader));
        let worker = shard.others(leader)[0];
        let heard = finished_heard_by(&client);
        let flow = shard
            .drive_until(composite_submitted_through(&client, &leader_id, flow_of_bound(&["load", "shout"])))
            .await;
        for (index, result) in [b"loaded".as_slice(), b"shouted"].into_iter().enumerate() {
            shard
                .drive_until(member_run_through(&nets[worker], &leader_id, &flow, index as u64, result))
                .await;
        }
        let finished = shard.drive_until(heard).await;
        assert_eq!(finished.state, CompositeState::Succeeded);
        assert_eq!(finished.composite_id, flow);
        let record = shard.drive_until(nets[worker].composite_record(&flow)).await.expect("the shard holds it");
        assert!(record.finished);
        // The client reads what the stages returned from the pages and blobs
        // the shard holds.
        let source = NetSource::new(Arc::clone(&nets[worker]));
        let read = shard.drive_until(results(&source, &flow)).await.expect("the results resolve");
        let Resolved::List(entries) = read else { panic!("a flow's results are a list: {read:?}") };
        let values: Vec<_> = entries
            .into_iter()
            .map(|entry| match entry {
                Resolved::Value { bytes, .. } => bytes,
                other => panic!("expected a value, got {other:?}"),
            })
            .collect();
        assert_eq!(values, vec![b"loaded".to_vec(), b"shouted".to_vec()]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_composite_whose_client_is_gone_completes_durably() {
    within_deadline(async {
        let (mut shard, client) = Voters::start(3).await;
        let leader = shard.drive_until_a_leader().await;
        shard.join_as_pending(&client, leader).await;
        let (nets, leader_id) = (shard.nets.clone(), shard.id(leader));
        let worker = shard.others(leader)[0];
        let flow = shard
            .drive_until(composite_submitted_through(&client, &leader_id, flow_of_bound(&["load"])))
            .await;
        drop(client);
        shard
            .drive_until(member_run_through(&nets[worker], &leader_id, &flow, 0, b"loaded"))
            .await;
        let record = shard
            .drive_until(async {
                loop {
                    if let Some(record) = nets[worker].composite_record(&flow).await
                        && record.finished
                    {
                        return record;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await;
        assert_eq!(record.state(), WireState::Succeeded);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stage_whose_worker_is_lost_is_replayed_and_the_flow_moves_on_once() {
    within_deadline(async {
        let (mut shard, client) = Voters::start(5).await;
        let leader = shard.drive_until_a_leader().await;
        shard.join_as_pending(&client, leader).await;
        let (nets, leader_id) = (shard.nets.clone(), shard.id(leader));
        let [lost, survivor, ..] = shard.others(leader)[..] else { unreachable!() };
        let heard = finished_heard_by(&client);
        let flow = shard
            .drive_until(composite_submitted_through(&client, &leader_id, flow_of_bound(&["load", "shout"])))
            .await;
        // The lost worker started the first stage and died before it
        // reported it done.
        shard.drive_until(member_started_through(&nets[lost], &leader_id, &flow, 0)).await;
        shard.kill(lost);
        shard
            .drive_until(member_run_through(&nets[survivor], &leader_id, &flow, 0, b"loaded"))
            .await;
        shard
            .drive_until(member_run_through(&nets[survivor], &leader_id, &flow, 1, b"shouted"))
            .await;
        let finished = shard.drive_until(heard).await;
        assert_eq!(finished.state, CompositeState::Succeeded);
        let member = flow.member_task(0);
        let runs = shard.with(leader, move |_, scheduler| scheduler.runs_of(&member)).await;
        assert_eq!(runs.len(), 2, "the stage ran again, once");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_composite_whose_record_is_re_placed_off_a_killed_holder_still_resolves_its_blob_backed_member_inputs() {
    within_deadline(async {
        let (mut shard, client) = Voters::start(5).await;
        let leader = shard.drive_until_a_leader().await;
        let leader_id = shard.id(leader);
        shard.join_as_pending(&client, leader).await;
        let nets = shard.nets.clone();
        // A flow whose first stage is bound to an input stored as blobs, and
        // whose second is a group whose one member is bound to another. The
        // flow is the tree's root and owns both. Nothing claims a member
        // until a holder is gone, so the group does not exist yet.
        let id = Uuid7Ids.next_composite_id();
        // The client has no driver to name its leader, so a voter stores the
        // payloads where the leader places the flow, as a worker would.
        let writer = &nets[shard.others(leader)[0]];
        let nested = id.child(1);
        let owner = BlobOwner::Composite(id.clone());
        let (input, nested_input) = (varied(3 * MIB / 2), varied(5 * MIB / 4));
        let chunks = shard
            .drive_until(writer.store_payload_at_leader(BlobRole::Input, &owner, &input, retry()))
            .await
            .expect("stored");
        let nested_chunks = shard
            .drive_until(writer.store_payload_at_leader(BlobRole::Input, &owner, &nested_input, retry()))
            .await
            .expect("stored");
        let keys: Vec<BlobKey> = chunks.blob_keys().chain(nested_chunks.blob_keys()).cloned().collect();
        let mut step = group_of_chunked("measure", chunks);
        step.kind = CompositeKind::Flow;
        step.stages.push(Stage::Composite(group_of_chunked("measure", nested_chunks)));
        shard.drive_until(composite_submitted_as(&client, &leader_id, &id, step)).await;
        let record_key = RecordKey::Composite(id.clone());
        let dead = (0..nets.len())
            .find(|voter| *voter != leader && nets[*voter].held_records().get_record(&record_key).is_some())
            .expect("a holder of the flow other than the leader");
        let dead_id = shard.id(dead);
        shard.kill(dead);
        let claimant = (0..nets.len()).find(|voter| ![leader, dead].contains(voter)).unwrap();
        shard
            .drive_until(async {
                // The leader places the flow, its page and its first member
                // off the dead holder, and every blob the flow owns follows,
                // the nested stage's too: a majority of the new placement
                // holds each. A member run before its records have moved
                // would have its end written to the dead holder too, and
                // refused.
                let survivors: Vec<usize> = (0..nets.len()).filter(|voter| *voter != dead).collect();
                let placed_off_the_dead = |key: &RecordKey| -> Option<Vec<WorkerId>> {
                    survivors.iter().find_map(|voter| {
                        let record = nets[*voter].held_records().get_record(key)?;
                        let placement: Vec<WorkerId> = record.placement().iter().cloned().map(WorkerId::from).collect();
                        (!placement.is_empty() && !placement.contains(&dead_id)).then_some(placement)
                    })
                };
                let moved = [RecordKey::Task(id.member_task(0))];
                wait_until(|| {
                    moved.iter().all(|key| placed_off_the_dead(key).is_some())
                        && placed_off_the_dead(&record_key).is_some_and(|placement| {
                            keys.iter().all(|key| {
                                survivors
                                    .iter()
                                    .filter(|holder| placement.contains(&nets[**holder].local_worker_id()))
                                    .filter(|holder| nets[**holder].held_records().blob(key).is_some())
                                    .count()
                                    >= placement.len() / 2 + 1
                            })
                        })
                })
                .await;
                let source = NetSource::new(Arc::clone(&nets[claimant]));
                let claim = member_claimed_through(&nets[claimant], &leader_id, &id, 0).await;
                let given = member_input(&source, &claim.task).await.expect("the first stage's input resolves");
                assert_eq!(given.own_input, Some(input.clone()));
                claimed_member_run_through(&nets[claimant], &leader_id, &claim, b"measured").await;
                // The nested stage starts only now. Its member carries the
                // group's id, and reads its input under the flow, the owner
                // its record names.
                let claim = member_claimed_through(&nets[claimant], &leader_id, &nested, 0).await;
                assert_eq!(claim.task.composite_id.clone().map(CompositeId::from), Some(nested.clone()));
                let given = member_input(&source, &claim.task).await.expect("the nested member's input resolves");
                assert_eq!(given.own_input, Some(nested_input.clone()));
            })
            .await;
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_counts_the_blobs_it_reads_as_its_input_in_flight_until_they_are_released() {
    within_deadline(async {
        let (mut shard, client) = Voters::start(3).await;
        let leader = shard.drive_until_a_leader().await;
        let leader_id = shard.id(leader);
        shard.join_as_pending(&client, leader).await;
        let nets = shard.nets.clone();
        let reader = &nets[shard.others(leader)[0]];
        let id = Uuid7Ids.next_composite_id();
        let input = varied(3 * MIB / 2);
        let chunks = shard
            .drive_until(reader.store_payload_at_leader(
                BlobRole::Input,
                &BlobOwner::Composite(id.clone()),
                &input,
                retry(),
            ))
            .await
            .expect("stored");
        shard
            .drive_until(composite_submitted_as(&client, &leader_id, &id, group_of_chunked("measure", chunks)))
            .await;
        let memory = Arc::clone(reader.payload_memory());
        shard
            .drive_until(async {
                let claim = member_claimed_through(reader, &leader_id, &id, 0).await;
                let source = CountingSource::new(Arc::clone(reader));

                memory.set_limits(Some(MemoryLimits { soft: 4 * MIB as u64, hard: 8 * MIB as u64 }));
                let given = member_input(&source, &claim.task).await.expect("the input resolves");
                assert_eq!(given.own_input, Some(input.clone()));
                let counted = source.counted();
                assert_eq!(memory.in_use(), input.len() as u64, "the run holds its input");
                drop(counted);
                assert_eq!(memory.in_use(), 0, "its input is released with the run");

                // An input alone past the hard limit is refused before it is fetched.
                memory.set_limits(Some(MemoryLimits { soft: MIB as u64 / 4, hard: MIB as u64 }));
                source.begin();
                assert_eq!(
                    member_input(&source, &claim.task).await.err(),
                    Some(Unresolved::TooLarge { size: input.len() as u64, limit: MIB as u64 })
                );
                assert_eq!(memory.in_use(), 0);
            })
            .await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_that_returns_a_step_is_certified_with_it_and_its_task_ends_only_when_the_step_does() {
    within_deadline(async {
        let (mut shard, client) = ThreeVoters::start().await;
        let mut executors: Vec<FakeExecutor> = (0..3)
            .map(|voter| {
                let (executor, endpoint) = FakeExecutor::new();
                shard.set_executor(voter, endpoint);
                executor
            })
            .collect();
        let leader = shard.drive_until_a_leader().await;
        let leader_id = shard.id(leader);
        shard.join_as_pending(&client, leader).await;
        let follower = shard.others(leader)[0];
        let nets = shard.nets.clone();

        // A follower runs a task whose body returns a step.
        executors[follower].grant(1);
        let task = shard.drive_until(submitted_through(&client, &leader_id, plain_with(b"in"))).await;
        let (run, _) = shard.drive_until(executors[follower].next_claim()).await;
        executors[follower].report(Report::Started { run: run.clone(), execution_version: None });
        let step = flow_of_bound(&["after"]);
        executors[follower].report(Report::Completed {
            run: run.clone(),
            digest: step.digest(),
            delivered: false,
            continuation: Some(step),
            persistence: PersistenceState::Disabled,
        });

        // The leader certifies the run and starts the step as a composite
        // derived from it; the follower's ledger lets the run go once it
        // heard so.
        let continued = CompositeId::continuation_of(&run);
        let follower_net = nets[follower].clone();
        shard
            .drive_until(poll_until("the step runs as a composite", || {
                nets.iter().any(|net| net.held_records().get_record(&RecordKey::Composite(continued.clone())).is_some())
                    && follower_net.claimed_runs().get(&run).is_none()
            }))
            .await;
        assert!(
            nets.iter().filter_map(|net| net.held_records().get(&task)).all(|record| !record.finished),
            "the task is not over while its step runs"
        );

        // The step's only stage ends, and the task with it.
        shard
            .drive_until(member_run_through(&nets[follower], &leader_id, &continued, 0, b"after"))
            .await;
        shard
            .drive_until(poll_until("the task ended with its step", || {
                nets.iter().filter_map(|net| net.held_records().get(&task)).any(|record| record.finished)
            }))
            .await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_stores_submits_and_cancels_a_composite_through_the_leader_its_node_names() {
    within_deadline(async {
        let mut shard = Voters::start_without_client(1, two_holders()).await;
        let leader = shard.drive_until_a_leader().await;
        let net = shard.nets[leader].clone();
        let submitted = SubmittedComposite {
            composite_id: Uuid7Ids.next_composite_id(),
            submitted_at: WallTime::now(&RealClock::new()),
            step: flow_of_bound(&["load"]),
            client: Some(net.local_worker_id()),
        };
        // An id with a `/` would name a page of another composite's records.
        let slashed = SubmittedComposite { composite_id: CompositeId::new("x/5"), ..submitted.clone() };
        let answer = shard
            .drive_until(net.submit_composite_to_leader(&slashed, retry()))
            .await
            .expect("a leader answers");
        let Some(task_response::Result::Reject(reject)) = answer.result else {
            panic!("the leader refuses an id that is a record key's page: {answer:?}");
        };
        assert_eq!(reject.reason, TaskRejectReason::TaskRejectMalformed as i32);

        // A value too large to inline is stored where the leader will place
        // the composite, before the composite is submitted.
        let owner = BlobOwner::Composite(submitted.composite_id.clone());
        let chunk = varied(MIB / 4);
        shard
            .drive_until(net.store_blobs_at_leader(BlobRole::Input, &owner, vec![chunk.clone()], retry()))
            .await
            .expect("the shard stores the value");
        let digest = BlobKey::of(&chunk).digest();
        assert_eq!(shard.drive_until(net.get_blob(owner, &digest)).await, Some(chunk));

        let answer = shard
            .drive_until(net.submit_composite_to_leader(&submitted, retry()))
            .await
            .expect("a leader answers");
        let Some(task_response::Result::Composite(accepted)) = answer.result else {
            panic!("the leader records the composite: {answer:?}");
        };
        assert_eq!(CompositeId::from(accepted.composite_id.expect("it names the composite")), submitted.composite_id);

        let answer = shard
            .drive_until(net.cancel_composite_at_leader(submitted.composite_id.clone(), retry()))
            .await
            .expect("a leader answers");
        let Some(task_response::Result::Cancelled(cancelled)) = answer.result else {
            panic!("the leader answers a cancel: {answer:?}");
        };
        assert_eq!(cancelled.outcome(), CancelOutcome::Cancelled);
    })
    .await;
}
