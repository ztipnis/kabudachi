use kabudachi_core::election::WorkerNode;
use kabudachi_core::protocol::ids::IdGenerator;
use kabudachi_core::protocol::messages::{ClaimOldest, claim_request};
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::task_record::{LeaderRecords, RecordOutbox, Settled};
use kabudachi_core::time::{Clock, WallTime};
use tokio::time::Instant as TokioInstant;

use super::answers::{HeldAnswer, Local, send_answer, task_named};
use super::stepper::NetRecords;
use crate::claim;
use crate::executor::{Executing, OwnAnswer, RunReport, granted};
use crate::messenger::Net;
use crate::task_exchange;
use crate::task_store::placement::ReplicationFactor;

/// Acts on what the executor reported and on the answers its claims and
/// reports got, sends each report the leader has yet to take, and claims
/// more work while the executor has room, the node knows its leader, and
/// some leader has vouched for hearing this worker
/// (`WorkerNode::has_contact_floor`): a run started before that would have no
/// abort deadline. While this worker leads, its own claims and reports are
/// decided by its own scheduler, as a remote worker's are, and held until the
/// writes they made are stored.
#[allow(clippy::too_many_arguments)]
pub(super) fn drive_executor<C: Clock, I: IdGenerator>(
    executing: &mut Executing<'_>,
    node: &WorkerNode<C>,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    net: &Net,
    clock: &C,
    records: &mut LeaderRecords<HeldAnswer>,
    factor: ReplicationFactor,
    settled: Vec<Settled<OwnAnswer>>,
) {
    executing.expire(TokioInstant::now());
    executing.take_arrived();
    for answer in settled {
        executing.own_settled(answer);
    }
    let Some((leader, _)) = node.known_leader() else {
        return;
    };
    let leads = leader == net.local_worker_id();
    loop {
        let mut local = Local::default();
        for request in executing.reports_to_send() {
            if leads {
                decide_own_report(
                    request, node, scheduler, net, clock, records, factor, &mut local,
                );
            } else {
                executing.send_report(leader.clone(), request);
            }
        }
        // While its own scheduler cannot grant yet (it holds no grant, or
        // reconciles), a claim of its own is not made: it would be refused,
        // and back off as one that found nothing.
        let can_claim = !leads || (scheduler.is_leader() && scheduler.reconciling().is_none());
        if node.has_contact_floor()
            && can_claim
            && let Some(places) = executing.places_to_claim()
        {
            if leads {
                executing.claiming_own(places);
                decide_own_claim(places, node, scheduler, net, records, factor, &mut local);
            } else {
                executing.discover(leader.clone(), places, WallTime::now(clock));
            }
        }
        // An answer with no write to wait for settles at once, and may let
        // the next report of its run go.
        if local.own.is_empty() {
            return;
        }
        for answer in local.own {
            executing.own_settled(answer);
        }
    }
}

/// Decides `request`, a report on one of this worker's own runs, with its
/// own scheduler, as [`respond_to_task_requests`] decides a remote worker's,
/// and holds the answer until the writes it made are stored.
#[allow(clippy::too_many_arguments)]
fn decide_own_report<C: Clock, I: IdGenerator>(
    request: RunReport,
    node: &WorkerNode<C>,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    net: &Net,
    clock: &C,
    records: &mut LeaderRecords<HeldAnswer>,
    factor: ReplicationFactor,
    local: &mut Local,
) {
    let named = task_named(scheduler, &request);
    net.note_sending(&request);
    let response = task_exchange::answer(scheduler, &net.local_worker_id(), &request, clock);
    let made = records.write(node, scheduler, &mut NetRecords { net, factor });
    let held = HeldAnswer::Own(OwnAnswer::Report { request, response });
    if let Some(settled) = records.hold(held, made, named.as_ref()) {
        send_answer(net, settled, local);
    }
}

/// Claims up to `places` of the oldest pending tasks for this worker's own
/// executor from its own scheduler, as [`respond_to_claim_requests`] decides
/// a remote worker's claim, and holds the answer until the writes it made
/// are stored.
fn decide_own_claim<C: Clock, I: IdGenerator>(
    places: u32,
    node: &WorkerNode<C>,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    net: &Net,
    records: &mut LeaderRecords<HeldAnswer>,
    factor: ReplicationFactor,
    local: &mut Local,
) {
    let request = claim_request::Request::Oldest(ClaimOldest { limit: places });
    let response = claim::answer(scheduler, &net.local_worker_id(), &request);
    // The ledger takes each grant now, as a remote claim's is entered inside
    // its discovery, so a driver dropped before the grant is stored or handed
    // over still finds the runs when the next one starts.
    for claim in granted(&response) {
        net.claimed_runs().claimed(claim);
    }
    let made = records.write(node, scheduler, &mut NetRecords { net, factor });
    let held = HeldAnswer::Own(OwnAnswer::Claim { response, reserved: places });
    if let Some(settled) = records.hold(held, made, None) {
        send_answer(net, settled, local);
    }
}
