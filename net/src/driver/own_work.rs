use kabudachi_core::protocol::ids::{IdGenerator, WorkerId};
use kabudachi_core::protocol::messages::{Claim, ClaimOldest, claim_request, claim_response};
use kabudachi_core::task_record::Settled;
use kabudachi_core::time::{Clock, WallTime};
use tokio::time::Instant as TokioInstant;

use super::answers::{Deciding, HeldAnswer, Local, record_named};
use crate::claim;
use crate::discovery::{Stage, Takes};
use crate::executor::{Executing, OwnAnswer, RunReport, granted};
use crate::task_exchange;

/// Whom this worker's own claims and reports go to.
enum OwnLeader {
    /// This worker leads: its own scheduler decides them.
    Itself,
    Remote(WorkerId),
}

/// Acts on what the executor reported and on the answers its claims and
/// reports got, sends each report the leader has yet to take, and claims
/// more work while the executor has room and [`may_claim`] allows. While
/// this worker leads, its own claims and reports are decided by its own
/// scheduler, as a remote worker's are, its claims in a follower's order:
/// the records it holds first, then the shard's oldest. Each is held until
/// the writes it made are stored.
pub(super) fn drive_executor<C: Clock, I: IdGenerator>(
    executing: &mut Executing<'_>,
    deciding: &mut Deciding<'_, C, I>,
    clock: &C,
    settled: Vec<Settled<OwnAnswer>>,
) {
    executing.expire(TokioInstant::now());
    executing.take_arrived();
    for answer in settled {
        executing.own_settled(answer);
    }
    let Some((leader, _)) = deciding.node.known_leader() else {
        return;
    };
    let to = if leader == deciding.net.local_worker_id() {
        OwnLeader::Itself
    } else {
        OwnLeader::Remote(leader)
    };
    loop {
        let mut local = Local::default();
        send_own_reports(executing, deciding, clock, &to, &mut local);
        claim_more(executing, deciding, clock, &to, &mut local);
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

fn send_own_reports<C: Clock, I: IdGenerator>(
    executing: &mut Executing<'_>,
    deciding: &mut Deciding<'_, C, I>,
    clock: &C,
    to: &OwnLeader,
    local: &mut Local,
) {
    for request in executing.reports_to_send() {
        match to {
            OwnLeader::Itself => decide_own_report(request, deciding, clock, local),
            OwnLeader::Remote(leader) => executing.send_report(leader.clone(), request),
        }
    }
}

fn claim_more<C: Clock, I: IdGenerator>(
    executing: &mut Executing<'_>,
    deciding: &mut Deciding<'_, C, I>,
    clock: &C,
    to: &OwnLeader,
    local: &mut Local,
) {
    if !may_claim(deciding, to) {
        return;
    }
    let Some(places) = executing.places_to_claim() else {
        return;
    };
    match to {
        OwnLeader::Itself => {
            executing.claiming_own(places);
            decide_own_claim(places, WallTime::now(clock), executing.takes(), deciding, local);
        }
        OwnLeader::Remote(leader) => {
            executing.discover(leader.clone(), places, WallTime::now(clock))
        }
    }
}

/// Whether this worker may claim now. Some leader must have vouched for
/// hearing it (`WorkerNode::has_contact_floor`): a run started before that
/// would have no abort deadline. While its own scheduler cannot grant yet
/// (it holds no grant, or reconciles), a claim of its own is not made: it
/// would be refused, and back off as one that found nothing.
fn may_claim<C: Clock, I: IdGenerator>(deciding: &Deciding<'_, C, I>, to: &OwnLeader) -> bool {
    let can_grant = match to {
        OwnLeader::Itself => {
            deciding.scheduler.is_leader() && deciding.scheduler.reconciling().is_none()
        }
        OwnLeader::Remote(_) => true,
    };
    deciding.node.has_contact_floor() && can_grant
}

/// Decides `request`, a report on one of this worker's own runs, with its
/// own scheduler, as [`super::answers::respond_to_task_requests`] decides a remote worker's,
/// and holds the answer until the writes it made are stored.
fn decide_own_report<C: Clock, I: IdGenerator>(
    request: RunReport,
    deciding: &mut Deciding<'_, C, I>,
    clock: &C,
    local: &mut Local,
) {
    let named = record_named(deciding.scheduler, &request);
    deciding.net.note_sending(&request);
    let response = task_exchange::answer(
        deciding.scheduler,
        &deciding.net.local_worker_id(),
        &request,
        clock,
    );
    deciding.hold(
        HeldAnswer::Own(OwnAnswer::Report { request, response }),
        named,
        local,
    );
}

/// Claims up to `places` tasks for this worker's own executor from its own
/// scheduler, in the order a follower's discovery takes them, less the peers:
/// first the pending tasks of the records this worker holds
/// (`Net::own_candidates`, judged due by `now`), each decided as
/// [`super::answers::respond_to_claim_requests`] decides a remote worker's
/// claim of one task; then, for the places left, the shard's oldest pending
/// tasks. Peers are not asked: every pending task is in this scheduler
/// already, so its oldest stand for whatever peers hold. A task refused is
/// passed over, as discovery passes over one its stale view showed. A held
/// task whose input is newer than `takes.runnable` is never claimed: it is
/// reported blocked first, as a follower reports it, so it stays queued for a
/// worker that can run it and the oldest tasks pass it over here. The answer
/// is held until the writes the claims and reports made are stored.
fn decide_own_claim<C: Clock, I: IdGenerator>(
    places: u32,
    now: WallTime,
    takes: &Takes,
    deciding: &mut Deciding<'_, C, I>,
    local: &mut Local,
) {
    let me = deciding.net.local_worker_id();
    let limit = usize::try_from(places).unwrap_or(usize::MAX);
    let (candidates, newer) = deciding.net.own_candidates(now, takes);
    for (task, reason) in newer {
        // A rejection (the task ended or is not known for certain yet) leaves
        // nothing to block; the task is not a candidate either way.
        let _ = deciding.scheduler.report_blocked(&me, &task, reason, false);
    }
    let mut room = deciding.net.payload_memory().room();
    let mut claims: Vec<(Stage, Claim)> = Vec::new();
    for (task, input) in candidates {
        if claims.len() >= limit {
            break;
        }
        if matches!((input, room), (Some(input), Some(left)) if input > left) {
            continue;
        }
        let request = claim_request::Request::TaskId(task.into());
        if let Some(claim_response::Result::Accept(claim)) =
            claim::answer(deciding.scheduler, &me, &request).result
        {
            room = room.map(|left| left.saturating_sub(input.unwrap_or(0)));
            claims.push((Stage::Own, claim));
        }
    }
    if claims.len() < limit {
        let wanted = u32::try_from(limit - claims.len()).unwrap_or(u32::MAX);
        let request = claim_request::Request::Oldest(ClaimOldest {
            limit: wanted,
            queues: takes.queues.names(),
            input_room: room,
        });
        let response = claim::answer(deciding.scheduler, &me, &request);
        claims.extend(granted(&response).into_iter().map(|claim| (Stage::Oldest, claim)));
    }
    // The ledger takes each grant now, as a remote claim's is entered inside
    // its discovery, so a driver dropped before the grant is stored or handed
    // over still finds the runs when the next one starts.
    for (_, claim) in &claims {
        deciding.net.claimed_runs().claimed(claim.clone());
    }
    deciding.hold(
        HeldAnswer::Own(OwnAnswer::Claim {
            claims,
            reserved: places,
        }),
        None,
        local,
    );
}
