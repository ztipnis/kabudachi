use kabudachi_core::protocol::ids::{IdGenerator, WorkerId};
use kabudachi_core::protocol::messages::{ClaimOldest, claim_request};
use kabudachi_core::task_record::Settled;
use kabudachi_core::time::{Clock, WallTime};
use tokio::time::Instant as TokioInstant;

use super::answers::{Deciding, HeldAnswer, Local, task_named};
use crate::claim;
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
/// scheduler, as a remote worker's are, and held until the writes they
/// made are stored.
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
            decide_own_claim(places, deciding, local);
        }
        OwnLeader::Remote(leader) => executing.discover(leader.clone(), places, WallTime::now(clock)),
    }
}

/// Whether this worker may claim now. Some leader must have vouched for
/// hearing it (`WorkerNode::has_contact_floor`): a run started before that
/// would have no abort deadline. While its own scheduler cannot grant yet
/// (it holds no grant, or reconciles), a claim of its own is not made: it
/// would be refused, and back off as one that found nothing.
fn may_claim<C: Clock, I: IdGenerator>(deciding: &Deciding<'_, C, I>, to: &OwnLeader) -> bool {
    let can_grant = match to {
        OwnLeader::Itself => deciding.scheduler.is_leader() && deciding.scheduler.reconciling().is_none(),
        OwnLeader::Remote(_) => true,
    };
    deciding.node.has_contact_floor() && can_grant
}

/// Decides `request`, a report on one of this worker's own runs, with its
/// own scheduler, as [`respond_to_task_requests`] decides a remote worker's,
/// and holds the answer until the writes it made are stored.
fn decide_own_report<C: Clock, I: IdGenerator>(
    request: RunReport,
    deciding: &mut Deciding<'_, C, I>,
    clock: &C,
    local: &mut Local,
) {
    let named = task_named(deciding.scheduler, &request);
    deciding.net.note_sending(&request);
    let response = task_exchange::answer(deciding.scheduler, &deciding.net.local_worker_id(), &request, clock);
    deciding.hold(HeldAnswer::Own(OwnAnswer::Report { request, response }), named.as_ref(), local);
}

/// Claims up to `places` of the oldest pending tasks for this worker's own
/// executor from its own scheduler, as [`respond_to_claim_requests`] decides
/// a remote worker's claim, and holds the answer until the writes it made
/// are stored.
fn decide_own_claim<C: Clock, I: IdGenerator>(places: u32, deciding: &mut Deciding<'_, C, I>, local: &mut Local) {
    let request = claim_request::Request::Oldest(ClaimOldest { limit: places });
    let response = claim::answer(deciding.scheduler, &deciding.net.local_worker_id(), &request);
    // The ledger takes each grant now, as a remote claim's is entered inside
    // its discovery, so a driver dropped before the grant is stored or handed
    // over still finds the runs when the next one starts.
    for claim in granted(&response) {
        deciding.net.claimed_runs().claimed(claim);
    }
    deciding.hold(HeldAnswer::Own(OwnAnswer::Claim { response, reserved: places }), None, local);
}
