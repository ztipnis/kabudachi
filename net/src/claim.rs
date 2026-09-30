//! Claim arbitration (README §8.2) on the network: everything about
//! `/kabudachi/claim/1` that is neither the wire codec ([`codec`]) nor the
//! transport's generic request/response machinery.
//!
//! The asking side is [`Net::request_claim`] and [`Net::claim_oldest`], sent to
//! the leader the caller names: the transport keeps no leader of its own. The
//! answering side is [`answer`], which the driver applies to each inbound
//! request: whether to grant a claim is `core::scheduler::Scheduler`'s decision
//! alone.

use std::str::FromStr;

use kabudachi_core::protocol::ids::{IdGenerator, TaskId, WorkerId};
use kabudachi_core::protocol::messages::{
    Claim, ClaimBatch, ClaimOldest, ClaimReject, ClaimRejectReason, ClaimRequest, ClaimResponse,
    claim_request, claim_response,
};
use kabudachi_core::scheduler::{self, ClaimRejection, Scheduler};
use kabudachi_core::time::Clock;
use libp2p::PeerId;

use crate::claim::codec::ClaimCodec;
use crate::exchange::Asked;
use crate::framing::MAX_MESSAGE_BYTES;
use crate::messenger::Net;
use crate::peers::worker_id_of;

pub mod codec;

/// Why [`Net::request_claim`] or [`Net::claim_oldest`] got no answer from
/// a leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimFailure {
    /// The leader named is this worker. Its own claims are its own scheduler's to
    /// decide, not a peer's, so nothing was sent.
    ThisWorkerLeads,
    /// No answer came: the request failed outright (such as a leader that
    /// cannot be dialed), the leader disconnected before answering, or
    /// nothing could be sent (this `Net` has stopped, or the leader's id
    /// names no libp2p peer).
    Unanswered,
}

/// An unanswered inbound `/kabudachi/claim/1` request, returned by
/// [`Net::poll_claim_requests`]. Answer it with [`Net::respond_claim`];
/// dropping it unanswered just lets the requester's substream eventually fail
/// with `OutboundFailure` on their side (nothing here relies on that
/// happening), the same contract as `JoinRequestHandle`.
pub struct ClaimRequestHandle(Asked<ClaimCodec>);

impl ClaimRequestHandle {
    /// The `WorkerId` of whoever sent this claim request.
    pub fn from(&self) -> WorkerId {
        worker_id_of(&self.0.from)
    }

    /// What this request asks for: one task, or some of the oldest pending
    /// ones.
    pub fn request(&self) -> &claim_request::Request {
        self.0
            .request
            .request
            .as_ref()
            .expect("the claim codec only accepts a request that asks for something")
    }
}

impl Net {
    /// Drains every inbound `/kabudachi/claim/1` request not yet answered.
    /// Answer each with `Self::respond_claim`.
    pub fn poll_claim_requests(&self) -> Vec<ClaimRequestHandle> {
        self.take_asked::<ClaimCodec>()
            .into_iter()
            .map(ClaimRequestHandle)
            .collect()
    }

    /// Answers a claim request obtained from `Self::poll_claim_requests`.
    /// Fire-and-forget like `respond_join`: if the driver task has already
    /// stopped, there's nowhere for the answer to go, and that's fine to
    /// drop.
    pub fn respond_claim(&self, handle: ClaimRequestHandle, response: ClaimResponse) {
        self.answer::<ClaimCodec>(handle.0.channel, response);
    }

    /// Asks `leader` for permission to run `task_id` (`REQUEST_CLAIM`,
    /// README §8.2), and awaits its answer: an accepted `Claim` or a
    /// `ClaimReject`. The caller names the leader, as its node knows it
    /// (`WorkerNode::known_leader`); a leader that has since lost office
    /// answers `NOT_LEADER`. See [`ClaimFailure`] for why there may be no
    /// answer.
    pub async fn request_claim(
        &self,
        leader: WorkerId,
        task_id: TaskId,
    ) -> Result<ClaimResponse, ClaimFailure> {
        self.ask_leader(leader, claim_request::Request::TaskId(task_id.into()))
            .await
    }

    /// Asks `leader` for up to `limit` of the oldest pending tasks
    /// (`CLAIM_OLDEST`), and awaits its answer: a batch of claims, oldest
    /// task first, or a `ClaimReject`. The batch may hold fewer than
    /// `limit`, or none: the leader hands out only as many as fit in one
    /// message. The caller names the leader, as for
    /// [`Self::request_claim`]. See [`ClaimFailure`] for why there may be no
    /// answer.
    pub async fn claim_oldest(
        &self,
        leader: WorkerId,
        limit: u32,
    ) -> Result<ClaimResponse, ClaimFailure> {
        self.ask_leader(leader, claim_request::Request::Oldest(ClaimOldest { limit }))
            .await
    }

    async fn ask_leader(
        &self,
        leader: WorkerId,
        request: claim_request::Request,
    ) -> Result<ClaimResponse, ClaimFailure> {
        if leader == self.local_worker_id() {
            return Err(ClaimFailure::ThisWorkerLeads);
        }
        let to = PeerId::from_str(leader.as_str()).map_err(|_| ClaimFailure::Unanswered)?;
        self.ask::<ClaimCodec>(
            to,
            ClaimRequest {
                request: Some(request),
            },
        )
        .await
        .ok_or(ClaimFailure::Unanswered)
    }
}

/// The leader's answer to `request` from `claimant`, as `scheduler` decides it: an
/// accepted claim, a batch of the oldest pending tasks cut at the first one that would
/// take the answer past one message, or a rejection naming `ClaimRejection`'s reason.
/// A scheduler holding no live grant refuses as `NotLeader` and claims nothing.
///
/// Whether this node leads is the scheduler's own call, from the leadership
/// grant `carry_out` last handed it and its clock, so nothing about the node
/// is read here.
pub(crate) fn answer<C: Clock, I: IdGenerator>(
    scheduler: &mut Scheduler<C, I>,
    claimant: &WorkerId,
    request: &claim_request::Request,
) -> ClaimResponse {
    let result = match request {
        claim_request::Request::TaskId(task_id) => scheduler
            .request_claim(claimant, &TaskId::from(task_id.clone()))
            .map(|claim| claim_response::Result::Accept(wire_claim(claim))),
        claim_request::Request::Oldest(oldest) => {
            // A limit past what this platform can count is no limit.
            let limit = usize::try_from(oldest.limit).unwrap_or(usize::MAX);
            let mut batch = Batch::default();
            // Every claim the scheduler makes is one `batch` accepted,
            // in the same order, so `batch` already holds the answer.
            scheduler
                .claim_oldest_fitting(claimant, limit, |claim| batch.try_add(claim))
                .map(|_| {
                    claim_response::Result::Batch(ClaimBatch {
                        claims: batch.claims,
                    })
                })
        }
    };
    let result = result.unwrap_or_else(|rejection| {
        claim_response::Result::Reject(ClaimReject {
            reason: claim_reject_reason(rejection) as i32,
        })
    });
    ClaimResponse {
        result: Some(result),
    }
}

/// A `CLAIM_OLDEST` answer as claims are added to it. A claim the
/// claimant could not decode would stay claimed by a worker that never
/// received it, so the leader claims only what fits in one message
/// (`MAX_MESSAGE_BYTES`); the tasks left over stay pending for the next ask.
#[derive(Default)]
struct Batch {
    claims: Vec<Claim>,
    /// The encoded length of `ClaimBatch { claims }`.
    encoded_len: usize,
}

impl Batch {
    /// Adds `claim` if the answer holding it still fits in one message, and
    /// says whether it did.
    fn try_add(&mut self, claim: &scheduler::Claim) -> bool {
        use prost::encoding::{encoded_len_varint, key_len, message};

        const CLAIMS_TAG: u32 = 1; // ClaimBatch.claims
        const BATCH_TAG: u32 = 3; // ClaimResponse.batch
        let claim = wire_claim(claim.clone());
        let encoded_len = self.encoded_len + message::encoded_len(CLAIMS_TAG, &claim);
        let response_len =
            key_len(BATCH_TAG) + encoded_len_varint(encoded_len as u64) + encoded_len;
        let fits = response_len <= MAX_MESSAGE_BYTES as usize;
        if fits {
            self.claims.push(claim);
            self.encoded_len = encoded_len;
        }
        fits
    }
}

fn wire_claim(claim: scheduler::Claim) -> Claim {
    Claim {
        task: Some(claim.task),
        task_run_id: Some(claim.task_run_id.into()),
        attempt_number: claim.attempt_number,
        chain: claim.chain,
    }
}

/// `core::scheduler::ClaimRejection` -> wire `ClaimRejectReason`, one arm per
/// variant and no wildcard arm, so a new `ClaimRejection` fails to compile
/// here instead of going out as the wrong reason.
fn claim_reject_reason(rejection: ClaimRejection) -> ClaimRejectReason {
    match rejection {
        ClaimRejection::NotLeader => ClaimRejectReason::ClaimRejectNotLeader,
        ClaimRejection::TaskUnknown => ClaimRejectReason::ClaimRejectTaskUnknown,
        ClaimRejection::NotReady => ClaimRejectReason::ClaimRejectNotReady,
        ClaimRejection::AlreadySelected => ClaimRejectReason::ClaimRejectAlreadySelected,
        ClaimRejection::Finished => ClaimRejectReason::ClaimRejectFinished,
        ClaimRejection::Superseded => ClaimRejectReason::ClaimRejectSuperseded,
        ClaimRejection::KeyBusy => ClaimRejectReason::ClaimRejectKeyBusy,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing::MAX_MESSAGE_BYTES;
    use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, Uuid7Ids};
    use kabudachi_core::protocol::messages::{
        ClaimOldest, ClaimRejectReason, claim_response,
    };
    use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd, MAX_SUBMISSION_BYTES, Submission};
    use kabudachi_core::time::{Duration, Instant};
    use kabudachi_core::protocol::messages::prelude::*;
    
    use prost::Message as _;

    /// A clock that never moves, so every task's `created_at_ticks` is 0 and
    /// encodes to nothing: a claim's length depends on its payload alone.
    #[derive(Debug, Clone, Copy)]
    struct Frozen;
    impl Clock for Frozen {
        fn now(&self) -> Instant {
            Instant::at(0)
        }
        fn wall_clock_millis(&self) -> u64 {
            0
        }
    }

    type Leader = Scheduler<Frozen, Uuid7Ids>;

    fn grant() -> LeadershipGrant {
        LeadershipGrant {
            term: 1,
            recovery_epoch: 0,
            valid_until: LeaseEnd::Unbounded,
        }
    }

    fn leading() -> Leader {
        let mut scheduler = Scheduler::new(Frozen, Uuid7Ids);
        scheduler.set_leadership_grant(Some(grant()));
        scheduler
    }

    fn submission(bytes: usize) -> Submission {
        Submission::new(TaskDefinitionId::new("demo.task"), 1, vec![7; bytes], "default")
    }

    fn submit(scheduler: &mut Leader, bytes: usize) -> TaskId {
        scheduler
            .submit(submission(bytes))
            .expect("no memory limits are set")
    }

    fn submit_keyed(scheduler: &mut Leader, key: &str) -> TaskId {
        scheduler
            .submit(submission(16).with_coalescing_key(key))
            .expect("no memory limits are set")
    }

    fn claimant() -> WorkerId {
        WorkerId::new("claimant")
    }

    fn oldest(limit: u32) -> claim_request::Request {
        claim_request::Request::Oldest(ClaimOldest { limit })
    }

    fn by_id(task: &TaskId) -> claim_request::Request {
        claim_request::Request::TaskId(task.clone().into())
    }

    fn claimed(response: &ClaimResponse) -> Vec<TaskId> {
        match &response.result {
            Some(claim_response::Result::Batch(batch)) => batch
                .claims
                .iter()
                .map(|claim| claim.task.as_ref().expect("a claim carries its Task").task_id())
                .collect(),
            other => panic!("expected a batch of claims, got {other:?}"),
        }
    }

    fn reason(response: &ClaimResponse) -> ClaimRejectReason {
        match &response.result {
            Some(claim_response::Result::Reject(reject)) => {
                ClaimRejectReason::try_from(reject.reason).expect("a reason this build knows")
            }
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    /// A claim of a coalescing task whose payload is `size` bytes and that
    /// carries one retained payload of `CHAIN` bytes: how a claim outgrows a
    /// message, since one task alone cannot (`MAX_SUBMISSION_BYTES`).
    fn submit_with_chain(scheduler: &mut Leader, size: usize) -> TaskId {
        const CHAIN: usize = 400_000;
        scheduler
            .submit(submission(CHAIN).with_coalescing_key("k"))
            .expect("no memory limits are set");
        scheduler
            .submit(submission(size).with_coalescing_key("k"))
            .expect("no memory limits are set")
    }

    #[test]
    fn a_batch_that_fills_one_message_exactly_goes_out_and_one_a_byte_over_does_not() {
        // Calibrated at a payload whose nested length prefixes are as wide as at the limit
        // (three-byte varints cover 16 KiB..2 MiB), so the overhead is the same there.
        const CALIBRATION: usize = 500_000;
        let mut scratch = leading();
        submit_with_chain(&mut scratch, CALIBRATION);
        let overhead = answer(&mut scratch, &claimant(), &oldest(1)).encoded_len() - CALIBRATION;
        let exact = MAX_MESSAGE_BYTES as usize - overhead;

        let mut fits = leading();
        let task = submit_with_chain(&mut fits, exact);
        let response = answer(&mut fits, &claimant(), &oldest(1));
        assert_eq!(response.encoded_len(), MAX_MESSAGE_BYTES as usize, "calibration is exact");
        assert_eq!(claimed(&response), vec![task]);

        let mut over = leading();
        submit_with_chain(&mut over, exact + 1);
        let small = submit(&mut over, 16);
        // One that could never fit is passed over (Scheduler::claim_oldest_fitting), so
        // the task behind it still goes out.
        assert_eq!(claimed(&answer(&mut over, &claimant(), &oldest(2))), vec![small]);
    }

    #[test]
    fn the_largest_task_the_scheduler_accepts_goes_out_in_one_message() {
        let mut scheduler = leading();
        let largest = MAX_SUBMISSION_BYTES as usize - "demo.task".len() - "default".len();
        let task = submit(&mut scheduler, largest);

        let response = answer(&mut scheduler, &claimant(), &by_id(&task));

        assert!(matches!(response.result, Some(claim_response::Result::Accept(_))));
        assert!(response.encoded_len() <= MAX_MESSAGE_BYTES as usize);
    }

    #[test]
    fn the_first_claim_that_does_not_fit_ends_the_batch() {
        const LARGE: usize = 600 * 1024;
        let mut scheduler = leading();
        let a = submit(&mut scheduler, 16);
        let b = submit(&mut scheduler, LARGE);
        let c = submit(&mut scheduler, LARGE);
        let d = submit(&mut scheduler, 16);
        assert_eq!(claimed(&answer(&mut scheduler, &claimant(), &oldest(10))), vec![a, b]);
        assert_eq!(claimed(&answer(&mut scheduler, &claimant(), &oldest(10))), vec![c, d]);
    }

    #[test]
    fn every_claim_rejection_goes_out_as_its_own_reason() {
        let mut scheduler = leading();
        let expect = |task: &TaskId, expected: ClaimRejectReason, scheduler: &mut Leader| {
            let response = answer(scheduler, &claimant(), &by_id(task));
            assert_eq!(reason(&response), expected, "for {task:?}");
        };

        let not_ready = scheduler
            .submit(submission(16).with_delay(Duration::from_secs(3600)))
            .expect("no memory limits are set");
        expect(&not_ready, ClaimRejectReason::ClaimRejectNotReady, &mut scheduler);

        let selected = submit(&mut scheduler, 16);
        let first = answer(&mut scheduler, &claimant(), &by_id(&selected));
        assert!(matches!(first.result, Some(claim_response::Result::Accept(_))));
        expect(&selected, ClaimRejectReason::ClaimRejectAlreadySelected, &mut scheduler);

        let finished = submit(&mut scheduler, 16);
        scheduler.cancel(&finished).expect("a pending task can be cancelled");
        expect(&finished, ClaimRejectReason::ClaimRejectFinished, &mut scheduler);

        let older = submit_keyed(&mut scheduler, "k");
        submit_keyed(&mut scheduler, "k");
        expect(&older, ClaimRejectReason::ClaimRejectSuperseded, &mut scheduler);

        let busy = submit_keyed(&mut scheduler, "busy");
        let claimed_busy = answer(&mut scheduler, &claimant(), &by_id(&busy));
        assert!(matches!(claimed_busy.result, Some(claim_response::Result::Accept(_))));
        let blocked = submit_keyed(&mut scheduler, "busy");
        expect(&blocked, ClaimRejectReason::ClaimRejectKeyBusy, &mut scheduler);

        expect(
            &TaskId::new("never-submitted"),
            ClaimRejectReason::ClaimRejectTaskUnknown,
            &mut scheduler,
        );
    }

    #[test]
    fn a_worker_without_a_grant_refuses_both_asks_as_not_leader_and_claims_nothing() {
        let mut scheduler: Leader = Scheduler::new(Frozen, Uuid7Ids);
        let task = submit(&mut scheduler, 16);
        assert_eq!(
            reason(&answer(&mut scheduler, &claimant(), &by_id(&task))),
            ClaimRejectReason::ClaimRejectNotLeader
        );
        assert_eq!(
            reason(&answer(&mut scheduler, &claimant(), &oldest(5))),
            ClaimRejectReason::ClaimRejectNotLeader
        );
        scheduler.set_leadership_grant(Some(grant()));
        assert_eq!(claimed(&answer(&mut scheduler, &claimant(), &oldest(5))), vec![task]);
    }

    #[tokio::test]
    async fn a_worker_that_names_itself_leader_is_told_to_decide_its_own_claims() {
        let net = Net::new();
        let itself = net.local_worker_id();
        assert_eq!(
            net.request_claim(itself.clone(), TaskId::new("any")).await,
            Err(ClaimFailure::ThisWorkerLeads)
        );
        assert_eq!(net.claim_oldest(itself, 1).await, Err(ClaimFailure::ThisWorkerLeads));
    }
}
