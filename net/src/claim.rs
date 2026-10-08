//! Claim arbitration on the network: everything about
//! `/kabudachi/claim/1` that is neither the wire codec ([`codec`]) nor the
//! transport's generic request/response machinery.
//!
//! The asking side is [`Net::request_claim`] and [`Net::claim_oldest`], sent to
//! the leader the caller names: the transport keeps no leader of its own; a
//! worker's driver claims for its executor (see `crate::executor`), and claims
//! from its own scheduler while it leads. The
//! answering side is [`answer`], which the driver applies to each inbound
//! request: whether to grant a claim is `core::scheduler::Scheduler`'s decision
//! alone. The decision is made at once, but the driver sends the answer only
//! once the revisions it wrote are acknowledged and the leader still leads;
//! otherwise the claimant is answered [`not_leader`]. Before the scheduler is
//! asked, the driver checks the claimant against the leader's roster: a
//! worker that is neither a voter nor a pending member of the shard is
//! answered [`not_member`] and must join the shard first. Each claim a leader
//! grants is also entered in the claimant's
//! [`ClaimedRuns`](crate::claimed_runs::ClaimedRuns).

use std::str::FromStr;

use kabudachi_core::protocol::ids::{IdGenerator, TaskId, WorkerId};
use kabudachi_core::protocol::messages::{
    Claim, ClaimBatch, ClaimOldest, ClaimReject, ClaimRejectReason, ClaimRequest, ClaimResponse,
    claim_request, claim_response,
};
use kabudachi_core::scheduler::{self, ClaimRejection, Observer, Scheduler};
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
    /// No answer came: the request failed outright (such as a leader that
    /// cannot be dialed), the leader disconnected before answering, or
    /// nothing could be sent (this `Net` has stopped, or the leader's id
    /// names no libp2p peer), or the leader named is this worker.
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

    /// Asks `leader` for permission to run `task_id` (`REQUEST_CLAIM`), and
    /// awaits its answer: an accepted `Claim` or a `ClaimReject`. The caller
    /// names the leader, as its node knows it (`WorkerNode::known_leader`); a
    /// leader that has since lost office answers `NOT_LEADER`. See
    /// [`ClaimFailure`] for why there may be no answer.
    pub async fn request_claim(
        &self,
        leader: WorkerId,
        task_id: TaskId,
    ) -> Result<ClaimResponse, ClaimFailure> {
        let response = self
            .ask_leader(leader, claim_request::Request::TaskId(task_id.into()))
            .await?;
        self.keep_granted(&response);
        Ok(response)
    }

    /// Asks `leader` for up to `limit` of the oldest pending tasks
    /// (`CLAIM_OLDEST`), and awaits its answer: a batch of claims, oldest
    /// task first, or a `ClaimReject`. The batch may hold fewer than
    /// `limit`, or none: the leader hands out only as many as fit in one
    /// message. The caller names the leader, as for
    /// [`Self::request_claim`]. See [`ClaimFailure`] for why there may be no
    /// answer. It is the last stage of [`Self::discover`], after the tasks
    /// this worker's own records and its peers' show.
    pub async fn claim_oldest(
        &self,
        leader: WorkerId,
        limit: u32,
    ) -> Result<ClaimResponse, ClaimFailure> {
        let response = self
            .ask_leader(leader, claim_request::Request::Oldest(ClaimOldest { limit }))
            .await?;
        self.keep_granted(&response);
        Ok(response)
    }

    /// Enters every claim `response` grants in this worker's ledger of the
    /// runs it holds.
    fn keep_granted(&self, response: &ClaimResponse) {
        match &response.result {
            Some(claim_response::Result::Accept(claim)) => self.claimed.claimed(claim.clone()),
            Some(claim_response::Result::Batch(batch)) => {
                for claim in &batch.claims {
                    self.claimed.claimed(claim.clone());
                }
            }
            Some(claim_response::Result::Reject(_)) | None => {}
        }
    }

    async fn ask_leader(
        &self,
        leader: WorkerId,
        request: claim_request::Request,
    ) -> Result<ClaimResponse, ClaimFailure> {
        if leader == self.local_worker_id() {
            // This worker's own claims are its driver's to decide, from its own
            // scheduler (see `crate::executor`): nothing is sent.
            return Err(ClaimFailure::Unanswered);
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
pub(crate) fn answer<C: Clock, I: IdGenerator, R: Observer>(
    scheduler: &mut Scheduler<C, I, R>,
    claimant: &WorkerId,
    request: &claim_request::Request,
) -> ClaimResponse {
    let result = match request {
        claim_request::Request::TaskId(task_id) => scheduler
            .request_claim(claimant, &TaskId::from(task_id.clone()))
            .map(|claim| claim_response::Result::Accept(Claim::from(claim))),
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

/// The answer to a claim whose writes were not acknowledged while the leader
/// still led: a retryable `NotLeader`. The claim may still have been stored;
/// the leader elected next decides what it meant.
pub(crate) fn not_leader() -> ClaimResponse {
    ClaimResponse {
        result: Some(claim_response::Result::Reject(ClaimReject {
            reason: ClaimRejectReason::ClaimRejectNotLeader as i32,
        })),
    }
}

/// The answer to a claim from a worker the leader's roster holds neither as a
/// voter nor as a pending member: it must join the shard first.
pub(crate) fn not_member() -> ClaimResponse {
    ClaimResponse {
        result: Some(claim_response::Result::Reject(ClaimReject {
            reason: ClaimRejectReason::ClaimRejectNotMember as i32,
        })),
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
        let claim = Claim::from(claim.clone());
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
        ClaimRejection::CannotRun => ClaimRejectReason::ClaimRejectCannotRun,
    }
}
