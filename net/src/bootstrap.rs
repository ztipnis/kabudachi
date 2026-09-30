//! Bootstraps a fresh `core::election::WorkerNode` into its shard (README
//! §27 Phase 2): the worker joins the shard that already exists, or founds
//! it when nothing shows that one exists. [`bootstrap`] runs this cascade
//! in rounds, `retry_interval` apart, until a round ends it:
//!
//! 1. **Seeds.** Ask the seeds who leads the shard
//!    ([`crate::join::ask_for_leader`]). A seed that points at a
//!    leader this worker can reach ends the cascade: the worker joins that
//!    leader.
//! 2. **No authority.** With no coordination authority configured, and no
//!    seed ever having answered, found the shard alone (genesis, at recovery
//!    epoch 0).
//! 3. **Registered peers.** Read the authority's live registrations. If any
//!    worker other than this one is registered, ask those workers, at the
//!    addresses they registered, the same way as seeds. One that points at a
//!    reachable leader ends the cascade: the worker joins that leader.
//! 4. **Ownership.** If no other worker is registered, no seed or
//!    registered peer has ever answered, and the authority has warmed up,
//!    register this worker at its listen address, then try to take
//!    ownership of the shard: create its recovery epoch at 0 if it is
//!    missing, or, if it already exists and the live registrations, read
//!    again, still list no other worker (see "Re-founding a shard with no one
//!    left to ask" below), re-found it one epoch on. The authority lets only
//!    one worker win either compare-and-swap, and that worker founds the
//!    shard (genesis, at the epoch it won).
//!
//! Anything else keeps the worker in `Bootstrapping` until the next round,
//! and the reason is logged: the authority is unreachable, it has not
//! answered the round's read within a retry interval, it is still
//! warming up, registered peers are listed but none has an address that
//! parses, registered peers were asked but none answered, someone answered
//! but none pointed at a leader this worker could reach, or an ownership
//! attempt lost a race or failed. A reason is logged at its own level in the
//! round it first appears or changes, and at `debug` while it repeats
//! unchanged.
//!
//! A bootstrapping worker answers no one's join or claim request: it leads
//! nothing and knows no leader, and even "no leader known" would tell
//! another bootstrapper that a shard exists when none may yet. Each round
//! drops the requests that arrived unanswered, so the asker hears at once
//! that no answer is coming, and they do not pile up while the cascade
//! waits.
//!
//! ## Why a configured authority is never bypassed
//!
//! Two workers that each found the shard split it in two. With no authority,
//! silent seeds are the only evidence there is, so the worker trusts them.
//! With one, only winning ownership shows that no other worker has founded
//! the shard. An authority that is unreachable shows nothing, and so does
//! one still warming up, which may not yet have heard from every live
//! worker. So the worker waits rather than founding the shard.
//!
//! Once any seed or registered peer has answered, even with "no leader
//! known", the shard is known to exist, and the worker never reaches
//! ownership or genesis. It keeps asking, every round, both its seeds and
//! whichever workers the authority lists by then, until one of them points
//! at a leader it can reach: a seed that answered once may itself be stale,
//! pointing at a leader long gone, while a worker that registered since
//! knows the current one.
//!
//! ## Re-founding a shard with no one left to ask
//!
//! A worker registers itself before it tries to take ownership, and its
//! node, once `net::driver::run_driver` drives it, renews that registration
//! every third of its TTL, fencing itself before it would lapse (a TTL less
//! drift: see `core::election::AuthorityTimings`). So every worker that ever
//! created or re-founded the epoch was registered before it did, and stays
//! registered while it can still lead. Once the authority is warm and lists
//! no live registration for the shard, either the shard was never founded,
//! or every worker that ever held it is gone or has already fenced itself
//! off from leading it.
//!
//! That makes it safe to treat "epoch exists, no one registered" the same as
//! "no epoch at all": re-found the shard one epoch on, of a new lineage
//! (`compare_and_swap_recovery_epoch(Some(e), e + 1)`) rather than wait
//! forever for workers that are never coming back — the authority still lets
//! only one bootstrapper win, and any worker still holding the fence from
//! the epoch being replaced (impossible by the argument above, but the
//! authority does not need to know that) makes the new leader wait it out
//! before it can act, exactly as an ordinary recovery does (ADR-0001 decision
//! 11.4). This also resolves the ambiguous create-if-absent whose own reply
//! was lost: the epoch sits with only the worker's own registration, which
//! it does not count, and the next warm round re-founds it instead of
//! waiting on it forever.
//!
//! The live registrations are read again after the create-if-absent finds
//! an epoch, because the first read may predate a worker that registered and
//! created the epoch since. The authority is linearizable, so a read made
//! after the conflict sees the registration its winner made before winning,
//! unless that registration has lapsed (the winner is gone), and the worker
//! asks the winner instead of founding a second shard beside it.
//!
//! The cascade registers once, without renewing: a worker that waits here
//! does not keep itself listed. Two workers whose registrations each show
//! the other, with no shard founded, each ask the other and wait until the
//! registrations lapse, one TTL at most, then race again. A founder's
//! registration lapses too if its node is not driven within the TTL.
//!
//! ## How each ending reaches `Active`
//!
//! The cascade ends in a `core::election::Entry`, which
//! `WorkerNode::start` builds the node from. Joining (`Entry::Joining`)
//! records the leader the worker was pointed at and drives `Bootstrapping ->
//! Joining -> Active` as a pending member, one no quorum counts yet, which
//! learns the shard's configuration from its leader's first ack.
//!
//! Founding (`Entry::Founding`) is not a join: the node starts as the only
//! voter of the genesis configuration. Ordinary
//! timer-driven election then runs unmodified (`Active -> LeaderSuspect ->
//! RollCall -> Candidate -> LeaderReconciling -> Leader`, the roll call
//! closing at its deadline) with whatever `suspect_timeout` and
//! `roll_call_deadline` this node was configured with: the same generic path
//! every other configuration size takes, not a special near-zero timeout (that
//! was `core::single_node`'s Phase-1-only shortcut, now `bindings`-local; see
//! `bindings/src/local_node.rs`).
//!
//! Whoever founds the shard, by creating its epoch or re-founding it, draws
//! a new lineage for it (`kabudachi_core::coordination_authority::
//! RecoveryEpoch::founding`), and its node carries that lineage. A worker
//! cut off from the authority while it lost its data, and then founded
//! afresh by someone else, finds a different lineage there when it
//! reconnects, even at the same epoch number, and so rejoins the new shard
//! rather than resume beside it.
//!
//! ## Calls on the authority
//!
//! The cascade makes its calls as the driver makes its node's: each is an
//! `AuthorityCall`, performed on Tokio's blocking pool, at most one of each
//! kind in flight (see `crate::driver::PoolPerformer`). What it does with
//! each reply is [`decide_round`], a function of the replies alone. An
//! authority that does not answer the round's read within a retry interval
//! holds up no seed: the round ends, and the next asks the seeds again,
//! while the read stays in flight rather than being asked for again.
//!
//! This module is pure orchestration (which source to try, in which order)
//! over machinery `net` and `core` already have; it holds no election logic
//! of its own.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration as StdDuration;

use kabudachi_core::coordination_authority::{AuthorityError, LiveRegistrations, RecoveryEpoch};
use kabudachi_core::election::{
    AuthorityCall, AuthorityPerformer, AuthorityReply, AuthorityRequest, CallKind, Entry, Issuer,
    ReplyTokens,
};
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::protocol::messages::JoinResponse;
use kabudachi_core::time::{Clock, Instant};
use libp2p::Multiaddr;
use tokio::sync::mpsc;

use crate::driver::{PoolPerformer, SharedAuthority};
use crate::join::{LeaderSearch, ask_for_leader, ask_registered_peers};
use crate::messenger::Net;

/// How long [`bootstrap`] waits, by default, between rounds of its
/// cascade.
pub const DEFAULT_RETRY_INTERVAL: StdDuration = StdDuration::from_millis(500);

/// Runs the bootstrap cascade for `net`'s local worker, `my_id`, into
/// `shard_id` (see the module doc), and returns how the worker enters its
/// shard: joining the leader that `seeds`, or the workers `authority` lists,
/// point it at, or founding the shard alone. The caller builds the node from
/// it (`WorkerNode::start`) and drives it (see `crate::driver::run_driver`),
/// which carries out the node's first step: for a joiner, its first
/// heartbeat to the leader it joined, sent at once.
///
/// This call returns only once the worker has a shard to be in, however long
/// that takes. It keeps retrying while a configured authority is
/// unreachable, slow to answer or still warming up, and while the shard
/// exists but nothing this worker
/// can ask answers for it. Once any seed or registered peer has answered
/// without pointing at a leader this worker can reach, it asks its seeds and
/// the workers registered by then again, every round, until one does.
///
/// `authority` is `None` when no coordination authority is configured. Each
/// call on it runs on Tokio's blocking pool, as `run_driver`'s do, and one
/// that panics counts as the authority being unavailable. The cascade
/// registers the worker once, before it tries to take ownership;
/// renewing that registration is the node's, once `run_driver` drives it,
/// and a founder counts the registration from when the cascade asked for it
/// (`Entry::Founding`'s `registered_at`). `clock` must be the clock the
/// node will read.
///
/// `per_peer_timeout` bounds each attempt to connect to, and hear from, one
/// seed or registered peer. `retry_interval` is the wait between rounds of
/// the cascade. See `crate::join::DEFAULT_JOIN_PEER_TIMEOUT` and
/// [`DEFAULT_RETRY_INTERVAL`] for defaults.
///
/// A worker that joins is a pending member (see `Entry::Joining`). One that
/// founds its shard does so at recovery epoch 0, or one epoch past an
/// existing one it re-founded, of a lineage it drew (see
/// `Entry::Founding`); its node then waits out its own `suspect_timeout`
/// before it leads, like any other node (contrast `bindings`'s deliberately
/// instant self-election for its single-process runtime, documented on
/// `bindings::local_node`).
#[allow(clippy::too_many_arguments)]
pub async fn bootstrap<C: Clock>(
    net: &Net,
    clock: &C,
    authority: Option<&SharedAuthority>,
    shard_id: &ShardId,
    my_id: &WorkerId,
    seeds: &[Multiaddr],
    per_peer_timeout: StdDuration,
    retry_interval: StdDuration,
) -> Entry {
    let mut wait_log = WaitLog::new(shard_id);
    let mut calls =
        authority.map(|authority| AuthorityCalls::new(authority, net, clock, shard_id, my_id));
    // Set once any seed or registered peer answers: the shard exists, so
    // this worker must never found it.
    let mut shard_exists = false;
    loop {
        refuse_requests(net);

        // With no seeds this finds no answer at once.
        match ask_for_leader(net, seeds, per_peer_timeout).await {
            LeaderSearch::Found(pointer) => return Entry::Joining(pointer),
            LeaderSearch::NoReachableLeader => shard_exists = true,
            LeaderSearch::NoAnswer => {}
        }

        match calls.as_mut() {
            None if !shard_exists => {
                return Entry::Founding {
                    recovery_epoch: RecoveryEpoch::founding(0),
                    registered_at: None,
                };
            }
            None => {}
            Some(calls) => {
                match consult_authority(
                    net,
                    calls,
                    my_id,
                    shard_exists,
                    per_peer_timeout,
                    retry_interval,
                    &mut wait_log,
                )
                .await
                {
                    AuthorityRound::Joined(pointer) => return Entry::Joining(pointer),
                    AuthorityRound::OwnershipWon {
                        recovery_epoch,
                        registered_at,
                    } => {
                        return Entry::Founding {
                            recovery_epoch,
                            registered_at: Some(registered_at),
                        };
                    }
                    AuthorityRound::ShardExists => shard_exists = true,
                    AuthorityRound::Wait => {}
                }
            }
        }

        if shard_exists {
            wait_log.log(WaitReason::NoReachableLeader);
        }
        wait_log.end_round();
        tokio::time::sleep(retry_interval).await;
    }
}

/// Drops every join and claim request `net` holds unanswered (see this
/// module's doc): dropping one closes its stream, so the asker hears at once
/// that no answer is coming.
fn refuse_requests(net: &Net) {
    drop(net.poll_join_requests());
    drop(net.poll_claim_requests());
}

/// What a round's consultation of the authority ended in.
enum AuthorityRound {
    /// A registered peer pointed at a leader this worker reaches.
    Joined(JoinResponse),
    /// This worker won the shard's recovery epoch (created it at 0, or
    /// re-founded it one on from an existing epoch with no one left to ask —
    /// see this module's "Re-founding a shard with no one left to ask"), so
    /// it founds the shard at this epoch. It asked to be registered at
    /// `registered_at`, before it took ownership.
    OwnershipWon {
        recovery_epoch: RecoveryEpoch,
        registered_at: Instant,
    },
    /// A registered peer answered without pointing at a leader this worker
    /// reaches: the shard exists.
    ShardExists,
    /// Stay in `Bootstrapping` until the next round. The reason is logged.
    Wait,
}

/// Reads the shard's live registrations and asks the workers other than
/// this one who leads the shard. With none listed, and while no one has
/// shown that the shard exists (`shard_exists`), tries to take ownership of
/// the shard once the authority has warmed up. Each call is made through
/// `calls`, and each reply decided on by [`decide_round`].
///
/// The first read has one `retry_interval` to be answered; if it is not,
/// the round ends and the next asks the seeds again rather than waiting on
/// the authority. The read stays in flight meanwhile, and is not asked for
/// again until it is answered (see [`AuthorityCalls`]): a later round
/// decides on its reply once it comes. Once ownership is being taken, each
/// call waits for its reply however long it takes: the second read must
/// follow the conflict it checks, and an epoch this worker won must not be
/// left behind.
async fn consult_authority<C: Clock>(
    net: &Net,
    calls: &mut AuthorityCalls<'_, C>,
    my_id: &WorkerId,
    shard_exists: bool,
    per_peer_timeout: StdDuration,
    retry_interval: StdDuration,
    wait_log: &mut WaitLog,
) -> AuthorityRound {
    let mut stage = Stage::ReadingRegistrations;
    calls.ask(AuthorityRequest::ReadLiveRegistrations);
    loop {
        let within = (stage == Stage::ReadingRegistrations).then_some(retry_interval);
        let Some(reply) = calls.next_reply(within).await else {
            wait_log.log(WaitReason::AuthorityNotAnswering);
            return AuthorityRound::Wait;
        };
        match decide_round(stage, reply, my_id, shard_exists) {
            Decision::Ask { request, then } => {
                calls.ask(request);
                stage = then;
            }
            Decision::AskPeers(peers) => {
                return match ask_registered_peers(net, &peers, 0, per_peer_timeout, wait_log).await
                {
                    LeaderSearch::Found(pointer) => AuthorityRound::Joined(pointer),
                    LeaderSearch::NoReachableLeader => AuthorityRound::ShardExists,
                    LeaderSearch::NoAnswer => AuthorityRound::Wait,
                };
            }
            Decision::OwnershipWon {
                recovery_epoch,
                registered_at,
            } => {
                return AuthorityRound::OwnershipWon {
                    recovery_epoch,
                    registered_at,
                };
            }
            Decision::Wait(reason) => {
                if let Some(reason) = reason {
                    wait_log.log(reason);
                }
                return AuthorityRound::Wait;
            }
            Decision::Ignore => {}
        }
    }
}

/// Which reply a round's consultation of the authority waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    /// The round's first read of the live registrations.
    ReadingRegistrations,
    /// This worker's registration, asked for before it takes ownership.
    Registering,
    /// The create-if-absent of the shard's recovery epoch at 0. The worker
    /// asked to be registered at `registered_at`.
    Creating { registered_at: Instant },
    /// A second read of the live registrations, after the create found
    /// `epoch` already there.
    ReReading {
        registered_at: Instant,
        epoch: RecoveryEpoch,
    },
    /// The re-founding swap one epoch past the one the create found.
    ReFounding { registered_at: Instant },
}

/// What the cascade does after one reply from the authority (see
/// [`decide_round`]).
#[derive(Debug)]
pub(crate) enum Decision {
    /// Make `request`, and wait for its reply at `then`.
    Ask {
        request: AuthorityRequest,
        then: Stage,
    },
    /// Ask these registered workers, other than this one, who leads.
    AskPeers(BTreeMap<WorkerId, String>),
    /// This worker won the shard's recovery epoch (see
    /// [`AuthorityRound::OwnershipWon`]).
    OwnershipWon {
        recovery_epoch: RecoveryEpoch,
        registered_at: Instant,
    },
    /// Stay in `Bootstrapping` until the next round, logging the reason if
    /// there is one.
    Wait(Option<WaitReason>),
    /// The reply answers a call the stage did not make: it changes nothing.
    Ignore,
}

/// The cascade's decision on `reply`, the authority's answer to the call it
/// made at `stage` (see this module's doc):
///
/// - The first read lists other workers: ask them. It lists no one else:
///   wait if the shard is known to exist (`shard_exists`) or the authority
///   is still warming up, and register this worker otherwise.
/// - Registered: create the shard's recovery epoch at 0. The registration
///   counts from when it was asked for, the reply's `sent_at`.
/// - Created: ownership won. The create found an epoch already there: read
///   the live registrations again (see "Re-founding a shard with no one
///   left to ask").
/// - The second read, warm, lists no one else: re-found the shard one epoch
///   past the one found, from exactly that one, of a new lineage. Anyone
///   else listed created the epoch since the first read, and the next round
///   asks them.
/// - Re-founded: ownership won.
///
/// Any failure waits for the next round, with its reason.
pub(crate) fn decide_round(
    stage: Stage,
    reply: AuthorityReply,
    my_id: &WorkerId,
    shard_exists: bool,
) -> Decision {
    match (stage, reply) {
        (Stage::ReadingRegistrations, AuthorityReply::LiveRegistrations { result, .. }) => {
            let registrations = match result {
                Ok(registrations) => registrations,
                Err(error) => return Decision::Wait(Some(WaitReason::AuthorityUnreachable(error))),
            };
            let peers = others_listed(&registrations, my_id);
            if !peers.is_empty() {
                return Decision::AskPeers(peers);
            }
            if shard_exists {
                return Decision::Wait(None);
            }
            // Until warm-up ends, an empty list may only mean the authority
            // has not heard from the shard's workers yet.
            if registrations.authoritative_count().is_none() {
                return Decision::Wait(Some(WaitReason::AuthorityWarmingUp));
            }
            Decision::Ask {
                request: AuthorityRequest::Register,
                then: Stage::Registering,
            }
        }
        (Stage::Registering, AuthorityReply::Registered { sent_at, result, .. }) => match result {
            Ok(_) => Decision::Ask {
                request: AuthorityRequest::SwapRecoveryEpoch {
                    expected: None,
                    new: RecoveryEpoch::founding(0),
                },
                then: Stage::Creating {
                    registered_at: sent_at,
                },
            },
            Err(error) => Decision::Wait(Some(WaitReason::AuthorityUnreachable(error))),
        },
        (
            Stage::Creating { registered_at },
            AuthorityReply::RecoveryEpochSwapped { new, result, .. },
        ) => match result {
            Ok(()) => Decision::OwnershipWon {
                recovery_epoch: new,
                registered_at,
            },
            // A conflict here cannot itself report the epoch as absent: the
            // create-if-absent's own `expected` was `None`, so a live
            // conflict's `current` is always `Some`.
            Err(AuthorityError::EpochConflict {
                current: Some(epoch),
            }) => Decision::Ask {
                request: AuthorityRequest::ReadLiveRegistrations,
                then: Stage::ReReading {
                    registered_at,
                    epoch,
                },
            },
            Err(error) => Decision::Wait(Some(WaitReason::OwnershipFailed(error))),
        },
        (
            Stage::ReReading {
                registered_at,
                epoch,
            },
            AuthorityReply::LiveRegistrations { result, .. },
        ) => match result {
            // No one else is listed, and the authority is still warm: no
            // live worker holds the epoch.
            Ok(registrations)
                if registrations.authoritative_count().is_some()
                    && others_listed(&registrations, my_id).is_empty() =>
            {
                match epoch.number.checked_add(1) {
                    Some(next) => Decision::Ask {
                        request: AuthorityRequest::SwapRecoveryEpoch {
                            expected: Some(epoch),
                            new: RecoveryEpoch::founding(next),
                        },
                        then: Stage::ReFounding { registered_at },
                    },
                    // No successor epoch exists to re-found at.
                    None => Decision::Wait(Some(WaitReason::RecoveryEpochExhausted)),
                }
            }
            // Someone registered and created the epoch since the first read,
            // or the authority lost its data meanwhile: the next round asks
            // or waits.
            Ok(_) => Decision::Wait(None),
            Err(error) => Decision::Wait(Some(WaitReason::AuthorityUnreachable(error))),
        },
        (
            Stage::ReFounding { registered_at },
            AuthorityReply::RecoveryEpochSwapped { new, result, .. },
        ) => match result {
            Ok(()) => Decision::OwnershipWon {
                recovery_epoch: new,
                registered_at,
            },
            // Another worker won the epoch between the create's conflict and
            // this swap: the next round re-reads.
            Err(error) => Decision::Wait(Some(WaitReason::OwnershipFailed(error))),
        },
        _ => Decision::Ignore,
    }
}

/// The workers other than `my_id` that `registrations` lists, at the
/// addresses they registered.
fn others_listed(
    registrations: &LiveRegistrations,
    my_id: &WorkerId,
) -> BTreeMap<WorkerId, String> {
    registrations
        .addresses()
        .iter()
        .filter(|(worker_id, _)| *worker_id != my_id)
        .map(|(worker_id, address)| (worker_id.clone(), address.clone()))
        .collect()
}

/// The cascade's calls on its authority, across its rounds, made as the
/// driver makes its node's (`crate::driver::PoolPerformer`): each call is an
/// `AuthorityCall` stamped on the node's clock, performed on Tokio's
/// blocking pool, at most one of each kind in flight.
///
/// A round takes the reply to its first read before it asks for anything
/// else, and each later call's reply before the next, so once the first
/// read is answered no other call is in flight, and each reply answers the
/// call the round last made. Only the first read can be left in flight
/// from an earlier round, and a round that asks for it again takes that
/// earlier one's reply instead.
struct AuthorityCalls<'a, C: Clock> {
    authority: &'a SharedAuthority,
    net: &'a Net,
    clock: &'a C,
    shard_id: ShardId,
    my_id: &'a WorkerId,
    sender: mpsc::UnboundedSender<AuthorityReply>,
    replies: mpsc::UnboundedReceiver<AuthorityReply>,
    in_flight: BTreeSet<CallKind>,
    /// Mints the token of every call the cascade asks. Its issuer is
    /// [`Issuer::Cascade`], so no token of it equals a node's.
    tokens: ReplyTokens,
}

impl<'a, C: Clock> AuthorityCalls<'a, C> {
    fn new(
        authority: &'a SharedAuthority,
        net: &'a Net,
        clock: &'a C,
        shard_id: &ShardId,
        my_id: &'a WorkerId,
    ) -> Self {
        let (sender, replies) = mpsc::unbounded_channel();
        AuthorityCalls {
            authority,
            net,
            clock,
            shard_id: shard_id.clone(),
            my_id,
            sender,
            replies,
            in_flight: BTreeSet::new(),
            tokens: ReplyTokens::new(Issuer::Cascade),
        }
    }

    /// Asks for `request` now, unless a call of its kind is unanswered.
    fn ask(&mut self, request: AuthorityRequest) {
        let call = AuthorityCall::new(request, &mut self.tokens, self.clock.now());
        let mut performer = PoolPerformer {
            authority: Some(self.authority),
            replies: &self.sender,
            in_flight: &mut self.in_flight,
            net: self.net,
            my_id: self.my_id,
            shard_id: self.shard_id.clone(),
        };
        // With an authority, every reply comes through `replies`.
        if let Some(reply) = performer.perform(call) {
            let _ = self.sender.send(reply);
        }
    }

    /// The next reply, or `None` if none arrives `within` that long.
    async fn next_reply(&mut self, within: Option<StdDuration>) -> Option<AuthorityReply> {
        // `sender` lives as long as `replies`, so the channel never closes.
        let reply = match within {
            Some(within) => tokio::time::timeout(within, self.replies.recv())
                .await
                .ok()??,
            None => self.replies.recv().await?,
        };
        self.in_flight.remove(&reply.token().kind);
        Some(reply)
    }
}

/// Why a round of the cascade left the worker in `Bootstrapping`.
#[derive(Debug, PartialEq)]
pub(crate) enum WaitReason {
    AuthorityUnreachable(AuthorityError),
    AuthorityWarmingUp,
    /// The authority has not answered a read within a retry interval.
    AuthorityNotAnswering,
    /// The shard's recovery epoch is already at `u64::MAX`, so it has no
    /// successor epoch to re-found at.
    RecoveryEpochExhausted,
    OwnershipFailed(AuthorityError),
    UnparseableAddress {
        worker: WorkerId,
        address: String,
        error: String,
    },
    NoRegisteredAddressParses {
        peers: Vec<WorkerId>,
    },
    RegisteredPeersSilent {
        peers: Vec<WorkerId>,
    },
    /// A seed or registered peer has answered, in this round or an earlier
    /// one, but none has pointed at a leader this worker could reach.
    NoReachableLeader,
}

/// Logs the reasons each round of the cascade, or each search of a rejoin
/// (see `crate::join::find_leader`), leaves the worker in `Bootstrapping`.
/// A reason is logged at its own level in the round it first
/// appears, or changes, and at `debug` in each later round that repeats it
/// unchanged, so a worker that waits for hours does not warn every round.
pub(crate) struct WaitLog {
    shard_id: ShardId,
    previous_round: Vec<WaitReason>,
    this_round: Vec<WaitReason>,
}

impl WaitLog {
    pub(crate) fn new(shard_id: &ShardId) -> Self {
        Self {
            shard_id: shard_id.clone(),
            previous_round: Vec::new(),
            this_round: Vec::new(),
        }
    }

    /// Whether `reason` was also logged in the previous round.
    fn repeats(&self, reason: &WaitReason) -> bool {
        self.previous_round.contains(reason)
    }

    pub(crate) fn log(&mut self, reason: WaitReason) {
        let repeated = self.repeats(&reason);
        log_wait_reason(&self.shard_id, &reason, repeated);
        self.this_round.push(reason);
    }

    /// Makes this round's reasons the ones the next round is compared with.
    pub(crate) fn end_round(&mut self) {
        self.previous_round = std::mem::take(&mut self.this_round);
    }
}

/// Logs at `$level`, or at `debug` when `$repeated`. `tracing` fixes an
/// event's level where the event is written, so the choice is a branch.
macro_rules! log_at_level_or_debug {
    ($repeated:expr, $level:ident, $($fields_and_message:tt)+) => {
        if $repeated {
            tracing::debug!($($fields_and_message)+)
        } else {
            tracing::$level!($($fields_and_message)+)
        }
    };
}

fn log_wait_reason(shard_id: &ShardId, reason: &WaitReason, repeated: bool) {
    let shard = shard_id.as_str();
    match reason {
        WaitReason::AuthorityUnreachable(error) => log_at_level_or_debug!(
            repeated,
            warn,
            shard,
            %error,
            "staying in Bootstrapping: the coordination authority is unreachable"
        ),
        WaitReason::AuthorityWarmingUp => log_at_level_or_debug!(
            repeated,
            info,
            shard,
            "staying in Bootstrapping: the coordination authority is still warming up \
             and may not know every live worker yet"
        ),
        WaitReason::AuthorityNotAnswering => log_at_level_or_debug!(
            repeated,
            warn,
            shard,
            "staying in Bootstrapping: the coordination authority has not answered a read \
             of the live registrations within a retry interval; asking the seeds again \
             meanwhile"
        ),
        WaitReason::RecoveryEpochExhausted => log_at_level_or_debug!(
            repeated,
            warn,
            shard,
            "staying in Bootstrapping: the shard's recovery epoch is already at u64::MAX, \
             so it cannot be re-founded"
        ),
        WaitReason::OwnershipFailed(error) => log_at_level_or_debug!(
            repeated,
            warn,
            shard,
            %error,
            "staying in Bootstrapping: could not take ownership of the shard \
             at the coordination authority"
        ),
        WaitReason::UnparseableAddress {
            worker,
            address,
            error,
        } => log_at_level_or_debug!(
            repeated,
            warn,
            shard,
            worker = worker.as_str(),
            address = address.as_str(),
            %error,
            "skipping a registered peer whose address does not parse"
        ),
        WaitReason::NoRegisteredAddressParses { peers } => log_at_level_or_debug!(
            repeated,
            warn,
            shard,
            peers = ?peers.iter().map(WorkerId::as_str).collect::<Vec<_>>(),
            "staying in Bootstrapping: the authority lists registered peers, \
             but none has an address that parses"
        ),
        WaitReason::RegisteredPeersSilent { peers } => log_at_level_or_debug!(
            repeated,
            warn,
            shard,
            peers = ?peers.iter().map(WorkerId::as_str).collect::<Vec<_>>(),
            "staying in Bootstrapping: the authority lists registered peers, but none answered"
        ),
        WaitReason::NoReachableLeader => log_at_level_or_debug!(
            repeated,
            info,
            shard,
            "staying in Bootstrapping: the shard exists, but no one asked has pointed at a \
             leader this worker could reach; asking again"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration as StdDuration;

    use kabudachi_core::coordination_authority::{CoordinationAuthority, LiveRegistrations};
    use kabudachi_core::election::{ElectionTimings, Identity, Input, WorkerNode};
    use kabudachi_core::in_memory_authority::InMemoryAuthority;
    use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids, WorkerId};
    use kabudachi_core::protocol::messages::JoinResponse;
    use kabudachi_core::protocol::messages::election_message::Payload;
    use kabudachi_core::scheduler::Scheduler;
    use kabudachi_core::time::{Duration, RealClock};
    use kabudachi_core::election::{CallKind, ReplyToken};
    use kabudachi_testkit::FaultingAuthority;
    use libp2p::identity;
    use tokio::time::timeout;

    use super::*;
    use crate::driver::run_driver;
    use crate::swarm::build_swarm;

    const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(10);
    const RETRY_INTERVAL: StdDuration = StdDuration::from_millis(50);

    fn real_clock() -> RealClock {
        RealClock::new()
    }

    /// Short, because the authority warms up for one TTL. A registration made
    /// once it is warm still outlives the few milliseconds each test takes
    /// to read it.
    fn authority_ttl() -> Duration {
        Duration::from_millis(300)
    }

    /// `authority`, as a worker's cascade and driver share it. Clones of an
    /// `InMemoryAuthority` share its state, so the test keeps its own.
    fn shared(authority: &InMemoryAuthority<RealClock>) -> SharedAuthority {
        Arc::new(authority.clone())
    }

    /// An authority that has finished warming up, so it reports an
    /// authoritative count of live registrations.
    async fn warmed_up_authority(shard_id: &ShardId) -> InMemoryAuthority<RealClock> {
        let authority = InMemoryAuthority::new(real_clock(), authority_ttl());
        while authority
            .live_registrations(shard_id)
            .expect("the in-memory authority is always reachable")
            .authoritative_count()
            .is_none()
        {
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
        authority
    }

    /// Spawns a background task that answers every `/kabudachi/join/1`
    /// request `net` receives with `response`, duplicated from
    /// `messenger`'s own private `#[cfg(test)]` helper of the same name
    /// rather than promoted to a shared dependency for two small test files.
    fn spawn_join_responder(net: Net, response: JoinResponse) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                for handle in net.poll_join_requests() {
                    net.respond_join(handle, response.clone());
                }
                tokio::time::sleep(StdDuration::from_millis(5)).await;
            }
        })
    }

    /// Starts a JOIN responder that names itself as the shard's leader (in
    /// term 1) and returns its worker id and listen address.
    async fn spawn_self_pointing_leader() -> (WorkerId, Multiaddr) {
        let leader_net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let leader_addr = timeout(
            TEST_TIMEOUT,
            leader_net.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("the leader produced a listen address within the timeout");
        let leader = leader_net.local_worker_id();

        let response = JoinResponse {
            leader_id: Some(leader.clone().into()),
            leader_multiaddr: leader_addr.to_string(),
            term: 1,
            recovery_epoch: 0,
            recovery_epoch_lineage: 0,
        };
        spawn_join_responder(leader_net, response);
        (leader, leader_addr)
    }

    /// Like [`spawn_join_responder`], but answers "no leader known" to the
    /// first request and `response` to every later one: a seed whose shard
    /// is still electing its leader when a joiner first asks.
    fn spawn_join_responder_that_learns_its_leader(
        net: Net,
        response: JoinResponse,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut knows_its_leader = false;
            loop {
                for handle in net.poll_join_requests() {
                    if knows_its_leader {
                        net.respond_join(handle, response.clone());
                    } else {
                        net.respond_join(handle, JoinResponse::default());
                        knows_its_leader = true;
                    }
                }
                tokio::time::sleep(StdDuration::from_millis(5)).await;
            }
        })
    }

    /// A `Net` listening on a loopback port, and that address.
    async fn listening_net() -> (Net, Multiaddr) {
        let net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let addr = timeout(
            TEST_TIMEOUT,
            net.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("the net produced a listen address within the timeout");
        (net, addr)
    }

    /// Runs the cascade for a fresh worker on `net` into `shard-1` through
    /// `seeds`, with `authority` if any, and returns how it enters.
    async fn bootstrap_through(
        net: &Net,
        authority: Option<&InMemoryAuthority<RealClock>>,
        seeds: &[Multiaddr],
        per_peer_timeout: StdDuration,
    ) -> Entry {
        bootstrap(
            net,
            &real_clock(),
            authority.map(shared).as_ref(),
            &ShardId::new("shard-1"),
            &net.local_worker_id(),
            seeds,
            per_peer_timeout,
            RETRY_INTERVAL,
        )
        .await
    }

    /// The leader `entry` joins, and that leader's term. Panics unless the
    /// worker joined rather than founded.
    fn joined_leader(entry: Entry) -> (WorkerId, u64) {
        match entry {
            Entry::Joining(pointer) => {
                let leader = pointer.leader_id.clone().map(WorkerId::from);
                (leader.expect("a join names a leader"), pointer.term)
            }
            other => panic!("the node founded rather than joined: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_seed_that_knows_no_leader_yet_is_asked_again_until_it_does() {
        // With no authority, a seed that answers at all shows the shard
        // exists, so the joiner waits for its leader rather than founding a
        // second shard of its own.
        let (seed_net, seed_addr) = listening_net().await;
        let seed = seed_net.local_worker_id();
        let response = JoinResponse {
            leader_id: Some(seed.clone().into()),
            leader_multiaddr: seed_addr.to_string(),
            term: 1,
            recovery_epoch: 0,
            recovery_epoch_lineage: 0,
        };
        let _responder = spawn_join_responder_that_learns_its_leader(seed_net, response);
        let joining_net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        let entry = timeout(
            TEST_TIMEOUT,
            bootstrap_through(&joining_net, None, &[seed_addr], StdDuration::from_secs(1)),
        )
        .await
        .expect("bootstrap completed within the timeout");

        assert_eq!(joined_leader(entry), (seed, 1));
    }

    #[tokio::test]
    async fn a_worker_whose_seed_has_answered_never_founds_the_shard_even_once_the_seed_goes_quiet()
    {
        let (seed_net, seed_addr) = listening_net().await;
        // Answers the first request with "no leader known", gives the answer
        // time to leave, then drops the seed's `Net`: its listener and
        // connections close, so every later ask fails to connect.
        let _responder = tokio::spawn(async move {
            loop {
                if let Some(handle) = seed_net.poll_join_requests().pop() {
                    seed_net.respond_join(handle, JoinResponse::default());
                    tokio::time::sleep(StdDuration::from_millis(200)).await;
                    return;
                }
                tokio::time::sleep(StdDuration::from_millis(5)).await;
            }
        });
        let joining_net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        let still_bootstrapping = timeout(
            RETRY_INTERVAL * 20,
            bootstrap_through(&joining_net, None, &[seed_addr], StdDuration::from_secs(1)),
        )
        .await;

        assert!(
            still_bootstrapping.is_err(),
            "the node founded a shard after its only seed had shown one exists"
        );
    }

    // A seed that answered once, pointing at a leader long gone, must not
    // keep the worker asking only it: a worker the authority lists by now
    // knows the current leader.
    #[tokio::test]
    async fn a_stale_seed_does_not_hide_a_registered_peer_that_knows_the_leader() {
        let (stale_seed_net, stale_seed_addr) = listening_net().await;
        // Nothing listens at the gone leader's address.
        let gone_leader =
            Net::new(build_swarm(identity::Keypair::generate_ed25519())).local_worker_id();
        let stale_pointer = JoinResponse {
            leader_id: Some(gone_leader.into()),
            leader_multiaddr: "/ip4/127.0.0.1/tcp/1".to_string(),
            term: 1,
            recovery_epoch: 0,
            recovery_epoch_lineage: 0,
        };
        let _stale_seed = spawn_join_responder(stale_seed_net, stale_pointer);

        let (leader, leader_addr) = spawn_self_pointing_leader().await;
        let shard_id = ShardId::new("shard-1");
        let authority = warmed_up_authority(&shard_id).await;
        let joining_net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        // The leader registers only once the joiner has heard the stale
        // seed, so the joiner is already asking again when it appears.
        let joined = async {
            timeout(
                TEST_TIMEOUT,
                bootstrap_through(
                    &joining_net,
                    Some(&authority),
                    &[stale_seed_addr],
                    StdDuration::from_secs(1),
                ),
            )
            .await
            .expect("bootstrap completed within the timeout")
        };
        let register_leader_later = async {
            tokio::time::sleep(RETRY_INTERVAL * 4).await;
            authority
                .register(&shard_id, &leader, &leader_addr.to_string())
                .expect("the in-memory authority is always reachable");
            std::future::pending::<()>().await;
        };
        let entry = tokio::select! {
            entry = joined => entry,
            () = register_leader_later => unreachable!("never returns"),
        };

        assert_eq!(joined_leader(entry), (leader, 1));
        assert_eq!(
            authority
                .read_recovery_epoch(&shard_id)
                .map(|epoch| epoch.map(|epoch| epoch.number)),
            Ok(None),
            "a node that knows the shard exists never takes ownership of it"
        );
    }

    #[test]
    fn a_wait_reason_is_news_only_in_the_round_it_first_appears_or_changes() {
        let mut wait_log = WaitLog::new(&ShardId::new("shard-1"));
        let unreachable = || WaitReason::AuthorityUnreachable(AuthorityError::Unavailable);
        let silent = |peer: &str| WaitReason::RegisteredPeersSilent {
            peers: vec![WorkerId::new(peer)],
        };

        assert!(
            !wait_log.repeats(&unreachable()),
            "the first round's reason is news"
        );
        wait_log.log(unreachable());
        wait_log.end_round();

        assert!(
            wait_log.repeats(&unreachable()),
            "the same reason as the previous round is a repeat"
        );
        wait_log.log(unreachable());
        wait_log.end_round();

        assert!(
            !wait_log.repeats(&WaitReason::AuthorityWarmingUp),
            "a different reason is news"
        );
        wait_log.log(WaitReason::AuthorityWarmingUp);
        wait_log.end_round();

        assert!(
            !wait_log.repeats(&unreachable()),
            "a reason that comes back after a round without it is news again"
        );
        wait_log.log(silent("peer-a"));
        wait_log.end_round();

        assert!(
            !wait_log.repeats(&silent("peer-b")),
            "the same kind of reason with different details is news"
        );
    }

    #[tokio::test]
    async fn a_responding_seed_wins_over_the_authority() {
        let joining_net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (seed_worker, seed_addr) = spawn_self_pointing_leader().await;

        // A warm authority listing a different leader that also answers
        // JOINs: had the node asked the authority before its seed, it would
        // have joined that leader instead.
        let (other_leader, other_leader_addr) = spawn_self_pointing_leader().await;
        let shard_id = ShardId::new("shard-1");
        let authority = warmed_up_authority(&shard_id).await;
        authority
            .register(&shard_id, &other_leader, &other_leader_addr.to_string())
            .expect("the in-memory authority is always reachable");

        let entry = timeout(
            TEST_TIMEOUT,
            bootstrap_through(
                &joining_net,
                Some(&authority),
                &[seed_addr],
                StdDuration::from_secs(5),
            ),
        )
        .await
        .expect("bootstrap completed within the timeout");

        assert_eq!(joined_leader(entry), (seed_worker, 1));
    }

    #[tokio::test]
    async fn a_joiner_heartbeats_its_leader_at_once_rather_than_an_interval_later() {
        let leader_net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let leader_addr = timeout(
            TEST_TIMEOUT,
            leader_net.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("the leader produced a listen address within the timeout");
        let leader = leader_net.local_worker_id();
        let pointer = JoinResponse {
            leader_id: Some(leader.clone().into()),
            leader_multiaddr: leader_addr.to_string(),
            term: 1,
            recovery_epoch: 0,
            recovery_epoch_lineage: 0,
        };
        let joining_net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let joiner = joining_net.local_worker_id();
        // A heartbeat that waited for its interval would come long after
        // this test gives up.
        let timings = ElectionTimings::new(Duration::from_secs(120), Duration::from_secs(50));

        let clock = real_clock();
        let entry = timeout(
            TEST_TIMEOUT,
            bootstrap_through(
                &joining_net,
                None,
                std::slice::from_ref(&leader_addr),
                StdDuration::from_secs(5),
            ),
        );
        let joined_and_heard = async {
            let entry = entry.await.expect("bootstrap completed within the timeout");
            let identity = Identity {
                id: joiner.clone(),
                incarnation: IncarnationId::new("incarnation-1"),
                shard: ShardId::new("shard-1"),
                timings,
            };
            let (mut node, first) = WorkerNode::start(identity, entry, clock, None);
            let mut scheduler = Scheduler::new(clock, Uuid7Ids);
            let driven = run_driver(
                &mut node,
                first,
                &joining_net,
                &mut scheduler,
                clock,
                None,
                |_, _, _| {},
            );
            let heard = async {
                loop {
                    let heard = leader_net.take_inputs().into_iter().any(|input| {
                        matches!(
                            input,
                            Input::Message { from, message }
                                if from == joiner
                                    && matches!(message.payload, Some(Payload::Heartbeat(_)))
                        )
                    });
                    if heard {
                        return;
                    }
                    leader_net.wait_for_arrival().await;
                }
            };
            tokio::select! {
                _ = driven => unreachable!("run_driver never returns"),
                () = heard => {}
            }
        };
        let answer_joins = async {
            loop {
                for handle in leader_net.poll_join_requests() {
                    leader_net.respond_join(handle, pointer.clone());
                }
                tokio::time::sleep(StdDuration::from_millis(5)).await;
            }
        };

        timeout(TEST_TIMEOUT, async {
            tokio::select! {
                () = joined_and_heard => {}
                () = answer_joins => unreachable!("the join responder never returns"),
            }
        })
        .await
        .expect("the joiner's first heartbeat reached its leader within the timeout");
    }

    fn me() -> WorkerId {
        WorkerId::new("me")
    }

    /// A live-registrations reply, sent at `sent_at`, listing `workers`.
    fn listing(workers: &[&str], warm: bool, sent_at: Instant) -> AuthorityReply {
        let addresses = workers
            .iter()
            .map(|worker| (WorkerId::new(*worker), format!("/memory/{worker}")))
            .collect();
        AuthorityReply::LiveRegistrations {
            token: ReplyToken {
                issuer: Issuer::Cascade,
                kind: CallKind::ReadLiveRegistrations,
                number: 0,
            },
            sent_at,
            result: Ok(LiveRegistrations::new(addresses, warm)),
        }
    }

    /// Feeds `reply` to the cascade waiting at `decision`'s stage.
    fn then(decision: &Decision, reply: AuthorityReply, shard_exists: bool) -> Decision {
        let Decision::Ask { then: stage, .. } = *decision else {
            panic!("the cascade asked for nothing");
        };
        decide_round(stage, reply, &me(), shard_exists)
    }

    // An ownerless epoch is re-founded one on (see
    // `a_bootstrapper_re_founds_a_shard_whose_epoch_exists_with_no_live_registration`),
    // but the last epoch there is has no successor to re-found at.
    #[test]
    fn an_ownerless_epoch_at_u64_max_is_not_re_founded() {
        let conflict = decide_round(
            Stage::Creating {
                registered_at: Instant::at(2),
            },
            AuthorityReply::RecoveryEpochSwapped {
                token: ReplyToken {
                    issuer: Issuer::Cascade,
                    kind: CallKind::SwapRecoveryEpoch,
                    number: 0,
                },
                expected: None,
                new: RecoveryEpoch::founding(0),
                sent_at: Instant::at(3),
                result: Err(AuthorityError::EpochConflict {
                    current: Some(RecoveryEpoch::founding(u64::MAX)),
                }),
            },
            &me(),
            false,
        );

        let reread = then(&conflict, listing(&["me"], true, Instant::at(4)), false);

        assert!(matches!(
            reread,
            Decision::Wait(Some(WaitReason::RecoveryEpochExhausted))
        ));
    }

    /// An authority whose first read of the live registrations panics, and
    /// which otherwise is `inner`.
    struct PanicsOnFirstRead {
        inner: InMemoryAuthority<RealClock>,
        panicked: std::sync::atomic::AtomicBool,
    }

    impl CoordinationAuthority for PanicsOnFirstRead {
        fn register(&self, shard: &ShardId, worker: &WorkerId, address: &str)
        -> Result<Duration, AuthorityError> {
            self.inner.register(shard, worker, address)
        }
        fn live_registrations(&self, shard: &ShardId) -> Result<LiveRegistrations, AuthorityError> {
            if !self.panicked.swap(true, std::sync::atomic::Ordering::SeqCst) {
                panic!("the authority's first read panics");
            }
            self.inner.live_registrations(shard)
        }
        fn read_recovery_epoch(&self, shard: &ShardId)
        -> Result<Option<RecoveryEpoch>, AuthorityError> {
            self.inner.read_recovery_epoch(shard)
        }
        fn compare_and_swap_recovery_epoch(
            &self,
            shard: &ShardId,
            expected: Option<RecoveryEpoch>,
            new: RecoveryEpoch,
        ) -> Result<(), AuthorityError> {
            self.inner.compare_and_swap_recovery_epoch(shard, expected, new)
        }
        fn acquire_fence(&self, shard: &ShardId, holder: &WorkerId, epoch: RecoveryEpoch)
        -> Result<Duration, AuthorityError> {
            self.inner.acquire_fence(shard, holder, epoch)
        }
    }

    // A call that panics on the blocking pool is answered as unavailable, so
    // its kind is not left in flight for ever: the next round reads again,
    // and the worker founds its ownerless shard. The driver performs its
    // node's calls through the same performer.
    #[tokio::test]
    async fn an_authority_call_that_panics_is_retried_as_unavailable() {
        let shard_id = ShardId::new("shard-1");
        let authority: SharedAuthority = Arc::new(PanicsOnFirstRead {
            inner: warmed_up_authority(&shard_id).await,
            panicked: std::sync::atomic::AtomicBool::new(false),
        });
        let net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        let entry = timeout(
            TEST_TIMEOUT,
            bootstrap(
                &net,
                &real_clock(),
                Some(&authority),
                &shard_id,
                &net.local_worker_id(),
                &[],
                StdDuration::from_secs(1),
                RETRY_INTERVAL,
            ),
        )
        .await
        .expect("the worker founded the shard within the timeout");

        assert!(matches!(
            entry,
            Entry::Founding { recovery_epoch, .. } if recovery_epoch.number == 0
        ));
    }

    // Review focus 5: an authority whose read hangs holds one blocking
    // thread, not one more every round, and holds up no seed. The cascade
    // keeps asking its seed each round; were a duplicate read made, it
    // would not be held and would find the shard ownerless, so the worker
    // would found it while the first read is still held.
    #[tokio::test]
    async fn the_cascade_drops_a_duplicate_in_flight_call() {
        let shard_id = ShardId::new("shard-1");
        let authority = FaultingAuthority::new(real_clock(), authority_ttl());
        while authority
            .live_registrations(&shard_id)
            .expect("the authority is reachable")
            .authoritative_count()
            .is_none()
        {
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
        authority.hold_next(CallKind::ReadLiveRegistrations);
        // A seed that is up but answers no one: each round asks it once.
        let (seed_net, seed_addr) = listening_net().await;
        let net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (clock, me, seeds) = (real_clock(), net.local_worker_id(), [seed_addr]);
        let shared: SharedAuthority = Arc::new(authority.clone());

        let mut bootstrapping = std::pin::pin!(bootstrap(
            &net,
            &clock,
            Some(&shared),
            &shard_id,
            &me,
            &seeds,
            StdDuration::from_secs(1),
            RETRY_INTERVAL,
        ));
        let asked_while_held = timeout(TEST_TIMEOUT, async {
            tokio::select! {
                entry = &mut bootstrapping => Err(entry),
                asked = async {
                    let mut asked = 0;
                    // The third ask follows a whole round that ran while
                    // the first read was held.
                    while asked < 3 {
                        asked += seed_net.poll_join_requests().len();
                        tokio::time::sleep(StdDuration::from_millis(5)).await;
                    }
                    Ok(authority.is_holding(CallKind::ReadLiveRegistrations))
                } => asked,
            }
        })
        .await;
        // Released before anything can fail, so no path leaves the held
        // thread parked and hangs the runtime's shutdown.
        authority.release(CallKind::ReadLiveRegistrations);

        match asked_while_held.expect("the seed was asked three times within the timeout") {
            Ok(held) => assert!(held, "the first read was answered before the third round"),
            Err(entry) => panic!("the worker entered while its first read was held: {entry:?}"),
        }
        // The held read, answered at last, is the cascade's: it takes
        // ownership of the ownerless shard.
        let entry = timeout(TEST_TIMEOUT, bootstrapping)
            .await
            .expect("the worker founded the shard within the timeout");
        assert!(matches!(
            entry,
            Entry::Founding { recovery_epoch, .. } if recovery_epoch.number == 0
        ));
    }
}
