//! Bootstraps a fresh `core::election::WorkerNode` into its shard (README
//! §27 Phase 2): the worker joins the shard that already exists, or founds
//! it when nothing shows that one exists. [`bootstrap_node`] runs this cascade
//! in rounds, `retry_interval` apart, until a round ends it:
//!
//! 1. **Seeds.** Ask the seeds who leads the shard
//!    ([`crate::messenger::Net::ask_for_leader`]). A seed that points at a
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
//! and the reason is logged: the authority is unreachable, it is still
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
//! Joining is `core::election::WorkerNode::finish_joining(pointer)`: it
//! records the leader the worker was pointed at and drives `Bootstrapping ->
//! Joining -> Active` as a pending member, one no quorum counts yet, which
//! learns the shard's configuration from its leader's first ack.
//!
//! Genesis is not a join: the node starts as the only voter of the genesis
//! configuration, which is exactly `WorkerNode::genesis`. Ordinary
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
//! This module is pure orchestration (which source to try, in which order)
//! over machinery `net` and `core` already have; it holds no election logic
//! of its own.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use kabudachi_core::coordination_authority::{
    AuthorityError, CoordinationAuthority, RecoveryEpoch,
};
use kabudachi_core::election::{AuthorityTimings, ElectionTimings, Output, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::JoinResponse;
use kabudachi_core::time::{Clock, Instant};
use libp2p::Multiaddr;

use crate::driver::SharedAuthority;
use crate::messenger::{LeaderSearch, Net};

/// How long [`bootstrap_node`] waits, by default, between rounds of its
/// cascade.
pub const DEFAULT_RETRY_INTERVAL: StdDuration = StdDuration::from_millis(500);

/// Bootstraps a fresh `WorkerNode` for `net`'s local worker into `shard_id`
/// (see the module doc for the cascade): it joins the leader that `seeds`,
/// or the workers `authority` lists, point it at, or founds the shard alone.
/// Returns a node already `Active`, connected to no one yet: its driver
/// feeds it the connection events `net` has queued (see
/// `crate::driver::run_driver`). A node that joined has already heartbeated
/// the leader it joined.
///
/// This call returns only once the worker has a shard to be in, however long
/// that takes. It keeps retrying while a configured authority is unreachable
/// or still warming up, and while the shard exists but nothing this worker
/// can ask answers for it. Once any seed or registered peer has answered
/// without pointing at a leader this worker can reach, it asks its seeds and
/// the workers registered by then again, every round, until one does.
///
/// `authority` is `None` when no coordination authority is configured, or
/// `Some((authority, timings))` — the authority to consult and the timings
/// the returned node keeps its own registration and, once it leads, its
/// recovery fence by (see `core::election::AuthorityTimings`). Each call on
/// the authority runs on Tokio's blocking pool, as `run_driver`'s do. The
/// cascade registers the worker once, before it tries to take ownership;
/// renewing that registration is the node's, once `net::driver::run_driver`
/// drives it, and a founder counts the registration from when the cascade
/// asked for it.
///
/// `per_peer_timeout` bounds each attempt to connect to, and hear from, one
/// seed or registered peer. `retry_interval` is the wait between rounds of
/// the cascade. See `crate::messenger::DEFAULT_JOIN_PEER_TIMEOUT` and
/// [`DEFAULT_RETRY_INTERVAL`] for defaults.
/// `timings` are the node's ordinary election timings (see `WorkerNode::new`'s
/// doc), used as-is by every path, including genesis: a node that founds its
/// shard alone still waits out its `suspect_timeout` like any other node
/// before leading (contrast `bindings`'s deliberately instant self-election
/// for its single-process runtime, documented on `bindings::local_node`).
///
/// A node that joins is a pending member: it has no admission generation, so
/// no quorum counts it, and the configuration of the members that answered
/// it is unchanged. The next election whose roll call it answers admits it;
/// until then the shard keeps the configuration it has. A node that founds
/// its shard does so at recovery epoch 0, or one epoch past an existing one
/// it re-founded.
#[allow(clippy::too_many_arguments)]
pub async fn bootstrap_node<C>(
    my_id: WorkerId,
    incarnation_id: IncarnationId,
    shard_id: ShardId,
    clock: C,
    net: &Net,
    authority: Option<(SharedAuthority, AuthorityTimings)>,
    timings: ElectionTimings,
    seeds: &[Multiaddr],
    per_peer_timeout: StdDuration,
    retry_interval: StdDuration,
) -> WorkerNode<C>
where
    C: Clock,
{
    let entry = run_cascade(
        net,
        &clock,
        authority.as_ref().map(|(authority, _)| authority),
        &shard_id,
        &my_id,
        seeds,
        per_peer_timeout,
        retry_interval,
    )
    .await;
    let authority_timings = authority.map(|(_, timings)| timings);

    match entry {
        Entry::Join(pointer) => {
            let mut node = WorkerNode::bootstrapping(
                my_id,
                incarnation_id,
                shard_id,
                clock,
                authority_timings,
                timings,
            );
            // Joining moves the node to `Active` as a pending member, which
            // is not leading, so the step reports no grant. It does heartbeat
            // the leader at once, and that heartbeat goes out now rather than
            // an interval later. Joining publishes nothing: `Net` is not yet
            // subscribed to the shard (`run_driver` subscribes it), so a
            // publish here would be lost. Its state changes need no action
            // (callers read the node's state from the node), nor does its
            // deadline (`run_driver` ticks the node at once to learn it).
            let joined = node.finish_joining(&pointer);
            for output in joined.outputs {
                debug_assert!(
                    !matches!(output, Output::Publish { .. }),
                    "joining publishes nothing"
                );
                if let Output::Send { to, message } = output {
                    net.send(to, message);
                }
            }
            node
        }
        Entry::Genesis {
            recovery_epoch,
            registered_at,
        } => {
            let node = WorkerNode::genesis(
                my_id,
                incarnation_id,
                shard_id,
                clock,
                recovery_epoch.number,
                authority_timings,
                timings,
            )
            .with_recovery_lineage(recovery_epoch.lineage);
            match registered_at {
                Some(sent_at) => node.registered_at(sent_at),
                None => node,
            }
        }
    }
}

/// How the cascade ends: the way this worker enters its shard.
enum Entry {
    /// Join the leader this pointer names, as a pending member.
    Join(JoinResponse),
    /// Found a new shard, with this worker as its only member, at this
    /// recovery epoch, of a lineage this worker drew (see
    /// `kabudachi_core::coordination_authority::RecoveryEpoch`): numbered 0
    /// for a shard that never existed, or one more than an existing epoch
    /// this worker re-founded (see this module's "Re-founding a shard with
    /// no one left to ask"). `registered_at` is when the cascade asked the
    /// authority to register this worker, before it took ownership; `None`
    /// with no authority.
    Genesis {
        recovery_epoch: RecoveryEpoch,
        registered_at: Option<Instant>,
    },
}

/// Runs rounds of the cascade, `retry_interval` apart, until one ends it.
#[allow(clippy::too_many_arguments)]
async fn run_cascade<C: Clock>(
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
    // Set once any seed or registered peer answers: the shard exists, so
    // this worker must never found it.
    let mut shard_exists = false;
    loop {
        refuse_requests(net);

        // With no seeds this finds no answer at once.
        match net.ask_for_leader(seeds, per_peer_timeout).await {
            LeaderSearch::Found(pointer) => return Entry::Join(pointer),
            LeaderSearch::NoReachableLeader => shard_exists = true,
            LeaderSearch::NoAnswer => {}
        }

        match authority {
            None if !shard_exists => {
                return Entry::Genesis {
                    recovery_epoch: RecoveryEpoch::founding(0),
                    registered_at: None,
                };
            }
            None => {}
            Some(authority) => {
                match consult_authority(
                    net,
                    authority,
                    clock,
                    shard_id,
                    my_id,
                    shard_exists,
                    per_peer_timeout,
                    &mut wait_log,
                )
                .await
                {
                    AuthorityRound::Joined(pointer) => return Entry::Join(pointer),
                    AuthorityRound::OwnershipWon {
                        recovery_epoch,
                        registered_at,
                    } => {
                        return Entry::Genesis {
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
/// the shard once the authority has warmed up (see [`take_ownership`]).
#[allow(clippy::too_many_arguments)]
async fn consult_authority<C: Clock>(
    net: &Net,
    authority: &SharedAuthority,
    clock: &C,
    shard_id: &ShardId,
    my_id: &WorkerId,
    shard_exists: bool,
    per_peer_timeout: StdDuration,
    wait_log: &mut WaitLog,
) -> AuthorityRound {
    let registered = match others_registered(authority, shard_id, my_id).await {
        Ok(registered) => registered,
        Err(error) => {
            wait_log.log(WaitReason::AuthorityUnreachable(error));
            return AuthorityRound::Wait;
        }
    };

    if !registered.peers.is_empty() {
        return match ask_registered_peers(net, &registered.peers, wait_log, per_peer_timeout).await
        {
            LeaderSearch::Found(pointer) => AuthorityRound::Joined(pointer),
            LeaderSearch::NoReachableLeader => AuthorityRound::ShardExists,
            LeaderSearch::NoAnswer => AuthorityRound::Wait,
        };
    }
    if shard_exists {
        return AuthorityRound::Wait;
    }
    // Until warm-up ends, an empty list may only mean the authority has not
    // heard from the shard's workers yet.
    if !registered.warm {
        wait_log.log(WaitReason::AuthorityWarmingUp);
        return AuthorityRound::Wait;
    }
    let address = net
        .local_multiaddr()
        .map(|address| address.to_string())
        .unwrap_or_default();
    take_ownership(authority, clock, shard_id, my_id, address, wait_log).await
}

/// Makes one call on `authority` on Tokio's blocking pool, as an authority
/// is typically a remote service whose calls block, and returns its answer.
async fn call<T: Send + 'static>(
    authority: &SharedAuthority,
    call: impl FnOnce(&dyn CoordinationAuthority) -> T + Send + 'static,
) -> T {
    let authority = Arc::clone(authority);
    match tokio::task::spawn_blocking(move || call(&*authority)).await {
        Ok(answer) => answer,
        Err(error) => std::panic::resume_unwind(error.into_panic()),
    }
}

/// The workers other than this one that the authority lists for the shard,
/// and whether it has warmed up (so a list with no one else on it means no
/// one else is live).
struct OthersRegistered {
    peers: BTreeMap<WorkerId, String>,
    warm: bool,
}

async fn others_registered(
    authority: &SharedAuthority,
    shard_id: &ShardId,
    my_id: &WorkerId,
) -> Result<OthersRegistered, AuthorityError> {
    let shard = shard_id.clone();
    let registrations = call(authority, move |authority| {
        authority.live_registrations(&shard)
    })
    .await?;
    let peers = registrations
        .addresses()
        .iter()
        .filter(|(worker_id, _)| *worker_id != my_id)
        .map(|(worker_id, address)| (worker_id.clone(), address.clone()))
        .collect();
    Ok(OthersRegistered {
        peers,
        warm: registrations.authoritative_count().is_some(),
    })
}

/// Registers this worker at `address` and tries to take ownership of the
/// shard: create its recovery epoch at 0 if it is missing, or, if the
/// create-if-absent conflicts with an epoch that already exists and a second
/// read still lists no other worker, re-found it one epoch on (see this
/// module's "Re-founding a shard with no one left to ask"). The authority
/// lets at most one worker win either compare-and-swap, so at most one
/// worker founds the shard this round. Called only once a warm read listed
/// no other worker.
async fn take_ownership<C: Clock>(
    authority: &SharedAuthority,
    clock: &C,
    shard_id: &ShardId,
    my_id: &WorkerId,
    address: String,
    wait_log: &mut WaitLog,
) -> AuthorityRound {
    // Registered before any swap, so a worker that loses the swap to this
    // one sees it (see this module's "Re-founding a shard with no one left
    // to ask").
    let registered_at = clock.now();
    let (shard, me) = (shard_id.clone(), my_id.clone());
    let registered = call(authority, move |authority| {
        authority.register(&shard, &me, &address)
    })
    .await;
    if let Err(error) = registered {
        wait_log.log(WaitReason::AuthorityUnreachable(error));
        return AuthorityRound::Wait;
    }

    // A conflict here cannot itself report the epoch as absent: the
    // create-if-absent's own `expected` was already `None`, so a live
    // conflict's `current` is always `Some` (had the epoch really been
    // absent, this swap would have succeeded instead). Any other failure
    // falls through to the same wait-and-retry as the re-founding attempt's
    // own.
    let founded = RecoveryEpoch::founding(0);
    let shard = shard_id.clone();
    let created = call(authority, move |authority| {
        authority.compare_and_swap_recovery_epoch(&shard, None, founded)
    })
    .await;
    match created {
        Ok(()) => AuthorityRound::OwnershipWon {
            recovery_epoch: founded,
            registered_at,
        },
        Err(AuthorityError::EpochConflict {
            current: Some(epoch),
        }) => match others_registered(authority, shard_id, my_id).await {
            // No one else is listed, and the authority is still warm: no
            // live worker holds the epoch.
            Ok(OthersRegistered { peers, warm: true }) if peers.is_empty() => {
                try_refound(authority, shard_id, epoch, registered_at, wait_log).await
            }
            // Someone registered and created the epoch since the first read,
            // or the authority lost its data meanwhile: the next round asks
            // or waits.
            Ok(_) => AuthorityRound::Wait,
            Err(error) => {
                wait_log.log(WaitReason::AuthorityUnreachable(error));
                AuthorityRound::Wait
            }
        },
        Err(error) => {
            wait_log.log(WaitReason::OwnershipFailed(error));
            AuthorityRound::Wait
        }
    }
}

/// Tries to re-found the shard one epoch past `current` (see this module's
/// "Re-founding a shard with no one left to ask"), of a new lineage: no
/// worker of the old one is left. `current` is what the create-if-absent
/// attempt just found in place of the missing epoch it expected, so this
/// swaps from exactly that value: a further conflict means another worker
/// won the epoch (this round's or a newer one) between that read and this
/// swap, so this worker waits and the next round re-reads.
async fn try_refound(
    authority: &SharedAuthority,
    shard_id: &ShardId,
    current: RecoveryEpoch,
    registered_at: Instant,
    wait_log: &mut WaitLog,
) -> AuthorityRound {
    let Some(next) = current.number.checked_add(1).map(RecoveryEpoch::founding) else {
        // No successor epoch exists to re-found at. This shard can never be
        // re-founded again; nothing to do but wait (and say so).
        wait_log.log(WaitReason::RecoveryEpochExhausted);
        return AuthorityRound::Wait;
    };
    let shard = shard_id.clone();
    let refounded = call(authority, move |authority| {
        authority.compare_and_swap_recovery_epoch(&shard, Some(current), next)
    })
    .await;
    match refounded {
        Ok(()) => AuthorityRound::OwnershipWon {
            recovery_epoch: next,
            registered_at,
        },
        Err(error) => {
            wait_log.log(WaitReason::OwnershipFailed(error));
            AuthorityRound::Wait
        }
    }
}

/// Asks `peers`, at their registered addresses, who leads the shard, the way
/// seeds are asked ([`Net::ask_for_leader`]). An address that does not parse
/// is skipped.
async fn ask_registered_peers(
    net: &Net,
    peers: &BTreeMap<WorkerId, String>,
    wait_log: &mut WaitLog,
    per_peer_timeout: StdDuration,
) -> LeaderSearch {
    let mut addresses: Vec<Multiaddr> = Vec::new();
    for (worker, address) in peers {
        match address.parse() {
            Ok(address) => addresses.push(address),
            Err(error) => wait_log.log(WaitReason::UnparseableAddress {
                worker: worker.clone(),
                address: address.clone(),
                error: error.to_string(),
            }),
        }
    }
    let peer_ids: Vec<WorkerId> = peers.keys().cloned().collect();
    if addresses.is_empty() {
        wait_log.log(WaitReason::NoRegisteredAddressParses { peers: peer_ids });
        return LeaderSearch::NoAnswer;
    }

    let search = net.ask_for_leader(&addresses, per_peer_timeout).await;
    if search == LeaderSearch::NoAnswer {
        wait_log.log(WaitReason::RegisteredPeersSilent { peers: peer_ids });
    }
    search
}

/// Why a round of the cascade left the worker in `Bootstrapping`.
#[derive(PartialEq)]
enum WaitReason {
    AuthorityUnreachable(AuthorityError),
    AuthorityWarmingUp,
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

/// Logs the reasons each round of the cascade leaves the worker in
/// `Bootstrapping`. A reason is logged at its own level in the round it first
/// appears, or changes, and at `debug` in each later round that repeats it
/// unchanged, so a worker that waits for hours does not warn every round.
struct WaitLog {
    shard_id: ShardId,
    previous_round: Vec<WaitReason>,
    this_round: Vec<WaitReason>,
}

impl WaitLog {
    fn new(shard_id: &ShardId) -> Self {
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

    fn log(&mut self, reason: WaitReason) {
        let repeated = self.repeats(&reason);
        log_wait_reason(&self.shard_id, &reason, repeated);
        self.this_round.push(reason);
    }

    /// Makes this round's reasons the ones the next round is compared with.
    fn end_round(&mut self) {
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
    use std::time::Duration as StdDuration;

    use kabudachi_core::election::Input;
    use kabudachi_core::in_memory_authority::InMemoryAuthority;
    use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
    use kabudachi_core::protocol::messages::JoinResponse;
    use kabudachi_core::protocol::messages::election_message::Payload;
    use kabudachi_core::protocol::worker_state::WorkerState;
    use kabudachi_core::time::{Duration, RealClock};
    use libp2p::identity;
    use tokio::time::timeout;

    use super::*;
    use crate::swarm::build_swarm;

    const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(10);
    const RETRY_INTERVAL: StdDuration = StdDuration::from_millis(50);

    fn real_clock() -> RealClock {
        RealClock::new()
    }

    fn timings() -> ElectionTimings {
        ElectionTimings::new(Duration::from_millis(300), Duration::from_millis(50))
            .with_roll_call_deadline(Duration::from_millis(100))
    }

    /// Short, because the authority warms up for one TTL. A registration made
    /// once it is warm still outlives the few milliseconds each test takes
    /// to read it.
    fn authority_ttl() -> Duration {
        Duration::from_millis(300)
    }

    /// The timings a node built in these tests keeps its own registration
    /// by, matching [`authority_ttl`].
    fn authority_timings() -> AuthorityTimings {
        AuthorityTimings {
            ttl: authority_ttl(),
        }
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

    /// Bootstraps a fresh worker on `net` into `shard-1` through `seeds`,
    /// with `authority` if any.
    async fn bootstrap(
        net: &Net,
        authority: Option<&InMemoryAuthority<RealClock>>,
        seeds: &[Multiaddr],
    ) -> WorkerNode<RealClock> {
        bootstrap_node(
            net.local_worker_id(),
            IncarnationId::new("incarnation-1"),
            ShardId::new("shard-1"),
            real_clock(),
            net,
            authority.map(|authority| (shared(authority), authority_timings())),
            timings(),
            seeds,
            StdDuration::from_secs(1),
            RETRY_INTERVAL,
        )
        .await
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

        let node = timeout(TEST_TIMEOUT, bootstrap(&joining_net, None, &[seed_addr]))
            .await
            .expect("bootstrap_node completed within the timeout");

        assert!(
            node.is_pending_member(),
            "the node joined rather than founded"
        );
        assert_eq!(node.known_leader(), Some((seed, 1)));
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
            bootstrap(&joining_net, None, &[seed_addr]),
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
                bootstrap(&joining_net, Some(&authority), &[stale_seed_addr]),
            )
            .await
            .expect("bootstrap_node completed within the timeout")
        };
        let register_leader_later = async {
            tokio::time::sleep(RETRY_INTERVAL * 4).await;
            authority
                .register(&shard_id, &leader, &leader_addr.to_string())
                .expect("the in-memory authority is always reachable");
            std::future::pending::<()>().await;
        };
        let node = tokio::select! {
            node = joined => node,
            () = register_leader_later => unreachable!("never returns"),
        };

        assert!(
            node.is_pending_member(),
            "the node joined rather than founded"
        );
        assert_eq!(node.known_leader(), Some((leader, 1)));
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

        let my_id = joining_net.local_worker_id();
        let node = timeout(
            TEST_TIMEOUT,
            bootstrap_node(
                my_id.clone(),
                IncarnationId::new("incarnation-1"),
                shard_id,
                real_clock(),
                &joining_net,
                Some((shared(&authority), authority_timings())),
                timings(),
                &[seed_addr],
                StdDuration::from_secs(5),
                RETRY_INTERVAL,
            ),
        )
        .await
        .expect("bootstrap_node completed within the timeout");

        assert_eq!(node.state(), WorkerState::Active);
        assert!(
            node.is_pending_member(),
            "a node that joined through a seed waits to be admitted before it votes"
        );
        assert_eq!(node.known_leader(), Some((seed_worker, 1)));
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

        let joined_and_heard = async {
            bootstrap_node(
                joiner.clone(),
                IncarnationId::new("incarnation-1"),
                ShardId::new("shard-1"),
                real_clock(),
                &joining_net,
                None,
                timings,
                std::slice::from_ref(&leader_addr),
                StdDuration::from_secs(5),
                RETRY_INTERVAL,
            )
            .await;
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

    #[tokio::test]
    async fn with_no_seeds_the_node_joins_through_a_registered_peers_address() {
        let joining_net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (leader, leader_addr) = spawn_self_pointing_leader().await;

        let shard_id = ShardId::new("shard-1");
        let authority = warmed_up_authority(&shard_id).await;
        authority
            .register(&shard_id, &leader, &leader_addr.to_string())
            .expect("the in-memory authority is always reachable");

        let my_id = joining_net.local_worker_id();
        let node = timeout(
            TEST_TIMEOUT,
            bootstrap_node(
                my_id.clone(),
                IncarnationId::new("incarnation-1"),
                shard_id.clone(),
                real_clock(),
                &joining_net,
                Some((shared(&authority), authority_timings())),
                timings(),
                &[],
                StdDuration::from_secs(5),
                RETRY_INTERVAL,
            ),
        )
        .await
        .expect("bootstrap_node completed within the timeout");

        assert_eq!(node.state(), WorkerState::Active);
        assert!(
            node.is_pending_member(),
            "a node that joined through a registered peer waits to be admitted before it votes"
        );
        assert_eq!(node.known_leader(), Some((leader, 1)));
        assert_eq!(
            authority
                .read_recovery_epoch(&shard_id)
                .map(|epoch| epoch.map(|epoch| epoch.number)),
            Ok(None),
            "a node that joins an existing shard never takes ownership of it"
        );
    }
}
