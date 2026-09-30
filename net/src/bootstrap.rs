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
//! kind in flight (see `crate::authority::AuthorityClient`). What it does with
//! each reply is [`decide_round`], a function of the replies alone. An
//! authority that does not answer the round's read within a retry interval
//! holds up no seed: the round ends, and the next asks the seeds again,
//! while the read stays in flight rather than being asked for again.
//!
//! This module is pure orchestration (which source to try, in which order)
//! over machinery `net` and `core` already have; it holds no election logic
//! of its own.

use std::collections::BTreeMap;
use std::time::Duration as StdDuration;

use kabudachi_core::coordination_authority::{
    AuthorityError, LineageSource, RecoveryEpoch, Uuid7Lineages,
};
use kabudachi_core::election::{AuthorityReply, AuthorityRequest, Entry};
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::protocol::messages::JoinResponse;
use kabudachi_core::time::{Clock, Instant};
use libp2p::Multiaddr;

use crate::authority::AuthorityClient;
use crate::leader_search::{AskWhoLeads, JoinOverNet, SearchRounds, others_listed};
use crate::messenger::Net;
use crate::wait_log::WaitReason;

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
    authority: Option<&mut AuthorityClient>,
    shard_id: &ShardId,
    my_id: &WorkerId,
    seeds: &[Multiaddr],
    per_peer_timeout: StdDuration,
    retry_interval: StdDuration,
) -> Entry {
    cascade(
        net,
        &mut JoinOverNet {
            net,
            per_peer_timeout,
        },
        &mut Uuid7Lineages,
        clock,
        authority,
        shard_id,
        my_id,
        seeds,
        retry_interval,
    )
    .await
}

/// [`bootstrap`] over any [`AskWhoLeads`] port: the port asks the seeds and
/// the registered peers, and `net` is only where unanswered join and claim
/// requests are dropped.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn cascade<C: Clock, P: AskWhoLeads>(
    net: &Net,
    port: &mut P,
    lineages: &mut impl LineageSource,
    clock: &C,
    mut authority: Option<&mut AuthorityClient>,
    shard_id: &ShardId,
    my_id: &WorkerId,
    seeds: &[Multiaddr],
    retry_interval: StdDuration,
) -> Entry {
    let mut search = SearchRounds::for_bootstrap(shard_id, my_id.clone(), seeds.to_vec());
    loop {
        refuse_requests(net);

        // With no seeds this finds no answer at once.
        let found = port.ask(search.seeds()).await;
        if let Some(pointer) = search.heard_from_seeds(found) {
            return Entry::Joining(pointer);
        }

        match authority.as_deref_mut() {
            None if !search.shard_exists() => {
                return Entry::Founding {
                    recovery_epoch: RecoveryEpoch::founding(0, lineages),
                    registered_at: None,
                };
            }
            None => {}
            Some(calls) => {
                match consult_authority(
                    calls,
                    port,
                    lineages,
                    &mut search,
                    clock,
                    my_id,
                    retry_interval,
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
                    AuthorityRound::Wait => {}
                }
            }
        }

        search.end_round();
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
/// again until it is answered (see [`AuthorityClient`]): a later round
/// decides on its reply once it comes. Once ownership is being taken, each
/// call waits for its reply however long it takes: the second read must
/// follow the conflict it checks, and an epoch this worker won must not be
/// left behind.
async fn consult_authority<C: Clock, P: AskWhoLeads>(
    calls: &mut AuthorityClient,
    port: &mut P,
    lineages: &mut impl LineageSource,
    search: &mut SearchRounds,
    clock: &C,
    my_id: &WorkerId,
    retry_interval: StdDuration,
) -> AuthorityRound {
    let mut stage = Stage::ReadingRegistrations;
    calls.ask(AuthorityRequest::ReadLiveRegistrations, clock.now());
    loop {
        let within = (stage == Stage::ReadingRegistrations).then_some(retry_interval);
        let Some(reply) = calls.next_reply(within).await else {
            search.log(WaitReason::AuthorityNotAnswering);
            return AuthorityRound::Wait;
        };
        match decide_round(stage, reply, my_id, search.shard_exists(), lineages) {
            Decision::Ask { request, then } => {
                calls.ask(request, clock.now());
                stage = then;
            }
            Decision::AskPeers(peers) => {
                let addresses = search.to_ask(&peers);
                let found = port.ask(&addresses).await;
                return match search.heard_from_listed(found) {
                    Some(pointer) => AuthorityRound::Joined(pointer),
                    None => AuthorityRound::Wait,
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
                    search.log(reason);
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
    lineages: &mut impl LineageSource,
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
                    new: RecoveryEpoch::founding(0, lineages),
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
                            new: RecoveryEpoch::founding(next, lineages),
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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::time::Duration as StdDuration;

    use kabudachi_core::coordination_authority::{CoordinationAuthority, LiveRegistrations};
    use kabudachi_core::election::{CallKind, Issuer, ReplyToken};
    use kabudachi_core::protocol::ids::{ShardId, WorkerId};
    use kabudachi_testkit::FaultingAuthority;
    
    use tokio::time::timeout;

    use super::*;
    use crate::test_support::{
        Answer, Scripted, TEST_TIMEOUT, TokioClock, address, epoch_number, pointer_to, register,
        wait_until_held, warm_authority,
    };

    const RETRY_INTERVAL: StdDuration = StdDuration::from_millis(50);

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

    // The in-process cascade tests run over `Scripted` seeds and peers and a
    // `FaultingAuthority` on tokio's paused clock, so twenty rounds cost
    // nothing. A call held on the blocking pool stops the clock's
    // auto-advance until it is released, so tests that hold one move time by
    // hand.

    /// A cascade for a fresh worker over `port`, on `authority` if any.
    struct InProcess {
        net: Net,
        me: WorkerId,
        shard_id: ShardId,
        clock: TokioClock,
        port: Scripted,
        lineages: Uuid7Lineages,
        client: Option<AuthorityClient>,
        seeds: Vec<Multiaddr>,
    }

    impl InProcess {
        fn new(
            port: &Scripted,
            authority: Option<&FaultingAuthority<TokioClock>>,
            clock: TokioClock,
            seeds: &[Multiaddr],
        ) -> Self {
            // Never listens, so no socket opens.
            let net = Net::new();
            let shard_id = ShardId::new("shard-1");
            let client = authority.map(|authority| {
                AuthorityClient::new(&net, shard_id.clone(), Arc::new(authority.clone()))
            });
            InProcess {
                me: net.local_worker_id(),
                net,
                shard_id,
                clock,
                port: port.clone(),
                lineages: Uuid7Lineages,
                client,
                seeds: seeds.to_vec(),
            }
        }

        fn run(&mut self) -> impl Future<Output = Entry> + '_ {
            cascade(
                &self.net,
                &mut self.port,
                &mut self.lineages,
                &self.clock,
                self.client.as_mut(),
                &self.shard_id,
                &self.me,
                &self.seeds,
                RETRY_INTERVAL,
            )
        }
    }

    fn founded_at_epoch_0(entry: &Entry) -> bool {
        matches!(entry, Entry::Founding { recovery_epoch, .. } if recovery_epoch.number == 0)
    }

    #[tokio::test(start_paused = true)]
    async fn a_seed_that_points_at_a_leader_wins_over_the_authority() {
        let (authority, clock) = warm_authority(StdDuration::from_secs(5)).await;
        let (seed, listed) = (address(1), address(2));
        register(&authority, "other", &listed.to_string());
        let port = Scripted::default();
        port.script(&seed, [Answer::Pointer(pointer_to("seed-leader", &seed))]);
        port.script(&listed, [Answer::Pointer(pointer_to("other", &listed))]);
        let mut cascade = InProcess::new(&port, Some(&authority), clock, &[seed.clone()]);

        let entry = cascade.run().await;

        assert_eq!(joined_leader(entry), (WorkerId::new("seed-leader"), 1));
        assert_eq!(port.passes(), vec![vec![seed]]);
    }

    #[tokio::test(start_paused = true)]
    async fn an_answered_seed_never_founds_even_once_it_goes_quiet() {
        let seed = address(1);
        let port = Scripted::default();
        port.script(&seed, [Answer::NoLeader, Answer::Silent]);
        let mut cascade = InProcess::new(&port, None, TokioClock::new(), &[seed]);

        let still_bootstrapping = timeout(RETRY_INTERVAL * 20, cascade.run()).await;

        assert!(
            still_bootstrapping.is_err(),
            "the node founded a shard after its only seed had shown one exists"
        );
        assert!(port.passes().len() >= 20, "it kept asking every round");
    }

    #[tokio::test(start_paused = true)]
    async fn a_seed_that_knows_no_leader_yet_is_asked_again_until_it_does() {
        let seed = address(1);
        let port = Scripted::default();
        port.script(
            &seed,
            [Answer::NoLeader, Answer::Pointer(pointer_to("seed", &seed))],
        );
        let mut cascade = InProcess::new(&port, None, TokioClock::new(), &[seed]);

        let entry = cascade.run().await;

        assert_eq!(joined_leader(entry), (WorkerId::new("seed"), 1));
        assert_eq!(port.passes().len(), 2);
    }

    // A seed that answered once, pointing at a leader long gone, must not
    // keep the worker asking only it: a worker the authority lists by now
    // knows the current leader.
    #[tokio::test(start_paused = true)]
    async fn a_stale_seed_does_not_hide_a_registered_peer_that_knows_the_leader() {
        let (authority, clock) = warm_authority(StdDuration::from_secs(5)).await;
        let (seed, leader_at) = (address(1), address(2));
        let port = Scripted::default();
        port.script(&seed, [Answer::NoLeader]);
        port.script(&leader_at, [Answer::Pointer(pointer_to("leader", &leader_at))]);
        let mut cascade = InProcess::new(&port, Some(&authority), clock, &[seed]);

        // The leader registers only once the joiner has heard the stale
        // seed, so the joiner is already asking again when it appears.
        let register_leader_later = async {
            tokio::time::sleep(RETRY_INTERVAL * 4).await;
            register(&authority, "leader", &leader_at.to_string());
        };
        let (entry, ()) = tokio::join!(cascade.run(), register_leader_later);

        assert_eq!(joined_leader(entry), (WorkerId::new("leader"), 1));
        assert_eq!(
            epoch_number(&authority),
            None,
            "a node that knows the shard exists never takes ownership of it"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_unreachable_authority_never_leads_to_founding_until_it_answers() {
        let (authority, clock) = warm_authority(StdDuration::from_secs(5)).await;
        authority.set_reachable(false);
        let mut cascade = InProcess::new(&Scripted::default(), Some(&authority), clock, &[]);
        let mut running = std::pin::pin!(cascade.run());

        let waiting = timeout(RETRY_INTERVAL * 20, &mut running).await;
        assert!(waiting.is_err(), "the node entered while its authority was unreachable");
        assert_eq!(epoch_number(&authority), None);

        authority.set_reachable(true);
        let entry = timeout(TEST_TIMEOUT, running)
            .await
            .expect("the node founded the shard once its authority answered");
        assert!(founded_at_epoch_0(&entry));
    }

    #[tokio::test(start_paused = true)]
    async fn a_registered_peer_that_never_answers_keeps_the_worker_bootstrapping() {
        let (authority, clock) = warm_authority(StdDuration::from_secs(5)).await;
        let (a, b) = (address(1), address(2));
        register(&authority, "peer-a", &a.to_string());
        register(&authority, "peer-b", &b.to_string());
        let port = Scripted::default();
        let mut cascade = InProcess::new(&port, Some(&authority), clock, &[]);

        let waiting = timeout(RETRY_INTERVAL * 20, cascade.run()).await;

        assert!(waiting.is_err(), "the node entered with peers listed and none answering");
        assert_eq!(epoch_number(&authority), None);
        let passes = port.passes();
        assert!(passes.len() >= 20);
        // The listing is ordered by worker id, and a bootstrap never rotates it.
        assert!(passes.iter().all(|pass| *pass == [a.clone(), b.clone()]));
    }

    #[tokio::test(start_paused = true)]
    async fn losing_the_create_to_an_unseen_rival_does_not_re_found_the_shard() {
        let (authority, clock) = warm_authority(StdDuration::from_secs(5)).await;
        authority.hold_next(CallKind::SwapRecoveryEpoch);
        let mut cascade = InProcess::new(&Scripted::default(), Some(&authority), clock, &[]);
        let mut running = std::pin::pin!(cascade.run());

        // The cascade registers and asks to create epoch 0: that call is
        // held. A rival registers and creates it meanwhile, then the create
        // is let through, and loses.
        let rival = authority.for_another_worker();
        tokio::select! {
            entry = &mut running => panic!("the cascade entered while its create was held: {entry:?}"),
            () = async {
                wait_until_held(&authority, CallKind::SwapRecoveryEpoch).await;
                let shard = ShardId::new("shard-1");
                rival.register(&shard, &WorkerId::new("rival"), "not a multiaddr").unwrap();
                rival
                    .compare_and_swap_recovery_epoch(&shard, None, RecoveryEpoch::founding(0, &mut Uuid7Lineages))
                    .unwrap();
                authority.release(CallKind::SwapRecoveryEpoch);
            } => {}
        }

        let waiting = timeout(RETRY_INTERVAL * 20, &mut running).await;

        assert!(waiting.is_err(), "the node entered after losing the create");
        assert_eq!(epoch_number(&authority), Some(0), "no one re-founded the shard");
    }

    // A listed peer that answered shows the shard exists, and that stays
    // shown when its registration lapses and the listing is warm and empty.
    #[tokio::test(start_paused = true)]
    async fn a_registered_peer_that_answered_keeps_the_worker_from_founding_after_it_leaves_the_listing()
     {
        let (authority, clock) = warm_authority(RETRY_INTERVAL * 4).await;
        let a = address(1);
        register(&authority, "peer-a", &a.to_string());
        let port = Scripted::default();
        port.script(&a, [Answer::NoLeader]);
        let mut cascade = InProcess::new(&port, Some(&authority), clock, &[]);

        // Well past the registration's lapse.
        let waiting = timeout(RETRY_INTERVAL * 30, cascade.run()).await;

        assert!(waiting.is_err(), "the node entered after its only peer left the listing");
        assert_eq!(port.passes().first(), Some(&vec![a]), "the peer was asked while listed");
        assert!(
            authority
                .for_another_worker()
                .live_registrations(&ShardId::new("shard-1"))
                .unwrap()
                .addresses()
                .is_empty(),
            "the registration lapsed"
        );
        assert_eq!(epoch_number(&authority), None);
    }

    // Review focus 5: an authority whose read hangs holds one blocking
    // thread, not one more every round, and holds up no seed. The cascade
    // keeps asking its seed each round; were a duplicate read made, it
    // would not be held and would find the shard ownerless, so the worker
    // would found it while the first read is still held.
    #[tokio::test(start_paused = true)]
    async fn the_cascade_drops_a_duplicate_in_flight_call() {
        let (authority, clock) = warm_authority(StdDuration::from_secs(5)).await;
        authority.hold_next(CallKind::ReadLiveRegistrations);
        // A seed that is up but answers no one.
        let seed = address(1);
        let port = Scripted::default();
        let mut cascade = InProcess::new(&port, Some(&authority), clock, &[seed]);
        let mut running = std::pin::pin!(cascade.run());

        let held = tokio::select! {
            entry = &mut running => panic!("the worker entered while its first read was held: {entry:?}"),
            held = async {
                wait_until_held(&authority, CallKind::ReadLiveRegistrations).await;
                // The third ask follows a whole round that ran while the
                // first read was held. Time moves by hand: a held call stops
                // auto-advance.
                while port.passes().len() < 3 {
                    tokio::time::advance(RETRY_INTERVAL).await;
                    tokio::task::yield_now().await;
                }
                authority.is_holding(CallKind::ReadLiveRegistrations)
            } => held,
        };
        // Released before anything can fail, so no path leaves the held
        // thread parked and hangs the runtime's shutdown.
        authority.release(CallKind::ReadLiveRegistrations);

        assert!(held, "the first read was answered before the third round");
        // The held read, answered at last, is the cascade's: it takes
        // ownership of the ownerless shard.
        let entry = timeout(TEST_TIMEOUT, running)
            .await
            .expect("the worker founded the shard within the timeout");
        assert!(founded_at_epoch_0(&entry));
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
        decide_round(stage, reply, &me(), shard_exists, &mut Uuid7Lineages)
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
                new: RecoveryEpoch::founding(0, &mut Uuid7Lineages),
                sent_at: Instant::at(3),
                result: Err(AuthorityError::EpochConflict {
                    current: Some(RecoveryEpoch::founding(u64::MAX, &mut Uuid7Lineages)),
                }),
            },
            &me(),
            false,
            &mut Uuid7Lineages,
        );

        let reread = then(&conflict, listing(&["me"], true, Instant::at(4)), false);

        assert!(matches!(
            reread,
            Decision::Wait(Some(WaitReason::RecoveryEpochExhausted))
        ));
    }

    /// Hands out lineages 7, 8, 9, ... in order.
    struct CountingLineages(u64);

    impl LineageSource for CountingLineages {
        fn fresh_lineage(&mut self) -> u64 {
            self.0 += 1;
            self.0 + 6
        }
    }

    fn swap_asked(decision: &Decision) -> (Option<RecoveryEpoch>, RecoveryEpoch) {
        match decision {
            Decision::Ask {
                request: AuthorityRequest::SwapRecoveryEpoch { expected, new },
                ..
            } => (*expected, *new),
            other => panic!("the cascade asked for no swap: {other:?}"),
        }
    }

    // A founder's lineage comes from the source the cascade is given, so a
    // test can fix it: the create takes the first draw and a re-founding the
    // next.
    #[test]
    fn a_founder_takes_the_lineage_of_a_new_epoch_from_the_source_it_is_given() {
        let mut lineages = CountingLineages(0);
        let registered = AuthorityReply::Registered {
            token: ReplyToken {
                issuer: Issuer::Cascade,
                kind: CallKind::Register,
                number: 0,
            },
            sent_at: Instant::at(1),
            result: Ok(kabudachi_core::time::Duration::from_ticks(30)),
        };
        let create = decide_round(Stage::Registering, registered, &me(), false, &mut lineages);
        assert_eq!(swap_asked(&create), (None, RecoveryEpoch::new(0, 7)));

        let found = RecoveryEpoch::new(4, 99);
        let refound = decide_round(
            Stage::ReReading {
                registered_at: Instant::at(1),
                epoch: found,
            },
            listing(&["me"], true, Instant::at(4)),
            &me(),
            false,
            &mut lineages,
        );
        assert_eq!(swap_asked(&refound), (Some(found), RecoveryEpoch::new(5, 8)));
    }
}
