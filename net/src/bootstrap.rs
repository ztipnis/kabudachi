//! Bootstraps a fresh `core::election::WorkerNode` into its shard: the
//! worker joins the shard that already exists, or founds
//! it when nothing shows that one exists. [`bootstrap`] runs this cascade
//! in rounds, `retry_interval` apart, until a round ends it:
//!
//! 1. **Seeds.** Ask the seeds who leads the shard
//!    ([`crate::join::ask_for_leader`]). A seed that points at a
//!    leader this worker can reach ends the cascade: the worker joins that
//!    leader.
//! 2. **No authority.** With no coordination authority configured, and no
//!    seed ever having answered, found the shard alone (genesis, at recovery
//!    epoch 0), but only once the seeds have stayed silent for `seed_rounds`
//!    rounds (each pause twice as long as the last). With no seeds at all
//!    the worker founds at once.
//! 3. **The record, the hint and the registered peers.** Read the
//!    authority's record of the shard. If it holds one, read the leader hint
//!    too, and the live registrations of the record's incarnation; if it
//!    holds none, read the registrations of the incarnation this worker
//!    would found. If a hinted leader of the record's incarnation (and of no
//!    older epoch) or any worker other than this one is registered, ask the
//!    hinted leader first and then those workers, at the addresses they
//!    registered, the same way as seeds. One that points at a reachable
//!    leader ends the cascade: the worker joins that leader. A hint is asked
//!    even while the authority warms up, which is how a worker finds a leader
//!    that republished after the authority lost its data. A worker of
//!    another incarnation is not counted.
//! 4. **Ownership.** If no other worker is registered, no seed or
//!    registered peer has ever answered, and the authority has warmed up,
//!    register this worker at its address (its external address, if it was
//!    given one, else its listen address), then try to take
//!    ownership of the shard: create its record, a new incarnation minted by
//!    this worker at recovery epoch 0, if the name has none, or, if it holds
//!    one whose incarnation lists no other live worker (see "Re-founding a
//!    shard with no one left to ask" below), re-found that incarnation one
//!    epoch on, keeping its id. The authority lets only one worker win either
//!    compare-and-swap, and that worker founds the shard (genesis, at the
//!    epoch it won). A worker that loses the create waits, and its next round
//!    reads the winner's record.
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
//! silent seeds are the only evidence there is, so the worker trusts them
//! once they have stayed silent for `seed_rounds` rounds.
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
//! created or re-founded the record was registered before it did, and stays
//! registered while it can still lead. Once the authority is warm and lists
//! no live registration of the record's incarnation, either it was never
//! founded, or every worker that ever held it is gone or has already fenced
//! itself off from leading it.
//!
//! That makes it safe to treat "record exists, no one registered" as
//! permission to re-found the incarnation one epoch on, of a new lineage
//! (`compare_and_swap_shard(Some(record), record + 1)`) rather than wait
//! forever for workers that are never coming back: the authority still lets
//! only one bootstrapper win, and any worker still holding the fence from
//! the epoch being replaced (impossible by the argument above, but the
//! authority does not need to know that) makes the new leader wait it out
//! before it can act, exactly as an ordinary recovery does. The re-founding
//! only ever follows a record read in the same round, and swaps from exactly
//! that record, so a record that changed in between (another incarnation
//! founded, or a worker that registered and re-founded) fails the swap and
//! the next round asks them. This also resolves the ambiguous
//! create-if-absent whose own reply was lost: the record sits with only the
//! worker's own registration, which it does not count, and the next warm
//! round re-founds it instead of waiting on it forever.
//!
//! A create-if-absent that loses to another worker's waits: the next round
//! reads the winner's record, and the winner's registration, made before it
//! won, lists it unless it has lapsed (the winner is gone), so the worker
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
    AuthorityError, LeaderHint, LineageSource, LiveRegistrations, RecoveryEpoch, ShardRecord,
    Uuid7Lineages,
};
use kabudachi_core::election::{AuthorityReply, AuthorityRequest, Entry, JoinFloor};
use kabudachi_core::protocol::ids::{ShardId, ShardName, WorkerId};
use kabudachi_core::protocol::messages::{JoinResponse, JoinResponseIds};
use kabudachi_core::time::{Clock, Duration, Instant};
use libp2p::Multiaddr;

use crate::authority::AuthorityClient;
use crate::join::LeaderSearch;
use crate::leader_search::{AskWhoLeads, JoinOverNet, SearchRounds, others_listed};
use crate::messenger::Net;
use crate::wait_log::WaitReason;

/// How long [`bootstrap`] waits, by default, between rounds of its
/// cascade.
pub const DEFAULT_RETRY_INTERVAL: StdDuration = StdDuration::from_millis(500);

/// How many full rounds a worker with seeds and no coordination authority
/// asks its seeds, all silent, before it founds its shard alone.
pub const DEFAULT_SEED_ROUNDS: u32 = 3;

/// Runs the bootstrap cascade for `net`'s local worker, `my_id`, into
/// the shard `name` (see the module doc), and returns how the worker enters its
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
/// seed or registered peer. `grace` bounds how long a round keeps listening
/// for more pointers once the first arrived (see
/// [`crate::join::ask_for_leader`]; the shard's suspicion timeout is the
/// natural value); it is internal to a round, not added to
/// `per_peer_timeout`. `retry_interval` is the wait between rounds of the
/// cascade. `seed_rounds` is how many rounds of silent seeds a worker with
/// seeds and no authority waits before it founds alone ([`DEFAULT_SEED_ROUNDS`]).
/// See `crate::join::DEFAULT_JOIN_PEER_TIMEOUT` and
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
    name: &ShardName,
    my_id: &WorkerId,
    seeds: &[Multiaddr],
    per_peer_timeout: StdDuration,
    grace: StdDuration,
    retry_interval: StdDuration,
    seed_rounds: u32,
) -> Entry {
    cascade(
        net,
        &mut JoinOverNet {
            net,
            per_peer_timeout,
            grace,
        },
        &mut Uuid7Lineages,
        clock,
        authority,
        name,
        my_id,
        seeds,
        retry_interval,
        seed_rounds,
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
    name: &ShardName,
    my_id: &WorkerId,
    seeds: &[Multiaddr],
    retry_interval: StdDuration,
    seed_rounds: u32,
) -> Entry {
    let mut silence = SeedSilence::new(seeds, seed_rounds);
    let mut search = SearchRounds::for_bootstrap(name, my_id.clone(), seeds.to_vec());
    // The incarnation a founding against an empty name would create.
    let candidate = authority
        .as_deref()
        .map_or_else(|| ShardId::mint(name), |calls| calls.shard_id().clone());
    // The incarnation the authority's record named when it was last read, if
    // it held one: the seeds' leaders must be of it too.
    let mut record_seen: Option<ShardId> = None;
    loop {
        refuse_requests(net);

        // With no seeds this finds no answer at once.
        let found = of_shard(
            port.ask(search.seeds(), JoinFloor::none()).await,
            name,
            record_seen.as_ref(),
        );
        if let Some(pointer) = search.heard_from_seeds(found) {
            return Entry::Joining(pointer);
        }

        let mut pause = retry_interval;
        match authority.as_deref_mut() {
            None if !search.shard_exists() => match silence.one_more(&mut search, retry_interval) {
                Some(longer) => pause = longer,
                None => return founding_alone(&candidate, lineages),
            },
            None => {}
            Some(calls) => {
                let round = consult_authority(
                    calls,
                    port,
                    lineages,
                    &mut search,
                    clock,
                    my_id,
                    &candidate,
                    &mut record_seen,
                    retry_interval,
                )
                .await;
                if let Some(entry) = round.entry() {
                    return entry;
                }
            }
        }

        search.end_round();
        tokio::time::sleep(pause).await;
    }
}

/// How many full rounds the seeds have been silent, for a worker with no
/// authority, and how many it waits before it founds alone.
struct SeedSilence {
    rounds: u32,
    bound: u32,
    no_seeds: bool,
}

impl SeedSilence {
    fn new(seeds: &[Multiaddr], seed_rounds: u32) -> Self {
        SeedSilence {
            rounds: 0,
            bound: seed_rounds.max(1),
            no_seeds: seeds.is_empty(),
        }
    }

    /// One more silent round. `None` once the worker should found alone:
    /// with no authority, silence is the only evidence that no shard
    /// exists, and a slow seed is silent too. Founding after a bounded
    /// number of rounds keeps a worker whose seeds are really gone from
    /// waiting for ever, at the cost that a seed that was only slow now
    /// leads a second shard beside this one. Nothing joins the two yet:
    /// that needs shard merging, run when the two sides reach each other
    /// again. Otherwise logs the wait and returns the pause before the next
    /// round: each silent round waits twice as long as the last, so a seed
    /// that is slow to start gets longer to answer.
    fn one_more(
        &mut self,
        search: &mut SearchRounds,
        retry_interval: StdDuration,
    ) -> Option<StdDuration> {
        self.rounds += 1;
        if self.no_seeds || self.rounds >= self.bound {
            return None;
        }
        search.log(WaitReason::SeedsSilent {
            rounds: self.rounds,
            bound: self.bound,
        });
        Some(retry_interval.saturating_mul(1 << (self.rounds - 1).min(16)))
    }
}

/// Founding the shard at epoch 0 with no registration, as a worker with no
/// authority does.
fn founding_alone(candidate: &ShardId, lineages: &mut impl LineageSource) -> Entry {
    Entry::Founding {
        shard_id: candidate.clone(),
        recovery_epoch: RecoveryEpoch::founding(0, lineages),
        registered_at: None,
    }
}

impl AuthorityRound {
    /// How the worker enters its shard after this round, if the round
    /// decided it.
    fn entry(self) -> Option<Entry> {
        match self {
            AuthorityRound::Joined(pointer) => Some(Entry::Joining(pointer)),
            AuthorityRound::OwnershipWon {
                shard_id,
                recovery_epoch,
                registered_at,
            } => Some(Entry::Founding {
                shard_id,
                recovery_epoch,
                registered_at: Some(registered_at),
            }),
            AuthorityRound::Wait => None,
        }
    }
}

/// `found`, unless it points at a leader of another shard than this worker
/// bootstraps into: one under another name, or, given the incarnation the
/// authority's record names, another incarnation of it. Such a leader is not
/// this worker's to join, so the pointer is dropped, but its answer still
/// shows a shard exists, which keeps this worker from founding beside it.
fn of_shard(found: LeaderSearch, name: &ShardName, record: Option<&ShardId>) -> LeaderSearch {
    match found {
        LeaderSearch::Found(pointer)
            if !pointer.shard_id().is_some_and(|shard| {
                shard.name() == *name && record.is_none_or(|record| *record == shard)
            }) =>
        {
            tracing::debug!(
                shard = ?pointer.shard_id(),
                "ignoring a pointer to a leader of another shard"
            );
            LeaderSearch::NoReachableLeader
        }
        found => found,
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
    /// This worker won the shard's record (created it, as a new incarnation,
    /// at epoch 0, or re-founded the incarnation it found one epoch on, with
    /// no one left to ask — see this module's "Re-founding a shard with no
    /// one left to ask"), so it founds that incarnation at this epoch. It asked to be registered at
    /// `registered_at`, before it took ownership.
    OwnershipWon {
        shard_id: ShardId,
        recovery_epoch: RecoveryEpoch,
        registered_at: Instant,
    },
    /// Stay in `Bootstrapping` until the next round. The reason is logged.
    Wait,
}

/// Reads the shard's record, then its leader hint and live registrations,
/// and asks the hinted leader and the workers other than this one who leads
/// the shard. With none to ask, and while no one has shown that the shard
/// exists (`shard_exists`), tries to take ownership of the shard once the
/// authority has warmed up. Each call is made through `calls`, serving the
/// incarnation the stage concerns (the record's, or `candidate` when the
/// name holds none), and each reply decided on by [`decide_round`].
///
/// The first read has one `retry_interval` to be answered; if it is not,
/// the round ends and the next asks the seeds again rather than waiting on
/// the authority. The read stays in flight meanwhile, and is not asked for
/// again until it is answered (see [`AuthorityClient`]): a later round
/// decides on its reply once it comes. Once the record is read, each call
/// waits for its reply however long it takes: the swap must follow the
/// registration it relies on, and an epoch this worker won must not be left
/// behind.
#[allow(clippy::too_many_arguments)]
async fn consult_authority<C: Clock, P: AskWhoLeads>(
    calls: &mut AuthorityClient,
    port: &mut P,
    lineages: &mut impl LineageSource,
    search: &mut SearchRounds,
    clock: &C,
    my_id: &WorkerId,
    candidate: &ShardId,
    record_seen: &mut Option<ShardId>,
    retry_interval: StdDuration,
) -> AuthorityRound {
    let mut stage = Stage::ReadingShard;
    calls.ask(AuthorityRequest::ReadRecoveryEpoch, clock.now());
    loop {
        let within = matches!(stage, Stage::ReadingShard).then_some(retry_interval);
        let Some(reply) = calls.next_reply(within).await else {
            search.log(WaitReason::AuthorityNotAnswering);
            return AuthorityRound::Wait;
        };
        note_record_read(&stage, &reply, record_seen);
        match decide_round(
            stage.clone(),
            reply,
            my_id,
            candidate,
            search.shard_exists(),
            lineages,
        ) {
            Decision::Ask {
                request,
                then,
                serve,
            } => {
                ask_next(calls, request, serve, clock);
                stage = then;
            }
            Decision::AskPeers(peers) => {
                return ask_peers(port, search, peers, &candidate.name()).await;
            }
            Decision::OwnershipWon {
                recovery_epoch,
                registered_at,
            } => {
                return AuthorityRound::OwnershipWon {
                    shard_id: calls.shard_id().clone(),
                    recovery_epoch,
                    registered_at,
                };
            }
            Decision::Wait(reason) => return wait_round(search, reason),
            Decision::Ignore => {}
        }
    }
}

/// Notes the incarnation the round's first read found under the shard's
/// name, if it found one: the seeds' leaders must be of it too.
fn note_record_read(stage: &Stage, reply: &AuthorityReply, record_seen: &mut Option<ShardId>) {
    if let (
        Stage::ReadingShard,
        AuthorityReply::RecoveryEpoch {
            result: Ok(held), ..
        },
    ) = (stage, reply)
    {
        *record_seen = held.as_ref().map(|record| record.shard_id.clone());
    }
}

/// Makes `request` on the authority, serving the incarnation `serve` names
/// first if it names one.
fn ask_next<C: Clock>(
    calls: &mut AuthorityClient,
    request: AuthorityRequest,
    serve: Option<ShardId>,
    clock: &C,
) {
    if let Some(shard_id) = serve {
        calls.serve(shard_id);
    }
    calls.ask(request, clock.now());
}

/// Asks the hinted leader, then the listed workers, who leads: joined if
/// one points at a leader of this shard that this worker reaches.
async fn ask_peers<P: AskWhoLeads>(
    port: &mut P,
    search: &mut SearchRounds,
    peers: PeersToAsk,
    name: &ShardName,
) -> AuthorityRound {
    let addresses = search.to_ask(peers.hinted.as_ref(), &peers.listed);
    let found = of_shard(
        port.ask(&addresses, JoinFloor::none()).await,
        name,
        peers.record.as_ref(),
    );
    match search.heard_from_listed(found) {
        Some(pointer) => AuthorityRound::Joined(pointer),
        None => AuthorityRound::Wait,
    }
}

/// Ends the round waiting, logging `reason` if there is one.
fn wait_round(search: &mut SearchRounds, reason: Option<WaitReason>) -> AuthorityRound {
    if let Some(reason) = reason {
        search.log(reason);
    }
    AuthorityRound::Wait
}

/// Which reply a round's consultation of the authority waits for.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Stage {
    /// The round's first read: the record under the shard's name.
    ReadingShard,
    /// The leader hint, after a read that found `record`.
    ReadingHint { record: ShardRecord },
    /// The live registrations of the incarnation the round concerns: the
    /// record's, or the candidate's when the name holds none.
    ReadingRegistrations {
        record: Option<ShardRecord>,
        hint: Option<LeaderHint>,
    },
    /// This worker's registration, asked for before it takes ownership.
    Registering { record: Option<ShardRecord> },
    /// The create-if-absent of the candidate's record at epoch 0. The worker
    /// asked to be registered at `registered_at`.
    Creating { registered_at: Instant },
    /// The re-founding swap one epoch past the record this round read.
    ReFounding { registered_at: Instant },
}

/// What the cascade does after one reply from the authority (see
/// [`decide_round`]).
#[derive(Debug)]
pub(crate) enum Decision {
    /// Make `request`, and wait for its reply at `then`. With `serve`, the
    /// client names that incarnation from now on.
    Ask {
        request: AuthorityRequest,
        then: Stage,
        serve: Option<ShardId>,
    },
    /// Ask the hinted leader, if any, and then these registered workers (the
    /// hinted one among them or not), who leads.
    AskPeers(PeersToAsk),
    /// This worker won the shard's record (see
    /// [`AuthorityRound::OwnershipWon`]); the incarnation is the one the
    /// client serves.
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

/// Whom a round asks who leads, once the authority has been read.
#[derive(Debug)]
pub(crate) struct PeersToAsk {
    pub(crate) hinted: Option<LeaderHint>,
    pub(crate) listed: BTreeMap<WorkerId, String>,
    /// The incarnation the authority's record names, if it holds one: a
    /// leader of any other is not this worker's to join.
    pub(crate) record: Option<ShardId>,
}

/// The cascade's decision on `reply`, the authority's answer to the call it
/// made at `stage` (see this module's doc). `candidate` is the incarnation a
/// worker would found where the name holds no record:
///
/// - The record read: none, then read the live registrations of
///   `candidate`; one, then read the leader hint and the live registrations
///   of the record's incarnation.
/// - The hint is kept only if it is of the record's incarnation and of no
///   older epoch than the record's: it only says whom to ask first.
/// - The registrations list other workers, or a hint names a leader other
///   than this worker: ask them. They list no one else: wait if the shard is
///   known to exist (`shard_exists`) or the authority is still warming up,
///   and register this worker otherwise.
/// - Registered: create the candidate's record at epoch 0 (no record was
///   read), or re-found the record read one epoch on, from exactly that
///   record, of a new lineage. The registration counts from when it was
///   asked for, the reply's `sent_at`.
/// - Created or re-founded: ownership won. A create that lost waits, and the
///   next round reads the winner's record.
///
/// Any failure waits for the next round, with its reason.
pub(crate) fn decide_round(
    stage: Stage,
    reply: AuthorityReply,
    my_id: &WorkerId,
    candidate: &ShardId,
    shard_exists: bool,
    lineages: &mut impl LineageSource,
) -> Decision {
    match (stage, reply) {
        (Stage::ReadingShard, AuthorityReply::RecoveryEpoch { result, .. }) => {
            decide_record_read(result, candidate)
        }
        (Stage::ReadingHint { record }, AuthorityReply::LeaderHint { result, .. }) => {
            decide_hint_read(record, result)
        }
        (
            Stage::ReadingRegistrations { record, hint },
            AuthorityReply::LiveRegistrations { result, .. },
        ) => decide_registrations_read(record, hint, result, my_id, shard_exists),
        (
            Stage::Registering { record },
            AuthorityReply::Registered {
                sent_at, result, ..
            },
        ) => decide_registered(record, sent_at, result, lineages),
        (
            Stage::Creating { registered_at } | Stage::ReFounding { registered_at },
            AuthorityReply::RecoveryEpochSwapped { new, result, .. },
        ) => decide_swapped(new, result, registered_at),
        _ => Decision::Ignore,
    }
}

/// The decision on the record the round's first read found under the
/// shard's name, if it found one.
fn decide_record_read(
    result: Result<Option<ShardRecord>, AuthorityError>,
    candidate: &ShardId,
) -> Decision {
    match result {
        Err(error) => Decision::Wait(Some(WaitReason::AuthorityUnreachable(error))),
        // Nothing under the name: the registrations that count are the
        // ones tagged with the id this worker would found.
        Ok(None) => Decision::Ask {
            request: AuthorityRequest::ReadLiveRegistrations,
            then: Stage::ReadingRegistrations {
                record: None,
                hint: None,
            },
            serve: Some(candidate.clone()),
        },
        Ok(Some(record)) => Decision::Ask {
            request: AuthorityRequest::ReadLeaderHint,
            serve: Some(record.shard_id.clone()),
            then: Stage::ReadingHint { record },
        },
    }
}

/// The decision on the leader hint read after `record`.
fn decide_hint_read(
    record: ShardRecord,
    result: Result<Option<LeaderHint>, AuthorityError>,
) -> Decision {
    // A hint only says whom to ask first: one that cannot be read, or
    // is of another incarnation or an older epoch, is passed over.
    let hint = result.ok().flatten().filter(|hint| {
        hint.shard_id == record.shard_id && hint.recovery_epoch >= record.recovery_epoch
    });
    Decision::Ask {
        request: AuthorityRequest::ReadLiveRegistrations,
        then: Stage::ReadingRegistrations {
            record: Some(record),
            hint,
        },
        serve: None,
    }
}

/// The decision on the live registrations: ask the workers they list, or
/// the hinted leader, who leads; otherwise wait, or register this worker.
fn decide_registrations_read(
    record: Option<ShardRecord>,
    hint: Option<LeaderHint>,
    result: Result<LiveRegistrations, AuthorityError>,
    my_id: &WorkerId,
    shard_exists: bool,
) -> Decision {
    let registrations = match result {
        Ok(registrations) => registrations,
        Err(error) => return Decision::Wait(Some(WaitReason::AuthorityUnreachable(error))),
    };
    let hinted = hint.filter(|hint| hint.leader != *my_id);
    let listed = others_listed(&registrations, my_id);
    // A hint is asked even while the authority warms up: that is how
    // a worker joins a leader that republished after a flush.
    if hinted.is_some() || !listed.is_empty() {
        return Decision::AskPeers(PeersToAsk {
            hinted,
            listed,
            record: record.map(|record| record.shard_id),
        });
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
        then: Stage::Registering { record },
        serve: None,
    }
}

/// The decision on this worker's registration: create the candidate's
/// record, or re-found the one read, one epoch on.
fn decide_registered(
    record: Option<ShardRecord>,
    sent_at: Instant,
    result: Result<Duration, AuthorityError>,
    lineages: &mut impl LineageSource,
) -> Decision {
    if let Err(error) = result {
        return Decision::Wait(Some(WaitReason::AuthorityUnreachable(error)));
    }
    let Some(record) = record else {
        return Decision::Ask {
            request: AuthorityRequest::SwapRecoveryEpoch {
                expected: None,
                new: RecoveryEpoch::founding(0, lineages),
            },
            then: Stage::Creating {
                registered_at: sent_at,
            },
            serve: None,
        };
    };
    let Some(next) = record.recovery_epoch.number.checked_add(1) else {
        // No successor epoch exists to re-found at.
        return Decision::Wait(Some(WaitReason::RecoveryEpochExhausted));
    };
    Decision::Ask {
        request: AuthorityRequest::SwapRecoveryEpoch {
            expected: Some(record.recovery_epoch),
            new: RecoveryEpoch::founding(next, lineages),
        },
        then: Stage::ReFounding {
            registered_at: sent_at,
        },
        serve: None,
    }
}

/// The decision on the swap that created or re-founded the record.
fn decide_swapped(
    new: RecoveryEpoch,
    result: Result<(), AuthorityError>,
    registered_at: Instant,
) -> Decision {
    match result {
        Ok(()) => Decision::OwnershipWon {
            recovery_epoch: new,
            registered_at,
        },
        // Another worker won between the read and this swap: the next
        // round reads its record.
        Err(error) => Decision::Wait(Some(WaitReason::OwnershipFailed(error))),
    }
}
