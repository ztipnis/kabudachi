//! The coordination authority the election tests run against: a
//! `FaultingAuthority` on the tests' `FakeClock`.

use kabudachi_core::coordination_authority::{CoordinationAuthority, RecoveryEpoch};
use kabudachi_core::election::{
    AuthorityCall, AuthorityPerformer, AuthorityReply, AuthorityRequest,
};
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::time::Duration;
use kabudachi_testkit::FaultingAuthority;

use crate::support::clock::FakeClock;

/// The TTL of every test authority's registrations, fences and warm-up: 30 s
/// of simulated time, ADR-0001's default. Nothing in these tests renews a
/// registration. A test that lets a full TTL pass between registering
/// workers and attempting a recovery, to wait out warm-up say, registers
/// them again first; the rest attempt it well within one TTL, so no
/// registration lapses first.
pub fn authority_ttl() -> Duration {
    Duration::from_secs(30)
}

/// Recovery epoch `number` of lineage 0, the lineage of every node started
/// inside a known configuration.
pub fn epoch(number: u64) -> RecoveryEpoch {
    RecoveryEpoch::new(number, 0)
}

/// A new authority on `clock` that is already past its warm-up: building it
/// advances `clock` by one TTL.
pub fn warmed_up_authority(clock: &FakeClock) -> FaultingAuthority<FakeClock> {
    let authority = FaultingAuthority::new(clock.clone(), authority_ttl());
    clock.advance(authority_ttl());
    authority
}

/// Registers each of `workers` for `shard_id`, with its id as its address.
pub fn register_all<'a>(
    authority: &impl CoordinationAuthority,
    shard_id: &ShardId,
    workers: impl IntoIterator<Item = &'a WorkerId>,
) {
    for worker in workers {
        authority
            .register(shard_id, worker, worker.as_str())
            .expect("the seeding handle is reachable");
    }
}

/// Gives `shard_id` what a forced recovery reads: `epoch` as its recovery
/// epoch, created because the shard has none yet, and a live registration
/// for each of `workers`.
pub fn seed_shard<'a>(
    authority: &impl CoordinationAuthority,
    shard_id: &ShardId,
    epoch: u64,
    workers: impl IntoIterator<Item = &'a WorkerId>,
) {
    authority
        .compare_and_swap_recovery_epoch(shard_id, None, self::epoch(epoch))
        .expect("the shard has no epoch yet, so create-if-absent succeeds");
    register_all(authority, shard_id, workers);
}

/// Performs every authority call a node asks for at once, on `authority`, as
/// `me` of `shard_id` registered at its own id, and keeps each request it
/// performed, in order.
pub struct AtOnce<'a> {
    pub authority: &'a FaultingAuthority<FakeClock>,
    pub shard_id: ShardId,
    pub me: WorkerId,
    pub performed: Vec<AuthorityRequest>,
}

impl<'a> AtOnce<'a> {
    pub fn new(
        authority: &'a FaultingAuthority<FakeClock>,
        shard_id: ShardId,
        me: WorkerId,
    ) -> Self {
        AtOnce {
            authority,
            shard_id,
            me,
            performed: Vec::new(),
        }
    }
}

impl AuthorityPerformer for AtOnce<'_> {
    fn perform(&mut self, call: AuthorityCall) -> Option<AuthorityReply> {
        self.performed.push(call.request);
        Some(call.perform(self.authority, &self.shard_id, &self.me, self.me.as_str()))
    }
}
