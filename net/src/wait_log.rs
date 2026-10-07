//! Why a round of the bootstrap cascade, or of a rejoin, found no leader to
//! join, and the log that says so.

use kabudachi_core::coordination_authority::AuthorityError;
use kabudachi_core::protocol::ids::{ShardId, WorkerId};

/// Why a round of the cascade, or of a rejoin, found no leader to join.
#[derive(Debug, PartialEq)]
pub(crate) enum WaitReason {
    AuthorityUnreachable(AuthorityError),
    AuthorityWarmingUp,
    /// The authority has not answered a read within a retry interval.
    AuthorityNotAnswering,
    /// A round that asked the seeds alone, with no listing to ask beside them,
    /// heard from none.
    SeedsNotAnswering,
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
    /// No seed has answered for `rounds` full rounds; with no authority the
    /// worker founds its shard alone after `bound`.
    SeedsSilent { rounds: u32, bound: u32 },
}

/// Logs the reasons each round of the cascade, or each search of a rejoin
/// (see `crate::leader_search::Rejoin`), found no leader.
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
            "the coordination authority is unreachable"
        ),
        WaitReason::AuthorityWarmingUp => log_at_level_or_debug!(
            repeated,
            info,
            shard,
            "the coordination authority is still warming up and may not know every live \
             worker yet"
        ),
        WaitReason::AuthorityNotAnswering => log_at_level_or_debug!(
            repeated,
            warn,
            shard,
            "the coordination authority has not answered a read of the live registrations \
             within a retry interval"
        ),
        WaitReason::SeedsNotAnswering => log_at_level_or_debug!(
            repeated,
            warn,
            shard,
            "no seed answered a round that had no list of registered peers to ask beside them"
        ),
        WaitReason::RecoveryEpochExhausted => log_at_level_or_debug!(
            repeated,
            warn,
            shard,
            "the shard's recovery epoch is already at u64::MAX, so it cannot be re-founded"
        ),
        WaitReason::OwnershipFailed(error) => log_at_level_or_debug!(
            repeated,
            warn,
            shard,
            %error,
            "could not take ownership of the shard at the coordination authority"
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
            "the authority lists registered peers, but none has an address that parses"
        ),
        WaitReason::RegisteredPeersSilent { peers } => log_at_level_or_debug!(
            repeated,
            warn,
            shard,
            peers = ?peers.iter().map(WorkerId::as_str).collect::<Vec<_>>(),
            "the authority lists registered peers, but none of the workers asked answered"
        ),
        WaitReason::NoReachableLeader => log_at_level_or_debug!(
            repeated,
            info,
            shard,
            "the shard exists, but no one asked has pointed at a leader this worker could \
             reach; asking again"
        ),
        WaitReason::SeedsSilent { rounds, bound } => log_at_level_or_debug!(
            repeated,
            info,
            shard,
            rounds,
            bound,
            "no seed has answered, and with no coordination authority the worker founds the \
             shard alone only after the bound"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
