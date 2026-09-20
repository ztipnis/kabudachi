use std::cmp::Reverse;

use kabudachi_core::election::candidate_priority;
use kabudachi_core::hashing::HashFunction;
use kabudachi_core::protocol::ids::{ShardId, WorkerId};

/// The worker that wins an election for `next_term` among `candidates` under
/// the default hash function: highest [`candidate_priority`], ties going to the
/// lower `WorkerId`. Lets a test choose in advance which node a real roll call
/// will pick.
pub fn predict_winner(
    shard_id: &ShardId,
    recovery_epoch: u64,
    next_term: u64,
    candidates: &[WorkerId],
) -> WorkerId {
    candidates
        .iter()
        .min_by_key(|candidate| {
            let priority = candidate_priority(
                &HashFunction::default(),
                shard_id,
                recovery_epoch,
                next_term,
                candidate,
            );
            (Reverse(priority), *candidate)
        })
        .expect("candidates must be non-empty")
        .clone()
}
