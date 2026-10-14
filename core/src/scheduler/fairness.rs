//! Which queue's task a claim takes next: the most urgent tier first, and
//! within a tier each queue in turn, as often as its weight says.

use std::collections::BTreeMap;

use crate::queues::{QueuePolicy, Rank};
use crate::time::Duration;

/// The task at the front of one queue that a claim could take now.
pub(super) struct Front<'a> {
    pub(super) queue: &'a str,
    /// When it started waiting, in queue order across every queue.
    pub(super) ticket: u64,
    pub(super) waited: Duration,
}

/// The turns each queue has left in the current round of its tier.
#[derive(Debug, Default)]
pub(super) struct Turns {
    left: BTreeMap<String, u64>,
    /// The queue whose turn the last pick would spend, if it spends one.
    owed: Option<String>,
}

impl Turns {
    /// Which of `fronts` to claim next. The turn it will cost its queue is
    /// spent by [`Self::charge`], once the claim is made, so a front passed
    /// over costs its queue nothing.
    ///
    /// The fronts of the highest rank form the tier. Starving fronts go
    /// oldest first. Otherwise the oldest front of a queue with a turn left
    /// goes. When no queue of the tier has one, every queue of the tier gains
    /// its weight in turns first, so a queue of weight three goes three times
    /// for each time a queue of weight one goes.
    pub(super) fn pick(&mut self, policy: &QueuePolicy, fronts: &[Front<'_>]) -> Option<usize> {
        self.owed = None;
        let ranks: Vec<Rank> = fronts
            .iter()
            .map(|front| policy.rank(front.queue, front.waited))
            .collect();
        let top = *ranks.iter().max()?;
        let tier: Vec<usize> = (0..fronts.len())
            .filter(|index| ranks[*index] == top)
            .collect();
        if top == Rank::Starving {
            return tier.into_iter().min_by_key(|index| fronts[*index].ticket);
        }
        let oldest_with_a_turn = |turns: &Turns| {
            tier.iter()
                .copied()
                .filter(|index| {
                    turns
                        .left
                        .get(fronts[*index].queue)
                        .is_some_and(|left| *left > 0)
                })
                .min_by_key(|index| fronts[*index].ticket)
        };
        let chosen = match oldest_with_a_turn(self) {
            Some(index) => index,
            None => {
                for index in &tier {
                    let queue = fronts[*index].queue;
                    *self.left.entry(queue.to_owned()).or_default() +=
                        u64::from(policy.options(queue).weight.get());
                }
                oldest_with_a_turn(self).expect("every queue of the tier just gained a turn")
            }
        };
        self.owed = Some(fronts[chosen].queue.to_owned());
        Some(chosen)
    }

    /// Spends the turn of the queue the last [`Self::pick`] chose, unless that
    /// pick was a starving front, which goes without one.
    pub(super) fn charge(&mut self) {
        if let Some(queue) = self.owed.take() {
            *self
                .left
                .get_mut(&queue)
                .expect("the chosen queue has a turn") -= 1;
        }
    }

    /// A queue with nothing waiting loses its turns, so work that arrives
    /// there later does not run ahead of its weight.
    pub(super) fn forget_empty(&mut self, has_waiting: impl Fn(&str) -> bool) {
        self.left.retain(|queue, _| has_waiting(queue));
    }

    pub(super) fn clear(&mut self) {
        self.left.clear();
        self.owed = None;
    }
}
