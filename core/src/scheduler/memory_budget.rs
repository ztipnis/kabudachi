//! The memory the scheduler holds for tasks that have not finished, the
//! limits on it, and the `SlowDown` signal that asks submitters to pause.

use super::{Event, MemoryLimits, SubmitRejection};

/// `SlowDown` is cleared once usage is at most this percent of the soft
/// limit, so it does not flap around the limit.
const SLOW_DOWN_CLEARS_AT_PERCENT: u64 = 80;

/// The serialized bytes of every task that has not finished, the limits on
/// them, and whether `SlowDown` is raised.
#[derive(Debug, Default)]
pub(super) struct MemoryBudget {
    limits: Option<MemoryLimits>,
    in_use: u64,
    slow_down: bool,
}

impl MemoryBudget {
    /// Sets the limits, or removes them with `None`, and returns the
    /// `SlowDown` change the new limits make at the current usage, if any.
    ///
    /// # Panics
    ///
    /// If the soft limit is above the hard one.
    pub(super) fn set_limits(&mut self, limits: Option<MemoryLimits>) -> Option<Event> {
        if let Some(limits) = limits {
            assert!(
                limits.soft <= limits.hard,
                "the soft limit is above the hard one"
            );
        }
        self.limits = limits;
        self.update_pressure()
    }

    /// Refuses `needed` more bytes past the hard limit, unless dropping what
    /// `droppable` returns (asked only when over) would make room.
    pub(super) fn check_room(
        &self,
        needed: u64,
        droppable: impl FnOnce() -> u64,
    ) -> Result<(), SubmitRejection> {
        let Some(limits) = self.limits else {
            return Ok(());
        };
        let over = (self.in_use + needed).saturating_sub(limits.hard);
        if over == 0 || over <= droppable() {
            return Ok(());
        }
        Err(SubmitRejection::Backpressure {
            hard_limit: limits.hard,
            in_use: self.in_use,
            needed,
        })
    }

    pub(super) fn take(&mut self, bytes: u64) {
        self.in_use += bytes;
    }

    pub(super) fn give_back(&mut self, bytes: u64) {
        self.in_use -= bytes;
    }

    pub(super) fn over_hard_limit(&self) -> bool {
        self.limits.is_some_and(|limits| self.in_use > limits.hard)
    }

    /// Raises `SlowDown` once usage is past the soft limit, and clears it
    /// once usage is at most 80% of it, or at once when there are no limits;
    /// returns the event when either happens.
    pub(super) fn update_pressure(&mut self) -> Option<Event> {
        let Some(limits) = self.limits else {
            return std::mem::take(&mut self.slow_down)
                .then_some(Event::SlowDown { active: false });
        };
        if !self.slow_down && self.in_use > limits.soft {
            self.slow_down = true;
            Some(Event::SlowDown { active: true })
        } else if self.slow_down
            && self.in_use * 100 <= limits.soft * SLOW_DOWN_CLEARS_AT_PERCENT
        {
            self.slow_down = false;
            Some(Event::SlowDown { active: false })
        } else {
            None
        }
    }

    /// For the notifications' `Counts::memory_in_use`.
    pub(super) fn in_use(&self) -> u64 {
        self.in_use
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMITS: MemoryLimits = MemoryLimits { soft: 100, hard: 200 };

    fn limited() -> MemoryBudget {
        let mut budget = MemoryBudget::default();
        assert_eq!(budget.set_limits(Some(LIMITS)), None);
        budget
    }

    #[test]
    fn slow_down_is_raised_only_once_usage_is_past_the_soft_limit() {
        let mut budget = limited();
        budget.take(100);
        assert_eq!(budget.update_pressure(), None, "at the soft limit is not past it");
        budget.take(1);
        assert_eq!(budget.update_pressure(), Some(Event::SlowDown { active: true }));
    }

    #[test]
    fn slow_down_is_raised_once_while_usage_stays_past_the_soft_limit() {
        let mut budget = limited();
        budget.take(101);
        assert_eq!(budget.update_pressure(), Some(Event::SlowDown { active: true }));
        budget.take(50);
        assert_eq!(budget.update_pressure(), None);
        budget.give_back(60);
        assert_eq!(budget.update_pressure(), None, "91 is inside the margin");
    }

    #[test]
    fn slow_down_clears_only_at_or_below_eighty_percent_of_the_soft_limit() {
        let mut budget = limited();
        budget.take(101);
        budget.update_pressure();
        budget.give_back(20);
        assert_eq!(budget.update_pressure(), None, "81% keeps it raised");
        budget.give_back(1);
        assert_eq!(budget.update_pressure(), Some(Event::SlowDown { active: false }));
        assert_eq!(budget.update_pressure(), None, "cleared once");
    }

    #[test]
    fn new_limits_are_checked_against_usage_at_once() {
        let mut budget = MemoryBudget::default();
        budget.take(60);
        assert_eq!(
            budget.set_limits(Some(MemoryLimits { soft: 50, hard: 200 })),
            Some(Event::SlowDown { active: true })
        );
        assert_eq!(budget.set_limits(Some(LIMITS)), Some(Event::SlowDown { active: false }));
    }
}
