use crate::protocol::generated::TaskRecord;
use crate::protocol::ids::TaskId;
use crate::task_record::store::identify;
use crate::task_record::version::RecordVersion;

/// One revision write: the task and the version written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write {
    pub task_id: TaskId,
    pub version: RecordVersion,
}

impl Write {
    /// The write of `record`, which the scheduler built, so it names its task
    /// and version.
    ///
    /// # Panics
    /// If `record` lacks its version, its task or its task id.
    pub fn of(record: &TaskRecord) -> Write {
        let (task_id, version) =
            identify(record).expect("the scheduler builds every record with its task and version");
        Write { task_id, version }
    }
}

/// How a held effect ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Settled<E> {
    /// Every write it waited for was acknowledged while this node still led:
    /// release it.
    Released(E),
    /// A write it waited for was refused, or acknowledged only after the
    /// lease had ended, or the lease ended first: answer it with a retryable
    /// `NotLeader` instead. The writes may still have landed; the next leader
    /// decides what they meant.
    NotLeader(E),
}

/// Holds each effect a leader's call produced (an answer that tells a client
/// or worker something was decided) until every revision the call wrote is
/// acknowledged as stored where it must be, and the lease is still valid
/// when the last acknowledgement arrives. An answer released earlier could
/// tell someone of a decision that a leader elected next never sees.
pub struct EffectGate<E> {
    held: Vec<Held<E>>,
}

struct Held<E> {
    effect: E,
    awaiting: Vec<Write>,
}

impl<E> EffectGate<E> {
    pub fn new() -> Self {
        EffectGate { held: Vec::new() }
    }

    /// Holds `effect` until every write in `writes` is acknowledged. With no
    /// writes it decided nothing that must last, so it is released at once.
    #[must_use]
    pub fn hold(
        &mut self,
        effect: E,
        writes: impl IntoIterator<Item = Write>,
    ) -> Option<Settled<E>> {
        let mut awaiting: Vec<Write> = Vec::new();
        for write in writes {
            if !awaiting.contains(&write) {
                awaiting.push(write);
            }
        }
        if awaiting.is_empty() {
            return Some(Settled::Released(effect));
        }
        self.held.push(Held { effect, awaiting });
        None
    }

    /// `write` was acknowledged; `leading` says whether the lease was still
    /// valid when the acknowledgement arrived. Effects settle in the order
    /// they were held.
    #[must_use]
    pub fn acknowledged(&mut self, write: &Write, leading: bool) -> Vec<Settled<E>> {
        let mut settled = Vec::new();
        let mut still_held = Vec::with_capacity(self.held.len());
        for mut held in self.held.drain(..) {
            let Some(position) = held.awaiting.iter().position(|w| w == write) else {
                still_held.push(held);
                continue;
            };
            if !leading {
                settled.push(Settled::NotLeader(held.effect));
                continue;
            }
            held.awaiting.remove(position);
            if held.awaiting.is_empty() {
                settled.push(Settled::Released(held.effect));
            } else {
                still_held.push(held);
            }
        }
        self.held = still_held;
        settled
    }

    /// `write` was refused or timed out: every effect waiting for it is
    /// answered `NotLeader`.
    #[must_use]
    pub fn refused(&mut self, write: &Write) -> Vec<E> {
        let (refused, still_held): (Vec<_>, Vec<_>) =
            self.held.drain(..).partition(|held| held.awaiting.contains(write));
        self.held = still_held;
        refused.into_iter().map(|held| held.effect).collect()
    }

    /// The lease ended: every held effect is answered `NotLeader`.
    #[must_use]
    pub fn lease_ended(&mut self) -> Vec<E> {
        self.held.drain(..).map(|held| held.effect).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }
}

impl<E> Default for EffectGate<E> {
    fn default() -> Self {
        Self::new()
    }
}
