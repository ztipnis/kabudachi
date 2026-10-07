use crate::protocol::generated::TaskRecord;
use crate::protocol::ids::{TaskId, WorkerId};
use crate::task_record::store::identify;
use crate::task_record::version::RecordVersion;

/// One revision write: the task and the version written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write {
    pub task_id: TaskId,
    pub version: RecordVersion,
}

impl Write {
    /// Whether storing `self` stores what `earlier` wrote: a revision of
    /// the same task by the same office (recovery epoch and leader term), at
    /// `earlier`'s revision or later. Within one office the leader's
    /// revisions carry the whole record as that leader holds it, so a later
    /// one stored where it must be stands for every decision an earlier one
    /// wrote. A later office rebuilt the record from what its holders had
    /// stored, which need not include a write that never reached its quorum,
    /// so its revisions stand for nothing an earlier office wrote.
    fn covers(&self, earlier: &Write) -> bool {
        self.task_id == earlier.task_id
            && self.version.recovery_epoch == earlier.version.recovery_epoch
            && self.version.leader_term == earlier.version.leader_term
            && self.version.revision >= earlier.version.revision
    }

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

/// A revision to write: `record.placement` names the holders, and `quorum`
/// of them must store it for the write to count.
///
/// A revision whose placement differs from where the task's earlier revisions
/// were placed is a joint write: it also goes to each placement in `prior`,
/// and counts only once the quorum of every one of them has stored it too. A
/// reader that hears most of an earlier placement then meets a holder of the
/// revision, and cannot take the task for older than it is.
#[derive(Debug, Clone)]
pub struct PlacedWrite {
    pub record: TaskRecord,
    pub quorum: usize,
    /// The other placements the revision must reach, each a placement some
    /// earlier revision of the task may still be known by.
    pub prior: Vec<PriorPlacement>,
}

/// A placement an earlier revision was written to, as far as it can still
/// know the record: its holders still in the configuration, and how many of
/// them must store the revision that moves the record from it, a majority of
/// the placement, or all of them if fewer remain. A holder that left the
/// configuration is not asked, and its acknowledgement counts for nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriorPlacement {
    pub holders: Vec<WorkerId>,
    pub quorum: usize,
}

impl PriorPlacement {
    /// The placement `holders`, of which only those `is_member` says are still
    /// in the configuration are kept.
    pub fn new(holders: Vec<WorkerId>, is_member: impl Fn(&WorkerId) -> bool) -> Self {
        let majority = holders.len() / 2 + 1;
        let members: Vec<WorkerId> = holders.into_iter().filter(|holder| is_member(holder)).collect();
        let quorum = majority.min(members.len());
        PriorPlacement { holders: members, quorum }
    }
}

impl PlacedWrite {
    /// `record`, already placed, written with no placement before it.
    pub fn new(record: TaskRecord, quorum: usize) -> Self {
        PlacedWrite {
            record,
            quorum,
            prior: Vec::new(),
        }
    }

    /// The holders of the record's placement.
    pub fn holders(&self) -> Vec<WorkerId> {
        self.record.placement.iter().cloned().map(WorkerId::from).collect()
    }

    /// Every holder the revision goes to: its placement, then each prior
    /// placement's holders not named before, so a holder in two is written
    /// once.
    pub fn recipients(&self) -> Vec<WorkerId> {
        let mut recipients = self.holders();
        for holder in self.prior.iter().flat_map(|prior| &prior.holders) {
            if !recipients.contains(holder) {
                recipients.push(holder.clone());
            }
        }
        recipients
    }
}

/// How a write ended: `stored` once `quorum` holders acknowledged it; not
/// when a holder refused it, too few were reachable, or the write timed out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteOutcome {
    pub write: Write,
    pub stored: bool,
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
/// or worker something was decided) until every revision the call wrote, or
/// a later revision of the same task by the same office, is acknowledged as stored where it
/// must be, and the lease is still valid when the last acknowledgement
/// arrives. An answer released earlier could tell someone of a decision that
/// a leader elected next never sees.
///
/// Two revisions of one task can be in flight to the same holder at once,
/// and the newer can arrive first; the holder then refuses the older. That
/// refusal decides nothing for an effect a newer revision stands for.
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
    /// valid when the acknowledgement arrived. It stands for every awaited
    /// revision of its task its office wrote at or below its revision. Effects settle in the
    /// order they were held.
    #[must_use]
    pub fn acknowledged(&mut self, write: &Write, leading: bool) -> Vec<Settled<E>> {
        let mut settled = Vec::new();
        let mut still_held = Vec::with_capacity(self.held.len());
        for mut held in self.held.drain(..) {
            if !held.awaiting.iter().any(|awaited| write.covers(awaited)) {
                still_held.push(held);
                continue;
            }
            if !leading {
                settled.push(Settled::NotLeader(held.effect));
                continue;
            }
            held.awaiting.retain(|awaited| !write.covers(awaited));
            if held.awaiting.is_empty() {
                settled.push(Settled::Released(held.effect));
            } else {
                still_held.push(held);
            }
        }
        self.held = still_held;
        settled
    }

    /// `write` was refused or timed out. `newer` are the revisions of its
    /// task still in flight that are newer than it: an effect waiting for
    /// `write` waits for those of them its own office wrote instead, since
    /// any of them stored stands for it. With none, every effect waiting for
    /// `write` is answered `NotLeader`.
    #[must_use]
    pub fn refused(&mut self, write: &Write, newer: &[Write]) -> Vec<E> {
        let newer: Vec<&Write> = newer
            .iter()
            .filter(|successor| *successor != write && successor.covers(write))
            .collect();
        let (refused, mut still_held): (Vec<_>, Vec<_>) =
            self.held.drain(..).partition(|held| held.awaiting.contains(write));
        if newer.is_empty() {
            self.held = still_held;
            return refused.into_iter().map(|held| held.effect).collect();
        }
        for mut held in refused {
            held.awaiting.retain(|awaited| awaited != write);
            for successor in &newer {
                if !held.awaiting.contains(*successor) {
                    held.awaiting.push((*successor).clone());
                }
            }
            still_held.push(held);
        }
        self.held = still_held;
        Vec::new()
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
