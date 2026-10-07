use crate::protocol::generated::TaskRecord;
use crate::task_record::gate::Write;

/// Holds back the revision that marks a coalescing generation superseded
/// until the revision of the generation that superseded it, published by
/// the same call, is stored. A reader that finds the older generation
/// superseded can then always find its successor; and if the successor's
/// write fails, the older generation is never marked superseded at all, so
/// a new leader finds the key as it was before the submission.
#[derive(Debug, Default)]
pub struct WriteOrder {
    waiting: Vec<(Write, Vec<TaskRecord>)>,
}

/// What a write's end means for the revisions held back behind it.
#[derive(Debug, PartialEq)]
pub enum Settlement {
    /// Write these now.
    Release(Vec<TaskRecord>),
    /// These were never written: count each as refused.
    Refuse(Vec<Write>),
}

impl WriteOrder {
    /// Of one call's revisions, in publication order: those to write now.
    /// Each superseded generation whose successor's revision is earlier in
    /// the batch is held back until that revision settles.
    ///
    /// One publication call carries at most one supersession per key. A
    /// successor is looked for only among the revisions already admitted to be
    /// written now, not among those held.
    pub fn admit(&mut self, revisions: Vec<TaskRecord>) -> Vec<TaskRecord> {
        let mut now: Vec<TaskRecord> = Vec::with_capacity(revisions.len());
        for record in revisions {
            let successor = record
                .link
                .as_ref()
                .and_then(|link| link.superseded_by.as_ref())
                .and_then(|newer| {
                    now.iter()
                        .map(Write::of)
                        .find(|write| write.task_id.as_str() == newer.value)
                });
            match successor {
                Some(write) => match self.waiting.iter_mut().find(|(key, _)| *key == write) {
                    Some((_, held)) => held.push(record),
                    None => self.waiting.push((write, vec![record])),
                },
                None => now.push(record),
            }
        }
        now
    }

    /// `write` ended, `stored` or not: the revisions held behind it, to write
    /// now if it was stored, or else to count as refused.
    pub fn settled(&mut self, write: &Write, stored: bool) -> Settlement {
        let held = match self.waiting.iter().position(|(key, _)| key == write) {
            Some(index) => self.waiting.remove(index).1,
            None => Vec::new(),
        };
        if stored {
            Settlement::Release(held)
        } else {
            Settlement::Refuse(held.iter().map(Write::of).collect())
        }
    }

    /// Forgets every held revision, for a leader whose lease has ended: the
    /// writes they wait behind no longer release anything.
    pub fn clear(&mut self) {
        self.waiting.clear();
    }
}
