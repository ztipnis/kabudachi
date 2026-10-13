//! A recording observer: it keeps the revisions a scheduler publishes, which
//! only the observer is handed. Everything else a test asks of a scheduler it
//! reads from the scheduler's own public state.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use kabudachi_core::protocol::generated::{CompositePage, CompositeRecord, TaskRecord};
use kabudachi_core::protocol::ids::{CompositeId, TaskId};
use kabudachi_core::protocol::messages::Task;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::scheduler::Observer;
use kabudachi_core::task_record::{PublishedRevisions, ShardRecord};

/// Hand the scheduler a clone (`Scheduler::with_observer`) and read through
/// the one you keep: every clone shares one log.
#[derive(Debug, Clone, Default)]
pub struct Spy(Rc<RefCell<Log>>);

#[derive(Debug, Default)]
struct Log {
    records: Vec<ShardRecord>,
    /// Whether a revision replaces the held revision of its key.
    newest_only: bool,
    /// Where the held revision of each key sits in `records`, kept only
    /// when `newest_only`.
    slots: BTreeMap<(u8, String, u32), usize>,
    page_revisions: BTreeMap<(String, u32), usize>,
    /// How many Task revisions were published in all, counting every one
    /// even when `newest_only` keeps just the newest of each Task.
    task_revisions: usize,
}

impl Log {
    fn expect_history(&self) -> &Log {
        assert!(!self.newest_only, "a newest-only spy keeps no history to read by position");
        self
    }

    fn record(&mut self, record: ShardRecord) {
        let key = match &record {
            ShardRecord::Task(record) => {
                self.task_revisions += 1;
                (0, record_task(record).task_id().as_str().to_string(), 0)
            }
            ShardRecord::Composite(record) => (1, composite_key(record.composite_id.as_ref()), 0),
            ShardRecord::Page(page) => {
                let id = composite_key(page.composite_id.as_ref());
                *self.page_revisions.entry((id.clone(), page.index)).or_default() += 1;
                (2, id, page.index)
            }
        };
        if !self.newest_only {
            self.records.push(record);
        } else if let Some(slot) = self.slots.get(&key) {
            self.records[*slot] = record;
        } else {
            self.slots.insert(key, self.records.len());
            self.records.push(record);
        }
    }
}

fn composite_key(id: Option<&kabudachi_core::protocol::generated::CompositeId>) -> String {
    id.map(|held| held.value.clone()).unwrap_or_default()
}

impl Observer for Spy {
    fn revision(&mut self, revision: TaskRecord) {
        self.0.borrow_mut().record(revision.into());
    }

    fn composite_revision(&mut self, revision: CompositeRecord) {
        self.0.borrow_mut().record(revision.into());
    }

    fn page_revision(&mut self, revision: CompositePage) {
        self.0.borrow_mut().record(revision.into());
    }
}

impl PublishedRevisions for Spy {
    fn take_published(&mut self) -> Vec<ShardRecord> {
        self.take_revisions()
    }
}

impl Spy {
    /// A spy that keeps only the newest revision of each Task record,
    /// composite record and page (and still counts every page revision and
    /// every Task revision), so a 100k-member test holds one record per key,
    /// not every revision. What reads the log as a history (`mark`,
    /// `published_since`, `revised_since`) has no meaning then and panics.
    pub fn newest_only() -> Spy {
        Spy(Rc::new(RefCell::new(Log { newest_only: true, ..Log::default() })))
    }

    /// How many revisions so far: a mark for `revised_since`.
    pub fn mark(&self) -> usize {
        self.0.borrow().expect_history().records.len()
    }

    /// Every record published after `mark`, in order, left in the log.
    pub fn published_since(&self, mark: usize) -> Vec<ShardRecord> {
        self.0.borrow().expect_history().records[mark..].to_vec()
    }

    /// The tasks revised after `mark`, in order of publication: what the
    /// calls since changed, while the scheduler led.
    pub fn revised_since(&self, mark: usize) -> Vec<TaskId> {
        self.0.borrow().expect_history().records[mark..]
            .iter()
            .filter_map(task_record)
            .map(|record| record_task(record).task_id())
            .collect()
    }

    /// Every Task revision the scheduler published, in order.
    pub fn revisions(&self) -> Vec<TaskRecord> {
        self.0.borrow().records.iter().filter_map(task_record).cloned().collect()
    }

    /// Every revision of every kind the scheduler published, in order.
    pub fn shard_revisions(&self) -> Vec<ShardRecord> {
        self.0.borrow().records.clone()
    }

    /// Every composite revision the scheduler published, in order.
    pub fn composites(&self) -> Vec<CompositeRecord> {
        self.0
            .borrow()
            .records
            .iter()
            .filter_map(|record| match record {
                ShardRecord::Composite(record) => Some(record.clone()),
                ShardRecord::Task(_) | ShardRecord::Page(_) => None,
            })
            .collect()
    }

    /// The newest revision published of the composite `id`, if any.
    pub fn newest_composite(&self, id: &CompositeId) -> Option<CompositeRecord> {
        self.composites()
            .into_iter()
            .rev()
            .find(|record| record.composite_id.as_ref().is_some_and(|held| held.value == id.as_str()))
    }

    /// The newest revision published of page `index` of the composite `id`,
    /// if any.
    pub fn newest_page(&self, id: &CompositeId, index: u32) -> Option<CompositePage> {
        let log = self.0.borrow();
        if log.newest_only {
            return match log.slots.get(&(2, id.as_str().to_string(), index)).map(|slot| &log.records[*slot]) {
                Some(ShardRecord::Page(page)) => Some(page.clone()),
                _ => None,
            };
        }
        log.records.iter().rev().find_map(|record| match record {
            ShardRecord::Page(page)
                if page.index == index
                    && page.composite_id.as_ref().is_some_and(|held| held.value == id.as_str()) =>
            {
                Some(page.clone())
            }
            _ => None,
        })
    }

    /// How many Task revisions were published in all, of every Task.
    pub fn task_revision_total(&self) -> usize {
        self.0.borrow().task_revisions
    }

    /// How many revisions of page `index` of the composite `id` were published.
    pub fn page_revisions(&self, id: &CompositeId, index: u32) -> usize {
        self.0
            .borrow()
            .page_revisions
            .get(&(id.as_str().to_string(), index))
            .copied()
            .unwrap_or(0)
    }

    /// Every revision the scheduler published of `task`, in order.
    pub fn revisions_of(&self, task: &TaskId) -> Vec<TaskRecord> {
        self.revisions()
            .into_iter()
            .filter(|record| record_task(record).task_id() == *task)
            .collect()
    }

    /// The newest revision published of `task`, if any. It borrows the log
    /// and clones one record, so a caller that asks per step stays linear.
    pub fn newest_revision_of(&self, task: &TaskId) -> Option<TaskRecord> {
        self.0
            .borrow()
            .records
            .iter()
            .rev()
            .filter_map(task_record)
            .find(|record| record_task(record).task_id() == *task)
            .cloned()
    }

    /// Every revision published since the last call, oldest first. A
    /// `Cluster` drains this after each step to write the revisions, so a
    /// cluster node's spy has an empty `revisions()` once a step has run.
    pub fn take_revisions(&self) -> Vec<ShardRecord> {
        let mut log = self.0.borrow_mut();
        log.slots.clear();
        std::mem::take(&mut log.records)
    }

    /// `task` as its newest revision carries it. Panics if none was
    /// published.
    pub fn task(&self, task: &TaskId) -> Task {
        self.newest_revision_of(task)
            .map(|record| record_task(&record))
            .unwrap_or_else(|| panic!("no revision of {task:?} was published"))
    }
}

fn task_record(record: &ShardRecord) -> Option<&TaskRecord> {
    match record {
        ShardRecord::Task(record) => Some(record),
        ShardRecord::Composite(_) | ShardRecord::Page(_) => None,
    }
}

fn record_task(record: &TaskRecord) -> Task {
    record.task.clone().expect("a revision carries its task")
}
