//! How much a claim and a record weigh, so that no claim outgrows one message
//! and no record outgrows what the network stores.

use prost::Message;

use crate::coalescing::{self, ChainItem};
use crate::protocol::digest::Digest;
use crate::protocol::generated::{CoalescingLink, TaskRecord};
use crate::protocol::ids::IdGenerator;
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{Task, TaskRun};
use crate::task_record::MAX_RECORD_BYTES;
use crate::time::Clock;

use super::{CHAIN_ENTRY_FRAMING_BYTES, Observer, RECORD_RESERVE_BYTES, Scheduler};

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// The payload one entry of a chain carries.
    pub(super) fn item_payload<'a>(&'a self, item: &'a ChainItem) -> &'a [u8] {
        match item {
            ChainItem::Absorbed(task_id) => &self.tasks[task_id].serialized_input,
            ChainItem::Folded(folded) => &folded.payload,
        }
    }

    /// The bytes of the payload one entry of a chain carries.
    pub(super) fn item_len(&self, item: &ChainItem) -> u64 {
        self.item_payload(item).len() as u64
    }

    /// What a claim weighs once it carries `chain`: `weight`, what the task
    /// itself weighs in a claim, then each payload and its framing.
    pub(super) fn claim_bytes<'a>(
        &self,
        weight: u64,
        chain: impl Iterator<Item = &'a ChainItem>,
    ) -> u64 {
        weight
            + chain
                .map(|item| self.item_len(item) + CHAIN_ENTRY_FRAMING_BYTES)
                .sum::<u64>()
    }

    /// The most a record's encoded size may be as measured here, which leaves
    /// room for what a measurement leaves out: the version and publication
    /// time, whose encoding grows with their values, and the placement.
    pub(super) fn record_limit() -> u64 {
        MAX_RECORD_BYTES - RECORD_RESERVE_BYTES
    }

    /// How large the record of `task` would be if it were submitted now as
    /// `run`, with the chain it would absorb from its coalescing key.
    pub(super) fn submitted_record_len(&self, task: &Task, run: &TaskRun, digest: &Digest) -> u64 {
        let retained = task
            .coalescing_key
            .as_deref()
            .map(|key| {
                self.occupancy
                    .retained(&coalescing::key(&task.task_definition_id(), key))
            })
            .unwrap_or_default();
        let waiting = retained.last().and_then(|item| match item {
            ChainItem::Absorbed(waiting) => Some(waiting),
            ChainItem::Folded(_) => None,
        });
        let link = waiting.map(|older| {
            let mut absorbed = self
                .links
                .get(older)
                .map(|link| link.absorbed.clone())
                .unwrap_or_default();
            absorbed.push(older.clone().into());
            CoalescingLink {
                superseded_by: None,
                absorbed,
            }
        });
        let record = TaskRecord {
            version: None,
            task: Some(task.clone()),
            runs: vec![run.clone()],
            retained_chain: retained
                .iter()
                .map(|item| self.chain_entry_of(item))
                .collect(),
            input_digest: Some(digest.clone().into()),
            link,
            placement: Vec::new(),
            prior_placements: Vec::new(),
            published_at: None,
            finished: false,
        };
        record.encoded_len() as u64
    }
}

/// What a claim of `task` weighs before its chain: its input, queue,
/// definition id and coalescing key.
pub(super) fn claim_weight_of(task: &Task) -> u64 {
    claim_weight_sum(
        task.serialized_input.len(),
        &task.queue,
        task.task_definition_id().as_str(),
        task.coalescing_key.as_deref(),
    )
}

/// The one sum of the parts a claim weighs before its chain, shared by a
/// recorded task and a submission not yet recorded.
pub(super) fn claim_weight_sum(
    input_len: usize,
    queue: &str,
    definition_id: &str,
    coalescing_key: Option<&str>,
) -> u64 {
    (input_len + queue.len() + definition_id.len() + coalescing_key.map_or(0, str::len)) as u64
}
