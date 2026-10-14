//! The contract every `ResultBackend` adapter keeps, as one suite an
//! adapter's own tests run: see [`check_result_backend_contract`].
//!
//! Expiry is checked with entries that expire `EXPIRY` after the
//! adapter's now, and time passed with a [`PassTime`]: on a simulated
//! clock in an instant, or with real waits against a real service.

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId, TaskId, TaskRunId};
use kabudachi_core::result_backend::{
    ResultBackend, ResultBounds, ResultEntry, ResultKey, ResultOwner, ResultTree, ResultValue,
    Stored, StoredBytes, StoredMember, composite_entries, read_composite, read_task_value,
    store_unless_complete, task_entries,
};
use kabudachi_core::time::{Duration, WallTime};

use crate::PassTime;

const EXPIRY: Duration = Duration::from_millis(300);

/// What the suite needs of one adapter: backends to test, and the clock
/// their entries expire by.
pub trait BackendAdapter {
    type Backend: ResultBackend;

    /// A backend holding no entries.
    fn fresh(&self) -> Self::Backend;

    /// Now, by the clock the backend's entries expire by.
    fn now(&self) -> WallTime;
}

/// Runs every clause of the contract against fresh backends from `adapter`,
/// passing time with `time`. Panics, naming the clause, on the first one the
/// adapter breaks.
pub fn check_result_backend_contract(adapter: &impl BackendAdapter, time: &impl PassTime) {
    a_key_never_stored_loads_nothing(adapter);
    every_kind_of_entry_loads_back_unchanged(adapter);
    a_later_store_replaces_the_entry_under_its_key(adapter);
    a_head_its_pages_and_its_chunks_are_different_keys(adapter);
    a_large_value_and_a_paged_composite_read_back_whole(adapter);
    an_entry_loads_until_it_expires_and_never_after(adapter, time);
    an_entry_stored_already_expired_never_loads(adapter);
}

/// Small bounds, so a few dozen entries carry real chunks and pages.
const BOUNDS: ResultBounds = ResultBounds {
    inline_bytes: 16,
    chunk_bytes: 64,
    page_bytes: 256,
};

fn in_an_hour(adapter: &impl BackendAdapter) -> WallTime {
    WallTime::from_unix_millis(adapter.now().as_unix_millis().saturating_add(3_600_000))
}

fn after(adapter: &impl BackendAdapter, span: Duration) -> WallTime {
    WallTime::from_unix_millis(
        adapter
            .now()
            .as_unix_millis()
            .saturating_add(span.as_ticks()),
    )
}

fn store(backend: &impl ResultBackend, entry: &ResultEntry, clause: &str) {
    backend
        .store(entry)
        .unwrap_or_else(|error| panic!("{clause}: store {:?} failed: {error}", entry.key));
}

fn load(backend: &impl ResultBackend, key: &ResultKey, clause: &str) -> Option<ResultEntry> {
    backend
        .load(key)
        .unwrap_or_else(|error| panic!("{clause}: load {key:?} failed: {error}"))
}

fn payload_entry(key: ResultKey, bytes: &[u8], expires_at: WallTime) -> ResultEntry {
    let digest = Digest::blake3(bytes);
    ResultEntry {
        key,
        run: Some(TaskRunId::new("contract-run")),
        digest: Some(digest),
        value: ResultValue::Payload(bytes.to_vec()),
        expires_at,
    }
}

fn a_key_never_stored_loads_nothing(adapter: &impl BackendAdapter) {
    let clause = "a key never stored loads nothing";
    let backend = adapter.fresh();
    for key in [
        ResultKey::Task(TaskId::new("never")),
        ResultKey::Composite(CompositeId::new("never")),
        ResultKey::Page(CompositeId::new("never"), 0),
        ResultKey::Chunk(ResultOwner::Task(TaskId::new("never")), 0),
    ] {
        assert_eq!(load(&backend, &key, clause), None, "{clause}: {key:?}");
    }
}

fn every_kind_of_entry_loads_back_unchanged(adapter: &impl BackendAdapter) {
    let clause = "every kind of entry loads back as it was stored";
    let backend = adapter.fresh();
    let expires_at = in_an_hour(adapter);
    let task = TaskId::new("kinds-task");
    let composite = CompositeId::new("kinds-composite");
    let entries = [
        payload_entry(ResultKey::Task(task.clone()), b"whole", expires_at),
        ResultEntry {
            key: ResultKey::Task(TaskId::new("kinds-chunked")),
            run: Some(TaskRunId::new("contract-run")),
            digest: Some(Digest::blake3(b"chunked")),
            value: ResultValue::Chunked {
                chunks: 3,
                total_bytes: 150,
            },
            expires_at,
        },
        ResultEntry {
            key: ResultKey::Composite(composite.clone()),
            run: None,
            digest: Some(Digest::blake3(b"composite")),
            value: ResultValue::Composite {
                pages: 2,
                members: 5,
            },
            expires_at,
        },
        ResultEntry {
            key: ResultKey::Page(composite.clone(), 1),
            run: None,
            digest: None,
            value: ResultValue::Page(vec![
                StoredMember::List(2),
                StoredMember::Value {
                    producer: TaskDefinitionId::new("producer"),
                    serializer: "protobuf".to_owned(),
                    bytes: StoredBytes::Inline(vec![0, 1, 2, 255]),
                },
                StoredMember::Value {
                    producer: TaskDefinitionId::new("producer"),
                    serializer: "json".to_owned(),
                    bytes: StoredBytes::Chunks {
                        first: 4,
                        count: 2,
                        total_bytes: 100,
                    },
                },
                StoredMember::Failure(
                    kabudachi_core::manifest::Failure::new("ValueError", "no")
                        .of_task(task.clone()),
                ),
            ]),
            expires_at,
        },
        ResultEntry {
            key: ResultKey::Chunk(ResultOwner::Task(task.clone()), 7),
            run: None,
            digest: None,
            value: ResultValue::Chunk(vec![9; 40]),
            expires_at,
        },
        ResultEntry {
            key: ResultKey::Chunk(ResultOwner::Composite(composite), 0),
            run: None,
            digest: None,
            value: ResultValue::Chunk(Vec::new()),
            expires_at,
        },
    ];
    for entry in &entries {
        store(&backend, entry, clause);
    }
    for entry in &entries {
        assert_eq!(
            load(&backend, &entry.key, clause).as_ref(),
            Some(entry),
            "{clause}: {:?}",
            entry.key
        );
    }
}

fn a_later_store_replaces_the_entry_under_its_key(adapter: &impl BackendAdapter) {
    let clause = "a later store replaces the entry under its key";
    let backend = adapter.fresh();
    let key = ResultKey::Task(TaskId::new("replaced"));
    let first = payload_entry(key.clone(), b"first", in_an_hour(adapter));
    let second = payload_entry(key.clone(), b"second", in_an_hour(adapter));
    store(&backend, &first, clause);
    store(&backend, &second, clause);
    assert_eq!(load(&backend, &key, clause), Some(second), "{clause}");
}

fn a_head_its_pages_and_its_chunks_are_different_keys(adapter: &impl BackendAdapter) {
    let clause = "a head, its pages and its chunks are different keys";
    let backend = adapter.fresh();
    let expires_at = in_an_hour(adapter);
    let task = TaskId::new("same");
    let composite = CompositeId::new("same");
    let keys = [
        ResultKey::Task(task.clone()),
        ResultKey::Composite(composite.clone()),
        ResultKey::Page(composite.clone(), 0),
        ResultKey::Chunk(ResultOwner::Task(task), 0),
        ResultKey::Chunk(ResultOwner::Composite(composite.clone()), 0),
        ResultKey::Chunk(ResultOwner::Composite(composite.clone()), 1),
        ResultKey::Page(composite, 1),
    ];
    let entries: Vec<ResultEntry> = keys
        .iter()
        .enumerate()
        .map(|(n, key)| match key {
            ResultKey::Task(_) => payload_entry(key.clone(), &[n as u8], expires_at),
            ResultKey::Composite(_) => ResultEntry {
                key: key.clone(),
                run: None,
                digest: Some(Digest::blake3(&[n as u8])),
                value: ResultValue::Composite {
                    pages: n as u32,
                    members: 1,
                },
                expires_at,
            },
            ResultKey::Page(..) => ResultEntry {
                key: key.clone(),
                run: None,
                digest: None,
                value: ResultValue::Page(vec![StoredMember::List(n as u64)]),
                expires_at,
            },
            ResultKey::Chunk(..) => ResultEntry {
                key: key.clone(),
                run: None,
                digest: None,
                value: ResultValue::Chunk(vec![n as u8]),
                expires_at,
            },
        })
        .collect();
    for entry in &entries {
        store(&backend, entry, clause);
    }
    for entry in &entries {
        assert_eq!(
            load(&backend, &entry.key, clause).as_ref(),
            Some(entry),
            "{clause}: {:?}",
            entry.key
        );
    }
}

fn a_large_value_and_a_paged_composite_read_back_whole(adapter: &impl BackendAdapter) {
    let clause = "a large value and a paged composite read back whole";
    let backend = adapter.fresh();
    let expires_at = in_an_hour(adapter);

    let task = TaskId::new("whole-task");
    let run = TaskRunId::new("whole-run");
    let payload: Vec<u8> = (0..200u32).map(|byte| byte as u8).collect();
    let digest = Digest::blake3(&payload);
    let entries = task_entries(&BOUNDS, &task, &run, &digest, &payload, expires_at);
    assert_eq!(
        store_unless_complete(&backend, &entries),
        Ok(Stored::Written),
        "{clause}: the value is stored"
    );
    assert_eq!(
        read_task_value(&backend, &task, &run, &digest),
        Ok(Some(payload)),
        "{clause}: the value"
    );

    let id = CompositeId::new("whole-composite");
    let members: Vec<ResultTree> = (0..40u8)
        .map(|n| ResultTree::Value {
            producer: TaskDefinitionId::new("member"),
            serializer: "protobuf".to_owned(),
            bytes: vec![n; usize::from(n % 3) * 30],
        })
        .collect();
    let results = ResultTree::List(members);
    let entries = composite_entries(&BOUNDS, &id, &results, expires_at);
    assert_eq!(
        store_unless_complete(&backend, &entries),
        Ok(Stored::Written),
        "{clause}: the composite is stored"
    );
    assert_eq!(
        read_composite(&backend, &id),
        Ok(Some(results)),
        "{clause}: the composite"
    );
}

fn an_entry_loads_until_it_expires_and_never_after(
    adapter: &impl BackendAdapter,
    time: &impl PassTime,
) {
    let clause = "an entry loads until it expires and never after";
    let backend = adapter.fresh();
    let key = ResultKey::Task(TaskId::new("expiring"));
    store(
        &backend,
        &payload_entry(key.clone(), b"soon gone", after(adapter, EXPIRY)),
        clause,
    );
    assert!(
        load(&backend, &key, clause).is_some(),
        "{clause}: loads at once"
    );
    time.pass(Duration::from_ticks(
        EXPIRY.as_ticks() + EXPIRY.as_ticks() / 2,
    ));
    assert_eq!(
        load(&backend, &key, clause),
        None,
        "{clause}: gone after it expired"
    );
}

fn an_entry_stored_already_expired_never_loads(adapter: &impl BackendAdapter) {
    let clause = "an entry stored already expired never loads";
    let backend = adapter.fresh();
    let key = ResultKey::Task(TaskId::new("stale"));
    let expired = WallTime::from_unix_millis(adapter.now().as_unix_millis().saturating_sub(1));
    store(
        &backend,
        &payload_entry(key.clone(), b"stale", expired),
        clause,
    );
    assert_eq!(load(&backend, &key, clause), None, "{clause}");
}
