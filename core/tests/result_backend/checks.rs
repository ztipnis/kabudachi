//! A task's value and a composite's results go into a backend as bounded
//! entries, and come back whole, and only as they were stored.

use kabudachi_core::manifest::Failure;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated;
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId, TaskId, TaskRunId};
use kabudachi_core::result_backend::{
    InMemoryResultBackend, Mismatch, PersistenceState, ReadError, ResultBackend, ResultBounds,
    ResultEntry, ResultKey, ResultOwner, ResultTree, ResultValue, Stored, TaskStanding,
    composite_entries, read_composite, read_task_value, store_unless_complete, task_entries,
    task_standing,
};
use kabudachi_core::time::WallTime;

use super::WallClock;

fn later() -> WallTime {
    WallTime::from_unix_millis(u64::MAX / 2)
}

fn value(text: &str, bytes: Vec<u8>) -> ResultTree {
    ResultTree::Value {
        producer: TaskDefinitionId::new(text),
        serializer: "protobuf".to_owned(),
        bytes,
    }
}

/// Small bounds, so a few hundred bytes cross every boundary.
const BOUNDS: ResultBounds = ResultBounds {
    inline_bytes: 16,
    chunk_bytes: 64,
    page_bytes: 256,
};

fn store_all(backend: &impl ResultBackend, entries: &[ResultEntry]) {
    for entry in entries {
        assert!(
            entry.encode().len() <= BOUNDS.page_bytes + 256,
            "{:?} is past the bound",
            entry.key
        );
    }
    assert_eq!(store_unless_complete(backend, entries), Ok(Stored::Written));
}

#[test]
fn values_and_composites_are_stored_in_bounded_parts_and_read_back_only_as_they_were() {
    let backend = InMemoryResultBackend::new(WallClock::default());

    // A task's large value goes as chunks under its head.
    let task = TaskId::new("t");
    let run = TaskRunId::new("r");
    let payload: Vec<u8> = (0..(BOUNDS.chunk_bytes * 2 + 17))
        .map(|byte| byte as u8)
        .collect();
    let digest = Digest::blake3(&payload);
    let entries = task_entries(&BOUNDS, &task, &run, &digest, &payload, later());
    assert_eq!(entries.len(), 4, "three chunks, then the head");
    assert_eq!(
        entries.last().map(|entry| &entry.key),
        Some(&ResultKey::Task(task.clone()))
    );
    store_all(&backend, &entries);
    assert_eq!(
        store_unless_complete(&backend, &entries),
        Ok(Stored::AlreadyComplete),
        "a complete result is never rewritten"
    );
    assert_eq!(
        read_task_value(&backend, &task, &run, &digest),
        Ok(Some(payload.clone()))
    );
    assert_eq!(
        read_task_value(&backend, &task, &TaskRunId::new("replay"), &digest),
        Err(ReadError::Mismatch(Mismatch::Run))
    );
    assert_eq!(
        read_task_value(&backend, &TaskId::new("other"), &run, &digest),
        Ok(None)
    );
    let chunk = ResultKey::Chunk(ResultOwner::Task(task.clone()), 1);
    let mut tampered = backend.load(&chunk).unwrap().unwrap();
    if let ResultValue::Chunk(bytes) = &mut tampered.value {
        bytes[0] ^= 1;
    }
    backend.store(&tampered).unwrap();
    assert_eq!(
        read_task_value(&backend, &task, &run, &digest),
        Err(ReadError::Mismatch(Mismatch::Digest)),
        "a changed chunk no longer hashes to the certified digest"
    );

    // A large map with a large member, a failure and a nested group.
    let id = CompositeId::new("c");
    let mut members: Vec<ResultTree> = (0..40)
        .map(|n| value("small", format!("{n:02}").into_bytes()))
        .collect();
    members.push(value("large", vec![7; BOUNDS.chunk_bytes + 1]));
    members.push(ResultTree::Failure(
        Failure::new("ValueError", "no").of_task(TaskId::new("member")),
    ));
    // A message past a quarter of a page is cut, on a character boundary.
    let long = "é".repeat(BOUNDS.page_bytes);
    members.push(ResultTree::Failure(Failure::new("TypeError", long)));
    members.push(ResultTree::List(vec![
        value("inner", b"a".to_vec()),
        ResultTree::List(Vec::new()),
    ]));
    let mut expected = members.clone();
    let cut = expected.len() - 2;
    expected[cut] = ResultTree::Failure(Failure::new("TypeError", format!("{}…", "é".repeat(30))));
    let results = ResultTree::List(members);
    let expected = ResultTree::List(expected);
    let entries = composite_entries(&BOUNDS, &id, &results, later());
    let pages = entries
        .iter()
        .filter(|entry| matches!(entry.key, ResultKey::Page(..)))
        .count();
    assert!(pages >= 2, "forty members do not fit one small page");
    store_all(&backend, &entries);
    assert_eq!(read_composite(&backend, &id), Ok(Some(expected)));

    // A page changed after it was stored no longer matches the head.
    let page = ResultKey::Page(id.clone(), 0);
    let mut tampered = backend.load(&page).unwrap().unwrap();
    if let ResultValue::Page(stored) = &mut tampered.value {
        stored.swap(0, 1);
    }
    backend.store(&tampered).unwrap();
    assert_eq!(
        read_composite(&backend, &id),
        Err(ReadError::Mismatch(Mismatch::Digest))
    );

    // A head whose parts are gone is no result.
    let gone = CompositeId::new("gone");
    let entries = composite_entries(
        &BOUNDS,
        &gone,
        &ResultTree::List(vec![value("x", b"x".to_vec())]),
        later(),
    );
    backend.store(entries.last().unwrap()).unwrap();
    assert_eq!(read_composite(&backend, &gone), Err(ReadError::Incomplete));
    assert_eq!(
        store_unless_complete(&backend, &entries),
        Ok(Stored::Written),
        "an incomplete result is written again"
    );
    assert!(read_composite(&backend, &gone).unwrap().is_some());
    // A head that counts other members than its pages hold is malformed.
    let mut head = entries.last().unwrap().clone();
    head.value = ResultValue::Composite {
        pages: 1,
        members: 3,
    };
    backend.store(&head).unwrap();
    assert_eq!(
        read_composite(&backend, &gone),
        Err(ReadError::Mismatch(Mismatch::Shape))
    );
    assert_eq!(
        read_composite(&backend, &CompositeId::new("never")),
        Ok(None)
    );
}

fn run(id: &str, state: generated::TaskRunState) -> generated::TaskRun {
    generated::TaskRun {
        identity: Some(generated::TaskRunIdentity {
            task_run_id: Some(TaskRunId::new(id).into()),
            ..Default::default()
        }),
        state: state as i32,
        ..Default::default()
    }
}

fn certified(id: &str, persistence: generated::PersistenceState) -> generated::TaskRun {
    generated::TaskRun {
        result_digest: Some(Digest::blake3(b"result").into()),
        persistence: persistence as i32,
        ..run(id, generated::TaskRunState::Succeeded)
    }
}

fn record(runs: Vec<generated::TaskRun>) -> generated::TaskRecord {
    generated::TaskRecord {
        runs,
        ..Default::default()
    }
}

#[test]
fn a_task_stands_where_its_newest_run_does() {
    assert_eq!(task_standing(None), TaskStanding::Unknown);
    assert_eq!(
        task_standing(Some(&record(Vec::new()))),
        TaskStanding::Unknown
    );
    assert_eq!(
        task_standing(Some(&record(vec![
            run("a", generated::TaskRunState::Failed),
            run("b", generated::TaskRunState::Running)
        ]))),
        TaskStanding::Unfinished,
        "only the newest run counts"
    );
    assert_eq!(
        task_standing(Some(&record(vec![generated::TaskRun {
            failure_kind: "ValueError".to_owned(),
            ..run("a", generated::TaskRunState::Failed)
        }]))),
        TaskStanding::Ended {
            state: generated::TaskRunState::Failed,
            failure_kind: "ValueError".to_owned()
        }
    );
    assert_eq!(
        task_standing(Some(&record(vec![certified(
            "a",
            generated::PersistenceState::Pending
        )]))),
        TaskStanding::Succeeded {
            run: TaskRunId::new("a"),
            digest: Digest::blake3(b"result"),
            persistence: Some(PersistenceState::Pending),
        }
    );
    assert_eq!(
        task_standing(Some(&record(vec![certified(
            "a",
            generated::PersistenceState::Unspecified
        )]))),
        TaskStanding::Succeeded {
            run: TaskRunId::new("a"),
            digest: Digest::blake3(b"result"),
            persistence: None,
        },
        "a run certified before a worker could say shows no state"
    );
    assert_eq!(
        task_standing(Some(&record(vec![run(
            "a",
            generated::TaskRunState::Succeeded
        )]))),
        TaskStanding::Unknown,
        "a success without a digest is no result"
    );
    let stepped = generated::TaskRun {
        continuation: Some(generated::StepDescription::default()),
        ..certified("a", generated::PersistenceState::Disabled)
    };
    assert_eq!(
        task_standing(Some(&record(vec![stepped]))),
        TaskStanding::Continued(CompositeId::continuation_of(&TaskRunId::new("a")))
    );
}
