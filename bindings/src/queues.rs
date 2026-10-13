//! Queue settings as Python passes them, checked and turned into the core's.

use std::collections::BTreeSet;
use std::num::NonZeroU32;

use kabudachi_core::queues::{QueueOptions, QueuePolicy, Subscription};
use kabudachi_core::time::Duration as CoreDuration;
use pyo3::PyResult;
use pyo3::exceptions::PyValueError;

/// One queue's options as Python passes them: (name, priority, weight,
/// max_running, starvation_age_ms).
pub(crate) type QueueOptionsArgument = (String, i32, u32, Option<u32>, Option<u64>);

/// The policy `options` give. Raises `ValueError` for a weight, a running
/// limit or a starvation age of zero, or a queue named twice.
pub(crate) fn queue_policy(options: Vec<QueueOptionsArgument>) -> PyResult<QueuePolicy> {
    let mut policy = QueuePolicy::new();
    let mut named = BTreeSet::new();
    for (queue, priority, weight, max_running, starvation_age_ms) in options {
        if !named.insert(queue.clone()) {
            return Err(PyValueError::new_err(format!(
                "queue {queue:?} is given twice"
            )));
        }
        let weight = NonZeroU32::new(weight).ok_or_else(|| {
            PyValueError::new_err(format!("queue {queue:?}: weight must be positive"))
        })?;
        let max_running = match max_running {
            None => None,
            Some(max) => Some(NonZeroU32::new(max).ok_or_else(|| {
                PyValueError::new_err(format!("queue {queue:?}: max_running must be positive"))
            })?),
        };
        if starvation_age_ms == Some(0) {
            return Err(PyValueError::new_err(format!(
                "queue {queue:?}: starvation_age must be positive"
            )));
        }
        let options = QueueOptions {
            priority,
            weight,
            max_running,
            starvation_age: starvation_age_ms.map(CoreDuration::from_millis),
        };
        policy = policy.with_queue(queue, options);
    }
    Ok(policy)
}

/// The queues `queues` names, or every queue for `None`. Raises
/// `ValueError` for an empty list or an empty name.
pub(crate) fn subscription(queues: Option<Vec<String>>) -> PyResult<Subscription> {
    match queues {
        None => Ok(Subscription::Every),
        Some(names) if names.is_empty() || names.iter().any(|name| name.trim().is_empty()) => {
            Err(PyValueError::new_err(
                "queues must name at least one queue, none of them empty",
            ))
        }
        Some(names) => Ok(Subscription::Only(names.into_iter().collect())),
    }
}
