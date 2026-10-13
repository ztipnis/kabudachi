//! Result backends: where certified results are mirrored, and how a reader
//! checks what it loads.

use crate::protocol::generated;

/// Whether a certified run's result was mirrored to a result backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceState {
    /// Nothing is mirrored: no backend, a task that keeps no result, or a
    /// result that is not a task's own value.
    Disabled,
    /// Its worker is mirroring it.
    Pending,
    Stored,
    /// The backend refused it or did not answer; the run's outcome stands.
    Failed,
}

impl From<PersistenceState> for generated::PersistenceState {
    fn from(state: PersistenceState) -> Self {
        match state {
            PersistenceState::Disabled => generated::PersistenceState::Disabled,
            PersistenceState::Pending => generated::PersistenceState::Pending,
            PersistenceState::Stored => generated::PersistenceState::Stored,
            PersistenceState::Failed => generated::PersistenceState::Failed,
        }
    }
}

/// The wire's `PERSISTENCE_STATE_UNSPECIFIED`, which no worker sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("no persistence state was given")]
pub struct UnspecifiedPersistence;

impl TryFrom<generated::PersistenceState> for PersistenceState {
    type Error = UnspecifiedPersistence;

    fn try_from(state: generated::PersistenceState) -> Result<Self, Self::Error> {
        match state {
            generated::PersistenceState::Disabled => Ok(PersistenceState::Disabled),
            generated::PersistenceState::Pending => Ok(PersistenceState::Pending),
            generated::PersistenceState::Stored => Ok(PersistenceState::Stored),
            generated::PersistenceState::Failed => Ok(PersistenceState::Failed),
            generated::PersistenceState::Unspecified => Err(UnspecifiedPersistence),
        }
    }
}
