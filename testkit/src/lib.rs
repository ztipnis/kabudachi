//! Test-only support shared across the workspace's crates. Every crate that
//! uses it takes it as a dev-dependency; production code never depends on it.

mod authority_contract;
mod faulting_authority;
mod record_space;
mod step_record;

pub use authority_contract::{AuthorityAdapter, PassTime, check_authority_contract};
pub use faulting_authority::FaultingAuthority;
pub use record_space::{RecordSpace, SpaceWrite};
pub use step_record::{
    GrantInterval, StepRecord, assert_at_most_one_leader, first_grant_overlap, grant_intervals,
};
