//! Test-only support shared across the workspace's crates. Every crate that
//! uses it takes it as a dev-dependency; production code never depends on it.

mod faulting_authority;

pub use faulting_authority::FaultingAuthority;
