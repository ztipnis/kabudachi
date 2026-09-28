pub(crate) mod coalescing;
pub mod configuration;
pub mod coordination_authority;
pub mod election;
pub mod hashing;
pub mod in_memory_authority;
pub mod protocol;
pub mod scheduler;
pub mod time;

pub fn version() -> &'static str {
    "0.1.0"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_reported() {
        assert_eq!(version(), "0.1.0");
    }
}
