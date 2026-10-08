//! A [`CoordinationAuthority`](kabudachi_core::coordination_authority::CoordinationAuthority)
//! over Redis or Valkey.

mod authority;
mod config;
mod connection;
mod keys;
mod view;

pub use authority::RedisAuthority;

pub use config::{ConfigError, DEFAULT_TTL, RedisAuthorityConfig};

/// Every command the adapter sends, spelled as ACL rules take them. An ACL
/// user needs exactly these and `~<key_prefix>*`.
pub const COMMANDS: &[&str] = &[
    "watch",
    "unwatch",
    "multi",
    "exec",
    "get",
    "set",
    "hgetall",
    "hset",
    "hdel",
    "time",
    "info",
    "select",
    "cluster|slots",
];
