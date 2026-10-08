//! A [`CoordinationAuthority`](kabudachi_core::coordination_authority::CoordinationAuthority)
//! over Redis or Valkey, using only `WATCH`/`MULTI`/`EXEC`.
//!
//! # What it keeps, and where
//!
//! Each shard name has four keys, `<prefix>{<name>}:shard`, `:regs`, `:fence`
//! and `:hint`, plus `:sentinel`. The name sits inside one hash tag, so a
//! cluster keeps all of them in one slot and every call is a single
//! transaction on one node. Braces and `%` in a name are escaped; the prefix
//! may not contain braces. Values are length-prefixed text, so ids and
//! addresses may hold any characters. No key has a TTL: every expiry is a
//! timestamp, read against the server's own clock (`TIME`), so workers'
//! clocks never matter.
//!
//! # Time bounds
//!
//! Every call, connecting and retrying included, is bounded by
//! [`RedisAuthorityConfig::call_timeout`] (a tenth of the TTL, at most 2 s,
//! unless set; always under a third of the TTL). When it passes, the call
//! answers [`AuthorityError::Unavailable`](kabudachi_core::coordination_authority::AuthorityError::Unavailable)
//! and drops its connection. A transaction that a concurrent writer aborts
//! is retried, at most eight times.
//!
//! # Warm-ups
//!
//! The sentinel records when a name's data began, when it last became
//! available, and the server's run id. A missing sentinel (the server is new
//! or was flushed) or a new run id (the server restarted, or a replica took
//! over) restarts both waits from the call that sees it: no fence for one
//! TTL, no authoritative count for one TTL. A restart or failover may lose
//! writes the server acknowledged, a granted fence among them, so no fence
//! is granted until every fence it might have lost has expired. A name first used on
//! a long-running server starts its own warm-up at that first call. The
//! count is also withheld for one TTL from the first success after a call
//! this client saw fail.
//!
//! # Cluster mode
//!
//! With `cluster` set, the slot of a name's hash tag is looked up once with
//! `CLUSTER SLOTS` and its owner cached. A `MOVED` reply replaces the cached
//! owner; `ASK`, `TRYAGAIN` and `CLUSTERDOWN` are retried after 20 ms within
//! the call's time bound.
//!
//! # Operating it
//!
//! - An ACL user needs exactly the commands in [`COMMANDS`] and the keys
//!   `~<key_prefix>*`.
//! - The server must never evict the sentinel: use `noeviction` or a
//!   `volatile-*` policy, since this adapter sets no key TTLs.
//! - A cluster has only database 0.
//! - A restarted cluster node refuses writes for its first seconds, so a
//!   call right after a restart may answer `Unavailable`.

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
