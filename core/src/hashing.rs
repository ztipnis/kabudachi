//! The hash function behind deterministic values derived from fixed inputs,
//! such as the suspicion jitter derived from a worker's ID and term.
//!
//! The function is chosen at runtime. SHA-256 is the default; any hash from
//! the RustCrypto `digest` ecosystem (SHA-3, BLAKE2, ...) can be substituted
//! with [`HashFunction::new`]. Whether the workers of a shard must agree on
//! it depends on the value: each worker computes only its own jitter, so for
//! that they need not.

use std::sync::Arc;

use digest::DynDigest;

/// A field of a hashed value. Fields are encoded explicitly (text is
/// length-prefixed, numbers are big-endian) so the encoding does not depend
/// on any Rust type layout.
pub(crate) enum Field<'a> {
    Text(&'a str),
    Number(u64),
}

#[derive(Clone)]
pub struct HashFunction {
    make_hasher: Arc<dyn Fn() -> Box<dyn DynDigest> + Send + Sync>,
}

impl HashFunction {
    /// Uses the hasher type `D` (for example `sha2::Sha256`).
    pub fn new<D>() -> Self
    where
        D: DynDigest + Default + 'static,
    {
        Self::from_factory(|| Box::new(D::default()))
    }

    /// Uses a hasher produced by `make_hasher`, for hashes that need
    /// construction arguments such as a key.
    pub fn from_factory(
        make_hasher: impl Fn() -> Box<dyn DynDigest> + Send + Sync + 'static,
    ) -> Self {
        HashFunction {
            make_hasher: Arc::new(make_hasher),
        }
    }

    /// The first 8 bytes of the hash of `fields` as a big-endian integer.
    /// Output shorter than 8 bytes is zero-padded on the right.
    pub(crate) fn hash_to_u64(&self, fields: &[Field<'_>]) -> u64 {
        u64::from_be_bytes(self.hash_to_prefix(fields))
    }

    /// The first 8 bytes of the hash of `fields`.
    pub(crate) fn hash_to_prefix(&self, fields: &[Field<'_>]) -> [u8; 8] {
        let mut hasher = (self.make_hasher)();
        for field in fields {
            match field {
                Field::Text(text) => {
                    hasher.update(&(text.len() as u64).to_be_bytes());
                    hasher.update(text.as_bytes());
                }
                Field::Number(number) => hasher.update(&number.to_be_bytes()),
            }
        }
        let output = hasher.finalize();
        let mut prefix = [0u8; 8];
        let length = output.len().min(8);
        prefix[..length].copy_from_slice(&output[..length]);
        prefix
    }
}

impl Default for HashFunction {
    fn default() -> Self {
        Self::new::<sha2::Sha256>()
    }
}
