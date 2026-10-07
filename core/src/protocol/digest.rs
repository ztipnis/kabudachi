//! Content hashes and the algorithm each was computed with, so a reader
//! knows how to check one and a later algorithm can be added beside BLAKE3.

use crate::protocol::generated;

/// How many bytes a BLAKE3 digest has.
pub const BLAKE3_DIGEST_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DigestAlgorithm {
    Blake3,
}

impl DigestAlgorithm {
    /// How many bytes a digest of this algorithm has.
    fn digest_bytes(self) -> usize {
        match self {
            DigestAlgorithm::Blake3 => BLAKE3_DIGEST_BYTES,
        }
    }
}

/// A content hash with the algorithm that made it. Every value has the
/// length its algorithm produces.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Digest {
    algorithm: DigestAlgorithm,
    value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidDigest {
    #[error("the digest names no algorithm this worker knows")]
    UnknownAlgorithm,
    #[error("a {algorithm:?} digest has {expected} bytes, not {actual}")]
    WrongLength {
        algorithm: DigestAlgorithm,
        expected: usize,
        actual: usize,
    },
}

impl Digest {
    /// The BLAKE3 digest of `data`.
    pub fn blake3(data: &[u8]) -> Self {
        Digest {
            algorithm: DigestAlgorithm::Blake3,
            value: blake3::hash(data).as_bytes().to_vec(),
        }
    }

    /// A digest computed elsewhere, checked to have its algorithm's length.
    pub fn new(algorithm: DigestAlgorithm, value: Vec<u8>) -> Result<Self, InvalidDigest> {
        let expected = algorithm.digest_bytes();
        if value.len() != expected {
            return Err(InvalidDigest::WrongLength {
                algorithm,
                expected,
                actual: value.len(),
            });
        }
        Ok(Digest { algorithm, value })
    }

    pub fn algorithm(&self) -> DigestAlgorithm {
        self.algorithm
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

impl From<Digest> for generated::Digest {
    fn from(digest: Digest) -> Self {
        let algorithm = match digest.algorithm {
            DigestAlgorithm::Blake3 => generated::DigestAlgorithm::Blake3,
        };
        generated::Digest {
            algorithm: algorithm as i32,
            value: digest.value,
        }
    }
}

impl TryFrom<&generated::Digest> for Digest {
    type Error = InvalidDigest;

    fn try_from(digest: &generated::Digest) -> Result<Self, InvalidDigest> {
        match generated::DigestAlgorithm::try_from(digest.algorithm) {
            Ok(generated::DigestAlgorithm::Blake3) => {
                Digest::new(DigestAlgorithm::Blake3, digest.value.clone())
            }
            Ok(generated::DigestAlgorithm::Unspecified) | Err(_) => {
                Err(InvalidDigest::UnknownAlgorithm)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_digest_survives_the_wire_and_an_unnamed_or_unknown_algorithm_is_refused() {
        let digest = Digest::blake3(b"result");
        let wire = generated::Digest::from(digest.clone());
        assert_eq!(Digest::try_from(&wire), Ok(digest));

        for algorithm in [0, 99] {
            let unknown = generated::Digest {
                algorithm,
                value: wire.value.clone(),
            };
            assert_eq!(
                Digest::try_from(&unknown),
                Err(InvalidDigest::UnknownAlgorithm)
            );
        }
    }

    #[test]
    fn a_value_of_the_wrong_length_is_refused() {
        assert_eq!(
            Digest::new(DigestAlgorithm::Blake3, vec![0; 31]),
            Err(InvalidDigest::WrongLength {
                algorithm: DigestAlgorithm::Blake3,
                expected: 32,
                actual: 31,
            })
        );
    }
}
