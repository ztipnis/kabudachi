//! Checked decode of the wire [`generated::Generation`]/[`generated::Configuration`]
//! into the domain [`configuration::Generation`]/[`configuration::Configuration`]
//! (ADR-0001 decision 1), and their infallible encode back to the wire.
//!
//! Decode is where untrusted configuration data from a peer is validated
//! (STYLE_GUIDE "validate untrusted input at the edge"); the domain
//! constructors it builds with only `debug_assert!` the same invariants, so
//! this is the one place a malformed peer message is turned into an error
//! instead of a panic. Encode is infallible and lives with the domain type
//! (`impl From<&Configuration> for generated::Configuration` in
//! `crate::configuration`), since it needs that type's private `Electorate`
//! field.

use crate::configuration::{self, Joint, Single};
use crate::protocol::generated;

/// Why a wire [`generated::Generation`] or [`generated::Configuration`] failed
/// to decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidConfiguration {
    #[error("a generation's counter is u64::MAX")]
    CounterAtMax,
    #[error("Configuration.generation is required but was absent")]
    MissingGeneration,
    #[error("Configuration.base is required but was absent")]
    MissingBase,
    #[error("Configuration.electorate is required but was absent")]
    MissingElectorate,
    #[error("JointElectorate.batch_generation is required but was absent")]
    MissingBatchGeneration,
    #[error("JointElectorate.old_base is required but was absent")]
    MissingOldBase,
    #[error("JointElectorate.old_generation is required but was absent")]
    MissingOldGeneration,
    #[error("base is later than generation")]
    BaseAfterGeneration,
    #[error("a joint electorate's base is later than its batch generation")]
    BaseAfterBatchGeneration,
    #[error("a joint electorate's batch generation is later than generation")]
    BatchGenerationAfterGeneration,
    #[error("a joint electorate's old base is later than its old generation")]
    OldBaseAfterOldGeneration,
    #[error("a joint electorate's old generation is not earlier than its batch generation")]
    OldGenerationNotBeforeBatchGeneration,
    #[error("a joint electorate's old base is later than its base")]
    OldBaseAfterBase,
    #[error("a voter count is zero")]
    ZeroVoterCount,
    #[error("a voter count does not fit in usize")]
    VoterCountTooLarge,
}

impl TryFrom<&generated::Generation> for configuration::Generation {
    type Error = InvalidConfiguration;

    fn try_from(raw: &generated::Generation) -> Result<Self, Self::Error> {
        if raw.counter == u64::MAX {
            return Err(InvalidConfiguration::CounterAtMax);
        }
        Ok(configuration::Generation::new(
            raw.recovery_epoch,
            raw.term,
            raw.counter,
        ))
    }
}

impl TryFrom<&generated::Configuration> for configuration::Configuration {
    type Error = InvalidConfiguration;

    fn try_from(raw: &generated::Configuration) -> Result<Self, Self::Error> {
        let generation = raw
            .generation
            .as_ref()
            .ok_or(InvalidConfiguration::MissingGeneration)?;
        let generation = configuration::Generation::try_from(generation)?;
        let base = raw.base.as_ref().ok_or(InvalidConfiguration::MissingBase)?;
        let base = configuration::Generation::try_from(base)?;
        if base > generation {
            return Err(InvalidConfiguration::BaseAfterGeneration);
        }

        use generated::configuration::Electorate;
        match raw
            .electorate
            .as_ref()
            .ok_or(InvalidConfiguration::MissingElectorate)?
        {
            Electorate::Single(single) => {
                let voter_count = checked_voter_count(single.voter_count)?;
                Ok(configuration::Configuration::single(Single {
                    generation,
                    base,
                    voter_count,
                }))
            }
            Electorate::Joint(joint) => {
                let batch_generation = joint
                    .batch_generation
                    .as_ref()
                    .ok_or(InvalidConfiguration::MissingBatchGeneration)?;
                let batch_generation = configuration::Generation::try_from(batch_generation)?;
                if base > batch_generation {
                    return Err(InvalidConfiguration::BaseAfterBatchGeneration);
                }
                if batch_generation > generation {
                    return Err(InvalidConfiguration::BatchGenerationAfterGeneration);
                }
                let old_base = joint
                    .old_base
                    .as_ref()
                    .ok_or(InvalidConfiguration::MissingOldBase)?;
                let old_base = configuration::Generation::try_from(old_base)?;
                let old_generation = joint
                    .old_generation
                    .as_ref()
                    .ok_or(InvalidConfiguration::MissingOldGeneration)?;
                let old_generation = configuration::Generation::try_from(old_generation)?;
                if old_base > old_generation {
                    return Err(InvalidConfiguration::OldBaseAfterOldGeneration);
                }
                if old_generation >= batch_generation {
                    return Err(InvalidConfiguration::OldGenerationNotBeforeBatchGeneration);
                }
                if old_base > base {
                    return Err(InvalidConfiguration::OldBaseAfterBase);
                }
                let old_voter_count = checked_voter_count(joint.old_voter_count)?;
                let new_voter_count = checked_voter_count(joint.new_voter_count)?;
                Ok(configuration::Configuration::joint(Joint {
                    generation,
                    base,
                    batch_generation,
                    old_base,
                    old_generation,
                    old_voter_count,
                    new_voter_count,
                }))
            }
        }
    }
}

/// A voter count as decoded from the wire: nonzero, and representable as
/// `usize`. On every platform this ships on, `usize` is 64 bits wide, so
/// `VoterCountTooLarge` cannot actually trigger here; the check exists so
/// decode stays correct if that ever changes.
fn checked_voter_count(raw: u64) -> Result<usize, InvalidConfiguration> {
    if raw == 0 {
        return Err(InvalidConfiguration::ZeroVoterCount);
    }
    usize::try_from(raw).map_err(|_| InvalidConfiguration::VoterCountTooLarge)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::{Configuration, Generation};

    fn generation(recovery_epoch: u64, term: u64, counter: u64) -> generated::Generation {
        generated::Generation {
            recovery_epoch,
            term,
            counter,
        }
    }

    fn single_electorate(voter_count: u64) -> Option<generated::configuration::Electorate> {
        Some(generated::configuration::Electorate::Single(
            generated::SingleElectorate { voter_count },
        ))
    }

    /// A joint electorate moving from a configuration based at (1, 1, 0)
    /// with generation (1, 1, 4).
    fn joint_electorate(
        batch_generation: Option<generated::Generation>,
        old_voter_count: u64,
        new_voter_count: u64,
    ) -> Option<generated::configuration::Electorate> {
        joint_electorate_from(
            Some(generation(1, 1, 0)),
            Some(generation(1, 1, 4)),
            batch_generation,
            old_voter_count,
            new_voter_count,
        )
    }

    fn joint_electorate_from(
        old_base: Option<generated::Generation>,
        old_generation: Option<generated::Generation>,
        batch_generation: Option<generated::Generation>,
        old_voter_count: u64,
        new_voter_count: u64,
    ) -> Option<generated::configuration::Electorate> {
        Some(generated::configuration::Electorate::Joint(
            generated::JointElectorate {
                batch_generation,
                old_voter_count,
                new_voter_count,
                old_base,
                old_generation,
            },
        ))
    }

    #[test]
    fn decode_rejects_each_invalid_configuration() {
        let base = generation(1, 2, 0);
        let current = generation(1, 2, 5);
        let max_counter = generation(1, 2, u64::MAX);

        let cases: Vec<(&str, generated::Configuration, InvalidConfiguration)> = vec![
            (
                "generation counter at u64::MAX",
                generated::Configuration {
                    generation: Some(max_counter),
                    base: Some(base),
                    electorate: single_electorate(1),
                },
                InvalidConfiguration::CounterAtMax,
            ),
            (
                "base counter at u64::MAX",
                generated::Configuration {
                    generation: Some(current),
                    base: Some(max_counter),
                    electorate: single_electorate(1),
                },
                InvalidConfiguration::CounterAtMax,
            ),
            (
                "missing generation",
                generated::Configuration {
                    generation: None,
                    base: Some(base),
                    electorate: single_electorate(1),
                },
                InvalidConfiguration::MissingGeneration,
            ),
            (
                "missing base",
                generated::Configuration {
                    generation: Some(current),
                    base: None,
                    electorate: single_electorate(1),
                },
                InvalidConfiguration::MissingBase,
            ),
            (
                "missing electorate",
                generated::Configuration {
                    generation: Some(current),
                    base: Some(base),
                    electorate: None,
                },
                InvalidConfiguration::MissingElectorate,
            ),
            (
                "joint missing batch_generation",
                generated::Configuration {
                    generation: Some(current),
                    base: Some(base),
                    electorate: joint_electorate(None, 1, 1),
                },
                InvalidConfiguration::MissingBatchGeneration,
            ),
            (
                "base after generation",
                generated::Configuration {
                    generation: Some(base),
                    base: Some(current),
                    electorate: single_electorate(1),
                },
                InvalidConfiguration::BaseAfterGeneration,
            ),
            (
                "joint base after batch_generation",
                generated::Configuration {
                    generation: Some(generation(1, 2, 5)),
                    base: Some(generation(1, 2, 4)),
                    electorate: joint_electorate(Some(generation(1, 2, 2)), 1, 1),
                },
                InvalidConfiguration::BaseAfterBatchGeneration,
            ),
            (
                "joint batch_generation after generation",
                generated::Configuration {
                    generation: Some(generation(1, 2, 5)),
                    base: Some(generation(1, 2, 0)),
                    electorate: joint_electorate(Some(generation(1, 2, 6)), 1, 1),
                },
                InvalidConfiguration::BatchGenerationAfterGeneration,
            ),
            (
                "joint missing old_base",
                generated::Configuration {
                    generation: Some(current),
                    base: Some(base),
                    electorate: joint_electorate_from(
                        None,
                        Some(generation(1, 1, 4)),
                        Some(base),
                        1,
                        1,
                    ),
                },
                InvalidConfiguration::MissingOldBase,
            ),
            (
                "joint missing old_generation",
                generated::Configuration {
                    generation: Some(current),
                    base: Some(base),
                    electorate: joint_electorate_from(
                        Some(generation(1, 1, 0)),
                        None,
                        Some(base),
                        1,
                        1,
                    ),
                },
                InvalidConfiguration::MissingOldGeneration,
            ),
            (
                "joint old_base after old_generation",
                generated::Configuration {
                    generation: Some(current),
                    base: Some(base),
                    electorate: joint_electorate_from(
                        Some(generation(1, 1, 4)),
                        Some(generation(1, 1, 0)),
                        Some(base),
                        1,
                        1,
                    ),
                },
                InvalidConfiguration::OldBaseAfterOldGeneration,
            ),
            (
                "joint old_generation at the batch generation",
                generated::Configuration {
                    generation: Some(current),
                    base: Some(base),
                    electorate: joint_electorate_from(
                        Some(generation(1, 1, 0)),
                        Some(base),
                        Some(base),
                        1,
                        1,
                    ),
                },
                InvalidConfiguration::OldGenerationNotBeforeBatchGeneration,
            ),
            (
                "joint old_base after base",
                generated::Configuration {
                    generation: Some(current),
                    base: Some(generation(1, 1, 0)),
                    electorate: joint_electorate_from(
                        Some(generation(1, 1, 2)),
                        Some(generation(1, 1, 4)),
                        Some(generation(1, 2, 0)),
                        1,
                        1,
                    ),
                },
                InvalidConfiguration::OldBaseAfterBase,
            ),
            (
                "joint old_generation counter at u64::MAX",
                generated::Configuration {
                    generation: Some(current),
                    base: Some(base),
                    electorate: joint_electorate_from(
                        Some(generation(1, 1, 0)),
                        Some(max_counter),
                        Some(base),
                        1,
                        1,
                    ),
                },
                InvalidConfiguration::CounterAtMax,
            ),
            (
                "zero single voter count",
                generated::Configuration {
                    generation: Some(current),
                    base: Some(base),
                    electorate: single_electorate(0),
                },
                InvalidConfiguration::ZeroVoterCount,
            ),
            (
                "zero joint old_voter_count",
                generated::Configuration {
                    generation: Some(current),
                    base: Some(base),
                    electorate: joint_electorate(Some(base), 0, 1),
                },
                InvalidConfiguration::ZeroVoterCount,
            ),
            (
                "zero joint new_voter_count",
                generated::Configuration {
                    generation: Some(current),
                    base: Some(base),
                    electorate: joint_electorate(Some(base), 1, 0),
                },
                InvalidConfiguration::ZeroVoterCount,
            ),
        ];

        for (case, wire, expected) in cases {
            assert_eq!(
                Configuration::try_from(&wire),
                Err(expected),
                "case: {case}"
            );
        }
    }

    #[test]
    fn a_valid_single_configuration_decodes() {
        let base = generation(1, 2, 0);
        let current = generation(1, 2, 5);
        let wire = generated::Configuration {
            generation: Some(current),
            base: Some(base),
            electorate: single_electorate(3),
        };

        let decoded = Configuration::try_from(&wire).expect("valid configuration must decode");

        assert_eq!(
            decoded,
            Configuration::single(Single {
                generation: Generation::new(1, 2, 5),
                base: Generation::new(1, 2, 0),
                voter_count: 3,
            })
        );
        assert_eq!(
            generated::Configuration::from(&decoded),
            wire,
            "and encodes back"
        );
    }

    #[test]
    fn a_valid_joint_configuration_decodes() {
        let base = generation(1, 2, 0);
        let batch = generation(1, 2, 3);
        let current = generation(1, 2, 5);
        let wire = generated::Configuration {
            generation: Some(current),
            base: Some(base),
            electorate: joint_electorate(Some(batch), 3, 5),
        };

        let decoded = Configuration::try_from(&wire).expect("valid configuration must decode");

        assert_eq!(
            decoded,
            Configuration::joint(Joint {
                generation: Generation::new(1, 2, 5),
                base: Generation::new(1, 2, 0),
                batch_generation: Generation::new(1, 2, 3),
                old_base: Generation::new(1, 1, 0),
                old_generation: Generation::new(1, 1, 4),
                old_voter_count: 3,
                new_voter_count: 5,
            })
        );
        assert_eq!(
            generated::Configuration::from(&decoded),
            wire,
            "and encodes back"
        );
    }

    #[test]
    fn a_counter_of_u64_max_minus_one_decodes() {
        let raw = generation(1, 2, u64::MAX - 1);

        let decoded = Generation::try_from(&raw).expect("u64::MAX - 1 must decode");

        assert_eq!(decoded, Generation::new(1, 2, u64::MAX - 1));
    }
}
