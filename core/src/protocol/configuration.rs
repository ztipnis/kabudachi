//! Checked decode of the wire [`generated::Generation`]/[`generated::Configuration`]
//! into the domain [`configuration::Generation`]/[`configuration::Configuration`]
//! and their infallible encode back to the wire.
//!
//! Decode is where untrusted configuration data from a peer is validated
//! (STYLE_GUIDE "validate untrusted input at the edge"). It checks only what
//! the wire can get wrong that a domain value cannot: a field left absent, a
//! counter at `u64::MAX`, a voter count too large for `usize`. The
//! configuration rules (ordering, no empty side) belong to the domain
//! constructors ([`configuration::Configuration::single`] and
//! [`configuration::Configuration::joint`]), which decode calls, so a peer's
//! configuration is held to the same rules as a local one. Encode is
//! infallible and lives with the domain type (`impl From<&Configuration> for
//! generated::Configuration` in `crate::configuration`), since it needs that
//! type's private `Electorate` field.

use crate::configuration::{self, InvalidConfiguration, Joint, Single};
use crate::protocol::generated;

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

        use generated::configuration::Electorate;
        match raw
            .electorate
            .as_ref()
            .ok_or(InvalidConfiguration::MissingElectorate)?
        {
            Electorate::Single(single) => {
                let voter_count = voter_count(single.voter_count)?;
                configuration::Configuration::single(Single {
                    generation,
                    base,
                    voter_count,
                })
            }
            Electorate::Joint(joint) => {
                let batch_generation = joint
                    .batch_generation
                    .as_ref()
                    .ok_or(InvalidConfiguration::MissingBatchGeneration)?;
                let batch_generation = configuration::Generation::try_from(batch_generation)?;
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
                let old_voter_count = voter_count(joint.old_voter_count)?;
                let new_voter_count = voter_count(joint.new_voter_count)?;
                configuration::Configuration::joint(Joint {
                    generation,
                    base,
                    batch_generation,
                    old_base,
                    old_generation,
                    old_voter_count,
                    new_voter_count,
                })
            }
        }
    }
}

/// A voter count as decoded from the wire, representable as `usize`. On every
/// platform this ships on, `usize` is 64 bits wide, so `VoterCountTooLarge`
/// cannot actually trigger here; the check exists so decode stays correct if
/// that ever changes. A zero count is the constructor's to refuse.
fn voter_count(raw: u64) -> Result<usize, InvalidConfiguration> {
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
            .expect("valid")
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
            .expect("valid")
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
