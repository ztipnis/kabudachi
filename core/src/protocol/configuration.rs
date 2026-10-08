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
use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::generated;

impl TryFrom<&generated::Generation> for configuration::Generation {
    type Error = InvalidConfiguration;

    fn try_from(raw: &generated::Generation) -> Result<Self, Self::Error> {
        if raw.counter == u64::MAX {
            return Err(InvalidConfiguration::CounterAtMax);
        }
        Ok(configuration::Generation::new(
            RecoveryEpoch::new(raw.recovery_epoch, raw.recovery_epoch_lineage),
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
