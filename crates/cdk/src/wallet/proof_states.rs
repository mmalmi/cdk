//! Validate a complete NUT-07 answer before changing local financial state.
use std::collections::HashSet;

use crate::nuts::{ProofState, PublicKey, State};
use crate::Error;

pub(super) fn validate(ys: &[PublicKey], states: &[ProofState]) -> Result<(), Error> {
    let mut seen = HashSet::new();
    if ys.is_empty()
        || states.len() != ys.len()
        || ys.iter().zip(states).any(|(y, state)| {
            !seen.insert(*y)
                || state.y != *y
                || !matches!(state.state, State::Unspent | State::Pending | State::Spent)
        })
    {
        return Err(Error::InvalidMintResponse(
            "proof states do not match the requested identities, order or mint states".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod spent_tests;
