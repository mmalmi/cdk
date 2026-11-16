//! Conditional changes to original wallet proof records.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::database::Error;
use crate::wallet::ProofInfo;
use crate::PublicKey;

/// One proof record to compare and replace within an atomic batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProofChange {
    /// Identity of the record being compared.
    pub y: PublicKey,
    /// Required original record; `None` requires that the identity is absent.
    pub before: Option<ProofInfo>,
    /// Replacement record; `None` removes the matching original record.
    pub after: Option<ProofInfo>,
}

/// Reject duplicate identities and records whose secrets do not match their keys.
///
/// Backends must call this before any write, even if the original records would
/// otherwise fail their comparison. An empty batch is a successful no-op.
pub fn validate_proof_changes(changes: &[ProofChange]) -> Result<(), Error> {
    let mut seen = HashSet::new();
    for change in changes {
        if !seen.insert(change.y) {
            return Err(Error::Duplicate);
        }
        for proof in change.before.iter().chain(&change.after) {
            if proof.y != change.y || proof.proof.y()? != change.y {
                return Err(Error::Internal("Proof change identity mismatch".to_owned()));
            }
        }
    }
    Ok(())
}
