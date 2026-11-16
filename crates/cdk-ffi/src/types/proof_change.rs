//! Atomic conditional wallet proof changes.
use super::{ProofInfo, PublicKey};
use crate::error::FfiError;

/// One complete proof record to compare and replace in an atomic batch.
#[derive(Debug, Clone, uniffi::Record)]
pub struct ProofChange {
    /// Identity of the compared record.
    pub y: PublicKey,
    /// Required original record; absent means the identity must not exist.
    pub before: Option<ProofInfo>,
    /// Replacement record; absent removes the matching record.
    pub after: Option<ProofInfo>,
}

impl From<cdk_common::database::wallet::ProofChange> for ProofChange {
    fn from(change: cdk_common::database::wallet::ProofChange) -> Self {
        Self {
            y: change.y.into(),
            before: change.before.map(Into::into),
            after: change.after.map(Into::into),
        }
    }
}

impl TryFrom<ProofChange> for cdk_common::database::wallet::ProofChange {
    type Error = FfiError;
    fn try_from(change: ProofChange) -> Result<Self, Self::Error> {
        Ok(Self {
            y: change.y.try_into()?,
            before: change.before.map(TryInto::try_into).transpose()?,
            after: change.after.map(TryInto::try_into).transpose()?,
        })
    }
}
