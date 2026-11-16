//! In-memory handles; complete private intent lives in the durable saga.
use core::fmt;

use cdk_common::wallet::{KeysetLoadPolicy, WalletSaga};
use uuid::Uuid;

use crate::nuts::Proofs;

/// Operation assigned before preparing its durable request.
#[derive(Debug)]
pub struct Initial {
    /// Unique operation identity.
    pub operation_id: Uuid,
    /// Original keyset loading policy.
    pub keyset_policy: KeysetLoadPolicy,
}

/// Exact request saved and inputs acquired.
#[derive(Debug)]
pub struct Prepared {
    /// Authoritative request, custody snapshots and optimistic version.
    pub saga: WalletSaga,
}

/// Swap completed successfully.
pub struct Finalized {
    /// Recipient outputs, when a recipient amount was requested.
    pub send_proofs: Option<Proofs>,
}

impl fmt::Debug for Finalized {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Finalized")
            .field("send_proof_count", &self.send_proofs.as_ref().map(Vec::len))
            .finish()
    }
}
