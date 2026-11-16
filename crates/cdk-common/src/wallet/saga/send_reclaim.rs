//! Exact reclaim intent owned by the original send operation.
use core::fmt;

use serde::{Deserialize, Serialize};

use crate::nuts::PreSwap;
use crate::wallet::{ProofInfo, Transaction};

/// Saved request and refund receipt, retained until the wallet handoff completes.
/// Contains bearer secrets; persist privately and never include in logs.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendReclaim {
    /// Original token records, including ownership and spending authorization.
    pub inputs: Vec<ProofInfo>,
    /// Exact mint request and its replacement secrets.
    pub pre_swap: PreSwap,
    /// Stable net refund and original fee, for retries after completion.
    pub transaction: Transaction,
    /// Complete verified replacement coins, saved before wallet import.
    pub outputs: Option<Vec<ProofInfo>>,
}

impl fmt::Debug for SendReclaim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendReclaim").finish_non_exhaustive()
    }
}
