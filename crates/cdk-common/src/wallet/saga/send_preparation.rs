//! Original reservation intent for a send that has not begun confirmation.
use core::fmt;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::wallet::ProofInfo;

/// Exact preparation retained until confirmation or cancellation handoff.
/// Contains bearer secrets; persist privately and never include in logs.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendPreparation {
    /// Exact expected reserved rows, including ownership and original authorization.
    /// In `PreparedAcquiring`, some or all rows may still be unacquired.
    pub inputs: Vec<ProofInfo>,
    /// Original caller metadata, including any application request binding.
    pub metadata: HashMap<String, String>,
    /// Keep completed cancellation evidence until the caller durably records it.
    pub retain_cancellation: bool,
}

impl fmt::Debug for SendPreparation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendPreparation").finish_non_exhaustive()
    }
}
