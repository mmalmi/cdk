//! Exact swap request and verified outputs, including nondeterministic secrets.
use serde::{Deserialize, Serialize};

use crate::amount::SplitTarget;
use crate::database::wallet::ProofChange;
use crate::nuts::{PreSwap, SpendingConditions};
use crate::wallet::SwapOutputs;
use crate::Amount;

/// Durable private intent for a wallet swap.
/// Contains bearer secrets and ephemeral keys; never log or publish it.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwapCheckpoint {
    /// Exact original and acquired records, saved before changing any input.
    /// A parent-owned input is represented by an unchanged reserved record.
    pub acquisition: Vec<ProofChange>,
    /// Original request, random secrets, blinding factors and P2BK keys.
    pub pre_swap: PreSwap,
    /// Nominal recipient value; absent when swapping all to self.
    pub amount: Option<Amount>,
    /// Original recipient/change selection policy.
    pub amount_split_target: SplitTarget,
    /// Conditions applied to recipient outputs.
    pub spending_conditions: Option<SpendingConditions>,
    /// Complete verified result saved before local proof handoff.
    pub ready: Option<SwapOutputs>,
}

impl std::fmt::Debug for SwapCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwapCheckpoint").finish_non_exhaustive()
    }
}
