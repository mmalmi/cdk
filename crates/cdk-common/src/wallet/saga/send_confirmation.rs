//! Exact wallet send intent, saved before contacting the mint.
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::nuts::{Id, PreSwap, Proofs, SpendingConditions};
use crate::Amount;

/// A verified split of swap outputs, before importing them into the wallet.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwapOutputs {
    /// Outputs intended for the recipient.
    pub send: Proofs,
    /// Outputs retained by the wallet.
    pub change: Proofs,
}

/// Immutable confirmation parameters and optional verified outputs.
///
/// Contains bearer secrets; persist privately and never include in logs.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendConfirmation {
    /// The exact optional swap request and its output secrets.
    pub pre_swap: Option<PreSwap>,
    /// Nominal value requested from the swap for the recipient.
    pub swap_amount: Amount,
    /// Conditions applied to the swap's recipient outputs.
    pub spending_conditions: Option<SpendingConditions>,
    /// Already reserved proofs that bypass the swap, with any required signatures.
    pub retained_proofs: Proofs,
    /// Reserve added to the requested amount for redemption and rounding.
    pub send_fee: Amount,
    /// Whether the recipient's redemption fee is covered.
    pub include_fee: bool,
    /// Maximum approved wallet debit, including fees.
    pub max_total_amount: Option<Amount>,
    /// Required keyset for all recipient proofs.
    pub required_keyset_id: Option<Id>,
    /// Memo included in the token and transaction.
    pub token_memo: Option<String>,
    /// Caller transaction metadata.
    pub metadata: HashMap<String, String>,
    /// Complete verified outputs saved before local import.
    pub ready: Option<SwapOutputs>,
}

impl std::fmt::Debug for SendConfirmation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendConfirmation").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for SwapOutputs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwapOutputs").finish_non_exhaustive()
    }
}
