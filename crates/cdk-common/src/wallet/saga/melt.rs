//! Melt saga types

use std::collections::HashMap;
use std::fmt;

use cashu::{BlindedMessage, PreSwap, PublicKey};
use serde::{Deserialize, Serialize};

use super::SwapOutputs;
use crate::database::wallet::ProofChange;
use crate::wallet::{ProofInfo, Transaction};
use crate::{Amount, Error};

/// States specific to melt saga
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeltSagaState {
    /// Proofs reserved and quote locked, ready to initiate payment
    ProofsReserved,
    /// Cancel preparation, recovering any preparatory swap before releasing coins.
    SwapCancelling,
    /// Melt request sent to mint, Lightning payment initiated
    MeltRequested,
    /// Lightning payment in progress, awaiting confirmation from network
    PaymentPending,
    /// Verified paid result retained until receipt and quote completion commit.
    PaidReady,
    /// Receipt, paid quote and terminal operation committed together.
    Completed,
}

impl fmt::Display for MeltSagaState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProofsReserved => write!(f, "proofs_reserved"),
            Self::SwapCancelling => write!(f, "swap_cancelling"),
            Self::MeltRequested => write!(f, "melt_requested"),
            Self::PaymentPending => write!(f, "payment_pending"),
            Self::PaidReady => write!(f, "paid_ready"),
            Self::Completed => write!(f, "completed"),
        }
    }
}

impl std::str::FromStr for MeltSagaState {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "proofs_reserved" => Ok(Self::ProofsReserved),
            "swap_cancelling" => Ok(Self::SwapCancelling),
            "melt_requested" => Ok(Self::MeltRequested),
            "payment_pending" => Ok(Self::PaymentPending),
            "paid_ready" => Ok(Self::PaidReady),
            "completed" => Ok(Self::Completed),
            _ => Err(Error::InvalidOperationState),
        }
    }
}

/// Operation-specific data for Melt operations
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeltOperationData {
    /// Canonical payment inputs, receipt and verified change retained for completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payment: Option<Box<MeltPayment>>,
    /// Exact preparatory swap owned by this melt, if denominations need changing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swap: Option<Box<MeltSwap>>,
    /// Exact original records and reservations committed with the quote and saga.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acquisition: Option<Vec<ProofChange>>,
    /// Quote ID
    pub quote_id: String,
    /// Amount to melt
    pub amount: Amount,
    /// Fee reserve
    pub fee_reserve: Amount,
    /// Derivation counter start
    pub counter_start: Option<u32>,
    /// Derivation counter end
    pub counter_end: Option<u32>,
    /// Change amount (if any)
    pub change_amount: Option<Amount>,
    /// User-defined metadata for the outgoing melt transaction.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
    /// Proof Ys actually sent to the melt request.
    ///
    /// Stored separately from `used_by_operation` so recovery can record the
    /// correct transaction inputs even after a pre-melt swap or after inputs
    /// were already marked spent before a crash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_proof_ys: Option<Vec<PublicKey>>,
    /// Blinded messages for change recovery
    ///
    /// Stored so that if a crash occurs after the mint accepts the melt,
    /// we can use these to query the mint for change signatures and reconstruct proofs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_blinded_messages: Option<Vec<BlindedMessage>>,
}

/// Exact mint request and verified outputs for a parent-owned denomination swap.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeltSwap {
    /// Complete reserved input records, retained when marking originals spent.
    pub inputs: Vec<ProofInfo>,
    /// Signed request and the exact output secrets allocated before submission.
    pub pre_swap: PreSwap,
    /// Amount reserved for the subsequent melt, including its fee budget.
    pub amount: Amount,
    /// Verified signatures unblinded into payment and change proofs.
    pub outputs: Option<SwapOutputs>,
}

impl fmt::Debug for MeltSwap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MeltSwap").finish_non_exhaustive()
    }
}

/// Exact payment evidence retained from submission through final receipt publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeltPayment {
    /// Complete original payment inputs in Pending state, in request order.
    pub inputs: Vec<ProofInfo>,
    /// Canonical Pending receipt, replaced by its verified Completed result.
    pub receipt: Transaction,
    /// Verified canonical change records; None means not yet verified, Some([]) means none owed.
    pub change: Option<Vec<ProofInfo>>,
}
