//! Swap saga types

use serde::{Deserialize, Serialize};

use crate::{Amount, Error};

/// States specific to swap saga (wallet-side)
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwapSagaState {
    /// Exact request and original reservation plan saved; admission may be incomplete.
    ProofsReserved,
    /// Unsubmitted acquisition withdrawn; restore only unchanged owned inputs.
    Cancelling,
    /// Verified outputs saved before the atomic proof handoff.
    OutputsReady,
    /// Swap request sent to mint, awaiting signatures for new proofs
    SwapRequested,
}

impl std::fmt::Display for SwapSagaState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SwapSagaState::ProofsReserved => write!(f, "proofs_reserved"),
            Self::Cancelling => write!(f, "cancelling"),
            Self::OutputsReady => write!(f, "outputs_ready"),
            SwapSagaState::SwapRequested => write!(f, "swap_requested"),
        }
    }
}

impl std::str::FromStr for SwapSagaState {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "proofs_reserved" => Ok(SwapSagaState::ProofsReserved),
            "cancelling" => Ok(Self::Cancelling),
            "outputs_ready" => Ok(Self::OutputsReady),
            "swap_requested" => Ok(SwapSagaState::SwapRequested),
            _ => Err(Error::InvalidOperationState),
        }
    }
}

/// Operation-specific data for Swap operations
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwapOperationData {
    /// Exact private intent required for new swap execution and recovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<Box<super::SwapCheckpoint>>,
    /// Input amount
    pub input_amount: Amount,
    /// Output amount
    pub output_amount: Amount,
}
