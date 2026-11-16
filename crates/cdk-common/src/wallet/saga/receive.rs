//! Receive saga types

use core::fmt;

use cashu::PreSwap;
use serde::{Deserialize, Serialize};

use crate::database::wallet::ProofChange;
use crate::wallet::{ProofInfo, Transaction};
use crate::Error;

/// States specific to receive saga
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiveSagaState {
    /// Exact inputs recorded; no mint request can have been sent.
    InputsRecorded,
    /// Rollback owns this journal version.
    Cancelling,
    /// The exact signed request may have reached the mint.
    Redeeming,
    /// Verified outputs saved before atomic proof import.
    OutputsReady,
    /// Proof import and incoming receipt publication completed.
    Completed,
}

impl std::fmt::Display for ReceiveSagaState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InputsRecorded => write!(f, "inputs_recorded"),
            Self::Cancelling => write!(f, "cancelling"),
            Self::Redeeming => write!(f, "redeeming"),
            Self::OutputsReady => write!(f, "outputs_ready"),
            Self::Completed => write!(f, "completed"),
        }
    }
}

impl std::str::FromStr for ReceiveSagaState {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "inputs_recorded" => Ok(Self::InputsRecorded),
            "cancelling" => Ok(Self::Cancelling),
            "redeeming" => Ok(Self::Redeeming),
            "outputs_ready" => Ok(Self::OutputsReady),
            "completed" => Ok(Self::Completed),
            _ => Err(Error::InvalidOperationState),
        }
    }
}

/// Operation-specific data for Receive operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiveOperationData {
    /// Exact evidence required for recovery; absent or incomplete data fails closed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<Box<ReceiveCheckpoint>>,
}

/// Durable receive evidence; contains bearer secrets and must be stored privately.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiveCheckpoint {
    /// Original records and their exact acquired replacements.
    pub inputs: Vec<ProofChange>,
    /// Full signed request, saved before the mint can be contacted.
    pub pre_swap: Option<PreSwap>,
    /// Stable incoming receipt, including the original timestamp and metadata.
    pub transaction: Transaction,
    /// Complete verified outputs, saved before local handoff.
    pub outputs: Option<Vec<ProofInfo>>,
}

impl fmt::Debug for ReceiveCheckpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceiveCheckpoint").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::{ReceiveOperationData, ReceiveSagaState};

    #[test]
    fn receive_phases_roundtrip() {
        for state in [
            ReceiveSagaState::InputsRecorded,
            ReceiveSagaState::Cancelling,
            ReceiveSagaState::Redeeming,
            ReceiveSagaState::OutputsReady,
            ReceiveSagaState::Completed,
        ] {
            assert_eq!(
                state.to_string().parse::<ReceiveSagaState>().unwrap(),
                state
            );
            let encoded = serde_json::to_string(&state).unwrap();
            assert_eq!(
                serde_json::from_str::<ReceiveSagaState>(&encoded).unwrap(),
                state
            );
        }
    }

    #[test]
    fn missing_receive_checkpoint_remains_explicitly_absent() {
        let data: ReceiveOperationData = serde_json::from_str("{}").unwrap();
        assert!(data.checkpoint.is_none());
        assert_eq!(serde_json::to_string(&data).unwrap(), "{}");
    }
}
