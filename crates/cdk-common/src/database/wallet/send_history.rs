//! Shared validation for explicitly released token history.
use std::collections::{HashMap, HashSet};

use crate::database::Error;
use crate::nuts::{PublicKey, State};
use crate::wallet::{
    receive_identity, ProofInfo, Transaction, TransactionDirection, TransactionId, TransactionRole,
    TransactionStatus,
};

/// Validate a bounded batch of outgoing token transactions, never refund/melt rows.
pub fn validate_send_history_retirement(expected: &[Transaction]) -> Result<(), Error> {
    validate_token_history_retirement(expected, TransactionDirection::Outgoing)
}

/// Validate a bounded batch of completed incoming token receipts, never mint/melt rows.
pub fn validate_incoming_history_retirement(expected: &[Transaction]) -> Result<(), Error> {
    validate_token_history_retirement(expected, TransactionDirection::Incoming)
}

/// Shared native-backend validation for one direction of token history.
pub fn validate_token_history_retirement(
    expected: &[Transaction],
    direction: TransactionDirection,
) -> Result<(), Error> {
    if expected.len() > 128 {
        return Err(Error::Internal(
            "Token history batch exceeds 128 records".to_owned(),
        ));
    }
    let mut ids = HashSet::new();
    let mut operations = HashSet::new();
    for tx in expected {
        let canonical = tx.saga_id.is_some_and(|operation| {
            tx.id()
                == match tx.role {
                    TransactionRole::Primary => match tx
                        .metadata
                        .get(receive_identity::METADATA_KEY)
                    {
                        Some(fingerprint) => TransactionId::from_receive_fingerprint(fingerprint),
                        None => TransactionId::from_saga_id(operation),
                    },
                    TransactionRole::SendRefund => TransactionId::from_send_refund(operation),
                }
        });
        let terminal = match direction {
            TransactionDirection::Outgoing => {
                tx.role == TransactionRole::Primary && tx.status != TransactionStatus::Pending
            }
            TransactionDirection::Incoming => tx.status == TransactionStatus::Completed,
        };
        if tx.validate_role().is_err()
            || tx.direction != direction
            || tx.saga_id.is_none()
            || !operations.insert(tx.saga_id)
            || tx.ys.is_empty()
            || tx.ys.iter().collect::<HashSet<_>>().len() != tx.ys.len()
            || !canonical
            || !terminal
            || !ids.insert(tx.id().as_slice().to_vec())
            || tx.quote_id.is_some()
            || tx.payment_request.is_some()
            || tx.payment_proof.is_some()
            || tx.payment_method.is_some()
        {
            return Err(Error::Internal("Invalid token history batch".to_owned()));
        }
    }
    Ok(())
}

/// The entire original token must remain recorded as spent under its send owner.
pub fn send_history_inputs_spent(tx: &Transaction, inputs: &HashMap<PublicKey, ProofInfo>) -> bool {
    tx.ys.iter().all(|y| {
        inputs
            .get(y)
            .is_some_and(|p| token_history_input_spent(tx, p, TransactionDirection::Outgoing))
    })
}

/// Received-token originals must remain spent, including self-receives that
/// preserve a prior send owner. Their gross value must match the saved receipt.
pub fn incoming_history_inputs_spent(
    tx: &Transaction,
    inputs: &HashMap<PublicKey, ProofInfo>,
) -> bool {
    if tx.metadata.contains_key(receive_identity::METADATA_KEY) {
        let proofs = tx
            .ys
            .iter()
            .filter_map(|y| inputs.get(y).map(|p| p.proof.clone()))
            .collect::<Vec<_>>();
        if receive_identity::transaction_id(&tx.mint_url, &tx.unit, &proofs).ok() != Some(tx.id()) {
            return false;
        }
    }
    let gross = tx.ys.iter().try_fold(0u64, |total, y| {
        let proof = inputs.get(y)?;
        if !token_history_input_spent(tx, proof, TransactionDirection::Incoming) {
            return None;
        }
        total.checked_add(proof.proof.amount.to_u64())
    });
    gross.is_some() && gross == tx.amount.to_u64().checked_add(tx.fee.to_u64())
}

/// Check one exact input's spent state, wallet scope and required operation owner.
/// Received tokens may retain a previous send owner after a self-receive.
pub fn token_history_input_spent(
    tx: &Transaction,
    proof: &ProofInfo,
    direction: TransactionDirection,
) -> bool {
    proof.state == State::Spent
        && proof.mint_url == tx.mint_url
        && proof.unit == tx.unit
        && ((direction == TransactionDirection::Incoming && tx.role == TransactionRole::Primary)
            || proof.used_by_operation == tx.saga_id)
}

/// Check the retained original coin evidence for the selected history direction.
pub fn token_history_inputs_spent(
    tx: &Transaction,
    inputs: &HashMap<PublicKey, ProofInfo>,
    direction: TransactionDirection,
) -> bool {
    match direction {
        TransactionDirection::Outgoing => send_history_inputs_spent(tx, inputs),
        TransactionDirection::Incoming => incoming_history_inputs_spent(tx, inputs),
    }
}
