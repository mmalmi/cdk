//! Shared identity and receipt rules for an atomic saga completion.
use crate::database::Error;
use crate::wallet::{
    receive_identity, OperationKind, SendSagaState, SwapSagaState, Transaction,
    TransactionDirection, TransactionId, TransactionRole, TransactionStatus, WalletSaga,
    WalletSagaState,
};

/// Validate an operation-preserving, single-version transition and its receipt.
pub fn validate_saga_completion(
    expected: &WalletSaga,
    next: &WalletSaga,
    receipt: &Transaction,
) -> Result<(), Error> {
    receipt
        .validate_role()
        .map_err(|e| Error::Internal(e.to_string()))?;
    if matches!(
        next.state,
        WalletSagaState::Send(SendSagaState::TokenCreated)
    ) && (receipt.role != TransactionRole::Primary
        || receipt.direction != TransactionDirection::Outgoing
        || receipt.amount != expected.amount
        || receipt.timestamp != expected.created_at)
    {
        return Err(Error::Internal(
            "Send completion requires a primary receipt".to_owned(),
        ));
    }
    if expected.kind == OperationKind::Send
        && receipt.role == TransactionRole::Primary
        && receipt.id() != TransactionId::from_saga_id(expected.id)
    {
        return Err(Error::Internal(
            "Send completion requires its canonical primary receipt ID".to_owned(),
        ));
    }
    validate_saga_transition(expected, next)?;
    if (receipt
        .metadata
        .contains_key(receive_identity::METADATA_KEY)
        && expected.kind != OperationKind::Receive)
        || (receipt.role == TransactionRole::SendRefund && expected.kind != OperationKind::Send)
        || receipt.saga_id != Some(expected.id)
        || receipt.mint_url != expected.mint_url
        || receipt.unit != expected.unit
    {
        return Err(Error::Internal(
            "Invalid saga completion receipt".to_owned(),
        ));
    }
    Ok(())
}

/// Validate an operation-preserving, single-version transition.
pub fn validate_saga_transition(expected: &WalletSaga, next: &WalletSaga) -> Result<(), Error> {
    if expected.id != next.id
        || expected.kind != next.kind
        || expected.mint_url != next.mint_url
        || expected.unit != next.unit
        || expected.amount != next.amount
        || expected.created_at != next.created_at
        || expected.quote_id != next.quote_id
        || expected.version.checked_add(1) != Some(next.version)
        || next.updated_at < expected.updated_at
        || expected.state.kind() != expected.kind
        || expected.data.kind() != expected.kind
        || next.state.kind() != next.kind
        || next.data.kind() != next.kind
    {
        return Err(Error::Internal(
            "Invalid saga identity or version".to_owned(),
        ));
    }
    Ok(())
}

/// Merge status only when every immutable receipt field matches.
/// A pending retry preserves a terminal receipt; conflicting terminal results
/// reject the completion instead of changing already published history.
pub fn merge_saga_receipt(
    current: Option<Transaction>,
    proposed: Transaction,
) -> Option<Transaction> {
    let Some(mut current) = current else {
        return Some(proposed);
    };
    let old_status = current.status;
    current.status = proposed.status;
    if current != proposed {
        return None;
    }
    if old_status != TransactionStatus::Pending {
        if proposed.status != TransactionStatus::Pending && proposed.status != old_status {
            return None;
        }
        current.status = old_status;
    }
    Some(current)
}

/// Validate an output transition or a new standalone swap without quote ownership.
/// Mint and melt creation must use their atomic quote-acquisition contracts.
pub fn validate_output_preparation(
    expected: Option<&WalletSaga>,
    next: &WalletSaga,
    receipt: Option<&Transaction>,
) -> Result<(), Error> {
    match (expected, receipt) {
        (Some(expected), Some(receipt)) => validate_saga_completion(expected, next, receipt),
        (Some(expected), None) => validate_saga_transition(expected, next),
        (None, None)
            if next.kind == OperationKind::Swap
                && next.state == WalletSagaState::Swap(SwapSagaState::ProofsReserved)
                && next.data.kind() == next.kind
                && next.version == 0
                && next.created_at == next.updated_at
                && next.quote_id.is_none() =>
        {
            Ok(())
        }
        _ => Err(Error::InvalidDbResponse),
    }
}
