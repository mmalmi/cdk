//! Canonical paid-melt publication derived entirely from saved payment evidence.
use std::collections::HashSet;

use super::{validate_saga_completion, MAX_MELT_PAYMENT_PROOF_BYTES};
use crate::database::Error;
use crate::wallet::{
    MeltQuote, MeltSagaState, OperationData, OperationKind, Transaction, TransactionDirection,
    TransactionId, TransactionRole, TransactionStatus, WalletSaga, WalletSagaState,
};
use crate::{Amount, MeltQuoteState, State};

/// The exact prior receipt and final records for one atomic paid-melt transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeltCompletion {
    /// The Pending receipt that must already exist without any changes.
    pub pending: Transaction,
    /// The verified Completed receipt to publish.
    pub receipt: Transaction,
    /// The paid, unlocked quote with its version advanced once.
    pub quote: MeltQuote,
}

/// Validate a PaidReady to Completed transition and derive its atomic publication.
pub fn validate_melt_completion(
    expected: &WalletSaga,
    next: &WalletSaga,
    quote: &MeltQuote,
) -> Result<MeltCompletion, Error> {
    let invalid = || Error::Internal("Invalid paid melt completion".to_owned());
    let OperationData::Melt(data) = &expected.data else {
        return Err(invalid());
    };
    let payment = data.payment.as_ref().ok_or_else(invalid)?;
    let receipt = &payment.receipt;
    validate_saga_completion(expected, next, receipt)?;
    if expected.kind != OperationKind::Melt
        || expected.state != WalletSagaState::Melt(MeltSagaState::PaidReady)
        || next.state != WalletSagaState::Melt(MeltSagaState::Completed)
        || next.data != expected.data
        || data.amount != expected.amount
        || data.quote_id != quote.id
        || expected.quote_id.as_deref() != Some(quote.id.as_str())
        || quote.amount != expected.amount
        || quote.unit != expected.unit
        || quote
            .mint_url
            .as_ref()
            .is_some_and(|url| url != &expected.mint_url)
        || quote.fee_reserve != data.fee_reserve
        || quote.used_by_operation.as_deref() != Some(expected.id.to_string().as_str())
        || receipt.role != TransactionRole::Primary
        || receipt.direction != TransactionDirection::Outgoing
        || receipt.status != TransactionStatus::Completed
        || receipt.id() != TransactionId::from_saga_id(expected.id)
        || receipt.timestamp != expected.created_at
        || receipt.amount != expected.amount
        || receipt.metadata != data.metadata
        || receipt.metadata.contains_key("batch_quote_id")
        || receipt.quote_id.as_deref() != Some(quote.id.as_str())
        || receipt.payment_request.as_deref() != Some(quote.request.as_str())
        || receipt.payment_method.as_ref() != Some(&quote.payment_method)
        || quote
            .payment_proof
            .as_deref()
            .filter(|proof| !proof.is_empty())
            .is_some_and(|proof| receipt.payment_proof.as_deref() != Some(proof))
        || receipt
            .payment_proof
            .as_ref()
            .is_some_and(|proof| proof.len() > MAX_MELT_PAYMENT_PROOF_BYTES)
        || payment.inputs.is_empty()
    {
        return Err(invalid());
    }
    let change = payment.change.as_ref().ok_or_else(invalid)?;
    let ys: Vec<_> = payment.inputs.iter().map(|input| input.y).collect();
    if receipt.ys != ys || data.final_proof_ys.as_ref() != Some(&ys) {
        return Err(invalid());
    }
    let mut seen = HashSet::new();
    for input in &payment.inputs {
        if !seen.insert(input.y)
            || input.proof.y()? != input.y
            || input.mint_url != expected.mint_url
            || input.unit != expected.unit
            || input.state != State::Pending
            || input.used_by_operation != Some(expected.id)
        {
            return Err(invalid());
        }
    }
    for output in change {
        if !seen.insert(output.y)
            || output.proof.y()? != output.y
            || output.mint_url != expected.mint_url
            || output.unit != expected.unit
            || output.state != State::Unspent
            || output.used_by_operation.is_some()
            || output.created_by_operation != Some(expected.id)
        {
            return Err(invalid());
        }
    }
    let total = Amount::try_sum(payment.inputs.iter().map(|input| input.proof.amount))
        .map_err(|_| invalid())?;
    let change_total =
        Amount::try_sum(change.iter().map(|output| output.proof.amount)).map_err(|_| invalid())?;
    let budget = data.change_amount.unwrap_or(Amount::ZERO);
    if total
        .checked_sub(data.amount)
        .is_none_or(|available| available < budget)
        || budget < data.fee_reserve
        || change_total > budget
        || change_total < budget.saturating_sub(data.fee_reserve)
        || total
            .checked_sub(data.amount)
            .and_then(|value| value.checked_sub(change_total))
            != Some(receipt.fee)
    {
        return Err(invalid());
    }
    let mut pending = receipt.clone();
    pending.status = TransactionStatus::Pending;
    pending.fee = data.fee_reserve;
    pending.payment_proof = None;
    let mut paid = quote.clone();
    paid.state = MeltQuoteState::Paid;
    paid.payment_proof = receipt.payment_proof.clone();
    paid.used_by_operation = None;
    paid.version = quote.version.checked_add(1).ok_or_else(invalid)?;
    Ok(MeltCompletion {
        pending,
        receipt: receipt.clone(),
        quote: paid,
    })
}
