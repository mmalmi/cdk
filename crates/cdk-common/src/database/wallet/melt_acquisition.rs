//! Validation shared by native atomic melt acquisition backends.
use super::{validate_proof_changes, ProofChange};
use crate::database::Error;
use crate::wallet::{MeltQuote, MeltSagaState, OperationData, WalletSaga, WalletSagaState};
use crate::{MeltQuoteState, State};

/// Validate a new melt intent and return its complete input acquisition.
pub fn validate_melt_acquisition<'a>(
    saga: &'a WalletSaga,
    quote: &MeltQuote,
) -> Result<&'a [ProofChange], Error> {
    let invalid = || Error::Internal("Invalid melt acquisition".to_owned());
    let OperationData::Melt(data) = &saga.data else {
        return Err(invalid());
    };
    if saga.state != WalletSagaState::Melt(MeltSagaState::ProofsReserved)
        || saga.version != 0
        || saga.created_at != saga.updated_at
        || saga.quote_id.as_deref() != Some(quote.id.as_str())
        || data.quote_id != quote.id
        || saga.amount != quote.amount
        || data.amount != quote.amount
        || saga.unit != quote.unit
        || quote
            .mint_url
            .as_ref()
            .is_some_and(|url| url != &saga.mint_url)
        || data.fee_reserve != quote.fee_reserve
        || quote.used_by_operation.is_some()
        || quote.payment_proof.is_some()
        || !matches!(quote.state, MeltQuoteState::Unpaid | MeltQuoteState::Failed)
        || data.final_proof_ys.is_some()
        || data.swap.is_some()
        || data.payment.is_some()
        || data.counter_start.is_some()
        || data.counter_end.is_some()
        || data.change_amount.is_some()
        || data.change_blinded_messages.is_some()
    {
        return Err(invalid());
    }
    quote.version.checked_add(1).ok_or_else(invalid)?;
    validate_melt_inputs(saga)
}

/// Validate the exact original and reserved input evidence retained by a melt.
pub fn validate_melt_inputs(saga: &WalletSaga) -> Result<&[ProofChange], Error> {
    let invalid = || Error::Internal("Invalid melt input evidence".to_owned());
    let OperationData::Melt(data) = &saga.data else {
        return Err(invalid());
    };
    if saga.state.kind() != saga.kind
        || saga.kind != crate::wallet::OperationKind::Melt
        || saga.amount != data.amount
        || saga.quote_id.as_deref() != Some(data.quote_id.as_str())
    {
        return Err(invalid());
    }
    let changes = data.acquisition.as_deref().ok_or_else(invalid)?;
    validate_proof_changes(changes)?;
    if changes.is_empty() {
        return Err(invalid());
    }
    for change in changes {
        let after = change.after.as_ref().ok_or_else(invalid)?;
        if after.state != State::Reserved
            || after.used_by_operation != Some(saga.id)
            || after.mint_url != saga.mint_url
            || after.unit != saga.unit
        {
            return Err(invalid());
        }
        if let Some(before) = &change.before {
            let mut expected = before.clone();
            expected.state = State::Reserved;
            expected.used_by_operation = Some(saga.id);
            if before.state != State::Unspent
                || before.used_by_operation.is_some()
                || &expected != after
            {
                return Err(invalid());
            }
        } else if after.created_by_operation.is_some() || after.derivation_index.is_some() {
            return Err(invalid());
        }
    }
    Ok(changes)
}
