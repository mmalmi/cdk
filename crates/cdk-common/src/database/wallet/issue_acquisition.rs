//! Validate the complete initial issue intent before acquiring any quote.
use super::super::Error;
use crate::wallet::{IssueSagaState, MintQuote, OperationData, WalletSaga, WalletSagaState};
use crate::Amount;

/// Validate an initial issue intent and derive the reserved quote snapshots.
/// No quotes or operation records may be changed unless the complete acquisition succeeds.
pub fn validate_issue_acquisition(
    saga: &WalletSaga,
    quotes: &[MintQuote],
) -> Result<Vec<MintQuote>, Error> {
    let invalid = || Error::Internal("Invalid issue acquisition".to_owned());
    let OperationData::Mint(data) = &saga.data else {
        return Err(invalid());
    };
    if saga.state != WalletSagaState::Issue(IssueSagaState::SecretsPrepared)
        || saga.kind != saga.state.kind()
        || saga.version != 0
        || saga.created_at != saga.updated_at
        || saga.amount != data.amount
        || saga.amount == Amount::ZERO
        || saga.quote_id.as_deref() != Some(data.primary_quote_id())
        || quotes.is_empty()
        || data.quote_ids()
            != quotes
                .iter()
                .map(|quote| quote.id.clone())
                .collect::<Vec<_>>()
    {
        return Err(invalid());
    }
    let original = data.original_request().map_err(|_| invalid())?;
    if original.quotes != quotes {
        return Err(invalid());
    }
    let amounts = match &data.batch_quote_amounts {
        Some(amounts) => amounts.as_slice(),
        None => std::slice::from_ref(&data.amount),
    };
    let mut reserved = Vec::with_capacity(quotes.len());
    for (quote, amount) in quotes.iter().zip(amounts) {
        if quote.mint_url != saga.mint_url
            || quote.unit != saga.unit
            || quote.payment_method != original.payment_method
            || quote.used_by_operation.is_some()
            || *amount > quote.amount_mintable()
        {
            return Err(invalid());
        }
        let mut next = quote.clone();
        next.version = quote.version.checked_add(1).ok_or_else(invalid)?;
        next.used_by_operation = Some(saga.id.to_string());
        reserved.push(next);
    }
    Ok(reserved)
}
