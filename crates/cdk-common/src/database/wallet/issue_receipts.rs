//! Canonical issue receipts derived only from the original checkpoint.
use std::collections::{HashMap, HashSet};

use crate::database::Error;
use crate::wallet::{
    OperationData, Transaction, TransactionDirection, TransactionRole, TransactionStatus,
    WalletSaga, WalletSagaState,
};
use crate::Amount;

/// Build pending receipts with stable identities, timestamps and original output ownership.
pub fn issue_receipts(saga: &WalletSaga) -> Result<Vec<Transaction>, Error> {
    let invalid = || Error::Internal("Invalid original issue receipts".to_owned());
    let OperationData::Mint(data) = &saga.data else {
        return Err(invalid());
    };
    if !matches!(saga.state, WalletSagaState::Issue(_))
        || saga.kind != saga.state.kind()
        || saga.amount != data.amount
        || saga.quote_id.as_deref() != Some(data.primary_quote_id())
    {
        return Err(invalid());
    }
    let saved = data.original_request().map_err(|_| invalid())?;
    if saved
        .quotes
        .iter()
        .map(|quote| &quote.id)
        .ne(data.quote_ids().iter())
        || saved.quotes.iter().any(|quote| {
            quote.mint_url != saga.mint_url
                || quote.unit != saga.unit
                || quote.payment_method != saved.payment_method
                || quote.used_by_operation.is_some()
        })
        || saved.premint_secrets.len() != saved.request.outputs().len()
    {
        return Err(invalid());
    }
    let counts = data
        .batch_output_counts
        .clone()
        .unwrap_or_else(|| vec![saved.premint_secrets.len()]);
    let amounts = data
        .batch_quote_amounts
        .clone()
        .unwrap_or_else(|| vec![data.amount]);
    let mut offset = 0usize;
    let mut seen = HashSet::new();
    let mut receipts = Vec::with_capacity(saved.quotes.len());
    for ((quote, count), amount) in saved.quotes.iter().zip(counts).zip(amounts) {
        let end = offset.checked_add(count).ok_or_else(invalid)?;
        let secrets = saved
            .premint_secrets
            .secrets
            .get(offset..end)
            .ok_or_else(invalid)?;
        offset = end;
        let ys: Vec<_> = secrets
            .iter()
            .map(|secret| cashu::dhke::hash_to_curve(secret.secret.as_bytes()))
            .collect::<Result<_, _>>()?;
        if ys.iter().any(|y| !seen.insert(*y)) {
            return Err(invalid());
        }
        let metadata = if saved.quotes.len() > 1 {
            HashMap::from([("batch_quote_id".to_owned(), quote.id.clone())])
        } else {
            HashMap::new()
        };
        receipts.push(Transaction {
            role: TransactionRole::Primary,
            mint_url: saga.mint_url.clone(),
            direction: TransactionDirection::Incoming,
            amount,
            fee: Amount::ZERO,
            unit: saga.unit.clone(),
            ys,
            timestamp: saga.created_at,
            memo: None,
            metadata,
            quote_id: Some(quote.id.clone()),
            payment_request: Some(quote.request.clone()),
            payment_proof: None,
            payment_method: Some(quote.payment_method.clone()),
            saga_id: Some(saga.id),
            status: TransactionStatus::Pending,
        });
    }
    if offset != saved.premint_secrets.len() || receipts.len() != saved.quotes.len() {
        return Err(invalid());
    }
    Ok(receipts)
}
