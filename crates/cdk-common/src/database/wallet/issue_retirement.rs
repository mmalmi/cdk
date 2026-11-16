//! One terminal transaction publishes original outputs and receipts and releases ownership.
use super::{issue_receipts, merge_saga_receipt, validate_proof_changes, ProofChange};
use crate::database::Error;
use crate::wallet::{
    IssueSagaState, MintQuote, OperationData, Transaction, TransactionStatus, WalletSaga,
    WalletSagaState,
};
use crate::{MintQuoteState, PaymentMethod, State};

/// Quote updates and canonical receipts for one exact issue journal.
#[derive(Debug)]
pub struct IssueRetirement {
    /// Updated and unlocked quote snapshots with checked versions.
    pub quotes: Vec<MintQuote>,
    receipts: Vec<Transaction>,
    submitted: bool,
}

/// Validate the original proof changes and complete quote ownership before any writes.
pub fn validate_issue_retirement(
    expected: &WalletSaga,
    quotes: &[MintQuote],
    proofs: Option<&[ProofChange]>,
) -> Result<IssueRetirement, Error> {
    let invalid = || Error::Internal("Invalid issue retirement".to_owned());
    let OperationData::Mint(data) = &expected.data else {
        return Err(invalid());
    };
    let WalletSagaState::Issue(state) = expected.state else {
        return Err(invalid());
    };
    let receipts = issue_receipts(expected)?;
    let original = data.original_request().map_err(|_| invalid())?;
    let submitted = state == IssueSagaState::MintRequested;
    if submitted != proofs.is_some() || quotes.len() != original.quotes.len() {
        return Err(invalid());
    }
    if let Some(changes) = proofs {
        validate_proof_changes(changes)?;
        if changes.len() != original.premint_secrets.len() {
            return Err(invalid());
        }
        for (change, secret) in changes.iter().zip(&original.premint_secrets.secrets) {
            let output = change.after.as_ref().ok_or_else(invalid)?;
            if output.proof.secret != secret.secret
                || output.proof.amount != secret.amount
                || output.proof.keyset_id != secret.blinded_message.keyset_id
                || output.mint_url != expected.mint_url
                || output.unit != expected.unit
                || output.created_by_operation != Some(expected.id)
                || output.derivation_index != secret.derivation_index
                || change
                    .before
                    .as_ref()
                    .is_some_and(|before| before != output)
                || (change.before.is_none()
                    && (output.state != State::Unspent || output.used_by_operation.is_some()))
            {
                return Err(invalid());
            }
        }
    }
    let owner = expected.id.to_string();
    let mut released = Vec::with_capacity(quotes.len());
    for ((quote, original), receipt) in quotes.iter().zip(&original.quotes).zip(&receipts) {
        if quote.id != original.id
            || quote.mint_url != original.mint_url
            || quote.unit != original.unit
            || quote.payment_method != original.payment_method
            || quote.request != original.request
            || quote.used_by_operation.as_ref() != Some(&owner)
        {
            return Err(invalid());
        }
        let mut next = quote.clone();
        if submitted {
            let issued = original
                .amount_issued
                .checked_add(receipt.amount)
                .ok_or_else(invalid)?;
            if quote.amount_issued < original.amount_issued || quote.amount_issued > issued {
                return Err(invalid());
            }
            next.amount_issued = issued;
            if quote.payment_method == PaymentMethod::BOLT11 {
                next.state = MintQuoteState::Issued;
            }
        }
        next.used_by_operation = None;
        next.version = next.version.checked_add(1).ok_or_else(invalid)?;
        released.push(next);
    }
    Ok(IssueRetirement {
        quotes: released,
        receipts,
        submitted,
    })
}

impl IssueRetirement {
    /// Canonical originals for bounded reads in the parent retirement transaction.
    pub fn original_receipts(&self) -> &[Transaction] {
        &self.receipts
    }

    /// Complete only matching pending receipts; preserve exact completed results.
    /// Cancellation may fail an existing pending receipt but cannot invent one.
    pub fn receipts(&self, receipts: Vec<Option<Transaction>>) -> Option<Vec<Transaction>> {
        if receipts.len() != self.receipts.len() {
            return None;
        }
        let mut updates = Vec::new();
        for (current, original) in receipts.into_iter().zip(&self.receipts) {
            let Some(current) = current else {
                if self.submitted {
                    return None;
                }
                continue;
            };
            let mut receipt = original.clone();
            receipt.status = if self.submitted {
                TransactionStatus::Completed
            } else {
                TransactionStatus::Failed
            };
            updates.push(merge_saga_receipt(Some(current), receipt)?);
        }
        Some(updates)
    }
}
