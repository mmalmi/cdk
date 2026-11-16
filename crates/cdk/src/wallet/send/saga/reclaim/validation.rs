//! Validate saved reclaim custody and the exact fee-adjusted refund.
use std::collections::{HashMap, HashSet};

use cdk_common::wallet::{
    OperationData, SendSagaState, TransactionDirection, TransactionRole, WalletSaga,
    WalletSagaState,
};

use super::intent;
use crate::nuts::{ProofsMethods, State};
use crate::wallet::swap::outputs;
use crate::{Error, Wallet};

pub(super) fn validate(wallet: &Wallet, saga: &WalletSaga) -> Result<(), Error> {
    if !matches!(
        saga.state,
        WalletSagaState::Send(
            SendSagaState::ReclaimPreparing
                | SendSagaState::ReclaimCancelling
                | SendSagaState::Reclaiming
                | SendSagaState::Reclaimed
        )
    ) || saga.mint_url != wallet.mint_url
        || saga.unit != wallet.unit
    {
        return Err(Error::InvalidOperationState);
    }
    let saved = intent(saga)?;
    let OperationData::Send(data) = &saga.data else {
        return Err(Error::InvalidOperationState);
    };
    if data.amount != saga.amount || data.confirmation.is_some() {
        return Err(Error::InvalidOperationState);
    }
    let token_proofs = data.proofs.as_ref().ok_or(Error::InvalidOperationState)?;
    outputs::validate_request(&saved.pre_swap)?;
    let requested = saved.pre_swap.swap_request.inputs();
    if token_proofs.clone().without_dleqs() != *requested {
        return Err(Error::InvalidOperationState);
    }
    let mut seen = HashSet::new();
    if saved.inputs.is_empty() || requested.len() != saved.inputs.len() {
        return Err(Error::InvalidOperationState);
    }
    for (input, requested) in saved.inputs.iter().zip(requested) {
        super::super::custody::validate_retained_proof(input, requested)?;
        if input.y != requested.y()?
            || !seen.insert(input.y)
            || input.state != State::PendingSpent
            || input.used_by_operation != Some(saga.id)
            || input.mint_url != wallet.mint_url
            || input.unit != wallet.unit
        {
            return Err(Error::InvalidOperationState);
        }
    }
    let tx = &saved.transaction;
    if tx.role != TransactionRole::SendRefund
        || tx.validate_role().is_err()
        || tx.timestamp != saga.created_at
        || tx.saga_id != Some(saga.id)
        || tx.direction != TransactionDirection::Incoming
        || tx.mint_url != wallet.mint_url
        || tx.unit != wallet.unit
        || tx.ys != requested.ys()?
        || tx.amount != saved.pre_swap.swap_request.output_amount()?
        || tx.fee != saved.pre_swap.fee
        || tx.amount.checked_add(tx.fee) != Some(requested.total_amount()?)
    {
        return Err(Error::InvalidOperationState);
    }
    if saga.state == WalletSagaState::Send(SendSagaState::Reclaimed) && saved.outputs.is_none() {
        return Err(Error::InvalidOperationState);
    }
    if matches!(
        saga.state,
        WalletSagaState::Send(SendSagaState::ReclaimPreparing | SendSagaState::ReclaimCancelling)
    ) && saved.outputs.is_some()
    {
        return Err(Error::InvalidOperationState);
    }
    if let Some(outputs) = &saved.outputs {
        let mut expected: HashMap<_, _> = saved
            .pre_swap
            .pre_mint_secrets
            .secrets
            .iter()
            .map(|s| (&s.secret, s))
            .collect();
        if expected.len() != outputs.len() {
            return Err(Error::InvalidOperationState);
        }
        for output in outputs {
            let original = expected
                .remove(&output.proof.secret)
                .ok_or(Error::InvalidOperationState)?;
            if output.proof.y()? != output.y
                || output.proof.amount != original.amount
                || output.proof.keyset_id != original.blinded_message.keyset_id
                || output.mint_url != wallet.mint_url
                || output.unit != wallet.unit
                || output.state != State::Unspent
                || output.used_by_operation.is_some()
                || output.created_by_operation != Some(saga.id)
                || output.derivation_index != original.derivation_index
            {
                return Err(Error::InvalidOperationState);
            }
        }
    }
    Ok(())
}
