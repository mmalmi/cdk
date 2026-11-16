//! Fence token custody before submitting the saved reclaim request.
use std::collections::HashMap;

use cdk_common::database::wallet::ProofChange;
use cdk_common::wallet::{OperationData, ProofInfo, SendSagaState, WalletSaga, WalletSagaState};

use super::{intent, transition};
use crate::nuts::{PublicKey, State};
use crate::{Error, Wallet};

pub(super) fn acquired(original: &ProofInfo) -> ProofInfo {
    let mut acquired = original.clone();
    acquired.state = State::Pending;
    acquired
}

pub(super) async fn acquire(wallet: &Wallet, saga: &mut WalletSaga) -> Result<(), Error> {
    if saga.state != WalletSagaState::Send(SendSagaState::ReclaimPreparing) {
        return Err(Error::InvalidOperationState);
    }
    // Claim this preparation before reading custody; stale workers cannot submit.
    transition(wallet, saga, SendSagaState::ReclaimPreparing).await?;
    let current = current_inputs(wallet, saga).await?;
    let mut changes = Vec::new();
    for original in &intent(saga)?.inputs {
        let acquired = acquired(original);
        let actual = current.get(&original.y);
        if actual != Some(original) && actual != Some(&acquired) {
            cancel(wallet, saga).await?;
            return Err(Error::ConcurrentUpdate);
        }
        changes.push(ProofChange {
            y: original.y,
            before: actual.cloned(),
            after: Some(acquired),
        });
    }
    if !wallet
        .localstore
        .compare_and_swap_proofs(changes, Some(saga))
        .await?
    {
        cancel(wallet, saga).await?;
        return Err(Error::ConcurrentUpdate);
    }
    transition(wallet, saga, SendSagaState::Reclaiming).await
}

pub(super) async fn cancel(wallet: &Wallet, saga: &mut WalletSaga) -> Result<(), Error> {
    if !matches!(
        saga.state,
        WalletSagaState::Send(SendSagaState::ReclaimPreparing | SendSagaState::ReclaimCancelling)
    ) {
        return Err(Error::InvalidOperationState);
    }
    // Fence admission and submission before restoring any of our own records.
    transition(wallet, saga, SendSagaState::ReclaimCancelling).await?;
    let current = current_inputs(wallet, saga).await?;
    let mut changes = Vec::new();
    for original in &intent(saga)?.inputs {
        let actual = current.get(&original.y);
        let mut spent = original.clone();
        spent.state = State::Spent;
        let after = if actual == Some(&acquired(original)) {
            Some(original.clone())
        } else {
            // Retain the intent if our own reservation changed unexpectedly;
            // resetting the send would lose the evidence needed to reconcile it.
            if actual.is_some_and(|p| p.used_by_operation == Some(saga.id))
                && actual != Some(original)
                && actual != Some(&spent)
            {
                return Err(Error::ConcurrentUpdate);
            }
            actual.cloned()
        };
        changes.push(ProofChange {
            y: original.y,
            before: actual.cloned(),
            after,
        });
    }
    if !wallet
        .localstore
        .compare_and_swap_proofs(changes, Some(saga))
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    let OperationData::Send(data) = &mut saga.data else {
        return Err(Error::InvalidOperationState);
    };
    data.reclaim = None;
    transition(wallet, saga, SendSagaState::TokenCreated).await
}

async fn current_inputs(
    wallet: &Wallet,
    saga: &WalletSaga,
) -> Result<HashMap<PublicKey, ProofInfo>, Error> {
    let ys = intent(saga)?.inputs.iter().map(|p| p.y).collect();
    Ok(wallet
        .localstore
        .get_proofs_by_ys(ys)
        .await?
        .into_iter()
        .map(|p| (p.y, p))
        .collect())
}
