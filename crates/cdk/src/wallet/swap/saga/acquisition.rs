//! Exact input admission and fenced cancellation before mint submission.
use std::collections::HashMap;

use cdk_common::database::wallet::{validate_proof_changes, ProofChange};
use cdk_common::wallet::{SwapSagaState, WalletSaga, WalletSagaState};

use super::{checkpoint, transition};
use crate::nuts::{Proofs, ProofsMethods, State};
use crate::wallet::util::same_coin;
use crate::{Error, Wallet};

pub(super) async fn plan(
    wallet: &Wallet,
    id: uuid::Uuid,
    proofs: &Proofs,
) -> Result<Vec<ProofChange>, Error> {
    let mut current: HashMap<_, _> = wallet
        .localstore
        .get_proofs_by_ys(proofs.ys()?)
        .await?
        .into_iter()
        .map(|p| (p.y, p))
        .collect();
    let mut plan = Vec::new();
    for proof in proofs {
        let original = current.remove(&proof.y()?).ok_or(Error::ConcurrentUpdate)?;
        if original.mint_url != wallet.mint_url
            || original.unit != wallet.unit
            || !same_coin(&original.proof, proof)
        {
            return Err(Error::ConcurrentUpdate);
        }
        let mut acquired = original.clone();
        if original.state != State::Unspent || original.used_by_operation.is_some() {
            return Err(cdk_common::database::Error::ProofNotUnspent.into());
        }
        acquired.state = State::Reserved;
        acquired.used_by_operation = Some(id);
        plan.push(ProofChange {
            y: original.y,
            before: Some(original),
            after: Some(acquired),
        });
    }
    Ok(plan)
}

pub(super) fn validate(saga: &WalletSaga, changes: &[ProofChange]) -> Result<(), Error> {
    validate_proof_changes(changes)?;
    if changes.is_empty() {
        return Err(Error::InvalidOperationState);
    }
    let mut owner = None;
    for change in changes {
        let before = change.before.as_ref().ok_or(Error::InvalidOperationState)?;
        let after = change.after.as_ref().ok_or(Error::InvalidOperationState)?;
        if before.mint_url != saga.mint_url
            || before.unit != saga.unit
            || after.state != State::Reserved
        {
            return Err(Error::InvalidOperationState);
        }
        let current_owner = after
            .used_by_operation
            .ok_or(Error::InvalidOperationState)?;
        if owner.is_some_and(|owner| owner != current_owner) {
            return Err(Error::InvalidOperationState);
        }
        owner = Some(current_owner);
        if current_owner == saga.id {
            let mut expected = before.clone();
            expected.state = State::Reserved;
            expected.used_by_operation = Some(saga.id);
            if before.state != State::Unspent
                || before.used_by_operation.is_some()
                || &expected != after
            {
                return Err(Error::InvalidOperationState);
            }
        } else if before != after {
            return Err(Error::InvalidOperationState);
        }
    }
    Ok(())
}

pub(super) async fn apply(wallet: &Wallet, saga: &WalletSaga) -> Result<(), Error> {
    if saga.state != WalletSagaState::Swap(SwapSagaState::ProofsReserved) {
        return Err(Error::InvalidOperationState);
    }
    checkpoint::validate(wallet, saga)?;
    if !wallet
        .localstore
        .compare_and_swap_proofs(checkpoint::get(saga)?.acquisition.clone(), Some(saga))
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    Ok(())
}

pub(super) async fn check(wallet: &Wallet, saga: &WalletSaga) -> Result<(), Error> {
    let WalletSagaState::Swap(state) = saga.state else {
        return Err(Error::InvalidOperationState);
    };
    if state == SwapSagaState::Cancelling {
        return Err(Error::InvalidOperationState);
    }
    let plan = &checkpoint::get(saga)?.acquisition;
    let current: HashMap<_, _> = wallet
        .localstore
        .get_proofs_by_ys(plan.iter().map(|p| p.y).collect())
        .await?
        .into_iter()
        .map(|p| (p.y, p))
        .collect();
    let mut changes = Vec::new();
    for change in plan {
        let acquired = change.after.as_ref().ok_or(Error::InvalidOperationState)?;
        let mut spent = acquired.clone();
        spent.state = State::Spent;
        let actual = current.get(&change.y).ok_or(Error::ConcurrentUpdate)?;
        if actual != acquired && (state == SwapSagaState::ProofsReserved || actual != &spent) {
            return Err(Error::ConcurrentUpdate);
        }
        changes.push(ProofChange {
            y: change.y,
            before: Some(actual.clone()),
            after: Some(actual.clone()),
        });
    }
    if !wallet
        .localstore
        .compare_and_swap_proofs(changes, Some(saga))
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    Ok(())
}

pub(super) async fn cancel(wallet: &Wallet, saga: &mut WalletSaga) -> Result<(), Error> {
    if !matches!(
        saga.state,
        WalletSagaState::Swap(SwapSagaState::ProofsReserved | SwapSagaState::Cancelling)
    ) {
        return Err(Error::InvalidOperationState);
    }
    checkpoint::validate(wallet, saga)?;
    transition(wallet, saga, SwapSagaState::Cancelling).await?;
    let plan = &checkpoint::get(saga)?.acquisition;
    let current: HashMap<_, _> = wallet
        .localstore
        .get_proofs_by_ys(plan.iter().map(|p| p.y).collect())
        .await?
        .into_iter()
        .map(|p| (p.y, p))
        .collect();
    let mut changes = Vec::new();
    for p in plan {
        let actual = current.get(&p.y).cloned();
        let mut spent = p.after.clone().ok_or(Error::InvalidOperationState)?;
        spent.state = State::Spent;
        let after = if actual == p.after {
            p.before.clone()
        } else {
            if actual
                .as_ref()
                .is_some_and(|p| p.used_by_operation == Some(saga.id))
                && actual.as_ref() != Some(&spent)
            {
                // Keep the journal for unexplained changes still owned by this swap.
                return Err(Error::ConcurrentUpdate);
            }
            actual.clone()
        };
        changes.push(ProofChange {
            y: p.y,
            before: actual,
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
    wallet.localstore.delete_saga(&saga.id).await?;
    Ok(())
}
