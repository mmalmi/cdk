//! Exact parent-fenced handoffs preserve original and later-owned proof metadata.
use std::collections::{HashMap, HashSet};

use cdk_common::database::wallet::ProofChange;
use cdk_common::wallet::{MeltSagaState, ProofInfo, WalletSaga, WalletSagaState};

use super::swap;
use crate::nuts::{Proofs, ProofsMethods, State};
use crate::wallet::util::same_coin;
use crate::{Error, Wallet};

pub(super) async fn records(wallet: &Wallet, saga: &WalletSaga) -> Result<Vec<ProofInfo>, Error> {
    swap::validate(wallet, saga)?;
    let swap = swap::saved(saga)?;
    let ready = swap.outputs.as_ref().ok_or(Error::InvalidOperationState)?;
    let mut expected: HashMap<_, _> = swap
        .pre_swap
        .pre_mint_secrets
        .secrets
        .iter()
        .map(|p| (&p.secret, p))
        .collect();
    if ready.send.total_amount()? != swap.amount
        || expected.len() != ready.send.len() + ready.change.len()
    {
        return Err(Error::InvalidOperationState);
    }
    let mut ys: HashSet<_> = swap.inputs.iter().map(|p| p.y).collect();
    let mut records = Vec::new();
    for (proofs, state, owner) in [
        (&ready.send, State::Reserved, Some(saga.id)),
        (&ready.change, State::Unspent, None),
    ] {
        for proof in proofs {
            let secret = expected
                .remove(&proof.secret)
                .ok_or(Error::InvalidOperationState)?;
            if proof.amount != secret.amount
                || proof.keyset_id != secret.blinded_message.keyset_id
                || !ys.insert(proof.y()?)
            {
                return Err(Error::InvalidOperationState);
            }
            if proof.dleq.is_some() {
                let keys = wallet.keyset(proof.keyset_id).await?.keys;
                proof.verify_dleq(keys.amount_key(proof.amount).ok_or(Error::AmountKey)?)?;
            }
            let mut info = ProofInfo::new_with_operations(
                proof.clone(),
                wallet.mint_url.clone(),
                state,
                wallet.unit.clone(),
                owner,
                Some(saga.id),
            )?;
            info.derivation_index = secret.derivation_index;
            records.push(info);
        }
    }
    Ok(records)
}

fn compatible(actual: &ProofInfo, expected: &ProofInfo) -> bool {
    same_coin(&actual.proof, &expected.proof)
        && actual.mint_url == expected.mint_url
        && actual.unit == expected.unit
        && actual.spending_condition == expected.spending_condition
        && actual.created_by_operation == expected.created_by_operation
        && actual.derivation_index == expected.derivation_index
}

pub(super) async fn apply(wallet: &Wallet, saga: &WalletSaga) -> Result<(), Error> {
    let outputs = records(wallet, saga).await?;
    let originals = &swap::saved(saga)?.inputs;
    let current: HashMap<_, _> = wallet
        .localstore
        .get_proofs_by_ys(originals.iter().chain(&outputs).map(|p| p.y).collect())
        .await?
        .into_iter()
        .map(|p| (p.y, p))
        .collect();
    let mut changes = Vec::new();
    for input in originals {
        let mut spent = input.clone();
        spent.state = State::Spent;
        let actual = current.get(&input.y).ok_or(Error::ConcurrentUpdate)?;
        if actual != input && actual != &spent {
            return Err(Error::ConcurrentUpdate);
        }
        changes.push(ProofChange {
            y: input.y,
            before: Some(actual.clone()),
            after: Some(spent),
        });
    }
    for output in outputs {
        let actual = current.get(&output.y);
        if actual.is_some_and(|actual| !compatible(actual, &output)) {
            return Err(Error::ConcurrentUpdate);
        }
        changes.push(ProofChange {
            y: output.y,
            before: actual.cloned(),
            after: Some(actual.unwrap_or(&output).clone()),
        });
    }
    commit(wallet, saga, changes).await
}

pub(super) async fn pending(
    wallet: &Wallet,
    saga: &WalletSaga,
    proofs: &Proofs,
) -> Result<Vec<ProofInfo>, Error> {
    if saga.state != WalletSagaState::Melt(MeltSagaState::ProofsReserved) {
        return Err(Error::InvalidOperationState);
    }
    let expected: Vec<_> = records(wallet, saga)
        .await?
        .into_iter()
        .filter(|p| p.used_by_operation == Some(saga.id))
        .collect();
    if proofs.len() != expected.len() || expected.iter().any(|p| !proofs.contains(&p.proof)) {
        return Err(Error::InvalidOperationState);
    }
    let current: HashMap<_, _> = wallet
        .localstore
        .get_proofs_by_ys(proofs.ys()?)
        .await?
        .into_iter()
        .map(|p| (p.y, p))
        .collect();
    let expected: HashMap<_, _> = expected.into_iter().map(|p| (p.y, p)).collect();
    let mut changes = Vec::new();
    for proof in proofs {
        let expected = expected
            .get(&proof.y()?)
            .ok_or(Error::InvalidOperationState)?;
        let actual = current.get(&expected.y).ok_or(Error::ConcurrentUpdate)?;
        if !compatible(actual, expected)
            || actual.state != State::Reserved
            || actual.used_by_operation != Some(saga.id)
        {
            return Err(Error::ConcurrentUpdate);
        }
        let mut pending = actual.clone();
        pending.state = State::Pending;
        changes.push(ProofChange {
            y: actual.y,
            before: Some(actual.clone()),
            after: Some(pending),
        });
    }
    let pending = changes.iter().filter_map(|p| p.after.clone()).collect();
    commit(wallet, saga, changes).await?;
    Ok(pending)
}

pub(super) async fn release(wallet: &Wallet, saga: &WalletSaga) -> Result<(), Error> {
    if saga.state != WalletSagaState::Melt(MeltSagaState::SwapCancelling) {
        return Err(Error::InvalidOperationState);
    }
    let expected: Vec<_> = records(wallet, saga)
        .await?
        .into_iter()
        .filter(|p| p.used_by_operation == Some(saga.id))
        .collect();
    let current: HashMap<_, _> = wallet
        .localstore
        .get_proofs_by_ys(expected.iter().map(|p| p.y).collect())
        .await?
        .into_iter()
        .map(|p| (p.y, p))
        .collect();
    let mut changes = Vec::new();
    for expected in expected {
        let actual = current.get(&expected.y).ok_or(Error::ConcurrentUpdate)?;
        if !compatible(actual, &expected) {
            return Err(Error::ConcurrentUpdate);
        }
        let mut released = actual.clone();
        if actual.used_by_operation == Some(saga.id)
            && matches!(actual.state, State::Reserved | State::Pending)
        {
            released.state = State::Unspent;
            released.used_by_operation = None;
        }
        changes.push(ProofChange {
            y: actual.y,
            before: Some(actual.clone()),
            after: Some(released),
        });
    }
    commit(wallet, saga, changes).await
}

async fn commit(
    wallet: &Wallet,
    saga: &WalletSaga,
    changes: Vec<ProofChange>,
) -> Result<(), Error> {
    if !wallet
        .localstore
        .compare_and_swap_proofs(changes, Some(saga))
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    Ok(())
}
