//! Atomic paid-melt input consumption and change import.
use std::collections::{HashMap, HashSet};

use cdk_common::database::wallet::ProofChange;
use cdk_common::wallet::{
    MeltOperationData, MeltSagaState, OperationData, ProofInfo, WalletSaga, WalletSagaState,
};

use crate::nuts::State;
use crate::{Error, Wallet};

pub(super) fn data<'a>(
    wallet: &Wallet,
    saga: &'a WalletSaga,
) -> Result<&'a MeltOperationData, Error> {
    let OperationData::Melt(data) = &saga.data else {
        return Err(Error::InvalidOperationState);
    };
    if saga.mint_url != wallet.mint_url
        || saga.unit != wallet.unit
        || saga.amount != data.amount
        || !matches!(
            saga.state,
            WalletSagaState::Melt(
                MeltSagaState::MeltRequested
                    | MeltSagaState::PaymentPending
                    | MeltSagaState::PaidReady
                    | MeltSagaState::Completed
            )
        )
    {
        return Err(Error::InvalidOperationState);
    }
    Ok(data)
}

/// Load every persisted payment input before awaiting mint status or change.
pub(super) async fn inputs(wallet: &Wallet, saga: &WalletSaga) -> Result<Vec<ProofInfo>, Error> {
    let data = data(wallet, saga)?;
    let payment = data.payment.as_ref().ok_or(Error::InvalidOperationState)?;
    let ys = data
        .final_proof_ys
        .as_ref()
        .filter(|ys| !ys.is_empty())
        .ok_or(Error::InvalidOperationState)?;
    if ys.iter().collect::<HashSet<_>>().len() != ys.len()
        || ys.len() != payment.inputs.len()
        || ys != &payment.receipt.ys
        || payment.inputs.iter().zip(ys).any(|(input, y)| {
            input.y != *y
                || input.state != State::Pending
                || input.used_by_operation != Some(saga.id)
                || input.mint_url != wallet.mint_url
                || input.unit != wallet.unit
        })
    {
        return Err(Error::InvalidOperationState);
    }
    let stored: HashMap<_, _> = wallet
        .localstore
        .get_proofs_by_ys(ys.clone())
        .await?
        .into_iter()
        .map(|proof| (proof.y, proof))
        .collect();
    payment
        .inputs
        .iter()
        .map(|expected| {
            let actual = stored.get(&expected.y).ok_or(Error::ConcurrentUpdate)?;
            let mut spent = expected.clone();
            spent.state = State::Spent;
            if actual != expected && actual != &spent {
                return Err(Error::ConcurrentUpdate);
            }
            Ok(actual.clone())
        })
        .collect()
}

pub(super) async fn apply(
    wallet: &Wallet,
    saga: &WalletSaga,
    inputs: &[ProofInfo],
    outputs: Vec<ProofInfo>,
) -> Result<(), Error> {
    let existing: HashMap<_, _> = if outputs.is_empty() {
        HashMap::new()
    } else {
        wallet
            .localstore
            .get_proofs_by_ys(outputs.iter().map(|proof| proof.y).collect())
            .await?
            .into_iter()
            .map(|proof| (proof.y, proof))
            .collect()
    };
    let mut seen: HashSet<_> = inputs.iter().map(|proof| proof.y).collect();
    let mut changes: Vec<_> = inputs
        .iter()
        .map(|input| {
            let mut spent = input.clone();
            spent.state = State::Spent;
            ProofChange {
                y: input.y,
                before: Some(input.clone()),
                after: Some(spent),
            }
        })
        .collect();
    for mut output in outputs {
        if !seen.insert(output.y)
            || output.mint_url != wallet.mint_url
            || output.unit != wallet.unit
        {
            return Err(Error::InvalidOperationState);
        }
        output.created_by_operation = Some(saga.id);
        let actual = existing.get(&output.y);
        // A later spend may add witnesses and ownership. Keep its whole record,
        // but never accept a different coin merely because its Y matches.
        if let Some(actual) = actual {
            if actual.mint_url != output.mint_url
                || actual.unit != output.unit
                || actual.proof.amount != output.proof.amount
                || actual.proof.keyset_id != output.proof.keyset_id
                || actual.proof.secret != output.proof.secret
                || actual.proof.c != output.proof.c
                || actual.proof.p2pk_e != output.proof.p2pk_e
                || actual.spending_condition != output.spending_condition
                || actual.created_by_operation != output.created_by_operation
                || actual.derivation_index != output.derivation_index
            {
                return Err(Error::ConcurrentUpdate);
            }
        }
        changes.push(ProofChange {
            y: output.y,
            before: actual.cloned(),
            after: Some(actual.cloned().unwrap_or(output)),
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
