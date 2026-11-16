//! Validate exact private intent and persist verified outputs before handoff.
use std::collections::{HashMap, HashSet};

use cdk_common::wallet::{
    OperationData, ProofInfo, SwapCheckpoint, SwapOutputs, SwapSagaState, WalletSaga,
    WalletSagaState,
};

use super::{acquisition, handoff, transition};
use crate::nuts::nut10::SpendingConditionVerification;
use crate::nuts::{State, SwapResponse};
use crate::wallet::swap::outputs;
use crate::wallet::util::same_coin;
use crate::{Error, Wallet};

pub(super) fn get(saga: &WalletSaga) -> Result<&SwapCheckpoint, Error> {
    let OperationData::Swap(data) = &saga.data else {
        return Err(Error::InvalidOperationState);
    };
    data.checkpoint
        .as_deref()
        .ok_or(Error::InvalidOperationState)
}

pub(super) fn validate(wallet: &Wallet, saga: &WalletSaga) -> Result<(), Error> {
    let OperationData::Swap(data) = &saga.data else {
        return Err(Error::InvalidOperationState);
    };
    let WalletSagaState::Swap(state) = saga.state else {
        return Err(Error::InvalidOperationState);
    };
    let saved = get(saga)?;
    let swap = &saved.pre_swap;
    acquisition::validate(saga, &saved.acquisition)?;
    outputs::validate_request(swap)?;
    swap.swap_request.verify_spending_conditions()?;
    if saga.mint_url != wallet.mint_url
        || saga.unit != wallet.unit
        || (saved.ready.is_none() && saga.version >= u32::MAX - 1)
        || data.input_amount != saga.amount
        || swap.swap_request.input_amount()? != data.input_amount
        || swap.swap_request.output_amount()? != data.output_amount
        || data.input_amount.checked_sub(data.output_amount) != Some(swap.fee)
        || swap.swap_request.inputs().len() != saved.acquisition.len()
        || (state == SwapSagaState::OutputsReady) != saved.ready.is_some()
    {
        return Err(Error::InvalidOperationState);
    }
    let mut seen = HashSet::new();
    for input in swap.swap_request.inputs() {
        let y = input.y()?;
        if !seen.insert(y)
            || !saved.acquisition.iter().any(|p| {
                p.y == y
                    && p.before
                        .as_ref()
                        .is_some_and(|p| same_coin(&p.proof, input))
            })
        {
            return Err(Error::InvalidOperationState);
        }
    }
    if let Some(ready) = &saved.ready {
        records(wallet, saga, ready)?;
    }
    Ok(())
}

pub(super) async fn resolve(
    wallet: &Wallet,
    saga: &WalletSaga,
    response: SwapResponse,
) -> Result<SwapOutputs, Error> {
    let saved = get(saga)?;
    outputs::resolve(
        wallet,
        &saved.pre_swap,
        saved.amount,
        &saved.amount_split_target,
        saved.spending_conditions.as_ref(),
        response,
    )
    .await
}

fn records(
    wallet: &Wallet,
    saga: &WalletSaga,
    ready: &SwapOutputs,
) -> Result<Vec<ProofInfo>, Error> {
    let saved = get(saga)?;
    if saved.amount.is_none() && !ready.send.is_empty() {
        return Err(Error::InvalidOperationState);
    }
    let mut expected: HashMap<_, _> = saved
        .pre_swap
        .pre_mint_secrets
        .secrets
        .iter()
        .map(|p| (&p.secret, p))
        .collect();
    if expected.len() != ready.send.len() + ready.change.len() {
        return Err(Error::InvalidOperationState);
    }
    let mut records = Vec::new();
    for (proofs, state) in [
        (&ready.send, State::Reserved),
        (&ready.change, State::Unspent),
    ] {
        for proof in proofs {
            let secret = expected
                .remove(&proof.secret)
                .ok_or(Error::InvalidOperationState)?;
            if proof.amount != secret.amount || proof.keyset_id != secret.blinded_message.keyset_id
            {
                return Err(Error::InvalidOperationState);
            }
            let mut info = ProofInfo::new(
                proof.clone(),
                wallet.mint_url.clone(),
                state,
                wallet.unit.clone(),
            )?;
            info.created_by_operation = Some(saga.id);
            info.derivation_index = secret.derivation_index;
            records.push(info);
        }
    }
    Ok(records)
}

pub(super) async fn finish(
    wallet: &Wallet,
    saga: &mut WalletSaga,
    ready: SwapOutputs,
) -> Result<(), Error> {
    validate(wallet, saga)?;
    let proofs = records(wallet, saga, &ready)?;
    for proof in &proofs {
        if proof.proof.dleq.is_some() {
            let keys = wallet.keyset(proof.proof.keyset_id).await?.keys;
            proof.proof.verify_dleq(
                keys.amount_key(proof.proof.amount)
                    .ok_or(Error::AmountKey)?,
            )?;
        }
    }
    let OperationData::Swap(data) = &mut saga.data else {
        return Err(Error::InvalidOperationState);
    };
    let saved = data
        .checkpoint
        .as_mut()
        .ok_or(Error::InvalidOperationState)?;
    match &saved.ready {
        Some(original) if original != &ready => return Err(Error::InvalidOperationState),
        Some(_) => {}
        None => {
            if saga.state != WalletSagaState::Swap(SwapSagaState::SwapRequested) {
                return Err(Error::InvalidOperationState);
            }
            saved.ready = Some(ready);
            transition(wallet, saga, SwapSagaState::OutputsReady).await?;
        }
    }
    handoff::complete(wallet, saga, proofs).await
}
