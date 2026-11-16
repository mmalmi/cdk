//! The parent journal retains the exact preparatory request until payment or cancellation.
use std::collections::HashSet;

use cdk_common::database::wallet::ProofChange;
use cdk_common::wallet::{
    KeysetLoadPolicy, MeltSagaState, MeltSwap, OperationData, WalletSaga, WalletSagaState,
};

use super::{admission, swap_custody};
use crate::amount::SplitTarget;
use crate::nuts::nut10::SpendingConditionVerification;
use crate::nuts::{Id, Proofs, State};
use crate::wallet::swap::outputs;
use crate::{Amount, Error, Wallet};

pub(super) fn saved(saga: &WalletSaga) -> Result<&MeltSwap, Error> {
    let OperationData::Melt(data) = &saga.data else {
        return Err(Error::InvalidOperationState);
    };
    data.swap.as_deref().ok_or(Error::InvalidOperationState)
}

async fn advance(
    wallet: &Wallet,
    saga: &mut WalletSaga,
    state: MeltSagaState,
) -> Result<(), Error> {
    if saga.version == u32::MAX {
        return Err(Error::InvalidOperationState);
    }
    saga.update_state(WalletSagaState::Melt(state));
    if !wallet.localstore.update_saga(saga.clone()).await? {
        return Err(Error::ConcurrentUpdate);
    }
    Ok(())
}

pub(super) fn validate(wallet: &Wallet, saga: &WalletSaga) -> Result<(), Error> {
    let OperationData::Melt(data) = &saga.data else {
        return Err(Error::InvalidOperationState);
    };
    let swap = saved(saga)?;
    let plan = admission::saved(wallet, saga)?;
    if swap.inputs.len() != plan.len()
        || swap
            .inputs
            .iter()
            .any(|input| !plan.iter().any(|p| p.after.as_ref() == Some(input)))
    {
        return Err(Error::InvalidOperationState);
    }
    outputs::validate_request(&swap.pre_swap)?;
    let request = &swap.pre_swap.swap_request;
    request.verify_spending_conditions()?;
    if saga.mint_url != wallet.mint_url
        || saga.unit != wallet.unit
        || saga.amount != data.amount
        || swap.inputs.is_empty()
        || swap.inputs.len() != request.inputs().len()
        || request
            .input_amount()?
            .checked_sub(request.output_amount()?)
            != Some(swap.pre_swap.fee)
        || swap.amount
            < data
                .amount
                .checked_add(data.fee_reserve)
                .ok_or(Error::AmountOverflow)?
        || swap.amount > request.output_amount()?
    {
        return Err(Error::InvalidOperationState);
    }
    let mut seen = HashSet::new();
    for input in &swap.inputs {
        // SwapRequest::new omits transport DLEQ only. Witnesses and P2BK evidence
        // must still match; the complete original is retained for the spent row.
        let mut wire = input.proof.clone();
        wire.dleq = None;
        if !seen.insert(input.y)
            || input.proof.y()? != input.y
            || input.mint_url != wallet.mint_url
            || input.unit != wallet.unit
            || input.state != State::Reserved
            || input.used_by_operation != Some(saga.id)
            || !request.inputs().contains(&wire)
        {
            return Err(Error::InvalidOperationState);
        }
    }
    Ok(())
}

pub(super) async fn start(
    wallet: &Wallet,
    saga: &mut WalletSaga,
    proofs: Proofs,
    amount: Amount,
    keyset: Id,
    policy: KeysetLoadPolicy,
) -> Result<Proofs, Error> {
    if saga.state != WalletSagaState::Melt(MeltSagaState::ProofsReserved) {
        return Err(Error::InvalidOperationState);
    }
    let OperationData::Melt(data) = &saga.data else {
        return Err(Error::InvalidOperationState);
    };
    if data.swap.is_some() {
        return Err(Error::InvalidOperationState);
    }
    admission::validate_inputs(wallet, saga, &proofs)?;
    let inputs = admission::saved(wallet, saga)?
        .iter()
        .map(|p| p.after.clone().ok_or(Error::InvalidOperationState))
        .collect::<Result<Vec<_>, _>>()?;
    let changes = inputs
        .iter()
        .map(|input| ProofChange {
            y: input.y,
            before: Some(input.clone()),
            after: Some(input.clone()),
        })
        .collect();
    // Prove backend support and exact parent custody before allocating secrets or posting.
    if !wallet
        .localstore
        .compare_and_swap_proofs(changes, Some(saga))
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    let amounts = wallet
        .get_keyset_fees_and_amounts_by_id_with_policy(keyset, policy)
        .await?;
    let fee = wallet.get_proofs_fee(&proofs).await?;
    let expected = saga.clone();
    let counter_start = wallet
        .localstore
        .increment_keyset_counter(&keyset, 0)
        .await?;
    let pre_swap = wallet
        .create_swap(
            keyset,
            &amounts,
            Some(amount),
            SplitTarget::None,
            proofs,
            None,
            false,
            false,
            &fee,
            counter_start,
        )
        .await?;
    let OperationData::Melt(data) = &mut saga.data else {
        return Err(Error::InvalidOperationState);
    };
    data.swap = Some(Box::new(MeltSwap {
        inputs,
        pre_swap,
        amount,
        outputs: None,
    }));
    validate(wallet, saga)?;
    if saga.version == u32::MAX {
        return Err(Error::InvalidOperationState);
    }
    saga.update_state(WalletSagaState::Melt(MeltSagaState::ProofsReserved));
    wallet
        .commit_swap_preparation(
            Some(&expected),
            saga,
            keyset,
            counter_start,
            &saved(saga)?.pre_swap,
        )
        .await?;
    finish(wallet, saga, false).await
}

pub(super) async fn finish(
    wallet: &Wallet,
    saga: &mut WalletSaga,
    recovering: bool,
) -> Result<Proofs, Error> {
    let WalletSagaState::Melt(
        state @ (MeltSagaState::ProofsReserved | MeltSagaState::SwapCancelling),
    ) = saga.state
    else {
        return Err(Error::InvalidOperationState);
    };
    validate(wallet, saga)?;
    admission::no_extra_inputs(wallet, saga).await?;
    let mut changes = Vec::new();
    let current = wallet
        .localstore
        .get_proofs_by_ys(saved(saga)?.inputs.iter().map(|p| p.y).collect())
        .await?;
    for original in &saved(saga)?.inputs {
        let actual = current
            .iter()
            .find(|p| p.y == original.y)
            .ok_or(Error::ConcurrentUpdate)?;
        let mut spent = original.clone();
        spent.state = State::Spent;
        if actual != original && actual != &spent {
            return Err(Error::ConcurrentUpdate);
        }
        changes.push(ProofChange {
            y: actual.y,
            before: Some(actual.clone()),
            after: Some(actual.clone()),
        });
    }
    // A stale actor may not replay even the saved request after another owner completes.
    if !wallet
        .localstore
        .compare_and_swap_proofs(changes, Some(saga))
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    if saved(saga)?.outputs.is_none() {
        let swap = saved(saga)?;
        let response = if recovering {
            outputs::restore(wallet, &swap.pre_swap).await?
        } else {
            wallet
                .client
                .post_swap(swap.pre_swap.swap_request.clone())
                .await?
        };
        let ready = outputs::resolve(
            wallet,
            &swap.pre_swap,
            Some(swap.amount),
            &SplitTarget::None,
            None,
            response,
        )
        .await?;
        let OperationData::Melt(data) = &mut saga.data else {
            return Err(Error::InvalidOperationState);
        };
        data.swap
            .as_mut()
            .ok_or(Error::InvalidOperationState)?
            .outputs = Some(ready);
        swap_custody::records(wallet, saga).await?;
        advance(wallet, saga, state).await?;
    }
    swap_custody::apply(wallet, saga).await?;
    Ok(saved(saga)?
        .outputs
        .as_ref()
        .ok_or(Error::InvalidOperationState)?
        .send
        .clone())
}

/// Fence confirmation before recovering and releasing replacement coins.
pub(in crate::wallet::melt) async fn cancel(
    wallet: &Wallet,
    saga: &mut WalletSaga,
) -> Result<(), Error> {
    let OperationData::Melt(data) = &saga.data else {
        return Err(Error::InvalidOperationState);
    };
    if saga.mint_url != wallet.mint_url || saga.unit != wallet.unit || saga.amount != data.amount {
        return Err(Error::InvalidOperationState);
    }
    let has_swap = data.swap.is_some();
    if has_swap {
        validate(wallet, saga)?;
    }
    match saga.state {
        WalletSagaState::Melt(MeltSagaState::ProofsReserved) => {
            advance(wallet, saga, MeltSagaState::SwapCancelling).await?;
        }
        WalletSagaState::Melt(MeltSagaState::SwapCancelling) => {}
        _ => return Err(Error::InvalidOperationState),
    }
    if has_swap {
        finish(wallet, saga, true).await?;
        swap_custody::release(wallet, saga).await?;
    } else {
        admission::release(wallet, saga).await?;
    }
    wallet.localstore.release_melt_quote(&saga.id).await?;
    wallet.localstore.delete_saga(&saga.id).await?;
    Ok(())
}
