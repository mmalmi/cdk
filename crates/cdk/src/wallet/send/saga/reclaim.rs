//! A send owns reclaim through mint spending, wallet custody and result delivery.
use std::collections::HashMap;

use cdk_common::wallet::{
    OperationData, ProofInfo, SendReclaim, SendSagaState, Transaction, TransactionDirection,
    TransactionId, TransactionRole, TransactionStatus, WalletSaga, WalletSagaState,
};

use crate::nuts::{Proofs, ProofsMethods, State};
use crate::wallet::swap::outputs;

mod admission;
mod validation;
use self::validation::validate;
use super::{handoff, terminal};
use crate::{Amount, Error, Wallet};

pub(crate) async fn receipt(wallet: &Wallet, id: uuid::Uuid) -> Result<Option<Amount>, Error> {
    let Some(tx) = wallet
        .localstore
        .get_transaction(TransactionId::from_send_refund(id))
        .await?
    else {
        return Ok(None);
    };
    if tx.mint_url != wallet.mint_url
        || tx.unit != wallet.unit
        || tx.direction != TransactionDirection::Incoming
        || tx.saga_id != Some(id)
        || tx.role != TransactionRole::SendRefund
        || tx.validate_role().is_err()
    {
        return Err(Error::InvalidOperationState);
    }
    Ok(Some(tx.amount))
}

fn intent(saga: &WalletSaga) -> Result<&SendReclaim, Error> {
    let OperationData::Send(data) = &saga.data else {
        return Err(Error::InvalidOperationState);
    };
    data.reclaim.as_deref().ok_or(Error::InvalidOperationState)
}

async fn advance(wallet: &Wallet, saga: &mut WalletSaga) -> Result<(), Error> {
    transition(wallet, saga, SendSagaState::Reclaiming).await
}

async fn transition(
    wallet: &Wallet,
    saga: &mut WalletSaga,
    state: SendSagaState,
) -> Result<(), Error> {
    if saga.version == u32::MAX {
        return Err(Error::InvalidOperationState);
    }
    saga.update_state(WalletSagaState::Send(state));
    if !wallet.localstore.update_saga(saga.clone()).await? {
        return Err(Error::ConcurrentUpdate);
    }
    Ok(())
}

pub(super) async fn start(
    wallet: &Wallet,
    mut saga: WalletSaga,
    proofs: Proofs,
) -> Result<Amount, Error> {
    if saga.state != WalletSagaState::Send(SendSagaState::TokenCreated)
        || saga.mint_url != wallet.mint_url
        || saga.unit != wallet.unit
    {
        return Err(Error::InvalidOperationState);
    }
    wallet.ensure_send_storage(Some(&saga)).await?;
    let mut current: HashMap<_, _> = wallet
        .localstore
        .get_proofs_by_ys(proofs.ys()?)
        .await?
        .into_iter()
        .map(|p| (p.y, p))
        .collect();
    let mut inputs = Vec::new();
    for proof in &proofs {
        let original = current.remove(&proof.y()?).ok_or(Error::ConcurrentUpdate)?;
        super::custody::validate_retained_proof(&original, proof)?;
        if original.state != State::PendingSpent
            || original.used_by_operation != Some(saga.id)
            || original.mint_url != wallet.mint_url
            || original.unit != wallet.unit
        {
            return Err(Error::ConcurrentUpdate);
        }
        inputs.push(original);
    }
    let keyset = wallet
        .active_keyset_with_policy(cdk_common::wallet::KeysetLoadPolicy::Refresh)
        .await?
        .id;
    let fee_and_amounts = wallet.get_keyset_fees_and_amounts_by_id(keyset).await?;
    let fee = wallet.get_proofs_fee(&proofs).await?;
    let counter_start = wallet
        .localstore
        .increment_keyset_counter(&keyset, 0)
        .await?;
    let pre_swap = wallet
        .create_swap(
            keyset,
            &fee_and_amounts,
            None,
            Default::default(),
            proofs,
            None,
            false,
            false,
            &fee,
            counter_start,
        )
        .await?;
    let transaction = Transaction {
        role: TransactionRole::SendRefund,
        status: TransactionStatus::Completed,
        mint_url: wallet.mint_url.clone(),
        direction: TransactionDirection::Incoming,
        amount: pre_swap.swap_request.output_amount()?,
        fee: pre_swap.fee,
        unit: wallet.unit.clone(),
        ys: pre_swap.swap_request.inputs().ys()?,
        timestamp: saga.created_at,
        memo: None,
        metadata: HashMap::new(),
        quote_id: None,
        payment_request: None,
        payment_proof: None,
        payment_method: None,
        saga_id: Some(saga.id),
    };
    let expected = saga.clone();
    let OperationData::Send(data) = &mut saga.data else {
        return Err(Error::InvalidOperationState);
    };
    data.reclaim = Some(Box::new(SendReclaim {
        inputs,
        pre_swap,
        transaction,
        outputs: None,
    }));
    if saga.version == u32::MAX {
        return Err(Error::InvalidOperationState);
    }
    saga.update_state(WalletSagaState::Send(SendSagaState::ReclaimPreparing));
    validate(wallet, &saga)?;
    wallet
        .commit_swap_preparation(
            Some(&expected),
            &saga,
            keyset,
            counter_start,
            &intent(&saga)?.pre_swap,
        )
        .await?;
    complete(wallet, saga, false).await
}

pub(crate) async fn complete(
    wallet: &Wallet,
    mut saga: WalletSaga,
    recovering: bool,
) -> Result<Amount, Error> {
    validate(wallet, &saga)?;
    wallet.ensure_send_storage(Some(&saga)).await?;
    if saga.state == WalletSagaState::Send(SendSagaState::Reclaimed) {
        return finish(wallet, saga).await;
    }
    match saga.state {
        WalletSagaState::Send(SendSagaState::ReclaimPreparing) => {
            admission::acquire(wallet, &mut saga).await?;
        }
        WalletSagaState::Send(SendSagaState::ReclaimCancelling) => {
            admission::cancel(wallet, &mut saga).await?;
            return Err(Error::ConcurrentUpdate);
        }
        _ => advance(wallet, &mut saga).await?,
    }
    if intent(&saga)?.outputs.is_none() {
        let swap = &intent(&saga)?.pre_swap;
        let response = if recovering {
            outputs::restore(wallet, swap).await?
        } else {
            wallet.client.post_swap(swap.swap_request.clone()).await?
        };
        let resolved =
            outputs::resolve(wallet, swap, None, &Default::default(), None, response).await?;
        let outputs = resolved
            .change
            .into_iter()
            .map(|proof| {
                let mut info = ProofInfo::new(
                    proof,
                    wallet.mint_url.clone(),
                    State::Unspent,
                    wallet.unit.clone(),
                )?;
                info.derivation_index = swap
                    .pre_mint_secrets
                    .secrets
                    .iter()
                    .find(|s| s.secret == info.proof.secret)
                    .and_then(|s| s.derivation_index);
                info.created_by_operation = Some(saga.id);
                Ok(info)
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let OperationData::Send(data) = &mut saga.data else {
            return Err(Error::InvalidOperationState);
        };
        data.reclaim
            .as_mut()
            .ok_or(Error::InvalidOperationState)?
            .outputs = Some(outputs);
        validate(wallet, &saga)?;
        advance(wallet, &mut saga).await?;
    }
    let saved = intent(&saga)?;
    handoff::apply(
        wallet,
        &saga,
        &saved
            .inputs
            .iter()
            .map(admission::acquired)
            .collect::<Vec<_>>(),
        saved.outputs.clone().ok_or(Error::InvalidOperationState)?,
    )
    .await?;
    let receipt = saved.transaction.clone();
    terminal::publish(wallet, &mut saga, receipt, SendSagaState::Reclaimed).await?;
    finish(wallet, saga).await
}

async fn finish(wallet: &Wallet, saga: WalletSaga) -> Result<Amount, Error> {
    let saved = intent(&saga)?;
    let actual = wallet
        .localstore
        .get_transaction(saved.transaction.id())
        .await?;
    if actual.as_ref() != Some(&saved.transaction) {
        return Err(Error::ConcurrentUpdate);
    }
    let amount = saved.transaction.amount;
    terminal::finish_primary(wallet, saga, TransactionStatus::Failed).await?;
    Ok(amount)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod admission_tests;
