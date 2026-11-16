//! Durable receive identity, phase transitions, and exact pre-request rollback.
use std::collections::HashMap;

use cdk_common::database::wallet::{validate_proof_changes, ProofChange};
use cdk_common::util::unix_time;
use cdk_common::wallet::{
    receive_identity, OperationData, ReceiveCheckpoint, ReceiveOperationData, ReceiveSagaState,
    Transaction, TransactionDirection, TransactionRole, TransactionStatus, WalletSaga,
    WalletSagaState,
};

use super::state::Prepared;
use crate::nuts::nut10::SpendingConditionVerification;
use crate::nuts::{ProofsMethods, State};
use crate::wallet::util::same_coin;
use crate::{Amount, Error, Wallet};

pub(super) fn checkpoint(saga: &WalletSaga) -> Result<&ReceiveCheckpoint, Error> {
    match &saga.data {
        OperationData::Receive(data) => data
            .checkpoint
            .as_deref()
            .ok_or(Error::InvalidOperationState),
        _ => Err(Error::InvalidOperationState),
    }
}

pub(super) fn checkpoint_mut(saga: &mut WalletSaga) -> Result<&mut ReceiveCheckpoint, Error> {
    match &mut saga.data {
        OperationData::Receive(data) => data
            .checkpoint
            .as_deref_mut()
            .ok_or(Error::InvalidOperationState),
        _ => Err(Error::InvalidOperationState),
    }
}

pub(super) fn validate(wallet: &Wallet, saga: &WalletSaga) -> Result<(), Error> {
    let saved = checkpoint(saga)?;
    validate_proof_changes(&saved.inputs)?;
    let mut proofs = Vec::new();
    for input in &saved.inputs {
        let acquired = input.after.as_ref().ok_or(Error::InvalidOperationState)?;
        if acquired.mint_url != wallet.mint_url
            || acquired.unit != wallet.unit
            || acquired.used_by_operation != Some(saga.id)
            || acquired.state != State::Pending
        {
            return Err(Error::InvalidOperationState);
        }
        if let Some(original) = &input.before {
            let mut expected = original.clone();
            expected.state = State::Pending;
            expected.used_by_operation = Some(saga.id);
            if &expected != acquired
                || !matches!(original.state, State::Unspent | State::PendingSpent)
                || (original.state == State::Unspent && original.used_by_operation.is_some())
                || (original.state == State::PendingSpent && original.used_by_operation.is_none())
            {
                return Err(Error::InvalidOperationState);
            }
        }
        proofs.push(acquired.proof.clone());
    }
    let transaction = &saved.transaction;
    if proofs.is_empty()
        || saga.mint_url != wallet.mint_url
        || saga.unit != wallet.unit
        || saga.amount != proofs.total_amount()?
        || transaction.saga_id != Some(saga.id)
        || transaction.mint_url != wallet.mint_url
        || transaction.unit != wallet.unit
        || transaction.role != TransactionRole::Primary
        || transaction.direction != TransactionDirection::Incoming
        || transaction.status != TransactionStatus::Completed
        || transaction.ys != proofs.ys()?
        || transaction.amount.checked_add(transaction.fee) != Some(saga.amount)
        || transaction.metadata.contains_key("batch_quote_id")
        || transaction.metadata.get(receive_identity::METADATA_KEY)
            != Some(&receive_identity::fingerprint(
                &wallet.mint_url,
                &wallet.unit,
                &proofs,
            )?)
    {
        return Err(Error::InvalidOperationState);
    }
    if let Some(swap) = &saved.pre_swap {
        crate::wallet::swap::outputs::validate_request(swap)?;
        swap.swap_request.verify_spending_conditions()?;
        if swap.swap_request.inputs().len() != proofs.len()
            || !swap
                .swap_request
                .inputs()
                .iter()
                .zip(&proofs)
                .all(|(a, b)| same_coin(a, b))
            || swap.swap_request.output_amount()? != transaction.amount
            || swap.fee != transaction.fee
        {
            return Err(Error::InvalidOperationState);
        }
    } else if !matches!(
        saga.state,
        WalletSagaState::Receive(ReceiveSagaState::InputsRecorded | ReceiveSagaState::Cancelling)
    ) {
        return Err(Error::InvalidOperationState);
    }
    Ok(())
}

pub(super) async fn advance(
    wallet: &Wallet,
    saga: &mut WalletSaga,
    state: ReceiveSagaState,
) -> Result<(), Error> {
    if saga.version == u32::MAX {
        return Err(Error::InvalidOperationState);
    }
    saga.update_state(WalletSagaState::Receive(state));
    if !wallet.localstore.update_saga(saga.clone()).await? {
        return Err(Error::ConcurrentUpdate);
    }
    Ok(())
}

pub(super) async fn start(
    wallet: &Wallet,
    prepared: &Prepared,
    inputs: Vec<ProofChange>,
) -> Result<WalletSaga, Error> {
    let mut metadata = prepared.options.metadata.clone();
    if metadata.contains_key(receive_identity::METADATA_KEY) {
        return Err(Error::InvalidOperationState);
    }
    metadata.insert(
        receive_identity::METADATA_KEY.to_owned(),
        receive_identity::fingerprint(&wallet.mint_url, &wallet.unit, &prepared.proofs)?,
    );
    let transaction = Transaction {
        role: TransactionRole::Primary,
        mint_url: wallet.mint_url.clone(),
        direction: TransactionDirection::Incoming,
        amount: prepared.proofs_amount,
        fee: Amount::ZERO,
        unit: wallet.unit.clone(),
        ys: inputs.iter().map(|p| p.y).collect(),
        timestamp: unix_time(),
        memo: prepared.memo.clone(),
        metadata,
        quote_id: None,
        payment_request: None,
        payment_proof: None,
        payment_method: None,
        saga_id: Some(prepared.operation_id),
        status: TransactionStatus::Completed,
    };
    let saga = WalletSaga::new(
        prepared.operation_id,
        WalletSagaState::Receive(ReceiveSagaState::InputsRecorded),
        prepared.proofs_amount,
        wallet.mint_url.clone(),
        wallet.unit.clone(),
        OperationData::Receive(ReceiveOperationData {
            checkpoint: Some(Box::new(ReceiveCheckpoint {
                inputs: inputs.clone(),
                pre_swap: None,
                transaction,
                outputs: None,
            })),
        }),
    );
    validate(wallet, &saga)?;
    wallet.localstore.add_saga(saga.clone()).await?;
    if !wallet
        .localstore
        .compare_and_swap_proofs(inputs, Some(&saga))
        .await?
    {
        // Only this creator acquires inputs; recovery can only cancel this
        // phase. A definite false acquired nothing and cannot reach the mint.
        wallet.localstore.delete_saga(&saga.id).await?;
        return Err(Error::ConcurrentUpdate);
    }
    Ok(saga)
}

pub(super) async fn cancel(wallet: &Wallet, mut saga: WalletSaga) -> Result<(), Error> {
    validate(wallet, &saga)?;
    if saga.state != WalletSagaState::Receive(ReceiveSagaState::Cancelling) {
        if saga.state != WalletSagaState::Receive(ReceiveSagaState::InputsRecorded) {
            return Err(Error::InvalidOperationState);
        }
        advance(wallet, &mut saga, ReceiveSagaState::Cancelling).await?;
    }
    let inputs = &checkpoint(&saga)?.inputs;
    let current: HashMap<_, _> = wallet
        .localstore
        .get_proofs_by_ys(inputs.iter().map(|p| p.y).collect())
        .await?
        .into_iter()
        .map(|p| (p.y, p))
        .collect();
    let mut changes = Vec::new();
    for input in inputs {
        let actual = current.get(&input.y);
        let acquired = input.after.as_ref().ok_or(Error::InvalidOperationState)?;
        let mut observed_spent = acquired.clone();
        observed_spent.state = State::Spent;
        let replacement = if actual == Some(acquired) {
            input.before.clone()
        } else if actual == Some(&observed_spent) {
            // A mint-status observation is irreversible evidence of spending,
            // even though this cancelled receive never submitted its request.
            let mut spent = input.before.as_ref().unwrap_or(acquired).clone();
            spent.state = State::Spent;
            Some(spent)
        } else {
            // Acquisition may never have won, or rollback may already have
            // completed. Preserve another owner's exact current record.
            if actual.is_some_and(|p| p.used_by_operation == Some(saga.id)) {
                return Err(Error::ConcurrentUpdate);
            }
            actual.cloned()
        };
        changes.push(ProofChange {
            y: input.y,
            before: actual.cloned(),
            after: replacement,
        });
    }
    if !wallet
        .localstore
        .compare_and_swap_proofs(changes, Some(&saga))
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    wallet.localstore.delete_saga(&saga.id).await?;
    Ok(())
}
