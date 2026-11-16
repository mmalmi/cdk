//! Verified paid outcomes survive proof handoff and final receipt publication.
use std::collections::{HashMap, HashSet};

use cdk_common::database::wallet::validate_melt_completion;
use cdk_common::wallet::{
    KeysetLoadPolicy, MeltPayment, MeltSagaState, OperationData, ProofInfo, TransactionStatus,
    WalletSaga, WalletSagaState,
};

use super::custody;
use crate::nuts::{BlindSignature, MeltQuoteState, PreMintSecrets};
use crate::types::FinalizedMelt;
use crate::wallet::recovery::{OutputRecoveryMode, OutputRecoveryResult};
use crate::{Amount, Error, Wallet};

fn payment<'a>(wallet: &Wallet, saga: &'a WalletSaga) -> Result<&'a MeltPayment, Error> {
    let data = custody::data(wallet, saga)?;
    data.payment.as_deref().ok_or(Error::InvalidOperationState)
}

fn advance(saga: &WalletSaga, state: MeltSagaState) -> Result<WalletSaga, Error> {
    saga.version
        .checked_add(1)
        .ok_or(Error::InvalidOperationState)?;
    let mut next = saga.clone();
    next.update_state(WalletSagaState::Melt(state));
    next.updated_at = next.updated_at.max(saga.updated_at);
    Ok(next)
}

/// Persist the verified outcome before making any paid proof changes.
pub(super) async fn record_paid(
    wallet: &Wallet,
    saga: &WalletSaga,
    payment_proof: Option<String>,
    signatures: Option<Vec<BlindSignature>>,
    policy: KeysetLoadPolicy,
) -> Result<FinalizedMelt, Error> {
    if matches!(
        saga.state,
        WalletSagaState::Melt(MeltSagaState::PaidReady | MeltSagaState::Completed)
    ) {
        return finish(wallet, saga).await;
    }
    let data = custody::data(wallet, saga)?;
    let saved = payment(wallet, saga)?;
    if saved.receipt.status != TransactionStatus::Pending || saved.change.is_some() {
        return Err(Error::InvalidOperationState);
    }
    if wallet
        .localstore
        .get_transaction(saved.receipt.id())
        .await?
        .as_ref()
        != Some(&saved.receipt)
    {
        return Err(Error::ConcurrentUpdate);
    }
    custody::inputs(wallet, saga).await?;
    let messages = data.change_blinded_messages.as_deref().unwrap_or_default();
    let mut outputs = if messages.is_empty() {
        if signatures
            .as_ref()
            .is_some_and(|signatures| !signatures.is_empty())
        {
            return Err(Error::InvalidMintResponse(
                "unrequested melt change".to_owned(),
            ));
        }
        Vec::new()
    } else {
        let start = data.counter_start.ok_or(Error::InvalidOperationState)?;
        let end = data.counter_end.ok_or(Error::InvalidOperationState)?;
        // The saved output keyset remains authoritative after key rotation.
        for id in messages
            .iter()
            .map(|message| message.keyset_id)
            .collect::<HashSet<_>>()
        {
            wallet.keyset_with_policy(id, policy).await?;
        }
        let restored = match signatures {
            Some(signatures) => {
                wallet
                    .recover_melt_change_signatures(&saga.id, messages, start, end, signatures)
                    .await?
            }
            None => {
                wallet
                    .restore_outputs_with_result(
                        &saga.id,
                        "Melt",
                        Some(messages),
                        Some(start),
                        Some(end),
                        OutputRecoveryMode::Partial,
                    )
                    .await?
            }
        };
        match restored {
            OutputRecoveryResult::Restored(outputs) => outputs,
            OutputRecoveryResult::EmptyResponse => Vec::new(),
            OutputRecoveryResult::Unavailable => return Err(Error::InvalidOperationState),
        }
    };
    for output in &mut outputs {
        output.created_by_operation = Some(saga.id);
    }
    let input_total = Amount::try_sum(saved.inputs.iter().map(|input| input.proof.amount))?;
    let change_total = Amount::try_sum(outputs.iter().map(|output| output.proof.amount))?;
    let quote = wallet
        .localstore
        .get_melt_quote(&data.quote_id)
        .await?
        .ok_or(Error::UnknownQuote)?;
    let mut ready = advance(saga, MeltSagaState::PaidReady)?;
    let OperationData::Melt(ref mut data) = ready.data else {
        return Err(Error::InvalidOperationState);
    };
    let payment = data.payment.as_mut().ok_or(Error::InvalidOperationState)?;
    payment.receipt.fee = input_total
        .checked_sub(data.amount)
        .and_then(|amount| amount.checked_sub(change_total))
        .ok_or(Error::AmountOverflow)?;
    payment.receipt.status = TransactionStatus::Completed;
    // An omitted proof does not erase evidence already saved on this owned quote.
    payment.receipt.payment_proof = payment_proof.or_else(|| quote.payment_proof.clone());
    payment.change = Some(outputs);
    let completed = advance(&ready, MeltSagaState::Completed)?;
    validate_melt_completion(&ready, &completed, &quote)?;
    validate_change(wallet, &ready).await?;
    if !wallet.localstore.update_saga(ready.clone()).await? {
        return Err(Error::ConcurrentUpdate);
    }
    finish(wallet, &ready).await
}

/// Complete a saved paid result without querying the mint or reconstructing history.
pub(super) async fn finish(wallet: &Wallet, saga: &WalletSaga) -> Result<FinalizedMelt, Error> {
    let saved = payment(wallet, saga)?;
    if saved.receipt.status != TransactionStatus::Completed || saved.change.is_none() {
        return Err(Error::InvalidOperationState);
    }
    match saga.state {
        WalletSagaState::Melt(MeltSagaState::PaidReady) => {
            let data = custody::data(wallet, saga)?;
            let quote = wallet
                .localstore
                .get_melt_quote(&data.quote_id)
                .await?
                .ok_or(Error::UnknownQuote)?;
            let next = advance(saga, MeltSagaState::Completed)?;
            validate_melt_completion(saga, &next, &quote)?;
            validate_change(wallet, saga).await?;
            let inputs = custody::inputs(wallet, saga).await?;
            custody::apply(
                wallet,
                saga,
                &inputs,
                saved.change.clone().ok_or(Error::InvalidOperationState)?,
            )
            .await?;
            if !wallet.localstore.complete_melt(saga, next, &quote).await? {
                return Err(Error::ConcurrentUpdate);
            }
        }
        WalletSagaState::Melt(MeltSagaState::Completed) => (),
        _ => return Err(Error::InvalidOperationState),
    }
    let change: Vec<_> = saved
        .change
        .as_ref()
        .ok_or(Error::InvalidOperationState)?
        .iter()
        .map(|output| output.proof.clone())
        .collect();
    let result = FinalizedMelt::new(
        saved
            .receipt
            .quote_id
            .clone()
            .ok_or(Error::InvalidOperationState)?,
        MeltQuoteState::Paid,
        saved.receipt.payment_proof.clone(),
        saved.receipt.amount,
        saved.receipt.fee,
        (!change.is_empty()).then_some(change),
    );
    // Completion is already durable. A failed cleanup leaves a local-only retry.
    if let Err(error) = wallet.localstore.delete_saga(&saga.id).await {
        tracing::warn!(
            "Failed to delete completed melt saga {}: {}",
            saga.id,
            error
        );
    }
    Ok(result)
}

/// Saved outcomes must still name the exact secrets and derivation allocated before payment.
async fn validate_change(wallet: &Wallet, saga: &WalletSaga) -> Result<(), Error> {
    let data = custody::data(wallet, saga)?;
    let outputs = payment(wallet, saga)?
        .change
        .as_ref()
        .ok_or(Error::InvalidOperationState)?;
    if outputs.is_empty() {
        return Ok(());
    }
    let messages = data
        .change_blinded_messages
        .as_ref()
        .ok_or(Error::InvalidOperationState)?;
    let id = messages
        .first()
        .ok_or(Error::InvalidOperationState)?
        .keyset_id;
    let secrets = PreMintSecrets::restore_batch(
        id,
        &wallet.seed,
        data.counter_start.ok_or(Error::InvalidOperationState)?,
        data.counter_end.ok_or(Error::InvalidOperationState)?,
    )?;
    let requested: HashSet<_> = messages
        .iter()
        .map(|message| message.blinded_secret)
        .collect();
    if requested.len() != messages.len() || messages.iter().any(|message| message.keyset_id != id) {
        return Err(Error::InvalidOperationState);
    }
    let mut allocated: HashMap<_, _> = secrets
        .secrets
        .iter()
        .filter(|secret| requested.contains(&secret.blinded_message.blinded_secret))
        .map(|secret| (&secret.secret, secret))
        .collect();
    if allocated.len() != messages.len() {
        return Err(Error::InvalidOperationState);
    }
    let keys = wallet
        .keyset_with_policy(id, KeysetLoadPolicy::CacheOnly)
        .await?
        .keys;
    for output in outputs {
        let secret = allocated
            .remove(&output.proof.secret)
            .ok_or(Error::InvalidOperationState)?;
        if output.proof.keyset_id != id || output.derivation_index != secret.derivation_index {
            return Err(Error::InvalidOperationState);
        }
        let mut canonical = ProofInfo::new_with_operations(
            output.proof.clone(),
            wallet.mint_url.clone(),
            crate::nuts::State::Unspent,
            wallet.unit.clone(),
            None,
            Some(saga.id),
        )?;
        canonical.derivation_index = secret.derivation_index;
        if &canonical != output {
            return Err(Error::InvalidOperationState);
        }
        if output.proof.dleq.is_some() {
            output.proof.verify_dleq(
                keys.amount_key(output.proof.amount)
                    .ok_or(Error::AmountKey)?,
            )?;
        }
    }
    Ok(())
}
