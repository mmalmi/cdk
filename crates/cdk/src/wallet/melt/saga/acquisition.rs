//! Select and validate first; the quote, full input batch and journal then commit once.
use std::collections::HashMap;

use cdk_common::database::wallet::ProofChange;
use cdk_common::wallet::{
    MeltOperationData, MeltQuote, MeltSagaState, OperationData, ProofInfo, WalletSaga,
    WalletSagaState,
};

use crate::nuts::{MeltQuoteState, Proofs, ProofsMethods, State};
use crate::util::unix_time;
use crate::{Amount, Error, Wallet};

pub(super) async fn quote(wallet: &Wallet, id: &str) -> Result<MeltQuote, Error> {
    let quote = wallet
        .localstore
        .get_melt_quote(id)
        .await?
        .ok_or(Error::UnknownQuote)?;
    if quote.expiry <= unix_time() {
        return Err(Error::ExpiredQuote(quote.expiry, unix_time()));
    }
    if quote.unit != wallet.unit
        || quote
            .mint_url
            .as_ref()
            .is_some_and(|url| url != &wallet.mint_url)
        || quote.used_by_operation.is_some()
        || quote.payment_proof.is_some()
        || !matches!(quote.state, MeltQuoteState::Unpaid | MeltQuoteState::Failed)
    {
        return Err(Error::InvalidOperationState);
    }
    Ok(quote)
}

pub(super) async fn acquire(
    wallet: &Wallet,
    id: uuid::Uuid,
    quote: &MeltQuote,
    proofs: &Proofs,
    external: bool,
    metadata: HashMap<String, String>,
) -> Result<(MeltQuote, WalletSaga), Error> {
    if metadata.contains_key("batch_quote_id") {
        return Err(Error::InvalidOperationState);
    }
    if !wallet.localstore.supports_atomic_saga_completion()
        || !wallet.localstore.supports_atomic_melt_completion()
    {
        return Err(cdk_common::database::Error::Internal(
            "Atomic completion is required for melts".to_owned(),
        )
        .into());
    }
    if !wallet
        .localstore
        .compare_and_swap_proofs(Vec::new(), None)
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    let mut current: HashMap<_, _> = wallet
        .localstore
        .get_proofs_by_ys(proofs.ys()?)
        .await?
        .into_iter()
        .map(|p| (p.y, p))
        .collect();
    let mut changes = Vec::new();
    for proof in proofs {
        let y = proof.y()?;
        let before = current.remove(&y);
        let mut after = match &before {
            Some(before) => {
                if before.mint_url != wallet.mint_url
                    || before.unit != wallet.unit
                    || before.proof != *proof
                    || before.state != State::Unspent
                    || before.used_by_operation.is_some()
                {
                    return Err(Error::ConcurrentUpdate);
                }
                before.clone()
            }
            None if external => ProofInfo::new(
                proof.clone(),
                wallet.mint_url.clone(),
                State::Unspent,
                wallet.unit.clone(),
            )?,
            None => return Err(Error::ConcurrentUpdate),
        };
        after.state = State::Reserved;
        after.used_by_operation = Some(id);
        changes.push(ProofChange {
            y,
            before,
            after: Some(after),
        });
    }
    let saga = WalletSaga::new(
        id,
        WalletSagaState::Melt(MeltSagaState::ProofsReserved),
        quote.amount,
        wallet.mint_url.clone(),
        wallet.unit.clone(),
        OperationData::Melt(MeltOperationData {
            payment: None,
            acquisition: Some(changes),
            swap: None,
            quote_id: quote.id.clone(),
            amount: quote.amount,
            fee_reserve: quote.fee_reserve,
            metadata,
            counter_start: None,
            counter_end: None,
            change_amount: None,
            final_proof_ys: None,
            change_blinded_messages: None,
        }),
    );
    if !wallet.localstore.acquire_melt(&saga, quote).await? {
        return Err(Error::ConcurrentUpdate);
    }
    let mut expected = quote.clone();
    expected.used_by_operation = Some(id.to_string());
    expected.version = expected
        .version
        .checked_add(1)
        .ok_or(Error::InvalidOperationState)?;
    let owned = wallet
        .localstore
        .get_melt_quote(&quote.id)
        .await?
        .ok_or(Error::ConcurrentUpdate)?;
    if owned != expected {
        return Err(Error::ConcurrentUpdate);
    }
    Ok((owned, saga))
}

pub(super) fn required(quote: &MeltQuote, fee: Amount) -> Result<Amount, Error> {
    quote
        .amount
        .checked_add(quote.fee_reserve)
        .and_then(|amount| amount.checked_add(fee))
        .ok_or(Error::AmountOverflow)
}
