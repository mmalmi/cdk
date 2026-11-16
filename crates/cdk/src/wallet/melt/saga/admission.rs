//! Confirmation and rollback use only the whole input batch saved at acquisition.
use std::collections::HashMap;

use cdk_common::database::wallet::{validate_melt_inputs, ProofChange};
use cdk_common::wallet::{
    MeltQuote, MeltSagaState, OperationData, ProofInfo, WalletSaga, WalletSagaState,
};

use crate::nuts::{Proofs, State};
use crate::wallet::util::same_coin;
use crate::{Error, Wallet};

pub(super) fn saved<'a>(wallet: &Wallet, saga: &'a WalletSaga) -> Result<&'a [ProofChange], Error> {
    if saga.mint_url != wallet.mint_url || saga.unit != wallet.unit {
        return Err(Error::InvalidOperationState);
    }
    Ok(validate_melt_inputs(saga)?)
}

pub(super) fn validate_inputs(
    wallet: &Wallet,
    saga: &WalletSaga,
    proofs: &Proofs,
) -> Result<(), Error> {
    let plan = saved(wallet, saga)?;
    let mut provided = HashMap::new();
    for proof in proofs {
        if provided.insert(proof.y()?, proof).is_some() {
            return Err(Error::InvalidOperationState);
        }
    }
    if provided.len() != plan.len() {
        return Err(Error::InvalidOperationState);
    }
    for change in plan {
        let expected = change.after.as_ref().ok_or(Error::InvalidOperationState)?;
        if provided.get(&change.y).copied() != Some(&expected.proof) {
            return Err(Error::InvalidOperationState);
        }
    }
    Ok(())
}

pub(super) async fn quote(
    wallet: &Wallet,
    saga: &WalletSaga,
    quote: &MeltQuote,
) -> Result<MeltQuote, Error> {
    if saga.state != WalletSagaState::Melt(MeltSagaState::ProofsReserved)
        || saga.quote_id.as_deref() != Some(quote.id.as_str())
    {
        return Err(Error::InvalidOperationState);
    }
    let OperationData::Melt(data) = &saga.data else {
        return Err(Error::InvalidOperationState);
    };
    if quote.fee_reserve != data.fee_reserve {
        return Err(Error::InvalidOperationState);
    }
    let actual = wallet
        .localstore
        .get_melt_quote(&quote.id)
        .await?
        .ok_or(Error::ConcurrentUpdate)?;
    let mut expected = quote.clone();
    expected.version = actual.version;
    expected.used_by_operation = Some(saga.id.to_string());
    if expected != actual || actual.amount != saga.amount || actual.unit != wallet.unit {
        return Err(Error::ConcurrentUpdate);
    }
    Ok(actual)
}

pub(super) async fn no_extra_inputs(wallet: &Wallet, saga: &WalletSaga) -> Result<(), Error> {
    let mut expected = saved(wallet, saga)?
        .iter()
        .map(|p| p.y)
        .collect::<std::collections::HashSet<_>>();
    if let cdk_common::wallet::OperationData::Melt(data) = &saga.data {
        if let Some(outputs) = data.swap.as_ref().and_then(|swap| swap.outputs.as_ref()) {
            for proof in outputs.send.iter().chain(&outputs.change) {
                expected.insert(proof.y()?);
            }
        }
    }
    if wallet
        .localstore
        .get_reserved_proofs(&saga.id)
        .await?
        .iter()
        .any(|p| !expected.contains(&p.y))
    {
        return Err(Error::InvalidOperationState);
    }
    Ok(())
}

pub(super) async fn pending(
    wallet: &Wallet,
    saga: &WalletSaga,
    proofs: &Proofs,
) -> Result<Vec<ProofInfo>, Error> {
    if saga.state != WalletSagaState::Melt(MeltSagaState::ProofsReserved) {
        return Err(Error::InvalidOperationState);
    }
    validate_inputs(wallet, saga, proofs)?;
    let plan = saved(wallet, saga)?;
    no_extra_inputs(wallet, saga).await?;
    let plan: HashMap<_, _> = plan.iter().map(|p| (p.y, p)).collect();
    let changes = proofs
        .iter()
        .map(|proof| {
            let p = plan.get(&proof.y()?).ok_or(Error::InvalidOperationState)?;
            let mut pending = p.after.clone().ok_or(Error::InvalidOperationState)?;
            pending.state = State::Pending;
            Ok(ProofChange {
                y: p.y,
                before: p.after.clone(),
                after: Some(pending),
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let pending = changes.iter().filter_map(|p| p.after.clone()).collect();
    commit(wallet, saga, changes).await?;
    Ok(pending)
}

fn same_record(actual: &ProofInfo, original: &ProofInfo) -> bool {
    same_coin(&actual.proof, &original.proof)
        && actual.mint_url == original.mint_url
        && actual.unit == original.unit
        && actual.spending_condition == original.spending_condition
        && actual.created_by_operation == original.created_by_operation
        && actual.derivation_index == original.derivation_index
}

pub(super) async fn release(wallet: &Wallet, saga: &WalletSaga) -> Result<(), Error> {
    if saga.state != WalletSagaState::Melt(MeltSagaState::SwapCancelling) {
        return Err(Error::InvalidOperationState);
    }
    let plan = saved(wallet, saga)?;
    no_extra_inputs(wallet, saga).await?;
    let current: HashMap<_, _> = wallet
        .localstore
        .get_proofs_by_ys(plan.iter().map(|p| p.y).collect())
        .await?
        .into_iter()
        .map(|p| (p.y, p))
        .collect();
    let mut changes = Vec::new();
    for p in plan {
        let acquired = p.after.as_ref().ok_or(Error::InvalidOperationState)?;
        let actual = current.get(&p.y).ok_or(Error::ConcurrentUpdate)?;
        let mut pending = acquired.clone();
        pending.state = State::Pending;
        let mut spent = acquired.clone();
        spent.state = State::Spent;
        let after = if actual == acquired || actual == &pending {
            let mut restored = p.before.as_ref().unwrap_or(acquired).clone();
            restored.state = State::Unspent;
            restored.used_by_operation = None;
            restored
        } else if actual == &spent
            || (actual.used_by_operation != Some(saga.id) && same_record(actual, acquired))
        {
            actual.clone()
        } else {
            return Err(Error::ConcurrentUpdate);
        };
        changes.push(ProofChange {
            y: p.y,
            before: Some(actual.clone()),
            after: Some(after),
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
