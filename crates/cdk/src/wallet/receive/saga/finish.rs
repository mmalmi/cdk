//! Fenced proof handoff followed by atomic receipt and terminal journal publication.
use std::collections::{HashMap, HashSet};

use cdk_common::database::wallet::ProofChange;
use cdk_common::wallet::{ProofInfo, ReceiveSagaState, WalletSaga, WalletSagaState};

use super::receipt;
use crate::amount::SplitTarget;
use crate::nuts::{State, SwapResponse};
use crate::wallet::util::output_change;
use crate::{Amount, Error, Wallet};

pub(super) async fn save_response(
    wallet: &Wallet,
    saga: &mut WalletSaga,
    response: SwapResponse,
) -> Result<(), Error> {
    let swap = receipt::checkpoint(saga)?
        .pre_swap
        .as_ref()
        .ok_or(Error::InvalidOperationState)?;
    let resolved = crate::wallet::swap::outputs::resolve(
        wallet,
        swap,
        None,
        &SplitTarget::None,
        None,
        response,
    )
    .await?;
    let outputs = resolved
        .change
        .into_iter()
        .zip(&swap.pre_mint_secrets.secrets)
        .map(|(proof, secret)| {
            let mut info = ProofInfo::new(
                proof,
                wallet.mint_url.clone(),
                State::Unspent,
                wallet.unit.clone(),
            )?;
            info.created_by_operation = Some(saga.id);
            info.derivation_index = secret.derivation_index;
            Ok(info)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    receipt::checkpoint_mut(saga)?.outputs = Some(outputs);
    validate_outputs(wallet, saga).await?;
    receipt::advance(wallet, saga, ReceiveSagaState::OutputsReady).await
}

async fn validate_outputs(wallet: &Wallet, saga: &WalletSaga) -> Result<(), Error> {
    receipt::validate(wallet, saga)?;
    let saved = receipt::checkpoint(saga)?;
    let swap = saved
        .pre_swap
        .as_ref()
        .ok_or(Error::InvalidOperationState)?;
    let outputs = saved.outputs.as_ref().ok_or(Error::InvalidOperationState)?;
    let mut expected: HashMap<_, _> = swap
        .pre_mint_secrets
        .secrets
        .iter()
        .map(|p| (&p.secret, p))
        .collect();
    let mut ys: HashSet<_> = saved.inputs.iter().map(|p| p.y).collect();
    if expected.len() != outputs.len() {
        return Err(Error::InvalidOperationState);
    }
    for proof in outputs {
        let secret = expected
            .remove(&proof.proof.secret)
            .ok_or(Error::InvalidOperationState)?;
        if proof.proof.y()? != proof.y
            || !ys.insert(proof.y)
            || proof.proof.amount != secret.amount
            || proof.proof.keyset_id != secret.blinded_message.keyset_id
            || proof.mint_url != wallet.mint_url
            || proof.unit != wallet.unit
            || proof.state != State::Unspent
            || proof.used_by_operation.is_some()
            || proof.created_by_operation != Some(saga.id)
            || proof.derivation_index != secret.derivation_index
        {
            return Err(Error::InvalidOperationState);
        }
        if proof.proof.dleq.is_some() {
            let keys = wallet.keyset(proof.proof.keyset_id).await?.keys;
            proof.proof.verify_dleq(
                keys.amount_key(proof.proof.amount)
                    .ok_or(Error::AmountKey)?,
            )?;
        }
    }
    Ok(())
}

pub(super) async fn complete(wallet: &Wallet, saga: &WalletSaga) -> Result<Amount, Error> {
    if saga.state != WalletSagaState::Receive(ReceiveSagaState::OutputsReady) {
        return Err(Error::InvalidOperationState);
    }
    validate_outputs(wallet, saga).await?;
    let saved = receipt::checkpoint(saga)?;
    let outputs = saved.outputs.as_ref().ok_or(Error::InvalidOperationState)?;
    let ys = saved
        .inputs
        .iter()
        .map(|p| p.y)
        .chain(outputs.iter().map(|p| p.y))
        .collect();
    let current: HashMap<_, _> = wallet
        .localstore
        .get_proofs_by_ys(ys)
        .await?
        .into_iter()
        .map(|p| (p.y, p))
        .collect();
    let mut changes = Vec::new();
    for input in &saved.inputs {
        let acquired = input.after.as_ref().ok_or(Error::InvalidOperationState)?;
        let mut spent = input.before.as_ref().unwrap_or(acquired).clone();
        spent.state = State::Spent;
        let actual = current.get(&input.y).ok_or(Error::ConcurrentUpdate)?;
        let mut observed_spent = acquired.clone();
        observed_spent.state = State::Spent;
        if actual != acquired && actual != &spent && actual != &observed_spent {
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
        changes.push(output_change(output.clone(), actual)?);
    }
    if !wallet
        .localstore
        .compare_and_swap_proofs(changes, Some(saga))
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    let mut completed = saga.clone();
    completed.update_state(WalletSagaState::Receive(ReceiveSagaState::Completed));
    if !wallet
        .localstore
        .complete_saga(saga, completed, saved.transaction.clone())
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    wallet.localstore.delete_saga(&saga.id).await?;
    Ok(saved.transaction.amount)
}
