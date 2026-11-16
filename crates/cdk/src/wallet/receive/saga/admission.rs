//! Whole-set input acquisition before secret allocation or mint submission.
use std::collections::HashMap;

use cdk_common::database::wallet::{validate_proof_changes, ProofChange};
use cdk_common::wallet::{OperationData, ProofInfo, SendSagaState, WalletSagaState};

use super::state::Prepared;
use crate::nuts::{ProofsMethods, State};
use crate::wallet::util::same_coin;
use crate::{Error, Wallet};

pub(super) async fn ensure_storage(wallet: &Wallet) -> Result<(), Error> {
    if !wallet.localstore.supports_atomic_saga_completion() {
        return Err(cdk_common::database::Error::Internal(
            "Atomic saga completion is required for receives".to_owned(),
        )
        .into());
    }
    if !wallet
        .localstore
        .compare_and_swap_proofs(vec![], None)
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    Ok(())
}

pub(super) async fn plan(wallet: &Wallet, prepared: &Prepared) -> Result<Vec<ProofChange>, Error> {
    ensure_storage(wallet).await?;
    if prepared.proofs.is_empty() || prepared.options.metadata.contains_key("batch_quote_id") {
        return Err(Error::InvalidOperationState);
    }
    let existing: HashMap<_, _> = wallet
        .localstore
        .get_proofs_by_ys(prepared.proofs.ys()?)
        .await?
        .into_iter()
        .map(|p| (p.y, p))
        .collect();
    let mut inputs = Vec::new();
    for proof in &prepared.proofs {
        let y = proof.y()?;
        let before = existing.get(&y).cloned();
        let mut after = match &before {
            Some(original) => {
                if original.mint_url != wallet.mint_url
                    || original.unit != wallet.unit
                    || !same_coin(&original.proof, proof)
                {
                    return Err(Error::InvalidOperationState);
                }
                match original.state {
                    State::Unspent if original.used_by_operation.is_none() => {}
                    State::PendingSpent => {
                        let owner = original
                            .used_by_operation
                            .ok_or(Error::InvalidOperationState)?;
                        let saga = wallet
                            .localstore
                            .get_saga(&owner)
                            .await?
                            .ok_or(Error::InvalidOperationState)?;
                        let OperationData::Send(data) = &saga.data else {
                            return Err(Error::InvalidOperationState);
                        };
                        if saga.state != WalletSagaState::Send(SendSagaState::TokenCreated)
                            || saga.mint_url != wallet.mint_url
                            || saga.unit != wallet.unit
                            || !data
                                .proofs
                                .as_ref()
                                .is_some_and(|ps| ps.iter().any(|p| same_coin(p, proof)))
                        {
                            return Err(Error::InvalidOperationState);
                        }
                    }
                    _ => return Err(Error::InvalidOperationState),
                }
                original.clone()
            }
            None => ProofInfo::new(
                proof.clone(),
                wallet.mint_url.clone(),
                State::Pending,
                wallet.unit.clone(),
            )?,
        };
        after.state = State::Pending;
        after.used_by_operation = Some(prepared.operation_id);
        inputs.push(ProofChange {
            y,
            before,
            after: Some(after),
        });
    }
    validate_proof_changes(&inputs)?;
    // Retired or externally spent coins have no local tombstone. Reject them
    // before acquiring inputs; once a swap is submitted its failure is ambiguous
    // and the original recovery evidence must remain durable.
    let unknown = prepared
        .proofs
        .iter()
        .zip(&inputs)
        .filter(|(_, input)| input.before.is_none())
        .map(|(proof, _)| proof.clone())
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        for proof in wallet.check_proofs_state(unknown).await? {
            match proof.state {
                State::Unspent => {}
                State::Spent => return Err(Error::TokenAlreadySpent),
                State::Pending => return Err(Error::TokenPending),
                _ => return Err(Error::InvalidOperationState),
            }
        }
    }
    Ok(inputs)
}

pub(super) async fn check_owned(
    wallet: &Wallet,
    saga: &cdk_common::wallet::WalletSaga,
) -> Result<(), Error> {
    let inputs = &super::receipt::checkpoint(saga)?.inputs;
    let current: HashMap<_, _> = wallet
        .localstore
        .get_proofs_by_ys(inputs.iter().map(|p| p.y).collect())
        .await?
        .into_iter()
        .map(|p| (p.y, p))
        .collect();
    let mut changes = Vec::new();
    for input in inputs {
        let acquired = input.after.as_ref().ok_or(Error::InvalidOperationState)?;
        let mut observed_spent = acquired.clone();
        observed_spent.state = State::Spent;
        let actual = current.get(&input.y).ok_or(Error::ConcurrentUpdate)?;
        if actual != acquired && actual != &observed_spent {
            return Err(Error::ConcurrentUpdate);
        }
        // A public mint-status check may record Spent. It is not delivery proof:
        // recovery must still obtain this receive's complete exact outputs.
        changes.push(ProofChange {
            y: input.y,
            before: Some(actual.clone()),
            after: Some(actual.clone()),
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
