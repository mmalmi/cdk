//! Atomic swap handoff: preserve input custody and never revive existing outputs.
use std::collections::{HashMap, HashSet};

use cdk_common::database::wallet::ProofChange;
use cdk_common::wallet::{ProofInfo, SendSagaState, WalletSaga, WalletSagaState};

use crate::nuts::State;
use crate::{Error, Wallet};

pub(super) async fn apply(
    wallet: &Wallet,
    saga: &WalletSaga,
    inputs: &[ProofInfo],
    outputs: Vec<ProofInfo>,
) -> Result<(), Error> {
    if saga.state != WalletSagaState::Send(SendSagaState::Reclaiming) {
        return Err(Error::InvalidOperationState);
    }
    let ys = inputs.iter().chain(&outputs).map(|p| p.y).collect();
    let current: HashMap<_, _> = wallet
        .localstore
        .get_proofs_by_ys(ys)
        .await?
        .into_iter()
        .map(|p| (p.y, p))
        .collect();
    let mut seen = HashSet::new();
    let mut changes = Vec::new();
    for input in inputs {
        if !seen.insert(input.y)
            || input.mint_url != wallet.mint_url
            || input.unit != wallet.unit
            || input.state != State::Pending
        {
            return Err(Error::InvalidOperationState);
        }
        let mut spent = input.clone();
        spent.state = State::Spent;
        let actual = current.get(&input.y).ok_or(Error::ConcurrentUpdate)?;
        if actual != input && actual != &spent {
            return Err(Error::ConcurrentUpdate);
        }
        changes.push(ProofChange {
            y: input.y,
            before: Some(actual.clone()),
            after: Some(spent),
        });
    }
    for mut output in outputs {
        if !seen.insert(output.y)
            || output.mint_url != wallet.mint_url
            || output.unit != wallet.unit
        {
            return Err(Error::InvalidOperationState);
        }
        output.created_by_operation = Some(saga.id);
        let actual = current.get(&output.y);
        // Preserve an output's current state and owner after later spending.
        // Compare immutable minted identity; a spending witness can change.
        if let Some(actual) = actual {
            if actual.created_by_operation != Some(saga.id)
                || actual.mint_url != output.mint_url
                || actual.unit != output.unit
                || actual.proof.amount != output.proof.amount
                || actual.proof.keyset_id != output.proof.keyset_id
                || actual.proof.secret != output.proof.secret
                || actual.proof.c != output.proof.c
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
