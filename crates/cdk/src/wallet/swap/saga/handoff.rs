//! One proof transaction consumes original inputs and preserves later output owners.
use std::collections::{HashMap, HashSet};

use cdk_common::database::wallet::ProofChange;
use cdk_common::wallet::{ProofInfo, SwapSagaState, WalletSaga, WalletSagaState};

use super::checkpoint;
use crate::nuts::State;
use crate::wallet::util::output_change;
use crate::{Error, Wallet};

pub(super) async fn complete(
    wallet: &Wallet,
    saga: &WalletSaga,
    outputs: Vec<ProofInfo>,
) -> Result<(), Error> {
    if saga.state != WalletSagaState::Swap(SwapSagaState::OutputsReady) {
        return Err(Error::InvalidOperationState);
    }
    let plan = &checkpoint::get(saga)?.acquisition;
    let ys = plan
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
    let mut seen = HashSet::new();
    for change in plan {
        let acquired = change.after.as_ref().ok_or(Error::InvalidOperationState)?;
        let mut spent = acquired.clone();
        spent.state = State::Spent;
        let actual = current.get(&change.y).ok_or(Error::ConcurrentUpdate)?;
        if !seen.insert(change.y) || (actual != acquired && actual != &spent) {
            return Err(Error::ConcurrentUpdate);
        }
        changes.push(ProofChange {
            y: change.y,
            before: Some(actual.clone()),
            after: Some(spent),
        });
    }
    for output in outputs {
        if !seen.insert(output.y) {
            return Err(Error::InvalidOperationState);
        }
        let actual = current.get(&output.y);
        if output.created_by_operation != Some(saga.id) {
            return Err(Error::InvalidOperationState);
        }
        changes.push(output_change(output, actual)?);
    }
    if !wallet
        .localstore
        .compare_and_swap_proofs(changes, Some(saga))
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    wallet.localstore.delete_saga(&saga.id).await?;
    Ok(())
}
