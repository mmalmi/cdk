//! Fence a recipient claim against reclaim before publishing terminal history.
use std::collections::HashMap;

use cdk_common::database::wallet::ProofChange;
use cdk_common::wallet::{
    OperationData, SendSagaState, TransactionStatus, WalletSaga, WalletSagaState,
};

use super::terminal;
use crate::nuts::{ProofsMethods, State};
use crate::{Error, Wallet};

pub(crate) async fn complete(wallet: &Wallet, mut saga: WalletSaga) -> Result<(), Error> {
    if saga.mint_url != wallet.mint_url || saga.unit != wallet.unit {
        return Err(Error::InvalidOperationState);
    }
    wallet.ensure_send_storage(Some(&saga)).await?;
    let OperationData::Send(data) = &saga.data else {
        return Err(Error::InvalidOperationState);
    };
    if data.confirmation.is_some() || data.reclaim.is_some() {
        return Err(Error::InvalidOperationState);
    }
    let proofs = data
        .proofs
        .as_ref()
        .filter(|p| !p.is_empty())
        .ok_or(Error::InvalidOperationState)?
        .clone();
    match saga.state {
        WalletSagaState::Send(SendSagaState::TokenCreated) => {
            if saga.version == u32::MAX {
                return Err(Error::InvalidOperationState);
            }
            saga.update_state(WalletSagaState::Send(SendSagaState::Claimed));
            if !wallet.localstore.update_saga(saga.clone()).await? {
                return Err(Error::ConcurrentUpdate);
            }
        }
        WalletSagaState::Send(SendSagaState::Claimed) => {}
        _ => return Err(Error::InvalidOperationState),
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
        let original = current.remove(&proof.y()?).ok_or(Error::ConcurrentUpdate)?;
        super::custody::validate_retained_proof(&original, &proof)?;
        if original.used_by_operation != Some(saga.id)
            || !matches!(original.state, State::PendingSpent | State::Spent)
            || original.mint_url != wallet.mint_url
            || original.unit != wallet.unit
        {
            return Err(Error::ConcurrentUpdate);
        }
        let mut spent = original.clone();
        spent.state = State::Spent;
        changes.push(ProofChange {
            y: original.y,
            before: Some(original),
            after: Some(spent),
        });
    }
    if !wallet
        .localstore
        .compare_and_swap_proofs(changes, Some(&saga))
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    terminal::finish_primary(wallet, saga, TransactionStatus::Completed).await
}
