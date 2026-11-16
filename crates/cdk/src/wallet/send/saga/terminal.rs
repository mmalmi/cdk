//! Publish terminal send history while the original saga still guards custody.
use cdk_common::wallet::{
    SendSagaState, Transaction, TransactionDirection, TransactionId, TransactionRole,
    TransactionStatus, WalletSaga, WalletSagaState,
};

use crate::{Error, Wallet};

pub(super) async fn publish(
    wallet: &Wallet,
    saga: &mut WalletSaga,
    receipt: Transaction,
    state: SendSagaState,
) -> Result<(), Error> {
    if saga.version == u32::MAX {
        return Err(Error::InvalidOperationState);
    }
    let mut next = saga.clone();
    next.update_state(WalletSagaState::Send(state));
    if !wallet
        .localstore
        .complete_saga(saga, next.clone(), receipt)
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    *saga = next;
    Ok(())
}

pub(super) async fn finish_primary(
    wallet: &Wallet,
    mut saga: WalletSaga,
    status: TransactionStatus,
) -> Result<(), Error> {
    let state = match (saga.state, status) {
        (WalletSagaState::Send(SendSagaState::Claimed), TransactionStatus::Completed) => {
            SendSagaState::Claimed
        }
        (WalletSagaState::Send(SendSagaState::Reclaimed), TransactionStatus::Failed) => {
            SendSagaState::Reclaimed
        }
        _ => return Err(Error::InvalidOperationState),
    };
    let mut transaction = wallet
        .localstore
        .get_transaction(TransactionId::from_saga_id(saga.id))
        .await?
        .ok_or(Error::InvalidOperationState)?;
    if transaction.role != TransactionRole::Primary
        || transaction.direction != TransactionDirection::Outgoing
        || transaction.saga_id != Some(saga.id)
        || transaction.mint_url != wallet.mint_url
        || transaction.unit != wallet.unit
        || transaction.amount != saga.amount
        || transaction.timestamp != saga.created_at
    {
        return Err(Error::InvalidOperationState);
    }
    transaction.status = status;
    publish(wallet, &mut saga, transaction, state).await?;
    // Terminal send states cannot start another mint request or reclaim. A stale
    // publisher cannot recreate history after this deletion: its saga comparison fails.
    wallet.localstore.delete_saga(&saga.id).await?;
    Ok(())
}
