//! Reserve cancellation space before acquisition; release after acknowledgment.
use cdk_common::database::Error;
use cdk_common::wallet::{
    MeltSagaState, OperationData, ReceiveSagaState, SendSagaState, SwapSagaState, WalletSaga,
    WalletSagaState,
};

use super::{claim_rows, release, require_fresh_owner, reserve_growth, schema, WalletStorageClaim};
use crate::database::DatabaseExecutor;
use crate::stmt::query;
use crate::wallet::sagas;

/// Provisional ownership stays inside the transaction admitting output keys and completion.
pub(super) async fn insert_initial<T>(
    conn: &T,
    saga: &WalletSaga,
    claims: &[WalletStorageClaim],
) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    if saga.id.is_nil() || saga.version != 0 {
        return Err(Error::InvalidDbResponse);
    }
    let owner = saga.id.to_string();
    require_fresh_owner(conn, &owner).await?;
    sagas::insert(conn, saga).await?;
    claim_rows(conn, &owner, claims).await?;
    // The caller adds its exact rows and reserves completion before commit.
    reserve_growth(conn, &owner, 0, 0).await
}

/// The caller holds a SQLite write transaction, including capacity activation.
pub(in crate::wallet) async fn insert_saga<T>(conn: &T, saga: &WalletSaga) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    if !schema::enabled(conn).await?
        || saga.state != WalletSagaState::Send(SendSagaState::PreparedAcquiring)
    {
        return sagas::insert(conn, saga).await;
    }
    let OperationData::Send(data) = &saga.data else {
        return Err(Error::InvalidDbResponse);
    };
    if saga.id.is_nil() || saga.version != 0 || data.preparation.is_none() {
        return Err(Error::InvalidDbResponse);
    }
    let owner = saga.id.to_string();
    require_fresh_owner(conn, &owner).await?;
    let growth =
        super::acquisition::cancellation_growth(&saga.state)?.ok_or(Error::InvalidDbResponse)?;
    sagas::insert(conn, saga).await?;
    // The ledger measures the actual row and its metadata, including arbitrary
    // input witnesses and caller strings. Before confirmation, cancellation
    // retains this same data and only changes state and fixed-width SQL integers.
    // Releasing Reserved inputs removes their UUID and shortens their state.
    // Confirmation/refund still require their own larger completion admission.
    let changed =
        query("UPDATE wallet_storage_accounts SET reserved=used+:growth WHERE owner=:owner")?
            .bind("growth", growth)
            .bind("owner", owner)
            .execute(conn)
            .await?;
    if changed != 1 {
        return Err(Error::InvalidDbResponse);
    }
    Ok(())
}

/// Release unused space when removing an operation with no remaining obligation.
pub(in crate::wallet) async fn delete_saga<T>(conn: &T, id: &uuid::Uuid) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    // Native send cancellation deletes after input release and caller handoff.
    // Receive deletes InputsRecorded only after losing acquisition, or Cancelling
    // after restoring its exact inputs. Neither receive phase can reach the mint.
    // Completed receives have handed off proofs and published their final receipt.
    // Standalone swaps delete only after cancellation or the exact proof handoff.
    // Their retained coins stay charged after releasing unused completion space.
    // Melts delete only after exact cancellation or paid completion.
    // Native deletes claimed/reclaimed sends after terminal receipt publication;
    // they cannot spend or reclaim again. Retained proofs and receipts stay charged.
    // Other saga endings may still own channel payouts/refunds, so retain those.
    let release_unused = if schema::enabled(conn).await? {
        let state = WalletSagaState::Send(SendSagaState::PreparedCancelled);
        let claimed = WalletSagaState::Send(SendSagaState::Claimed);
        let reclaimed = WalletSagaState::Send(SendSagaState::Reclaimed);
        let initial = WalletSagaState::Receive(ReceiveSagaState::InputsRecorded);
        let cancelling = WalletSagaState::Receive(ReceiveSagaState::Cancelling);
        let receive_done = WalletSagaState::Receive(ReceiveSagaState::Completed);
        let swap_cancel = WalletSagaState::Swap(SwapSagaState::Cancelling);
        let swap_ready = WalletSagaState::Swap(SwapSagaState::OutputsReady);
        let melt_cancel = WalletSagaState::Melt(MeltSagaState::SwapCancelling);
        let melt_done = WalletSagaState::Melt(MeltSagaState::Completed);
        query("SELECT 1 FROM wallet_sagas WHERE id=:id AND ((kind=:kind AND state IN (:state,:claimed,:reclaimed)) OR (kind=:receive AND state IN (:initial,:cancelling,:receive_done)) OR (kind=:swap AND state IN (:swap_cancel,:swap_ready)) OR (kind=:melt AND state IN (:melt_cancel,:melt_done)))")?
            .bind("id", id.to_string())
            .bind("kind", state.kind().to_string())
            .bind("state", serde_json::to_string(&state)?)
            .bind("claimed", serde_json::to_string(&claimed)?)
            .bind("reclaimed", serde_json::to_string(&reclaimed)?)
            .bind("receive", initial.kind().to_string())
            .bind("initial", serde_json::to_string(&initial)?)
            .bind("cancelling", serde_json::to_string(&cancelling)?)
            .bind("receive_done", serde_json::to_string(&receive_done)?)
            .bind("swap", swap_cancel.kind().to_string())
            .bind("swap_cancel", serde_json::to_string(&swap_cancel)?)
            .bind("swap_ready", serde_json::to_string(&swap_ready)?)
            .bind("melt", melt_cancel.kind().to_string())
            .bind("melt_cancel", serde_json::to_string(&melt_cancel)?)
            .bind("melt_done", serde_json::to_string(&melt_done)?)
            .pluck(conn)
            .await?
            .is_some()
    } else {
        false
    };
    sagas::delete(conn, id).await?;
    if release_unused {
        release(conn, &id.to_string()).await?;
    }
    Ok(())
}
