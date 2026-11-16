//! Atomic proof and key-value comparisons fenced by an optional saved operation.
use cdk_common::database::wallet::{validate_proof_changes, ProofChange};
use cdk_common::database::Error;
use cdk_common::{database, wallet};

use super::storage_capacity::WalletStorageReservation;
use super::{kv_batch, proofs, sagas, SQLWalletDatabase, WalletKvChange};
use crate::database::{ConnectionWithTransaction, DatabaseExecutor};
use crate::pool::DatabasePool;
use crate::stmt::query;

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    pub(super) async fn apply_proof_changes(
        &self,
        changes: Vec<ProofChange>,
        expected_saga: Option<&wallet::WalletSaga>,
    ) -> Result<bool, database::Error> {
        self.apply_wallet_changes(changes, expected_saga, &[], None)
            .await
    }

    pub(super) async fn apply_wallet_changes(
        &self,
        changes: Vec<ProofChange>,
        expected_saga: Option<&wallet::WalletSaga>,
        kv_changes: &[WalletKvChange<'_>],
        reservation: Option<WalletStorageReservation<'_>>,
    ) -> Result<bool, database::Error> {
        validate_proof_changes(&changes)?;
        kv_batch::validate(kv_changes)?;
        if changes.is_empty() && expected_saga.is_none() && kv_changes.is_empty() {
            return Ok(true);
        }
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        let tx = ConnectionWithTransaction::new(conn).await?;
        let outcome: Result<bool, Error> = async {
            // SQLite's BEGIN IMMEDIATE serializes writers. PostgreSQL needs a
            // serializable snapshot, including identities that are currently absent.
            if RM::Connection::name() == "postgres" {
                query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")?
                    .execute(&tx)
                    .await?;
            }
            if let Some(expected) = expected_saga {
                // Hold the operation row until the proof writes commit. A
                // serializable read alone could order this transaction before
                // cancellation while cancellation had already read old proofs.
                if !sagas::matches_locked(
                    &tx,
                    &expected.id,
                    Some(expected),
                    self.saga_read_limit,
                    RM::Connection::name() == "postgres",
                )
                .await?
                {
                    return Ok(false);
                }
            }
            let storage_saga = (RM::Connection::name() == "sqlite")
                .then_some(expected_saga)
                .flatten();
            if !proofs::apply(&tx, changes, storage_saga).await? {
                return Ok(false);
            }
            if !kv_batch::apply_part::<RM>(&tx, kv_changes, true).await? {
                return Ok(false);
            }
            if let Some(reservation) = &reservation {
                reservation.prepare(&tx).await?;
            }
            if !kv_batch::apply_part::<RM>(&tx, kv_changes, false).await? {
                return Ok(false);
            }
            if let Some(reservation) = &reservation {
                reservation.finish(&tx).await?;
            }
            Ok(true)
        }
        .await;
        match outcome {
            Ok(true) => {
                tx.commit().await?;
                Ok(true)
            }
            other => {
                // Finish rollback before returning. Deferring it to Drop can
                // leave a writer lock behind while the caller starts recovery.
                tx.rollback().await?;
                other
            }
        }
    }
}
