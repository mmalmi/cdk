//! Receipt publication and exact parent transition in one storage transaction.
use cdk_common::database::wallet::{merge_saga_receipt, validate_saga_completion};
use cdk_common::database::Error;
use cdk_common::wallet::{Transaction, WalletSaga};

use super::{sagas, storage_capacity, transactions, SQLWalletDatabase};
use crate::database::{ConnectionWithTransaction, DatabaseExecutor};
use crate::pool::DatabasePool;
use crate::stmt::query;

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    pub(super) async fn commit_saga_receipt(
        &self,
        expected: &WalletSaga,
        next: WalletSaga,
        receipt: Transaction,
    ) -> Result<bool, Error> {
        validate_saga_completion(expected, &next, &receipt)?;
        self.check_saga_read_limit(&next)?;
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        let tx = ConnectionWithTransaction::new(conn).await?;
        let outcome: Result<bool, Error> = async {
            if RM::Connection::name() == "postgres" {
                query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")?.execute(&tx).await?;
            }
            if !sagas::matches_locked(&tx, &expected.id, Some(expected), self.saga_read_limit, RM::Connection::name() == "postgres").await? { return Ok(false); }
            if RM::Connection::name() == "sqlite" {
                storage_capacity::admit_melt_payment(&tx, expected, &next).await?;
            }
            if !store_receipt(&tx, receipt).await? { return Ok(false); }
            query("UPDATE wallet_sagas SET state = :state, updated_at = :updated_at, data = :data, version = :version WHERE id = :id")?
                .bind("id", next.id.to_string())
                .bind("state", serde_json::to_string(&next.state)?)
                .bind("updated_at", i64::try_from(next.updated_at).map_err(|_| Error::Internal("Saga time out of range".to_owned()))?)
                .bind("data", serde_json::to_string(&next.data)?)
                .bind("version", i64::from(next.version))
                .execute(&tx).await?;
            Ok(true)
        }.await;
        match outcome {
            Ok(true) => {
                tx.commit().await?;
                Ok(true)
            }
            other => {
                tx.rollback().await?;
                other
            }
        }
    }
}

/// Apply the receipt conflict rules inside the caller's transaction.
pub(super) async fn store_receipt<T>(tx: &T, receipt: Transaction) -> Result<bool, Error>
where
    T: DatabaseExecutor,
{
    let current = transactions::load_for_comparison(tx, &receipt).await?;
    let Some(receipt) = merge_saga_receipt(current, receipt) else {
        return Ok(false);
    };
    transactions::store(tx, receipt).await?;
    Ok(true)
}
