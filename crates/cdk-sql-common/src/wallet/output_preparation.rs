//! Commit a private output proposal without orphaning keys or consuming counters.
use cdk_common::database::wallet::validate_output_preparation;
use cdk_common::database::Error;
use cdk_common::wallet::{Transaction, WalletSaga};
use cdk_common::KeySet;

use crate::database::{ConnectionWithTransaction, DatabaseExecutor};
use crate::pool::DatabasePool;
use crate::stmt::query;
use crate::wallet::{keysets, sagas, storage_capacity, SQLWalletDatabase};

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    pub(super) async fn commit_output_preparation(
        &self,
        expected: Option<&WalletSaga>,
        next: WalletSaga,
        keyset: KeySet,
        counter_start: u32,
        counter_count: u32,
        receipt: Option<Transaction>,
    ) -> Result<bool, Error> {
        validate_output_preparation(expected, &next, receipt.as_ref())?;
        self.check_saga_read_limit(&next)?;
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        let tx = ConnectionWithTransaction::new(conn).await?;
        let result = async {
            if receipt.is_some() && RM::Connection::name() == "postgres" {
                query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")?
                    .execute(&tx)
                    .await?;
            }
            if !sagas::matches_locked(
                &tx,
                &next.id,
                expected,
                self.saga_read_limit,
                RM::Connection::name() == "postgres",
            )
            .await?
            {
                return Ok(false);
            }
            let inserted = expected.is_none()
                && RM::Connection::name() == "sqlite"
                && storage_capacity::insert_initial_swap(&tx, &next).await?;
            if !keysets::prepare_range(
                &tx,
                &next,
                keyset,
                counter_start,
                counter_count,
                self.saga_read_limit,
                RM::Connection::name() == "sqlite",
            )
            .await?
            {
                return Ok(false);
            }
            if let Some(receipt) = receipt {
                if !super::saga_completion::store_receipt(&tx, receipt).await? {
                    return Ok(false);
                }
            }
            match expected {
                Some(_) => {
                    sagas::update(
                        &tx,
                        &next,
                        self.saga_read_limit,
                        self.bounded_transaction_reads,
                        RM::Connection::name() == "sqlite",
                    )
                    .await
                }
                None if inserted => Ok(true),
                None => {
                    sagas::insert(&tx, &next).await?;
                    Ok(true)
                }
            }
        }
        .await;
        match result {
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
