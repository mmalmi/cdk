//! The initial recovery record and all reservations commit together.
use cdk_common::database;
use cdk_common::database::wallet::validate_melt_acquisition;
use cdk_common::wallet::{MeltQuote, WalletSaga};

use super::{melt_quotes, proofs, sagas, storage_capacity, SQLWalletDatabase};
use crate::database::{ConnectionWithTransaction, DatabaseExecutor};
use crate::pool::DatabasePool;
use crate::stmt::query;

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    pub(super) async fn acquire_melt_transaction(
        &self,
        saga: &WalletSaga,
        quote: &MeltQuote,
    ) -> Result<bool, database::Error> {
        melt_quotes::check_admission(quote, self.quote_read_limit)?;
        let changes = validate_melt_acquisition(saga, quote)?.to_vec();
        self.check_saga_read_limit(saga)?;
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| database::Error::Database(Box::new(e)))?;
        let tx = ConnectionWithTransaction::new(conn).await?;
        let outcome: Result<bool, database::Error> = async {
            if RM::Connection::name() == "postgres" {
                query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")?
                    .execute(&tx)
                    .await?;
            }
            if !melt_quotes::matches_locked(
                &tx,
                quote,
                self.quote_read_limit,
                RM::Connection::name() == "postgres",
            )
            .await?
            {
                return Ok(false);
            }
            if query("SELECT id FROM wallet_sagas WHERE id=:id")?
                .bind("id", saga.id.to_string())
                .fetch_one(&tx)
                .await?
                .is_some()
            {
                return Ok(false);
            }
            let storage = if RM::Connection::name() == "sqlite" {
                storage_capacity::MeltAcquisition::begin(&tx, saga).await?
            } else {
                None
            };
            if !proofs::apply(&tx, changes, None).await? {
                return Ok(false);
            }
            query("UPDATE melt_quote SET used_by_operation=:owner, version=:version WHERE id=:id")?
                .bind("owner", saga.id.to_string())
                .bind("version", i64::from(quote.version) + 1)
                .bind("id", quote.id.clone())
                .execute(&tx)
                .await?;
            if let Some(storage) = storage {
                storage.finish(&tx).await?;
            } else {
                sagas::insert(&tx, saga).await?;
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
                tx.rollback().await?;
                other
            }
        }
    }
}
