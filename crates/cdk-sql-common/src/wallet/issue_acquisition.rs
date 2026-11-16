//! The complete issue intent and all quote reservations commit together.
use cdk_common::database::wallet::{issue_receipts, validate_issue_acquisition};
use cdk_common::wallet::{MintQuote, WalletSaga};
use cdk_common::{database, KeySet};

use super::{mint_quotes, sagas, storage_capacity, transactions, SQLWalletDatabase};
use crate::database::{ConnectionWithTransaction, DatabaseExecutor};
use crate::pool::DatabasePool;
use crate::stmt::query;

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    pub(super) async fn acquire_issue_transaction(
        &self,
        saga: &WalletSaga,
        quotes: &[MintQuote],
        keyset: KeySet,
        counter_start: u32,
        counter_count: u32,
    ) -> Result<bool, database::Error> {
        let reserved = validate_issue_acquisition(saga, quotes)?;
        for quote in quotes {
            mint_quotes::check_admission(quote, self.quote_read_limit)?;
        }
        self.check_saga_read_limit(saga)?;
        let receipts = issue_receipts(saga)?;
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| database::Error::Database(Box::new(e)))?;
        let tx = ConnectionWithTransaction::new(conn).await?;
        let outcome: Result<bool, database::Error> = async {
            let postgres = RM::Connection::name() == "postgres";
            if postgres {
                query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")?
                    .execute(&tx)
                    .await?;
            }
            // Stable quote locking order avoids reversed-batch lock cycles.
            let mut ordered: Vec<_> = quotes.iter().collect();
            ordered.sort_by(|left, right| left.id.cmp(&right.id));
            for quote in ordered {
                if !mint_quotes::matches_locked(&tx, quote, self.quote_read_limit, postgres).await?
                {
                    return Ok(false);
                }
            }
            if query("SELECT id FROM wallet_sagas WHERE id=:id")?
                .bind("id", saga.id.to_string())
                .fetch_one(&tx)
                .await?
                .is_some()
            {
                return Ok(false);
            }
            for receipt in &receipts {
                if query("SELECT 1 FROM transactions WHERE id=:id")?
                    .bind("id", receipt.id().as_slice().to_vec())
                    .fetch_one(&tx)
                    .await?
                    .is_some()
                {
                    return Ok(false);
                }
            }
            let inserted = RM::Connection::name() == "sqlite"
                && storage_capacity::insert_initial_issue(&tx, saga, quotes).await?;
            if !super::keysets::prepare_range(
                &tx,
                saga,
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
            for quote in reserved {
                if mint_quotes::issue_statement(&quote)?.execute(&tx).await? != 1 {
                    return Err(database::Error::ConcurrentUpdate);
                }
            }
            if !inserted {
                sagas::insert(&tx, saga).await?;
            }
            for receipt in receipts {
                transactions::store(&tx, receipt).await?;
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
