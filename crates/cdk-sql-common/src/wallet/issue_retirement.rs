//! Proof publication, receipts, quote updates and exact journal deletion commit once.
use cdk_common::database::wallet::{validate_issue_retirement, ProofChange};
use cdk_common::database::Error;
use cdk_common::wallet::{MintQuote, WalletSaga};

use super::{mint_quotes, proofs, sagas, storage_capacity, transactions, SQLWalletDatabase};
use crate::database::{ConnectionWithTransaction, DatabaseExecutor};
use crate::pool::DatabasePool;
use crate::stmt::query;

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    pub(super) async fn retire_issue_transaction(
        &self,
        expected: &WalletSaga,
        quotes: &[MintQuote],
        changes: Option<Vec<ProofChange>>,
    ) -> Result<bool, Error> {
        let plan = validate_issue_retirement(expected, quotes, changes.as_deref())?;
        for quote in &plan.quotes {
            mint_quotes::check_admission(quote, self.quote_read_limit)?;
        }
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        let tx = ConnectionWithTransaction::new(conn).await?;
        let outcome: Result<bool, Error> = async {
            let postgres = RM::Connection::name() == "postgres";
            if postgres {
                query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")?
                    .execute(&tx)
                    .await?;
            }
            if !sagas::matches_locked(
                &tx,
                &expected.id,
                Some(expected),
                self.saga_read_limit,
                postgres,
            )
            .await?
            {
                return Ok(false);
            }
            let mut ordered: Vec<_> = quotes.iter().collect();
            ordered.sort_by(|left, right| left.id.cmp(&right.id));
            for quote in ordered {
                if !mint_quotes::matches_locked(&tx, quote, self.quote_read_limit, postgres).await?
                {
                    return Ok(false);
                }
            }
            let mut receipts = Vec::with_capacity(quotes.len());
            for original in plan.original_receipts() {
                receipts.push(transactions::load_for_comparison(&tx, original).await?);
            }
            let Some(updates) = plan.receipts(receipts) else {
                return Ok(false);
            };
            if let Some(changes) = changes {
                if !proofs::apply(&tx, changes, None).await? {
                    return Ok(false);
                }
            }
            for receipt in updates {
                transactions::store(&tx, receipt).await?;
            }
            for quote in plan.quotes {
                let written = mint_quotes::issue_statement(&quote)?.execute(&tx).await?;
                if written != 1 {
                    return Err(Error::ConcurrentUpdate);
                }
            }
            if query("DELETE FROM wallet_sagas WHERE id=:id")?
                .bind("id", expected.id.to_string())
                .execute(&tx)
                .await?
                != 1
            {
                return Err(Error::ConcurrentUpdate);
            }
            if RM::Connection::name() == "sqlite" {
                storage_capacity::release_completed(&tx, &expected.id).await?;
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
