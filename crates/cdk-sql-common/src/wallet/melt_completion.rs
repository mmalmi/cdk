//! Atomic paid receipt, quote release and terminal parent publication.
use cdk_common::database::wallet::validate_melt_completion;
use cdk_common::database::Error;
use cdk_common::wallet::{MeltQuote, WalletSaga};

use super::{melt_quotes, sagas, transactions, SQLWalletDatabase};
use crate::database::{ConnectionWithTransaction, DatabaseExecutor};
use crate::pool::DatabasePool;
use crate::stmt::query;

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    pub(super) async fn complete_melt_transaction(
        &self,
        expected: &WalletSaga,
        next: WalletSaga,
        quote: &MeltQuote,
    ) -> Result<bool, Error> {
        let result = validate_melt_completion(expected, &next, quote)?;
        melt_quotes::check_admission(&result.quote, self.quote_read_limit)?;
        self.check_saga_read_limit(&next)?;
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        let tx = ConnectionWithTransaction::new(conn).await?;
        let outcome: Result<bool, Error> = async {
            let postgres = RM::Connection::name() == "postgres";
            if postgres {
                query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")?.execute(&tx).await?;
            }
            if !sagas::matches_locked(&tx, &expected.id, Some(expected), self.saga_read_limit, postgres).await?
                || !melt_quotes::matches_locked(&tx, quote, self.quote_read_limit, postgres).await?
                || transactions::load_for_comparison(&tx, &result.pending).await?.as_ref() != Some(&result.pending)
            { return Ok(false); }
            transactions::store(&tx, result.receipt).await?;
            let written = query("UPDATE melt_quote SET state=:state, payment_proof=:proof, used_by_operation=NULL, version=:version WHERE id=:id")?
                .bind("id", result.quote.id)
                .bind("state", result.quote.state.to_string())
                .bind("proof", result.quote.payment_proof)
                .bind("version", i64::from(result.quote.version))
                .execute(&tx).await?;
            if written != 1 { return Err(Error::ConcurrentUpdate); }
            let written = query("UPDATE wallet_sagas SET state=:state, updated_at=:updated_at, data=:data, version=:version WHERE id=:id")?
                .bind("id", next.id.to_string())
                .bind("state", serde_json::to_string(&next.state)?)
                .bind("updated_at", i64::try_from(next.updated_at).map_err(|_| Error::Internal("Saga time out of range".to_owned()))?)
                .bind("data", serde_json::to_string(&next.data)?)
                .bind("version", i64::from(next.version))
                .execute(&tx).await?;
            if written != 1 { return Err(Error::ConcurrentUpdate); }
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
