//! Exact token history removal inside one native storage transaction.
use cdk_common::database::wallet::{
    token_history_input_spent, validate_token_history_retirement, MAX_PROOF_PAGE_SIZE,
};
use cdk_common::database::Error;
use cdk_common::wallet::{Transaction, TransactionDirection};

use super::{kv_batch, proofs, storage_capacity, transactions, SQLWalletDatabase, WalletKvChange};
use crate::database::{ConnectionWithTransaction, DatabaseExecutor};
use crate::pool::DatabasePool;
use crate::stmt::query;

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    /// Retire exact incoming token receipts and update local accounting together.
    ///
    /// The caller must durably release result/retry rights before requesting
    /// retirement. Original receipt, spent-input and inactive-operation checks
    /// are shared with ordinary retirement. Deletion and release of unused native
    /// completion allowance precede the conditional checkpoint writes. Retained
    /// coins stay charged. Any mismatch or error rolls back the entire batch.
    #[tracing::instrument(skip_all)]
    pub async fn retire_incoming_transactions_with_kv(
        &self,
        expected: Vec<Transaction>,
        changes: &[WalletKvChange<'_>],
    ) -> Result<bool, Error> {
        self.retire_token_history(expected, TransactionDirection::Incoming, changes)
            .await
    }

    #[tracing::instrument(skip_all)]
    pub(super) async fn token_history_retirable(
        &self,
        expected: &Transaction,
    ) -> Result<bool, Error> {
        let direction = expected.direction;
        if validate_token_history_retirement(std::slice::from_ref(expected), direction).is_err() {
            return Ok(false);
        }
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        if transactions::load_ids_bounded(&*conn, &[expected.id()])
            .await?
            .as_slice()
            != std::slice::from_ref(expected)
        {
            return Ok(false);
        }
        inputs_retirable(&*conn, expected, direction).await
    }

    pub(super) async fn retire_token_history(
        &self,
        expected: Vec<Transaction>,
        direction: TransactionDirection,
        changes: &[WalletKvChange<'_>],
    ) -> Result<bool, Error> {
        validate_token_history_retirement(&expected, direction)?;
        kv_batch::validate(changes)?;
        if expected.is_empty() && changes.is_empty() {
            return Ok(true);
        }
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        let tx = ConnectionWithTransaction::new(conn).await?;
        let outcome: Result<bool, Error> = async {
            if RM::Connection::name() == "postgres" {
                query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")?
                    .execute(&tx)
                    .await?;
            }
            let ids = expected.iter().map(Transaction::id).collect::<Vec<_>>();
            if transactions::load_ids_bounded(&tx, &ids).await? != expected {
                return Ok(false);
            }
            for original in &expected {
                if !inputs_retirable(&tx, original, direction).await? {
                    return Ok(false);
                }
            }
            for original in expected {
                query("DELETE FROM transactions WHERE id = :id")?
                    .bind("id", original.id().as_slice().to_vec())
                    .execute(&tx)
                    .await?;
                if RM::Connection::name() == "sqlite" {
                    let operation = original
                        .saga_id
                        .ok_or_else(|| Error::Internal("Token operation missing".to_owned()))?;
                    storage_capacity::release_completed(&tx, &operation).await?;
                }
            }
            kv_batch::apply::<RM>(&tx, changes).await
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

async fn inputs_retirable<T>(
    conn: &T,
    original: &Transaction,
    direction: TransactionDirection,
) -> Result<bool, Error>
where
    T: DatabaseExecutor,
{
    let operation = original
        .saga_id
        .ok_or_else(|| Error::Internal("Token operation missing".to_owned()))?;
    if query("SELECT id FROM wallet_sagas WHERE id = :id")?
        .bind("id", operation.to_string())
        .fetch_one(conn)
        .await?
        .is_some()
    {
        return Ok(false);
    }
    let mut gross = 0u64;
    let mut remaining = original.ys.as_slice();
    while !remaining.is_empty() {
        let mut count = remaining.len().min(usize::from(MAX_PROOF_PAGE_SIZE));
        let inputs = loop {
            match proofs::load_selected_page(conn, &remaining[..count]).await {
                // At most eight reads, from 128 inputs down to one. An
                // individually oversized input remains an error, never skipped.
                Err(Error::ProofPageTooLarge { .. }) if count > 1 => count /= 2,
                result => break result?,
            }
        };
        if inputs.len() != count {
            return Ok(false);
        }
        for proof in inputs {
            if !token_history_input_spent(original, &proof, direction) {
                return Ok(false);
            }
            if direction == TransactionDirection::Incoming {
                let Some(total) = gross.checked_add(proof.proof.amount.to_u64()) else {
                    return Ok(false);
                };
                gross = total;
            }
        }
        remaining = &remaining[count..];
    }
    Ok(direction != TransactionDirection::Incoming
        || Some(gross) == original.amount.to_u64().checked_add(original.fee.to_u64()))
}
