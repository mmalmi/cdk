//! Exact receipt batches use the same stored-byte and decoding bounds as pages.
use std::collections::HashMap;

use cdk_common::database::wallet::{MAX_TRANSACTION_PAGE_BYTES, MAX_TRANSACTION_PAGE_SIZE};
use cdk_common::database::Error;
use cdk_common::wallet::{Transaction, TransactionId, TransactionStatus};

use super::{
    page::{bounded_projection, decode_page_row},
    statement, COLUMNS,
};
use crate::database::DatabaseExecutor;
use crate::pool::DatabasePool;
use crate::stmt::query;
use crate::wallet::{query_budget, SQLWalletDatabase};

pub(in crate::wallet) async fn load_ids_bounded<T>(
    conn: &T,
    ids: &[TransactionId],
) -> Result<Vec<Transaction>, Error>
where
    T: DatabaseExecutor,
{
    load_ids_with_budget(conn, ids, MAX_TRANSACTION_PAGE_BYTES).await
}

/// Read a receipt for typed comparison without letting stored growth enlarge its budget.
/// Status is the only mergeable field; Completed has its longest legal encoding.
pub(in crate::wallet) async fn load_for_comparison<T>(
    conn: &T,
    proposed: &Transaction,
) -> Result<Option<Transaction>, Error>
where
    T: DatabaseExecutor,
{
    let mut ceiling = proposed.clone();
    ceiling.status = TransactionStatus::Completed;
    let bytes = query_budget::encoded_payload_bytes(&COLUMNS, &statement(ceiling)?, 24)?
        .max(MAX_TRANSACTION_PAGE_BYTES);
    Ok(load_ids_with_budget(conn, &[proposed.id()], bytes)
        .await?
        .pop())
}

async fn load_ids_with_budget<T>(
    conn: &T,
    ids: &[TransactionId],
    max_bytes: u64,
) -> Result<Vec<Transaction>, Error>
where
    T: DatabaseExecutor,
{
    if ids.len() > usize::from(MAX_TRANSACTION_PAGE_SIZE) {
        return Err(Error::Internal(
            "Transaction batch exceeds 128 records".to_owned(),
        ));
    }
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let lengths = query_budget::payload_lengths::<T>(&COLUMNS, &["amount", "fee", "timestamp"])?;
    let projection = bounded_projection();
    let sql = format!(
        "WITH page AS (
            SELECT id, 24 + {lengths} AS payload_bytes FROM transactions WHERE id IN (:ids)
        ), budget AS (
            SELECT CAST(COALESCE(SUM(payload_bytes), 0) AS BIGINT) AS bytes FROM page
        )
        SELECT {projection}, budget.bytes
        FROM page LEFT JOIN transactions t ON t.id = page.id CROSS JOIN budget"
    );
    let mut found: HashMap<_, _> = query(&sql)?
        .bind_vec("ids", ids.iter().map(|id| id.as_slice().to_vec()).collect())?
        .bind(
            "max_bytes",
            i64::try_from(max_bytes).map_err(|_| Error::AmountOverflow)?,
        )
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(|row| decode_page_row(row, max_bytes))
        .map(|tx| tx.map(|tx| (tx.id().as_slice().to_vec(), tx)))
        .collect::<Result<_, _>>()?;
    Ok(ids
        .iter()
        .filter_map(|id| found.remove(id.as_slice()))
        .collect())
}

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    /// Apply the transaction page's 4-MiB budget to exact receipt reads.
    ///
    /// This includes native wallet recovery through `get_transaction` and the
    /// original receipt read inside reclaim storage admission. Oversized payloads
    /// are rejected before driver projection and decoding. Clones inherit
    /// this policy; reopening a database requires configuring it again. This does
    /// not reserve storage capacity or bound the database engine's own memory.
    pub fn with_bounded_transaction_reads(mut self) -> Self {
        self.bounded_transaction_reads = true;
        self
    }

    /// Read up to 128 exact receipts within the transaction page's 4-MiB budget.
    ///
    /// Missing IDs are omitted; present rows follow the requested order, once
    /// per ID. Selected malformed or oversized records fail before retirement;
    /// oversized payloads never cross the driver boundary. No rows are changed.
    #[tracing::instrument(skip_all)]
    pub async fn get_transactions_by_ids_bounded(
        &self,
        ids: &[TransactionId],
    ) -> Result<Vec<Transaction>, Error> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        load_ids_bounded(&*conn, ids).await
    }
}
