//! Receipt queries usable inside the caller's transaction.
use cdk_common::database::{self, Error};
use cdk_common::wallet::{Transaction, TransactionId};

use super::sql_row_to_transaction;
use crate::database::DatabaseExecutor;
use crate::stmt::{query, Statement};

mod bounded;
pub(super) use bounded::{load_for_comparison, load_ids_bounded};

mod page;
pub(super) use page::load_page;

mod references;
pub use self::references::TransactionProofReferences;

// Keep receipt lookup, legacy enumeration and indexed pages on one projection.
pub(super) const COLUMNS: [&str; 17] = [
    "mint_url",
    "direction",
    "unit",
    "amount",
    "fee",
    "ys",
    "timestamp",
    "memo",
    "metadata",
    "quote_id",
    "payment_request",
    "payment_proof",
    "payment_method",
    "saga_id",
    "status",
    "role",
    "id",
];

pub(super) async fn load<T>(
    conn: &T,
    transaction_id: TransactionId,
    bounded: bool,
) -> Result<Option<Transaction>, database::Error>
where
    T: DatabaseExecutor,
{
    if bounded {
        return Ok(load_ids_bounded(conn, &[transaction_id]).await?.pop());
    }
    query(&format!(
        "SELECT {} FROM transactions WHERE id = :id",
        COLUMNS.join(", ")
    ))?
    .bind("id", transaction_id.as_slice().to_vec())
    .fetch_one(conn)
    .await?
    .map(sql_row_to_transaction)
    .transpose()
}

pub(super) async fn store<T>(conn: &T, transaction: Transaction) -> Result<(), database::Error>
where
    T: DatabaseExecutor,
{
    if statement(transaction)?.execute(conn).await? != 1 {
        return Err(Error::ConcurrentUpdate);
    }
    Ok(())
}

pub(super) fn statement(transaction: Transaction) -> Result<Statement, Error> {
    transaction
        .validate_role()
        .map_err(|e| Error::Internal(e.to_string()))?;
    let mint_url = transaction.mint_url.to_string();
    let direction = transaction.direction.to_string();
    let unit = transaction.unit.to_string();
    let amount = i64::try_from(u64::from(transaction.amount))
        .map_err(|_| Error::Internal("Transaction amount out of range".to_owned()))?;
    let fee = i64::try_from(u64::from(transaction.fee))
        .map_err(|_| Error::Internal("Transaction fee out of range".to_owned()))?;
    let timestamp = i64::try_from(transaction.timestamp)
        .map_err(|_| Error::Internal("Transaction time out of range".to_owned()))?;
    let ys = transaction
        .ys
        .iter()
        .flat_map(|y| y.to_bytes().to_vec())
        .collect::<Vec<_>>();

    let id = transaction.id();

    Ok(query(
               r#"
   INSERT INTO transactions
   (id, mint_url, direction, unit, amount, fee, ys, timestamp, memo, metadata, quote_id, payment_request, payment_proof, payment_method, saga_id, status, role)
   VALUES
   (:id, :mint_url, :direction, :unit, :amount, :fee, :ys, :timestamp, :memo, :metadata, :quote_id, :payment_request, :payment_proof, :payment_method, :saga_id, :status, :role)
   ON CONFLICT(id) DO UPDATE SET
       mint_url = excluded.mint_url,
       direction = excluded.direction,
       unit = excluded.unit,
       amount = excluded.amount,
       fee = excluded.fee,
       timestamp = excluded.timestamp,
       memo = excluded.memo,
       metadata = excluded.metadata,
       quote_id = excluded.quote_id,
       payment_request = excluded.payment_request,
       payment_proof = excluded.payment_proof,
       payment_method = excluded.payment_method,
       saga_id = excluded.saga_id,
       status = excluded.status,
       role = excluded.role
   ;
           "#,
           )?
           .bind("id", id.as_slice().to_vec())
           .bind("mint_url", mint_url)
           .bind("direction", direction)
           .bind("unit", unit)
           .bind("amount", amount)
           .bind("fee", fee)
           .bind("ys", ys)
           .bind("timestamp", timestamp)
           .bind("memo", transaction.memo)
           .bind(
               "metadata",
               serde_json::to_string(&transaction.metadata).map_err(Error::from)?,
           )
           .bind("quote_id", transaction.quote_id)
           .bind("payment_request", transaction.payment_request)
           .bind("payment_proof", transaction.payment_proof)
           .bind("payment_method", transaction.payment_method.map(|pm| pm.to_string()))
           .bind("saga_id", transaction.saga_id.map(|id| id.to_string()))
           .bind("status", transaction.status.to_string())
           .bind("role", transaction.role.to_string())
)
}
