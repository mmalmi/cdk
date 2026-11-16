//! One indexed, byte-bounded read; no write transaction or cross-query snapshot.
use cdk_common::database::wallet::{
    TransactionCursor, MAX_TRANSACTION_PAGE_BYTES, MAX_TRANSACTION_PAGE_SIZE,
};
use cdk_common::database::Error;
use cdk_common::mint_url::MintUrl;
use cdk_common::wallet::{Transaction, TransactionDirection, TransactionId};
use cdk_common::CurrencyUnit;

use super::{sql_row_to_transaction, COLUMNS};
use crate::column_as_number;
use crate::database::DatabaseExecutor;
use crate::stmt::{query, Column};

pub(in crate::wallet) async fn load_page<T>(
    conn: &T,
    mint_url: Option<MintUrl>,
    direction: Option<TransactionDirection>,
    unit: Option<CurrencyUnit>,
    after: Option<TransactionCursor>,
    limit: u16,
) -> Result<Vec<Transaction>, Error>
where
    T: DatabaseExecutor,
{
    load_page_rows(conn, mint_url, direction, unit, after, limit, &COLUMNS)
        .await?
        .into_iter()
        .map(|row| decode_page_row(row, MAX_TRANSACTION_PAGE_BYTES))
        .collect()
}

pub(super) async fn load_page_rows<T>(
    conn: &T,
    mint_url: Option<MintUrl>,
    direction: Option<TransactionDirection>,
    unit: Option<CurrencyUnit>,
    after: Option<TransactionCursor>,
    limit: u16,
    columns: &[&str],
) -> Result<Vec<Vec<Column>>, Error>
where
    T: DatabaseExecutor,
{
    if limit == 0 || limit > MAX_TRANSACTION_PAGE_SIZE {
        return Err(Error::Internal("Invalid transaction page limit".to_owned()));
    }
    let timestamp = after
        .map(|cursor| i64::try_from(cursor.timestamp))
        .transpose()
        .map_err(|_| Error::Internal("Invalid transaction page cursor timestamp".to_owned()))?;

    let lengths = crate::wallet::query_budget::payload_lengths::<T>(
        &COLUMNS,
        &["amount", "fee", "timestamp"],
    )?;
    let projection = project(columns);
    let cursor_predicate = match after {
        Some(_) => "AND (timestamp, id) > (:after_timestamp, :after_id)",
        None => "",
    };
    let mut filters = vec!["1 = 1"];
    if mint_url.is_some() {
        filters.push("mint_url = :mint_url");
    }
    if direction.is_some() {
        filters.push("direction = :direction");
    }
    if unit.is_some() {
        filters.push("unit = :unit");
    }
    let filters = filters.join(" AND ");
    // The LIMIT applies before aggregation or payload projection. Both references
    // to page use the same statement snapshot; concurrent edits cannot grow a
    // record between its length check and decoding. Oversized values never cross
    // the driver boundary. The LEFT JOIN makes a corrupt NULL SQLite ID fail
    // decoding rather than silently disappearing from the selected page.
    let sql = format!(
        "WITH page AS (
            SELECT id, timestamp, 24 + {lengths} AS payload_bytes
            FROM transactions
            WHERE {filters} {cursor_predicate}
            ORDER BY timestamp, id LIMIT :limit
        ), budget AS (
            SELECT CAST(COALESCE(SUM(payload_bytes), 0) AS BIGINT) AS bytes FROM page
        )
        SELECT {projection}, budget.bytes
        FROM page LEFT JOIN transactions t ON t.id = page.id CROSS JOIN budget
        ORDER BY page.timestamp, page.id"
    );
    let mut statement = query(&sql)?
        .bind("limit", i64::from(limit))
        .bind("max_bytes", MAX_TRANSACTION_PAGE_BYTES as i64);
    if let Some(mint_url) = mint_url {
        statement = statement.bind("mint_url", mint_url.to_string());
    }
    if let Some(direction) = direction {
        statement = statement.bind("direction", direction.to_string());
    }
    if let Some(unit) = unit {
        statement = statement.bind("unit", unit.to_string());
    }
    if let Some(after) = after {
        statement = statement
            .bind("after_timestamp", timestamp)
            .bind("after_id", after.id.as_slice().to_vec());
    }
    statement.fetch_all(conn).await
}

pub(super) fn decode_page_row(mut row: Vec<Column>, max_bytes: u64) -> Result<Transaction, Error> {
    check_budget(&mut row, max_bytes)?;
    let cursor = stored_cursor(row.last(), row.get(6))?;
    let transaction = sql_row_to_transaction(row)?;
    if transaction.id() != cursor.id {
        return Err(Error::Internal(
            "Noncanonical transaction page ID".to_owned(),
        ));
    }
    Ok(transaction)
}

pub(super) fn check_budget(row: &mut Vec<Column>, max_bytes: u64) -> Result<(), Error> {
    let bytes = row.pop().ok_or(Error::InvalidDbResponse)?;
    let bytes: u64 = column_as_number!(bytes);
    if bytes > max_bytes {
        return Err(Error::TransactionPageTooLarge {
            bytes,
            limit: max_bytes,
        });
    }
    Ok(())
}

pub(super) fn stored_cursor(
    id: Option<&Column>,
    timestamp: Option<&Column>,
) -> Result<TransactionCursor, Error> {
    let id = match id {
        Some(Column::Blob(id)) => TransactionId::from_slice(id)
            .map_err(|_| Error::Internal("Invalid transaction page ID".to_owned()))?,
        _ => return Err(Error::Internal("Invalid transaction page ID".to_owned())),
    };
    let timestamp = match timestamp {
        Some(Column::Integer(value)) if *value >= 0 => *value as u64,
        _ => {
            return Err(Error::Internal(
                "Invalid transaction page timestamp".to_owned(),
            ))
        }
    };
    Ok(TransactionCursor { timestamp, id })
}

pub(super) fn bounded_projection() -> String {
    project(&COLUMNS)
}

fn project(columns: &[&str]) -> String {
    columns
        .iter()
        .map(|column| format!("CASE WHEN budget.bytes <= :max_bytes THEN t.{column} END"))
        .collect::<Vec<_>>()
        .join(", ")
}
