//! Exact quote reads and admission share stored-byte sizing with their writers.
use cdk_common::database::Error;

use super::{query_budget, SQLWalletDatabase};
use crate::column_as_number;
use crate::database::DatabaseExecutor;
use crate::pool::DatabasePool;
use crate::stmt::{query, Column, Statement};

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    /// Bound individual quote reads before projecting stored payloads.
    ///
    /// New quotes and atomic issue/melt admission also reserve the byte widths
    /// of later scalar/owner updates and the supported withdrawal payment proof.
    /// This is an instance policy; apply it on every open. It does not bound
    /// legacy bulk quote APIs or reserve physical storage.
    pub fn with_quote_read_limit(mut self, max_bytes: u32) -> Self {
        self.quote_read_limit = Some(max_bytes);
        self
    }
}

pub(super) fn check_admission(
    columns: &str,
    numeric: &[&str],
    statement: &Statement,
    maximum: Option<u32>,
    growth_fields: &[(&str, u64)],
) -> Result<(), Error> {
    let Some(limit) = maximum.map(u64::from) else {
        return Ok(());
    };
    let bytes = columns.split(", ").try_fold(24u64, |total, column| {
        let current = query_budget::encoded_payload_bytes(&[column], statement, 0)?;
        // Every SQL integer fits in twenty decimal bytes, including its sign.
        let floor = if numeric.contains(&column) {
            20
        } else {
            growth_fields
                .iter()
                .find(|(name, _)| *name == column)
                .map_or(0, |(_, bytes)| *bytes)
        };
        total
            .checked_add(current.max(floor))
            .ok_or(Error::AmountOverflow)
    })?;
    if bytes > limit {
        return Err(Error::QuoteTooLarge { bytes, limit });
    }
    Ok(())
}

pub(super) async fn load_for_comparison<T>(
    conn: &T,
    table: &str,
    columns: &str,
    numeric: &[&str],
    expected: &Statement,
    configured_limit: Option<u32>,
    postgres: bool,
) -> Result<Option<Vec<Column>>, Error>
where
    T: DatabaseExecutor,
{
    let names = columns.split(", ").collect::<Vec<_>>();
    let max_bytes = query_budget::encoded_payload_bytes(&names, expected, 24)?.max(4 * 1024 * 1024);
    let max_bytes = configured_limit.map_or(max_bytes, |limit| max_bytes.min(u64::from(limit)));
    let Column::Text(id) = expected.bound_value("id")? else {
        return Err(Error::InvalidDbResponse);
    };
    load(conn, table, columns, numeric, id, Some(max_bytes), postgres).await
}

pub(super) async fn load<T>(
    conn: &T,
    table: &str,
    columns: &str,
    numeric: &[&str],
    id: &str,
    maximum: Option<u64>,
    postgres: bool,
) -> Result<Option<Vec<Column>>, Error>
where
    T: DatabaseExecutor,
{
    let lock = if postgres { " FOR UPDATE" } else { "" };
    let Some(max_bytes) = maximum else {
        return query(&format!("SELECT {columns} FROM {table} WHERE id=:id{lock}"))?
            .bind("id", id)
            .fetch_one(conn)
            .await;
    };
    let columns = columns.split(", ").collect::<Vec<_>>();
    let lengths = format!(
        "24 + {}",
        query_budget::payload_lengths::<T>(&columns, numeric)?
    );
    let projection = columns
        .iter()
        .map(|column| format!("CASE WHEN {lengths} <= :max_bytes THEN {column} END"))
        .collect::<Vec<_>>()
        .join(", ");
    // Sizing, projection and the optional lock use one indexed statement snapshot.
    query(&format!(
        "SELECT {projection}, {lengths} FROM {table} WHERE id=:id{lock}"
    ))?
    .bind("id", id)
    .bind(
        "max_bytes",
        i64::try_from(max_bytes).map_err(|_| Error::AmountOverflow)?,
    )
    .fetch_one(conn)
    .await?
    .map(|mut row| {
        let bytes: u64 = column_as_number!(row.pop().ok_or(Error::InvalidDbResponse)?);
        if bytes > max_bytes {
            return Err(Error::QuoteTooLarge {
                bytes,
                limit: max_bytes,
            });
        }
        Ok(row)
    })
    .transpose()
}
