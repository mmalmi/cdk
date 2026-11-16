//! Bounded payload batches with the same keyset order as operation identities.
use cdk_common::database::wallet::SagaCursor;
use cdk_common::database::Error;
use cdk_common::wallet::WalletSaga;

use super::{header, query_budget, sql_row_to_wallet_saga, COLUMNS};
use crate::column_as_number;
use crate::database::DatabaseExecutor;
use crate::stmt::query;

pub(super) async fn load<T>(
    conn: &T,
    after: Option<SagaCursor>,
    limit: u16,
    max_bytes: u32,
    record_limit: Option<u32>,
) -> Result<Vec<WalletSaga>, Error>
where
    T: DatabaseExecutor,
{
    let lengths = query_budget::payload_lengths::<T>(
        &COLUMNS,
        &["amount", "created_at", "updated_at", "version"],
    )?;
    let predicate = if after.is_some() {
        "WHERE (created_at, id) > (:created_at, :id)"
    } else {
        ""
    };
    let projection = COLUMNS
        .iter()
        .map(|column| format!("CASE WHEN budget.bytes <= :max_bytes AND budget.largest <= :record_limit THEN s.{column} END"))
        .collect::<Vec<_>>()
        .join(", ");
    // Limit the indexed selection before aggregation; budget and payload share
    // one statement snapshot. The left join keeps malformed NULL IDs visible.
    let sql = format!(
        "WITH page AS (
            SELECT id, created_at, {lengths} AS payload_bytes
            FROM wallet_sagas {predicate} ORDER BY created_at, id LIMIT :limit
        ), budget AS (
            SELECT CAST(COALESCE(SUM(payload_bytes), 0) AS BIGINT) AS bytes,
                   CAST(COALESCE(MAX(payload_bytes), 0) AS BIGINT) AS largest FROM page
        )
        SELECT {projection}, budget.bytes, budget.largest
        FROM page LEFT JOIN wallet_sagas s ON s.id = page.id CROSS JOIN budget
        ORDER BY page.created_at, page.id"
    );
    let mut statement = query(&sql)?
        .bind("limit", i64::from(limit))
        .bind("max_bytes", i64::from(max_bytes))
        .bind("record_limit", record_limit.map_or(i64::MAX, i64::from));
    if let Some(after) = after {
        statement = statement
            .bind("created_at", after.created_at as i64)
            .bind("id", after.id.to_string());
    }
    statement
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(|mut row| {
            let largest: u64 = column_as_number!(row.pop().ok_or(Error::InvalidDbResponse)?);
            let bytes: u64 = column_as_number!(row.pop().ok_or(Error::InvalidDbResponse)?);
            if let Some(limit) = record_limit {
                if largest > u64::from(limit) {
                    return Err(Error::SagaTooLarge {
                        bytes: largest,
                        limit,
                    });
                }
            }
            if bytes > u64::from(max_bytes) {
                return Err(Error::SagaPageTooLarge {
                    bytes,
                    limit: max_bytes,
                });
            }
            header(
                row.first().ok_or(Error::InvalidDbResponse)?,
                row.get(7).ok_or(Error::InvalidDbResponse)?,
            )?;
            sql_row_to_wallet_saga(row)
        })
        .collect()
}
