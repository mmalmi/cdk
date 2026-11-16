//! Scoped recovery reads size selected rows before projecting their payloads.
use cdk_common::database::wallet::{MAX_MINT_QUOTE_PAGE_BYTES, MAX_MINT_QUOTE_PAGE_SIZE};
use cdk_common::database::Error;
use cdk_common::mint_url::MintUrl;
use cdk_common::wallet::MintQuote;
use cdk_common::CurrencyUnit;

use super::{sql_row_to_mint_quote, COLUMNS, NUMERIC};
use crate::column_as_number;
use crate::database::DatabaseExecutor;
use crate::stmt::query;

pub(in crate::wallet) async fn load_page<T>(
    conn: &T,
    mint_url: MintUrl,
    unit: CurrencyUnit,
    after: Option<String>,
    limit: u16,
) -> Result<Vec<MintQuote>, Error>
where
    T: DatabaseExecutor,
{
    if limit == 0 || limit > MAX_MINT_QUOTE_PAGE_SIZE {
        return Err(Error::Internal("Invalid mint quote page limit".to_owned()));
    }
    if after
        .as_ref()
        .is_some_and(|id| id.len() as u64 > MAX_MINT_QUOTE_PAGE_BYTES)
    {
        return Err(Error::Internal("Invalid mint quote page cursor".to_owned()));
    }
    let columns = COLUMNS.split(", ").collect::<Vec<_>>();
    let lengths = crate::wallet::query_budget::payload_lengths::<T>(&columns, NUMERIC)?;
    let projection = columns
        .iter()
        .map(|column| format!("CASE WHEN budget.bytes <= :max_bytes THEN q.{column} END"))
        .collect::<Vec<_>>()
        .join(", ");
    let cursor = if after.is_some() {
        "AND id > :after"
    } else {
        ""
    };
    // One statement snapshot prevents a payload growing between sizing and
    // projection. The partial scope index skips completed and unrelated history.
    let sql = format!(
        "WITH page AS (
            SELECT id, 24 + {lengths} AS payload_bytes FROM mint_quote
            WHERE mint_url = :mint_url AND unit = :unit
              AND (amount_issued = 0 OR payment_method = 'bolt12') {cursor}
            ORDER BY id LIMIT :limit
        ), budget AS (
            SELECT CAST(COALESCE(SUM(payload_bytes), 0) AS BIGINT) AS bytes FROM page
        )
        SELECT {projection}, budget.bytes
        FROM page LEFT JOIN mint_quote q ON q.id = page.id CROSS JOIN budget
        ORDER BY page.id"
    );
    let mut statement = query(&sql)?
        .bind("mint_url", mint_url.to_string())
        .bind("unit", unit.to_string())
        .bind("limit", i64::from(limit))
        .bind("max_bytes", MAX_MINT_QUOTE_PAGE_BYTES as i64);
    if let Some(after) = after {
        statement = statement.bind("after", after);
    }
    statement
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(|mut row| {
            let bytes = row.pop().ok_or(Error::InvalidDbResponse)?;
            let bytes: u64 = column_as_number!(bytes);
            if bytes > MAX_MINT_QUOTE_PAGE_BYTES {
                return Err(Error::MintQuotePageTooLarge {
                    bytes,
                    limit: MAX_MINT_QUOTE_PAGE_BYTES,
                });
            }
            sql_row_to_mint_quote(row)
        })
        .collect()
}
