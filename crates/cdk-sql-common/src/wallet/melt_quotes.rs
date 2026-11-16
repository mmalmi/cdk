//! Complete quote snapshots read under the caller's native transaction.
use cdk_common::database::wallet::MAX_MELT_PAYMENT_PROOF_BYTES;
use cdk_common::database::Error;
use cdk_common::wallet::MeltQuote;

use super::{quote_records, sql_row_to_melt_quote};
use crate::database::DatabaseExecutor;
use crate::stmt::{query, Statement};

pub(super) const COLUMNS: &str = "id, unit, amount, request, fee_reserve, state, expiry, payment_proof, payment_method, estimated_blocks, fee_index, used_by_operation, version, mint_url";
const NUMERIC: &[&str] = &[
    "amount",
    "fee_reserve",
    "expiry",
    "estimated_blocks",
    "fee_index",
    "version",
];

pub(super) async fn load<T>(
    conn: &T,
    id: &str,
    limit: Option<u32>,
) -> Result<Option<MeltQuote>, Error>
where
    T: DatabaseExecutor,
{
    quote_records::load(
        conn,
        "melt_quote",
        COLUMNS,
        NUMERIC,
        id,
        limit.map(u64::from),
        false,
    )
    .await?
    .map(sql_row_to_melt_quote)
    .transpose()
}

pub(super) fn check_admission(quote: &MeltQuote, limit: Option<u32>) -> Result<(), Error> {
    if limit.is_none() {
        return Ok(());
    }
    quote_records::check_admission(
        COLUMNS,
        NUMERIC,
        &statement(quote.clone())?,
        limit,
        &[
            ("state", 7),
            ("used_by_operation", 36),
            ("payment_proof", MAX_MELT_PAYMENT_PROOF_BYTES as u64),
        ],
    )
}

pub(super) fn statement(quote: MeltQuote) -> Result<Statement, Error> {
    let expected_version = quote.version;
    let new_version = expected_version.wrapping_add(1);

    let statement = query(
            r#"
 INSERT INTO melt_quote
 (id, unit, amount, request, fee_reserve, state, expiry, payment_proof, payment_method, estimated_blocks, fee_index, version, mint_url, used_by_operation)
 VALUES
 (:id, :unit, :amount, :request, :fee_reserve, :state, :expiry, :payment_proof, :payment_method, :estimated_blocks, :fee_index, :version, :mint_url, :used_by_operation)
 ON CONFLICT(id) DO UPDATE SET
     unit = excluded.unit,
     amount = excluded.amount,
     request = excluded.request,
     fee_reserve = excluded.fee_reserve,
     state = excluded.state,
     expiry = excluded.expiry,
     payment_proof = COALESCE(excluded.payment_proof, melt_quote.payment_proof),
     payment_method = excluded.payment_method,
     estimated_blocks = excluded.estimated_blocks,
     fee_index = excluded.fee_index,
     version = :new_version,
     mint_url = excluded.mint_url,
     used_by_operation = excluded.used_by_operation
 WHERE melt_quote.version = :expected_version
 ;
         "#,
        )?
        .bind("id", quote.id.to_string())
        .bind("unit", quote.unit.to_string())
        .bind("amount", u64::from(quote.amount) as i64)
        .bind("request", quote.request)
        .bind("fee_reserve", u64::from(quote.fee_reserve) as i64)
        .bind("state", quote.state.to_string())
        .bind("expiry", quote.expiry as i64)
        .bind("payment_proof", quote.payment_proof)
        .bind("payment_method", quote.payment_method.to_string())
        .bind("estimated_blocks", quote.estimated_blocks.map(i64::from))
        .bind("fee_index", quote.fee_index.map(i64::from))
        .bind("version", quote.version as i64)
        .bind("new_version", new_version as i64)
        .bind("expected_version", expected_version as i64)
        .bind("mint_url", quote.mint_url.map(|m| m.to_string()))
        .bind("used_by_operation", quote.used_by_operation);

    Ok(statement)
}

/// Compare the full caller-owned original without trusting stored payload growth.
pub(super) async fn matches_locked<T>(
    conn: &T,
    expected: &MeltQuote,
    limit: Option<u32>,
    postgres: bool,
) -> Result<bool, Error>
where
    T: DatabaseExecutor,
{
    let row = super::quote_records::load_for_comparison(
        conn,
        "melt_quote",
        COLUMNS,
        NUMERIC,
        &statement(expected.clone())?,
        limit,
        postgres,
    )
    .await?;
    Ok(row.map(sql_row_to_melt_quote).transpose()?.as_ref() == Some(expected))
}
