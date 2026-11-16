//! Complete mint quote snapshots shared by reads and atomic acquisition.
use cdk_common::database::Error;
use cdk_common::wallet::MintQuote;

mod page;
pub(super) use page::load_page;

use super::{quote_records, sql_row_to_mint_quote};
use crate::database::DatabaseExecutor;
use crate::stmt::{query, Statement};

pub(super) const COLUMNS: &str = "id, mint_url, amount, unit, request, state, expiry, secret_key, payment_method, amount_issued, amount_paid, updated_at, estimated_blocks, used_by_operation, version";
pub(super) const NUMERIC: &[&str] = &[
    "amount",
    "expiry",
    "amount_issued",
    "amount_paid",
    "updated_at",
    "estimated_blocks",
    "version",
];

pub(super) const ISSUE_COLUMNS: [&str; 4] =
    ["used_by_operation", "version", "amount_issued", "state"];

/// The mutable issue fields share one encoder for acquisition, retirement and sizing.
pub(super) fn issue_statement(quote: &MintQuote) -> Result<Statement, Error> {
    Ok(query("UPDATE mint_quote SET used_by_operation=:used_by_operation, version=:version, amount_issued=:amount_issued, state=:state WHERE id=:id")?
        .bind("used_by_operation", quote.used_by_operation.clone())
        .bind("version", i64::from(quote.version))
        .bind("amount_issued", quote.amount_issued.to_i64())
        .bind("state", quote.state.to_string())
        .bind("id", quote.id.clone()))
}

pub(super) async fn load<T>(
    conn: &T,
    id: &str,
    limit: Option<u32>,
) -> Result<Option<MintQuote>, Error>
where
    T: DatabaseExecutor,
{
    quote_records::load(
        conn,
        "mint_quote",
        COLUMNS,
        NUMERIC,
        id,
        limit.map(u64::from),
        false,
    )
    .await?
    .map(sql_row_to_mint_quote)
    .transpose()
}

pub(super) fn check_admission(quote: &MintQuote, limit: Option<u32>) -> Result<(), Error> {
    if limit.is_none() {
        return Ok(());
    }
    quote_records::check_admission(
        COLUMNS,
        NUMERIC,
        &statement(quote.clone())?,
        limit,
        &[("state", 7), ("used_by_operation", 36)],
    )
}

pub(super) fn statement(quote: MintQuote) -> Result<Statement, Error> {
    let expected_version = quote.version;
    let new_version = expected_version.wrapping_add(1);

    let statement = query(
                r#"
    INSERT INTO mint_quote
    (id, mint_url, amount, unit, request, state, expiry, secret_key, payment_method, amount_issued, amount_paid, updated_at, estimated_blocks, version, used_by_operation)
    VALUES
    (:id, :mint_url, :amount, :unit, :request, :state, :expiry, :secret_key, :payment_method, :amount_issued, :amount_paid, :updated_at, :estimated_blocks, :version, :used_by_operation)
    ON CONFLICT(id) DO UPDATE SET
        mint_url = excluded.mint_url,
        amount = excluded.amount,
        unit = excluded.unit,
        request = excluded.request,
        state = excluded.state,
        expiry = excluded.expiry,
        secret_key = excluded.secret_key,
        payment_method = excluded.payment_method,
        amount_issued = excluded.amount_issued,
        amount_paid = excluded.amount_paid,
        updated_at = excluded.updated_at,
        estimated_blocks = excluded.estimated_blocks,
        version = :new_version,
        used_by_operation = excluded.used_by_operation
    WHERE mint_quote.version = :expected_version
    ;
            "#,
            )?
            .bind("id", quote.id.to_string())
            .bind("mint_url", quote.mint_url.to_string())
            .bind("amount", quote.amount.map(|a| a.to_i64()))
            .bind("unit", quote.unit.to_string())
            .bind("request", quote.request)
            .bind("state", quote.state.to_string())
            .bind("expiry", quote.expiry as i64)
            .bind(
                "secret_key",
                quote.secret_key.map(|key| key.to_secret_hex()),
            )
            .bind("payment_method", quote.payment_method.to_string())
            .bind("amount_issued", quote.amount_issued.to_i64())
            .bind("amount_paid", quote.amount_paid.to_i64())
            .bind("updated_at", quote.updated_at as i64)
            .bind("estimated_blocks", quote.estimated_blocks.map(i64::from))
            .bind("version", quote.version as i64)
            .bind("new_version", new_version as i64)
            .bind("expected_version", expected_version as i64)
            .bind("used_by_operation", quote.used_by_operation);

    Ok(statement)
}

/// Compare the full caller-owned original without trusting stored payload growth.
pub(super) async fn matches_locked<T>(
    conn: &T,
    expected: &MintQuote,
    limit: Option<u32>,
    postgres: bool,
) -> Result<bool, Error>
where
    T: DatabaseExecutor,
{
    let row = super::quote_records::load_for_comparison(
        conn,
        "mint_quote",
        COLUMNS,
        NUMERIC,
        &statement(expected.clone())?,
        limit,
        postgres,
    )
    .await?;
    Ok(row.map(sql_row_to_mint_quote).transpose()?.as_ref() == Some(expected))
}
