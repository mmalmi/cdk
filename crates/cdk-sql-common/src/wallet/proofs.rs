//! Shared proof queries, used inside the caller's storage transaction.
use cdk_common::database::wallet::MAX_PROOF_PAGE_BYTES;
use cdk_common::database::Error;
use cdk_common::wallet::ProofInfo;
use cdk_common::PublicKey;

use crate::database::DatabaseExecutor;
use crate::stmt::{query, Column, Statement};
use crate::{column_as_nullable_binary, column_as_number};

mod selected;
pub(super) use selected::load_selected_page;

mod comparison;
pub(super) use comparison::matches;

mod inventory;
pub(super) use inventory::{load_inventory, Scope as InventoryScope, MIN_PROOF_BYTES};

mod admission;
pub(super) use admission::check as check_recovery_read_limit;

mod owners;
pub use owners::ProofOperationOwners;

pub(super) const COLUMNS: [&str; 17] = [
    "amount",
    "unit",
    "keyset_id",
    "secret",
    "c",
    "witness",
    "dleq_e",
    "dleq_s",
    "dleq_r",
    "y",
    "mint_url",
    "state",
    "spending_condition",
    "used_by_operation",
    "created_by_operation",
    "derivation_index",
    "p2pk_e",
];

pub(super) async fn load<T>(conn: &T, ys: Vec<PublicKey>) -> Result<Vec<ProofInfo>, Error>
where
    T: DatabaseExecutor,
{
    let sql = format!("SELECT {} FROM proof WHERE y IN (:ys)", COLUMNS.join(", "));
    query(&sql)?
        .bind_vec("ys", ys.iter().map(|y| y.to_bytes().to_vec()).collect())?
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(super::sql_row_to_proof_info)
        .collect()
}

pub(super) async fn load_created_by_operation_page<T>(
    conn: &T,
    operation: uuid::Uuid,
    after_y: Option<PublicKey>,
    limit: u16,
) -> Result<Vec<ProofInfo>, Error>
where
    T: DatabaseExecutor,
{
    // Separate first/next-page predicates keep both queries on the compound index.
    let cursor = match after_y {
        Some(_) => "AND y > :after_y",
        None => "",
    };
    let lengths =
        super::query_budget::payload_lengths::<T>(&COLUMNS, &["amount", "derivation_index"])?;
    let projection = bounded_projection();
    // Bound the indexed page before projecting payloads, within one statement
    // snapshot. Keep corrupt NULL identities in the page so decoding rejects them.
    let sql = format!(
        "WITH page AS (
            SELECT y, 16 + {lengths} AS payload_bytes FROM proof
            WHERE created_by_operation = :operation {cursor}
            ORDER BY y LIMIT :limit
        ), budget AS (
            SELECT CAST(COALESCE(SUM(payload_bytes), 0) AS BIGINT) AS bytes FROM page
        )
        SELECT {projection}, budget.bytes
        FROM page LEFT JOIN proof p ON p.y = page.y CROSS JOIN budget
        ORDER BY page.y"
    );
    let mut statement = query(&sql)?
        .bind("operation", operation.to_string())
        .bind("limit", i64::from(limit))
        .bind("max_bytes", MAX_PROOF_PAGE_BYTES as i64);
    if let Some(after_y) = after_y {
        statement = statement.bind("after_y", after_y.to_bytes().to_vec());
    }
    statement
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(|row| decode_bounded_row(row, MAX_PROOF_PAGE_BYTES))
        .collect()
}

fn decode_bounded_row(mut row: Vec<Column>, limit: u64) -> Result<ProofInfo, Error> {
    let bytes: u64 = column_as_number!(row.pop().ok_or(Error::InvalidDbResponse)?);
    if bytes > limit {
        return Err(Error::ProofPageTooLarge { bytes, limit });
    }
    super::sql_row_to_proof_info(row)
}

pub(super) async fn store<T>(
    conn: &T,
    added: Vec<ProofInfo>,
    preserve_derivation_index: bool,
) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    for proof in added {
        statement(proof, preserve_derivation_index)?
            .execute(conn)
            .await?;
    }
    Ok(())
}

pub(super) fn statement(
    proof: ProofInfo,
    preserve_derivation_index: bool,
) -> Result<Statement, Error> {
    Ok(query(
                r#"
    INSERT INTO proof
    (y, mint_url, state, spending_condition, unit, amount, keyset_id, secret, c, witness, dleq_e, dleq_s, dleq_r, used_by_operation, created_by_operation, derivation_index, p2pk_e)
    VALUES
    (:y, :mint_url, :state, :spending_condition, :unit, :amount, :keyset_id, :secret, :c, :witness, :dleq_e, :dleq_s, :dleq_r, :used_by_operation, :created_by_operation, :derivation_index, :p2pk_e)
    ON CONFLICT(y) DO UPDATE SET
        mint_url = excluded.mint_url,
        state = excluded.state,
        spending_condition = excluded.spending_condition,
        unit = excluded.unit,
        amount = excluded.amount,
        keyset_id = excluded.keyset_id,
        secret = excluded.secret,
        c = excluded.c,
        witness = excluded.witness,
        dleq_e = excluded.dleq_e,
        dleq_s = excluded.dleq_s,
        dleq_r = excluded.dleq_r,
        used_by_operation = excluded.used_by_operation,
        created_by_operation = excluded.created_by_operation,
        derivation_index = CASE WHEN :preserve_derivation_index = 1 THEN COALESCE(excluded.derivation_index, proof.derivation_index) ELSE excluded.derivation_index END,
        p2pk_e = excluded.p2pk_e
    ;
            "#,
            )?
            .bind("preserve_derivation_index", i64::from(preserve_derivation_index))
            .bind("derivation_index", proof.derivation_index.map(i64::from))
            .bind("y", proof.y.to_bytes().to_vec())
            .bind("mint_url", proof.mint_url.to_string())
            .bind("state", proof.state.to_string())
            .bind(
                "spending_condition",
                proof
                    .spending_condition
                    .map(|s| serde_json::to_string(&s)).transpose()?,
            )
            .bind("unit", proof.unit.to_string())
            .bind("amount", i64::try_from(proof.proof.amount.to_u64()).map_err(|_| Error::AmountOverflow)?)
            .bind("keyset_id", proof.proof.keyset_id.to_string())
            .bind("secret", proof.proof.secret.to_string())
            .bind("c", proof.proof.c.to_bytes().to_vec())
            .bind(
                "witness",
                proof
                    .proof
                    .witness
                    .map(|w| serde_json::to_string(&w)).transpose()?,
            )
            .bind(
                "dleq_e",
                proof.proof.dleq.as_ref().map(|dleq| dleq.e.to_secret_bytes().to_vec()),
            )
            .bind(
                "dleq_s",
                proof.proof.dleq.as_ref().map(|dleq| dleq.s.to_secret_bytes().to_vec()),
            )
            .bind(
                "dleq_r",
                proof.proof.dleq.as_ref().map(|dleq| dleq.r.to_secret_bytes().to_vec()),
            )
            .bind("used_by_operation", proof.used_by_operation.map(|id| id.to_string()))
            .bind("created_by_operation", proof.created_by_operation.map(|id| id.to_string()))
            .bind(
                "p2pk_e",
                proof
                    .proof
                    .p2pk_e
                    .as_ref()
                    .map(|pk| pk.to_bytes().to_vec()),
            ))
}

// Typed comparisons must not conflate corrupt optional evidence with SQL NULL.
pub(super) fn optional_json<T>(column: Column) -> Result<Option<T>, Error>
where
    T: serde::de::DeserializeOwned,
{
    column_as_nullable_binary!(column)
        .map(|bytes| serde_json::from_slice(&bytes))
        .transpose()
        .map_err(Error::from)
}

pub(super) fn optional_uuid(column: Column) -> Result<Option<uuid::Uuid>, Error> {
    let Some(bytes) = column_as_nullable_binary!(column) else {
        return Ok(None);
    };
    let invalid = || Error::InvalidUuid("proof operation identifier".to_owned());
    let text = std::str::from_utf8(&bytes).map_err(|_| invalid())?;
    uuid::Uuid::parse_str(text).map(Some).map_err(|_| invalid())
}

/// Compare and write inside a transaction already locked by the caller.
pub(super) async fn apply<T: DatabaseExecutor>(
    conn: &T,
    changes: Vec<cdk_common::database::wallet::ProofChange>,
    storage_saga: Option<&cdk_common::wallet::WalletSaga>,
) -> Result<bool, Error> {
    if changes.is_empty() {
        return Ok(true);
    }
    if !matches(
        conn,
        changes
            .iter()
            .map(|change| (change.y, change.before.as_ref()))
            .collect(),
    )
    .await?
    {
        return Ok(false);
    }
    let storage = super::storage_capacity::Acquisition::begin(conn, storage_saga, &changes).await?;
    let (added, removed): (Vec<_>, Vec<_>) = changes.into_iter().partition(|c| c.after.is_some());
    store(
        conn,
        added.into_iter().filter_map(|c| c.after).collect(),
        false,
    )
    .await?;
    if !removed.is_empty() {
        query("DELETE FROM proof WHERE y IN (:ys)")?
            .bind_vec(
                "ys",
                removed
                    .into_iter()
                    .map(|c| c.y.to_bytes().to_vec())
                    .collect(),
            )?
            .execute(conn)
            .await?;
    }
    if let Some(storage) = storage {
        storage.finish(conn).await?;
    }
    Ok(true)
}

fn bounded_projection() -> String {
    COLUMNS
        .iter()
        .map(|column| format!("CASE WHEN budget.bytes <= :max_bytes THEN p.{column} END"))
        .collect::<Vec<_>>()
        .join(", ")
}
