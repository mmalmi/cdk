//! Output metadata writes shared by cache updates and atomic preparation.
use cdk_common::database::wallet::MAX_PROOF_PAGE_BYTES;
use cdk_common::database::Error;
use cdk_common::mint_url::MintUrl;
use cdk_common::{Id, KeySet, KeySetInfo, Keys};
use uuid::Uuid;

use super::{query_budget, storage_capacity, SQLWalletDatabase};
use crate::database::{ConnectionWithTransaction, DatabaseExecutor};
use crate::pool::DatabasePool;
use crate::stmt::{query, Column, Statement};

pub(super) const INFO_COLUMNS: [&str; 7] = [
    "mint_url",
    "id",
    "unit",
    "active",
    "input_fee_ppk",
    "final_expiry",
    "keyset_u32",
];
pub(super) const KEYS_COLUMNS: [&str; 3] = ["id", "keys", "keyset_u32"];
pub(super) const COUNTER_COLUMNS: [&str; 2] = ["keyset_id", "counter"];

pub(super) async fn store_info<T>(
    conn: &T,
    mint_url: &MintUrl,
    keyset: KeySetInfo,
) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    info_statement(mint_url, keyset)?.execute(conn).await?;
    Ok(())
}

pub(super) fn info_statement(mint_url: &MintUrl, keyset: KeySetInfo) -> Result<Statement, Error> {
    Ok(query(
        r#"
        INSERT INTO keyset
        (mint_url, id, unit, active, input_fee_ppk, final_expiry, keyset_u32)
        VALUES
        (:mint_url, :id, :unit, :active, :input_fee_ppk, :final_expiry, :keyset_u32)
        ON CONFLICT(id) DO UPDATE SET
            active = excluded.active,
            input_fee_ppk = excluded.input_fee_ppk
        "#,
    )?
    .bind("mint_url", mint_url.to_string())
    .bind("id", keyset.id.to_string())
    .bind("unit", keyset.unit.to_string())
    .bind("active", keyset.active)
    .bind("input_fee_ppk", keyset.input_fee_ppk as i64)
    .bind("final_expiry", keyset.final_expiry.map(|v| v as i64))
    .bind("keyset_u32", u32::from(keyset.id)))
}

pub(super) async fn store_keys<T>(conn: &T, keyset: KeySet) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    keyset.verify_id()?;

    keys_statement(keyset)?.execute(conn).await?;
    Ok(())
}

pub(super) fn keys_statement(keyset: KeySet) -> Result<Statement, Error> {
    Ok(query(
        r#"
                INSERT INTO key
                (id, keys, keyset_u32)
                VALUES
                (:id, :keys, :keyset_u32)
            "#,
    )?
    .bind("id", keyset.id.to_string())
    .bind(
        "keys",
        serde_json::to_string(&keyset.keys).map_err(Error::from)?,
    )
    .bind("keyset_u32", u32::from(keyset.id)))
}

pub(super) fn counter_statement(id: Id) -> Result<Statement, Error> {
    Ok(query("INSERT INTO keyset_counter(keyset_id,counter) VALUES(:keyset_id,:counter) ON CONFLICT(keyset_id) DO NOTHING")?
        .bind("keyset_id", id.to_string()).bind("counter", 0i64))
}

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    pub(super) async fn prepare_output_keys(
        &self,
        owner: Uuid,
        mint_url: MintUrl,
        keyset: KeySet,
    ) -> Result<(), Error> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        let tx = ConnectionWithTransaction::new(conn).await?;
        let outcome = prepare(
            &tx,
            owner,
            &mint_url,
            keyset,
            RM::Connection::name() == "sqlite",
            None,
            self.saga_read_limit,
        )
        .await;
        match outcome {
            Ok(()) => tx.commit().await,
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }
}

pub(super) async fn prepare<T>(
    conn: &T,
    owner: Uuid,
    mint_url: &MintUrl,
    keyset: KeySet,
    sqlite: bool,
    proposed: Option<&cdk_common::wallet::WalletSaga>,
    saga_read_limit: Option<u32>,
) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    keyset.verify_id()?;
    let limit =
        query_budget::encoded_payload_bytes(&["keys"], &keys_statement(keyset.clone())?, 0)?
            .max(MAX_PROOF_PAGE_BYTES);
    let length = query_budget::payload_lengths::<T>(&["keys"], &[])?;
    // A larger caller-owned original may progress; stored growth cannot enlarge
    // its budget or reach the driver before this indexed statement checks it.
    let stored = query(&format!(
        "SELECT CASE WHEN {length} <= :limit THEN keys END, {length} FROM key WHERE id=:id"
    ))?
    .bind("id", keyset.id.to_string())
    .bind(
        "limit",
        i64::try_from(limit).map_err(|_| Error::AmountOverflow)?,
    )
    .fetch_one(conn)
    .await?;
    if let Some(stored) = &stored {
        let [value, Column::Integer(bytes)] = stored.as_slice() else {
            return Err(Error::InvalidDbResponse);
        };
        let bytes = u64::try_from(*bytes).map_err(|_| Error::InvalidDbResponse)?;
        if bytes > limit {
            return Err(Error::KeysetTooLarge { bytes, limit });
        }
        let Column::Text(json) = value else {
            return Err(Error::InvalidDbResponse);
        };
        if serde_json::from_str::<Keys>(json)? != keyset.keys {
            return Err(Error::InvalidDbResponse);
        }
    }
    let admission = if sqlite {
        storage_capacity::OutputKeys::begin(
            conn,
            owner,
            mint_url,
            keyset.id,
            proposed,
            saga_read_limit,
        )
        .await?
    } else {
        None
    };
    store_info(
        conn,
        mint_url,
        KeySetInfo {
            id: keyset.id,
            unit: keyset.unit.clone(),
            active: keyset.active.unwrap_or(false),
            input_fee_ppk: keyset.input_fee_ppk,
            final_expiry: keyset.final_expiry,
        },
    )
    .await?;
    if stored.is_none() {
        store_keys(conn, keyset).await?;
    }
    if let Some(admission) = admission {
        admission.finish(conn).await?;
    }
    Ok(())
}

/// A prospective counter read must not create a row for an abandoned keyset.
pub(super) async fn counter<T>(conn: &T, id: Id) -> Result<u32, Error>
where
    T: DatabaseExecutor,
{
    match query("SELECT counter FROM keyset_counter WHERE keyset_id=:id")?
        .bind("id", id.to_string())
        .pluck(conn)
        .await?
    {
        None => Ok(0),
        Some(Column::Integer(value)) => u32::try_from(value).map_err(|_| Error::AmountOverflow),
        _ => Err(Error::InvalidDbResponse),
    }
}

/// Reserve the proposed range and recovery keys within the caller's transaction.
pub(super) async fn prepare_range<T>(
    conn: &T,
    saga: &cdk_common::wallet::WalletSaga,
    keyset: KeySet,
    start: u32,
    count: u32,
    saga_read_limit: Option<u32>,
    sqlite: bool,
) -> Result<bool, Error>
where
    T: DatabaseExecutor,
{
    if keyset.unit != saga.unit {
        return Err(Error::InvalidDbResponse);
    }
    let end = start.checked_add(count).ok_or(Error::AmountOverflow)?;
    let id = keyset.id;
    if counter(conn, id).await? != start {
        return Ok(false);
    }
    prepare(
        conn,
        saga.id,
        &saga.mint_url,
        keyset,
        sqlite,
        Some(saga),
        saga_read_limit,
    )
    .await?;
    counter_statement(id)?.execute(conn).await?;
    Ok(
        query("UPDATE keyset_counter SET counter=:end WHERE keyset_id=:id AND counter=:start")?
            .bind("end", end)
            .bind("id", id.to_string())
            .bind("start", start)
            .execute(conn)
            .await?
            == 1,
    )
}
