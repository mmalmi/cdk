//! Individual operation reads and exact transactional comparisons.
use cdk_common::database::wallet::{validate_saga_page_request, SagaCursor, MAX_PROOF_PAGE_BYTES};
use cdk_common::database::Error;
use cdk_common::wallet::WalletSaga;

use super::{query_budget, sql_row_to_wallet_saga, storage_capacity, SQLWalletDatabase};
use crate::column_as_number;
use crate::database::DatabaseExecutor;
use crate::pool::DatabasePool;
use crate::stmt::{query, Column, Statement};

mod limits;
mod page;

pub(super) async fn load_locked<T>(
    conn: &T,
    id: &uuid::Uuid,
    max_bytes: Option<u32>,
    postgres: bool,
) -> Result<Option<WalletSaga>, Error>
where
    T: DatabaseExecutor,
{
    if let Some(limit) = max_bytes {
        return load_bounded(conn, id, limit, postgres).await;
    }
    let lock = if postgres { " FOR UPDATE" } else { "" };
    query(&format!("SELECT id, kind, state, amount, mint_url, unit, quote_id, created_at, updated_at, data, version FROM wallet_sagas WHERE id = :id{lock}"))?
        .bind("id", id.to_string()).fetch_one(conn).await?
        .map(sql_row_to_wallet_saga).transpose()
}

/// Compare a caller-owned original without letting stored growth enlarge the read.
pub(super) async fn matches_locked<T>(
    conn: &T,
    id: &uuid::Uuid,
    expected: Option<&WalletSaga>,
    max_bytes: Option<u32>,
    postgres: bool,
) -> Result<bool, Error>
where
    T: DatabaseExecutor,
{
    let Some(expected) = expected else {
        let lock = if postgres { " FOR UPDATE" } else { "" };
        return Ok(
            query(&format!("SELECT 1 FROM wallet_sagas WHERE id=:id{lock}"))?
                .bind("id", id.to_string())
                .fetch_one(conn)
                .await?
                .is_none(),
        );
    };
    let bytes = query_budget::encoded_payload_bytes(&COLUMNS, &statement(expected)?, 0)?
        .max(MAX_PROOF_PAGE_BYTES);
    let bytes = max_bytes.map_or(bytes, |limit| bytes.min(u64::from(limit)));
    let limit = u32::try_from(bytes).map_err(|_| Error::AmountOverflow)?;
    Ok(load_bounded(conn, id, limit, postgres).await?.as_ref() == Some(expected))
}

async fn load_bounded<T>(
    conn: &T,
    id: &uuid::Uuid,
    max_bytes: u32,
    postgres: bool,
) -> Result<Option<WalletSaga>, Error>
where
    T: DatabaseExecutor,
{
    let lengths = query_budget::payload_lengths::<T>(
        &COLUMNS,
        &["amount", "created_at", "updated_at", "version"],
    )?;
    let projection = COLUMNS
        .iter()
        .map(|column| format!("CASE WHEN {lengths} <= :max_bytes THEN {column} END"))
        .collect::<Vec<_>>()
        .join(", ");
    let lock = if postgres { " FOR UPDATE" } else { "" };
    // Length, payload and optional row lock share one indexed statement snapshot.
    query(&format!(
        "SELECT {projection}, {lengths} FROM wallet_sagas WHERE id=:id{lock}"
    ))?
    .bind("id", id.to_string())
    .bind("max_bytes", i64::from(max_bytes))
    .fetch_one(conn)
    .await?
    .map(|mut row| {
        let bytes: u64 = column_as_number!(row.pop().ok_or(Error::InvalidDbResponse)?);
        if bytes > u64::from(max_bytes) {
            return Err(Error::SagaTooLarge {
                bytes,
                limit: max_bytes,
            });
        }
        sql_row_to_wallet_saga(row)
    })
    .transpose()
}

/// Insert the new parent inside the caller's transaction.
pub(super) async fn insert<T>(conn: &T, saga: &WalletSaga) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    statement(saga)?.execute(conn).await?;
    Ok(())
}

pub(super) const COLUMNS: [&str; 11] = [
    "id",
    "kind",
    "state",
    "amount",
    "mint_url",
    "unit",
    "quote_id",
    "created_at",
    "updated_at",
    "data",
    "version",
];

pub(super) fn statement(saga: &WalletSaga) -> Result<Statement, Error> {
    Ok(query("INSERT INTO wallet_sagas (id, kind, state, amount, mint_url, unit, quote_id, created_at, updated_at, data, version) VALUES (:id, :kind, :state, :amount, :mint_url, :unit, :quote_id, :created_at, :updated_at, :data, :version)")?
        .bind("id", saga.id.to_string())
        .bind("kind", saga.kind.to_string())
        .bind("state", serde_json::to_string(&saga.state)?)
        .bind("amount", u64::from(saga.amount) as i64)
        .bind("mint_url", saga.mint_url.to_string())
        .bind("unit", saga.unit.to_string())
        .bind("quote_id", saga.quote_id.clone())
        .bind("created_at", saga.created_at as i64)
        .bind("updated_at", saga.updated_at as i64)
        .bind("data", serde_json::to_string(&saga.data)?)
        .bind("version", saga.version as i64))
}

pub(super) async fn delete<T>(conn: &T, id: &uuid::Uuid) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    query("DELETE FROM wallet_sagas WHERE id=:id")?
        .bind("id", id.to_string())
        .execute(conn)
        .await?;
    Ok(())
}

/// Select only bounded headers; recovery data stays in its original exact read.
pub(super) async fn identities<T>(
    conn: &T,
    after: Option<SagaCursor>,
    limit: u16,
) -> Result<Vec<SagaCursor>, Error>
where
    T: DatabaseExecutor,
{
    validate_saga_page_request(after, limit)?;
    let length = super::query_budget::payload_lengths::<T>(&["id"], &[])?;
    let timestamp = if T::name() == "sqlite" {
        "CASE WHEN typeof(created_at) = 'integer' THEN created_at END"
    } else {
        "created_at"
    };
    let predicate = if after.is_some() {
        "WHERE (created_at, id) > (:created_at, :id)"
    } else {
        ""
    };
    // A malformed, oversized ID never crosses the driver boundary. Returning
    // NULL makes the selected corrupt header fail rather than disappear.
    let sql = format!("SELECT CASE WHEN {length} = 36 THEN id END, {timestamp} FROM wallet_sagas {predicate} ORDER BY created_at, id LIMIT :limit");
    let mut statement = query(&sql)?.bind("limit", i64::from(limit));
    if let Some(after) = after {
        statement = statement
            .bind("created_at", after.created_at as i64)
            .bind("id", after.id.to_string());
    }
    statement
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(|row| {
            let [id, created_at] = row.as_slice() else {
                return Err(Error::InvalidDbResponse);
            };
            header(id, created_at)
        })
        .collect()
}

fn header(id: &Column, created_at: &Column) -> Result<SagaCursor, Error> {
    let (Column::Text(id), Column::Integer(created_at)) = (id, created_at) else {
        return Err(Error::InvalidDbResponse);
    };
    let parsed = uuid::Uuid::parse_str(id).map_err(|_| Error::InvalidDbResponse)?;
    if parsed.to_string() != *id || *created_at < 0 {
        return Err(Error::InvalidDbResponse);
    }
    Ok(SagaCursor {
        id: parsed,
        created_at: *created_at as u64,
    })
}

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    pub(super) fn check_saga_read_limit(&self, saga: &WalletSaga) -> Result<(), Error> {
        if let Some(limit) = self.saga_read_limit {
            limits::check(saga, limit)?;
        }
        if let Some(limit) = self.proof_read_limit {
            super::proofs::check_recovery_read_limit(saga, limit)?;
        }
        Ok(())
    }

    /// Limit individual operation reads before SQL payload projection.
    ///
    /// Applies to `get_saga`, native paged recovery, transactional parent reads
    /// and comparisons, caps explicit `get_saga_bounded` budgets, and applies
    /// to each record returned by `get_incomplete_sagas_page`.
    /// Oversized records return `SagaTooLarge` without
    /// changing stored evidence. Clones inherit this instance's policy; opening
    /// the database again requires configuring it again. No limit is set by default.
    /// New writes must fit this budget, including state/scalar growth and the
    /// completion records derived from their saved mint requests. This check
    /// grants no spending authority or storage reservation. Legacy bulk reads,
    /// SQL engine memory and total storage are separate.
    pub fn with_saga_read_limit(mut self, max_bytes: u32) -> Self {
        self.saga_read_limit = Some(max_bytes);
        self
    }

    /// Read original operations in `(created_at, id)` order within a byte budget.
    ///
    /// Accepts 1..=128 rows. The exclusive cursor need not still exist. Aggregate
    /// stored bytes are checked before payload projection or decoding; an oversized
    /// page returns `SagaPageTooLarge` and may be retried with fewer rows at the
    /// same cursor. The configured individual read limit also applies and returns
    /// `SagaTooLarge` instead. Corrupt headers or payloads fail without skipping.
    /// Pages are not a cross-query snapshot: serialize writers for a stable scan.
    /// This bounds reads, not SQL engine memory or total storage.
    #[tracing::instrument(skip_all)]
    pub async fn get_incomplete_sagas_page(
        &self,
        after: Option<SagaCursor>,
        limit: u16,
        max_bytes: u32,
    ) -> Result<Vec<WalletSaga>, Error> {
        validate_saga_page_request(after, limit)?;
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        page::load(&*conn, after, limit, max_bytes, self.saga_read_limit).await
    }

    /// Inspect one saved operation within a stored-byte budget, before decoding.
    ///
    /// The configured individual read limit, if any, also applies. Oversized
    /// records remain unchanged for recovery with an appropriately configured
    /// reader. This is a read bound, not an admission or whole-database limit.
    pub async fn get_saga_bounded(
        &self,
        id: &uuid::Uuid,
        max_bytes: u32,
    ) -> Result<Option<WalletSaga>, Error> {
        let max_bytes = self
            .saga_read_limit
            .map_or(max_bytes, |limit| limit.min(max_bytes));
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        load_bounded(&*conn, id, max_bytes, false).await
    }
}

/// Update a parent and its completion allowance in the caller's transaction.
pub(super) async fn update<T>(
    conn: &T,
    saga: &WalletSaga,
    max_bytes: Option<u32>,
    bounded_transaction_reads: bool,
    sqlite: bool,
) -> Result<bool, Error>
where
    T: DatabaseExecutor,
{
    let admission = if sqlite {
        storage_capacity::CompletionAdmission::begin(
            conn,
            saga,
            max_bytes,
            bounded_transaction_reads,
        )
        .await?
    } else {
        None
    };

    let state_json = serde_json::to_string(&saga.state).map_err(|e| {
        Error::Database(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Failed to serialize saga state: {}", e),
        )))
    })?;

    let data_json = serde_json::to_string(&saga.data).map_err(|e| {
        Error::Database(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Failed to serialize saga data: {}", e),
        )))
    })?;

    // Optimistic locking: only update if the version matches the expected value.
    // The saga.version has already been incremented by the caller, so we check
    // for (saga.version - 1) in the WHERE clause.
    let expected_version = saga.version.saturating_sub(1);

    let rows_affected = query(
        r#"
    UPDATE wallet_sagas
    SET kind = :kind, state = :state, amount = :amount, mint_url = :mint_url,
        unit = :unit, quote_id = :quote_id, updated_at = :updated_at, data = :data,
        version = :new_version
    WHERE id = :id AND version = :expected_version
    "#,
    )?
    .bind("id", saga.id.to_string())
    .bind("kind", saga.kind.to_string())
    .bind("state", state_json)
    .bind("amount", u64::from(saga.amount) as i64)
    .bind("mint_url", saga.mint_url.to_string())
    .bind("unit", saga.unit.to_string())
    .bind("quote_id", saga.quote_id.clone())
    .bind("updated_at", saga.updated_at as i64)
    .bind("data", data_json)
    .bind("new_version", saga.version as i64)
    .bind("expected_version", expected_version as i64)
    .execute(conn)
    .await?;

    if rows_affected == 0 {
        return Ok(false);
    }
    if let Some(admission) = admission {
        admission.finish(conn).await?;
    }
    Ok(true)
}
