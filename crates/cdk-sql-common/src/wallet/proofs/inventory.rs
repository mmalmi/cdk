//! Reject oversized inventories before proof payloads cross the driver boundary.
use cdk_common::database::Error;
use cdk_common::mint_url::MintUrl;
use cdk_common::wallet::ProofInfo;
use cdk_common::{CurrencyUnit, PublicKey, State};

use super::COLUMNS;
use crate::column_as_number;
use crate::database::DatabaseExecutor;
use crate::pool::DatabasePool;
use crate::stmt::query;
use crate::wallet::{query_budget, sql_row_to_proof_info, SQLWalletDatabase};

// Per-row overhead also bounds the probe when only a byte limit is configured.
pub(in crate::wallet) const MIN_PROOF_BYTES: u32 = 16;

pub(in crate::wallet) enum Scope {
    Unspent(MintUrl, CurrencyUnit),
    Reserved(uuid::Uuid),
    Selected(Vec<PublicKey>),
    Filtered {
        mint_url: Option<MintUrl>,
        unit: Option<CurrencyUnit>,
        states: Option<Vec<State>>,
    },
}

pub(in crate::wallet) async fn load_inventory<T>(
    conn: &T,
    scope: Scope,
    max_proofs: u32,
    max_bytes: u32,
) -> Result<Vec<ProofInfo>, Error>
where
    T: DatabaseExecutor,
{
    let (predicate, order) = match &scope {
        Scope::Unspent(_, _) => (
            "mint_url = :mint_url AND unit = :unit AND state = 'UNSPENT'".to_owned(),
            "ORDER BY y",
        ),
        // The existing operation index bounds this probe. Sorting that entire
        // scope before LIMIT would scan every unexpected owned row.
        Scope::Reserved(_) => ("used_by_operation = :operation_id".to_owned(), ""),
        Scope::Selected(_) => ("y IN (:ys)".to_owned(), ""),
        Scope::Filtered {
            mint_url,
            unit,
            states,
        } => {
            if states.as_ref().is_some_and(Vec::is_empty) {
                return Ok(Vec::new());
            }
            let mut filters = vec!["1 = 1"];
            if mint_url.is_some() {
                filters.push("mint_url = :mint_url");
            }
            if unit.is_some() {
                filters.push("unit = :unit");
            }
            if states.is_some() {
                filters.push("state IN (:states)");
            }
            (filters.join(" AND "), "")
        }
    };
    let lengths = query_budget::payload_lengths::<T>(&COLUMNS, &["amount", "derivation_index"])?;
    let projection = COLUMNS.iter().map(|column| {
        format!("CASE WHEN budget.proofs <= :max_proofs AND budget.bytes <= :max_bytes THEN p.{column} END")
    }).collect::<Vec<_>>().join(", ");
    // One extra row proves overflow without scanning the full scope. Counting
    // and conditional payload projection share the statement snapshot, so an
    // intervening write cannot defeat the budget. LEFT JOIN retains corrupt
    // NULL identities for strict decoding instead of silently dropping them.
    let sql = format!(
        "WITH inventory AS (
            SELECT y, {MIN_PROOF_BYTES} + {lengths} AS payload_bytes FROM proof
            WHERE {predicate} {order} LIMIT :probe_limit
        ), budget AS (
            SELECT CAST(COUNT(*) AS BIGINT) AS proofs,
                   CAST(COALESCE(SUM(payload_bytes), 0) AS BIGINT) AS bytes
            FROM inventory
        )
        SELECT {projection}, budget.proofs, budget.bytes
        FROM budget
        LEFT JOIN inventory ON budget.proofs <= :max_proofs AND budget.bytes <= :max_bytes
        LEFT JOIN proof p ON p.y = inventory.y
        WHERE budget.proofs > 0
        ORDER BY inventory.y"
    );
    let statement = match scope {
        Scope::Unspent(mint_url, unit) => query(&sql)?
            .bind("mint_url", mint_url.to_string())
            .bind("unit", unit.to_string()),
        Scope::Reserved(operation_id) => {
            query(&sql)?.bind("operation_id", operation_id.to_string())
        }
        Scope::Selected(ys) => {
            query(&sql)?.bind_vec("ys", ys.iter().map(|y| y.to_bytes().to_vec()).collect())?
        }
        Scope::Filtered {
            mint_url,
            unit,
            states,
        } => {
            let mut statement = query(&sql)?;
            if let Some(mint_url) = mint_url {
                statement = statement.bind("mint_url", mint_url.to_string());
            }
            if let Some(unit) = unit {
                statement = statement.bind("unit", unit.to_string());
            }
            if let Some(states) = states {
                statement = statement
                    .bind_vec("states", states.iter().map(ToString::to_string).collect())?;
            }
            statement
        }
    };
    statement
        .bind("probe_limit", i64::from(max_proofs) + 1)
        .bind("max_proofs", i64::from(max_proofs))
        .bind("max_bytes", i64::from(max_bytes))
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(|mut row| {
            let bytes: u64 = column_as_number!(row.pop().ok_or(Error::InvalidDbResponse)?);
            let proofs: u64 = column_as_number!(row.pop().ok_or(Error::InvalidDbResponse)?);
            if proofs > u64::from(max_proofs) || bytes > u64::from(max_bytes) {
                return Err(Error::ProofInventoryTooLarge {
                    proofs,
                    bytes,
                    max_proofs,
                    max_bytes,
                });
            }
            sql_row_to_proof_info(row)
        })
        .collect()
}

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    /// Limit aggregate stored bytes in proof reads.
    ///
    /// Applies to `get_proofs`, `get_proofs_by_ys` and `get_reserved_proofs` before
    /// payloads cross the SQL driver. Oversized batches and corrupt matching records
    /// fail without changing evidence. Ordinary lists filter mint, unit and state
    /// in SQL; their candidate batch must fit before filtering spending conditions.
    /// Clones inherit the policy; reopening requires configuring it again.
    /// No limit is set by default. New operation writes must also fit their complete
    /// recovery batches, including projected outputs, before spending. This does not reserve
    /// physical storage or bound SQL engine memory.
    pub fn with_proof_read_limit(mut self, max_bytes: u32) -> Self {
        self.proof_read_limit = Some(max_bytes);
        self
    }

    /// Read every proof owned by one operation within row and stored-byte budgets.
    ///
    /// Includes all states, mints and units, in binary Y order. Inspect at most
    /// `max_proofs + 1` rows and reject excess rows or bytes before projecting
    /// payloads across the driver. Selected corrupt records fail without being
    /// skipped. Zero budgets admit an empty scope. This is one statement snapshot,
    /// not a reservation or authorization to change any financial evidence.
    #[tracing::instrument(skip_all)]
    pub async fn get_reserved_proofs_bounded(
        &self,
        operation_id: &uuid::Uuid,
        max_proofs: u32,
        max_bytes: u32,
    ) -> Result<Vec<ProofInfo>, Error> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        load_inventory(
            &*conn,
            Scope::Reserved(*operation_id),
            max_proofs,
            max_bytes,
        )
        .await
    }
}
