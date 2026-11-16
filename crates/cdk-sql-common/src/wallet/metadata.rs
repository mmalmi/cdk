//! Bound mint metadata projections before rows cross the SQL driver.
use cdk_common::database::Error;
use cdk_common::mint_url::MintUrl;
use cdk_common::Id;

use super::{query_budget, SQLWalletDatabase};
use crate::column_as_number;
use crate::database::DatabaseExecutor;
use crate::pool::DatabasePool;
use crate::stmt::{query, Column};

const MINT_COLUMNS: [&str; 13] = [
    "name",
    "pubkey",
    "version",
    "description",
    "description_long",
    "contact",
    "nuts",
    "icon_url",
    "motd",
    "urls",
    "mint_time",
    "tos_url",
    "mint_url",
];
const KEYSET_COLUMNS: [&str; 5] = ["id", "unit", "active", "input_fee_ppk", "final_expiry"];

#[derive(Debug, Clone, Copy)]
pub(super) struct ReadLimits {
    max_mints: u32,
    max_keysets: u32,
    max_bytes: u32,
}

pub(super) enum Scope<'a> {
    Mint(&'a MintUrl),
    Mints,
    MintKeysets(&'a MintUrl),
    Keyset(&'a Id),
    Keys(&'a Id),
}

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    /// Limit mint metadata rows and stored bytes before SQL payload projection.
    ///
    /// `max_mints` bounds `get_mints`; `max_keysets` bounds `get_mint_keysets`.
    /// The byte budget applies to each whole result, including individual mint,
    /// keyset and keys reads, and counts stored scalar bytes plus 16 bytes per row.
    /// Zero row limits admit only empty corresponding lists; zero bytes admit
    /// only empty results. Excess rows are counted only up to the limit plus one.
    ///
    /// Clones inherit this policy; reopening requires configuring it again. No
    /// limit is set by default. Refused records remain unchanged. These budgets
    /// are separate from canonical retained-cache bytes, SQL engine memory and
    /// physical storage capacity. They grant no financial authority.
    pub fn with_mint_metadata_read_limits(
        mut self,
        max_mints: u32,
        max_keysets: u32,
        max_bytes: u32,
    ) -> Self {
        self.metadata_read_limits = Some(ReadLimits {
            max_mints,
            max_keysets,
            max_bytes,
        });
        self
    }
}

pub(super) async fn load<T>(
    conn: &T,
    scope: Scope<'_>,
    limits: Option<ReadLimits>,
) -> Result<Vec<Vec<Column>>, Error>
where
    T: DatabaseExecutor,
{
    let (table, identity, columns, numeric, predicate): (_, _, &[_], &[_], _) = match &scope {
        Scope::Mint(_) => (
            "mint",
            "mint_url",
            &MINT_COLUMNS[..12],
            &["mint_time"],
            "mint_url=:identity",
        ),
        Scope::Mints => ("mint", "mint_url", &MINT_COLUMNS[..], &["mint_time"], "1=1"),
        Scope::MintKeysets(_) => (
            "keyset",
            "id",
            &KEYSET_COLUMNS[..],
            &["active", "input_fee_ppk", "final_expiry"],
            "mint_url=:identity",
        ),
        Scope::Keyset(_) => (
            "keyset",
            "id",
            &KEYSET_COLUMNS[..],
            &["active", "input_fee_ppk", "final_expiry"],
            "id=:identity",
        ),
        Scope::Keys(_) => ("key", "id", &["keys"][..], &[][..], "id=:identity"),
    };
    let budget = limits.map(|limits| {
        let max_records = match &scope {
            Scope::Mints => limits.max_mints,
            Scope::MintKeysets(_) => limits.max_keysets,
            _ => 1,
        };
        (max_records, limits.max_bytes)
    });
    let sql = match budget {
        Some(_) => {
            let lengths = query_budget::payload_lengths::<T>(columns, numeric)?;
            let projection = columns.iter().map(|column| {
                format!("CASE WHEN budget.records <= :max_records AND budget.bytes <= :max_bytes THEN p.{column} END")
            }).collect::<Vec<_>>().join(", ");
            // Count at most one extra row, without sorting the entire scope.
            // Count, byte budget and projection share one statement snapshot.
            format!(
                "WITH inventory AS (
                    SELECT {identity} AS identity, 16 + {lengths} AS payload_bytes
                    FROM {table} WHERE {predicate} LIMIT :probe_limit
                ), budget AS (
                    SELECT CAST(COUNT(*) AS BIGINT) AS records,
                           CAST(COALESCE(SUM(payload_bytes), 0) AS BIGINT) AS bytes
                    FROM inventory
                )
                SELECT {projection}, budget.records, budget.bytes FROM budget
                LEFT JOIN inventory ON budget.records <= :max_records AND budget.bytes <= :max_bytes
                LEFT JOIN {table} p ON p.{identity} = inventory.identity
                WHERE budget.records > 0"
            )
        }
        None => format!(
            "SELECT {} FROM {table} WHERE {predicate}",
            columns.join(", ")
        ),
    };
    let mut statement = query(&sql)?;
    statement = match scope {
        Scope::Mint(mint) | Scope::MintKeysets(mint) => {
            statement.bind("identity", mint.to_string())
        }
        Scope::Keyset(id) | Scope::Keys(id) => statement.bind("identity", id.to_string()),
        Scope::Mints => statement,
    };
    if let Some((max_records, max_bytes)) = budget {
        statement = statement
            .bind("probe_limit", i64::from(max_records) + 1)
            .bind("max_records", i64::from(max_records))
            .bind("max_bytes", i64::from(max_bytes));
    }
    let rows = statement.fetch_all(conn).await?;
    match budget {
        Some((max_records, max_bytes)) => rows
            .into_iter()
            .map(|mut row| {
                let bytes: u64 = column_as_number!(row.pop().ok_or(Error::InvalidDbResponse)?);
                let records: u64 = column_as_number!(row.pop().ok_or(Error::InvalidDbResponse)?);
                if records > u64::from(max_records) || bytes > u64::from(max_bytes) {
                    return Err(Error::MintMetadataReadTooLarge {
                        records,
                        bytes,
                        max_records,
                        max_bytes,
                    });
                }
                Ok(row)
            })
            .collect(),
        None => Ok(rows),
    }
}
