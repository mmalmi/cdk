use std::fmt::Write as _;

use cdk_common::database::Error;

use super::WalletStorageClaim;
use crate::database::DatabaseExecutor;
use crate::stmt::{query, Column};

mod ddl;

const TABLES: [&str; 13] = [
    "derivation_counter",
    "key",
    "keyset",
    "keyset_counter",
    "kv_store",
    "melt_quote",
    "migrations",
    "mint",
    "mint_quote",
    "p2pk_signing_key",
    "proof",
    "transactions",
    "wallet_sagas",
];
const INTERNAL: [&str; 3] = [
    "wallet_storage_total",
    "wallet_storage_accounts",
    "wallet_storage_objects",
];

pub(super) struct Table {
    name: String,
    columns: Vec<String>,
    primary: Vec<String>,
}

pub(super) fn invalid() -> Error {
    Error::Internal("Invalid or unconfigured wallet storage accounting".to_owned())
}

pub(super) async fn enabled<T>(conn: &T) -> Result<bool, Error>
where
    T: DatabaseExecutor,
{
    Ok(
        query("SELECT 1 FROM sqlite_schema WHERE name GLOB 'wallet_storage_*' LIMIT 1")?
            .pluck(conn)
            .await?
            .is_some(),
    )
}

pub(super) async fn catalog<T>(conn: &T) -> Result<Vec<Table>, Error>
where
    T: DatabaseExecutor,
{
    let rows = query("SELECT CASE WHEN length(CAST(name AS BLOB))<=64 THEN name END FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name LIMIT 17")?
        .fetch_all(conn).await?;
    let mut native = Vec::new();
    let mut internal = 0;
    for row in rows {
        let [Column::Text(name)] = row.as_slice() else {
            return Err(invalid());
        };
        if INTERNAL.contains(&name.as_str()) {
            internal += 1;
        } else {
            native.push(name.clone());
        }
    }
    if (internal != 0 && internal != INTERNAL.len()) || native != TABLES {
        return Err(invalid());
    }
    let mut result = Vec::new();
    for name in native {
        let rows =
            query("SELECT CASE WHEN length(CAST(name AS BLOB))<=64 THEN name END,pk,hidden FROM pragma_table_xinfo(:table) ORDER BY cid LIMIT 65")?
                .bind("table", name.clone())
                .fetch_all(conn)
                .await?;
        if rows.is_empty() || rows.len() > 64 {
            return Err(invalid());
        }
        let mut columns = Vec::new();
        let mut primary = Vec::new();
        for row in rows {
            let [Column::Text(column), Column::Integer(order), Column::Integer(hidden)] =
                row.as_slice()
            else {
                return Err(invalid());
            };
            if *hidden != 0
                || column.is_empty()
                || !column
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'_')
            {
                return Err(invalid());
            }
            columns.push(column.clone());
            if *order > 0 {
                primary.push((*order, column.clone()));
            }
        }
        if primary.is_empty() {
            return Err(invalid());
        }
        primary.sort_unstable_by_key(|(order, _)| *order);
        result.push(Table {
            name,
            columns,
            primary: primary.into_iter().map(|(_, name)| name).collect(),
        });
    }
    Ok(result)
}

pub(in crate::wallet) async fn validate_if_enabled<T>(conn: &T) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    // Partial tables or orphaned triggers must not look like an ordinary wallet.
    if enabled(conn).await? {
        validate(conn, &catalog(conn).await?).await?;
    }
    Ok(())
}

pub(super) async fn validate<T>(conn: &T, tables: &[Table]) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    for (name, expected) in ddl::definitions(tables) {
        let actual = query("SELECT CASE WHEN length(CAST(sql AS BLOB))<=:bytes THEN sql END FROM sqlite_schema WHERE name=:name")?
            .bind("bytes", i64::try_from(expected.len()).map_err(|_| Error::AmountOverflow)?)
            .bind("name", name)
            .pluck(conn)
            .await?;
        let Some(Column::Text(actual)) = actual else {
            return Err(invalid());
        };
        if actual.trim().trim_end_matches(';') != expected.trim().trim_end_matches(';') {
            return Err(invalid());
        }
    }
    if query("SELECT version FROM wallet_storage_total WHERE id=1")?
        .pluck(conn)
        .await?
        != Some(Column::Integer(1))
    {
        return Err(invalid());
    }
    Ok(())
}

pub(super) async fn install<T>(conn: &T, tables: &[Table], maximum: i64) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    for (_, sql) in ddl::definitions(tables) {
        query(&sql)?.execute(conn).await?;
    }
    query("INSERT INTO wallet_storage_total(id,version,maximum,charged) VALUES(1,1,:maximum,64)")?
        .bind("maximum", maximum)
        .execute(conn)
        .await?;
    for table in tables {
        let sql = format!("INSERT INTO wallet_storage_objects(object_key,owner,payload_bytes) SELECT {},{},{} FROM \"{}\"",
            table.key(""), table.owner(""), table.size(""), table.name);
        query(&sql)?.execute(conn).await?;
    }
    Ok(())
}

pub(super) fn claim_key(tables: &[Table], claim: &WalletStorageClaim) -> Result<String, Error> {
    let table = tables
        .iter()
        .find(|t| t.name == claim.table)
        .ok_or_else(invalid)?;
    if table.primary.len() != claim.primary_key.len() {
        return Err(invalid());
    }
    let mut key = table.name.clone();
    for part in &claim.primary_key {
        key.push(':');
        for byte in part {
            write!(&mut key, "{byte:02X}").map_err(|_| invalid())?;
        }
    }
    Ok(key)
}

impl Table {
    pub(super) fn matches_columns(&self, table: &str, columns: &[&str]) -> bool {
        self.name == table
            && self.columns.len() == columns.len()
            && columns
                .iter()
                .all(|column| self.columns.iter().any(|name| name == column))
    }

    fn key(&self, prefix: &str) -> String {
        format!(
            "'{}:' || {}",
            self.name,
            self.primary
                .iter()
                .map(|c| format!("hex({prefix}\"{c}\")"))
                .collect::<Vec<_>>()
                .join(" || ':' || ")
        )
    }

    fn size(&self, prefix: &str) -> String {
        format!("32+{}",self.columns.iter().map(|c|format!("CASE typeof({prefix}\"{c}\") WHEN 'null' THEN 0 WHEN 'integer' THEN 8 WHEN 'real' THEN 8 ELSE length(CAST({prefix}\"{c}\" AS BLOB)) END")).collect::<Vec<_>>().join("+"))
    }

    fn owner(&self, prefix: &str) -> String {
        let columns: &[&str] = match self.name.as_str() {
            "wallet_sagas" => &["id"],
            "proof" => &["created_by_operation", "used_by_operation"],
            "transactions" => &["saga_id"],
            _ => return "''".to_owned(),
        };
        format!(
            "COALESCE({},'')",
            columns
                .iter()
                .map(|c| format!("{prefix}\"{c}\""))
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}
