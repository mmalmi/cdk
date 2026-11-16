//! Move a future allowance without a release/reacquire capacity race.
use cdk_common::database::Error;
use uuid::Uuid;

use super::{claim_rows, read, release, schema, WalletStorageCapacity, WalletStorageClaim};
use crate::database::{ConnectionWithTransaction, DatabaseExecutor};
use crate::keyvalue;
use crate::pool::DatabasePool;
use crate::stmt::{query, Column};
use crate::wallet::SQLWalletDatabase;

const NAMESPACE: &str = "cdk_storage_transfer";

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    /// Storage retained by an unused-allowance transfer's durable retry marker.
    ///
    /// Include this charge in the source allowance before admitting its future
    /// obligation. The existing source account is charged separately.
    #[tracing::instrument(skip_all)]
    pub async fn storage_reservation_transfer_bytes(&self, source: Uuid) -> Result<u64, Error> {
        self.kv_storage_bytes(source, NAMESPACE, "", &source.to_string(), 16)
            .await
    }

    /// Atomically move a source's unused allowance to an existing reservation.
    ///
    /// Retained source rows and a durable source-to-destination marker stay
    /// charged to the source. Everything else in its allowance is added to the
    /// destination. Total charged capacity stays unchanged, including at the
    /// configured limit. Insufficient source space or any write failure leaves
    /// both owners unchanged. An exact retry is harmless after reopen or after
    /// the destination releases its allowance; redirection is rejected.
    ///
    /// The caller must durably fence the original obligation before transfer.
    /// This moves storage only, not coins or authority to reclaim them. Do not
    /// readmit the source or retire its marker while a transfer can be retried.
    #[tracing::instrument(skip_all)]
    pub async fn transfer_storage_reservation(
        &self,
        source: Uuid,
        destination: Uuid,
    ) -> Result<WalletStorageCapacity, Error> {
        Self::require_sqlite_capacity()?;
        if source.is_nil() || destination.is_nil() || source == destination {
            return Err(Error::Internal(
                "Invalid storage transfer owners".to_owned(),
            ));
        }
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        let tx = ConnectionWithTransaction::new(conn).await?;
        match transfer(&tx, source, destination).await {
            Ok(capacity) => {
                tx.commit().await?;
                Ok(capacity)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }
    /// Remove a transfer marker after the caller durably excludes transfer retries.
    ///
    /// Both the completed financial outcome and stale callers must be fenced
    /// before this call. Exact retirement retries are harmless. Conflicting
    /// markers and readmitted source owners are rejected; other retained source
    /// rows remain charged. This does not release the destination's allowance.
    #[tracing::instrument(skip_all)]
    pub async fn retire_storage_reservation_transfer(
        &self,
        source: Uuid,
        destination: Uuid,
    ) -> Result<WalletStorageCapacity, Error> {
        Self::require_sqlite_capacity()?;
        if source.is_nil() || destination.is_nil() || source == destination {
            return Err(Error::Internal(
                "Invalid storage transfer owners".to_owned(),
            ));
        }
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        let tx = ConnectionWithTransaction::new(conn).await?;
        let result = async {
            if !schema::enabled(&tx).await? { return Err(schema::invalid()); }
            let source = source.to_string();
            let (marker, owner) = read_marker(&tx, &source).await?;
            let reserved = query("SELECT reserved FROM wallet_storage_accounts WHERE owner=:owner")?
                .bind("owner", source.clone()).pluck(&tx).await?;
            if !matches!(reserved, None | Some(Column::Integer(0))) {
                return Err(Error::Internal("Storage transfer source was readmitted".to_owned()));
            }
            match marker {
                Some(marker) if marker == Column::Blob(destination.as_bytes().to_vec())
                    && owner == Some(Column::Text(source.clone())) => {
                    query("DELETE FROM kv_store WHERE primary_namespace=:namespace AND secondary_namespace='' AND key=:key")?
                        .bind("namespace", NAMESPACE.to_owned()).bind("key", source.clone()).execute(&tx).await?;
                    release(&tx, &source).await?;
                }
                None if owner.is_none() => (),
                _ => return Err(Error::Internal("Conflicting storage transfer evidence".to_owned())),
            }
            read(&tx).await
        }.await;
        match result {
            Ok(capacity) => {
                tx.commit().await?;
                Ok(capacity)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }
}

fn marker_claim(source: &str) -> WalletStorageClaim {
    WalletStorageClaim {
        table: "kv_store".to_owned(),
        primary_key: vec![
            NAMESPACE.as_bytes().to_vec(),
            vec![],
            source.as_bytes().to_vec(),
        ],
    }
}

async fn read_marker<T>(conn: &T, source: &str) -> Result<(Option<Column>, Option<Column>), Error>
where
    T: DatabaseExecutor,
{
    let key = schema::claim_key(&schema::catalog(conn).await?, &marker_claim(source))?;
    let owner = query("SELECT owner FROM wallet_storage_objects WHERE object_key=:key")?
        .bind("key", key)
        .pluck(conn)
        .await?;
    let marker = query("SELECT CASE WHEN length(value)=16 THEN value END FROM kv_store WHERE primary_namespace=:namespace AND secondary_namespace='' AND key=:key")?
        .bind("namespace", NAMESPACE.to_owned()).bind("key", source.to_owned()).pluck(conn).await?;
    Ok((marker, owner))
}

async fn transfer<T>(
    conn: &T,
    source: Uuid,
    destination: Uuid,
) -> Result<WalletStorageCapacity, Error>
where
    T: DatabaseExecutor,
{
    if !schema::enabled(conn).await? {
        return Err(schema::invalid());
    }
    let source = source.to_string();
    let claim = marker_claim(&source);
    let (marker, owner) = read_marker(conn, &source).await?;
    let (_, source_reserved) = account(conn, &source).await?;
    if let Some(marker) = marker {
        if marker != Column::Blob(destination.as_bytes().to_vec())
            || owner != Some(Column::Text(source.clone()))
            || source_reserved != 0
        {
            return Err(Error::Internal(
                "Conflicting storage transfer evidence".to_owned(),
            ));
        }
        return read(conn).await;
    }
    if owner.is_some() || source_reserved == 0 {
        return Err(Error::Internal(
            "Storage transfer source is not available".to_owned(),
        ));
    }
    let (target_used, target_reserved) = account(conn, &destination.to_string()).await?;
    if target_reserved == 0 {
        return Err(Error::Internal(
            "Storage destination has no reservation".to_owned(),
        ));
    }

    // Withdraw and reassign inside one transaction; never expose free capacity
    // to another writer before the destination and retry evidence are durable.
    query("UPDATE wallet_storage_accounts SET reserved=0 WHERE owner=:owner")?
        .bind("owner", source.clone())
        .execute(conn)
        .await?;
    claim_rows(conn, &source, &[claim]).await?;
    keyvalue::write_statement(NAMESPACE, "", &source, destination.as_bytes())?
        .execute(conn)
        .await?;
    let (source_used, _) = account(conn, &source).await?;
    let available = source_reserved
        .checked_sub(source_used)
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| Error::Internal("Storage transfer has no unused allowance".to_owned()))?;
    let reserved = target_reserved
        .max(target_used)
        .checked_add(available)
        .ok_or(Error::AmountOverflow)?;
    query("UPDATE wallet_storage_accounts SET reserved=:reserved WHERE owner=:owner")?
        .bind("reserved", reserved)
        .bind("owner", destination.to_string())
        .execute(conn)
        .await?;
    read(conn).await
}

async fn account<T>(conn: &T, owner: &str) -> Result<(i64, i64), Error>
where
    T: DatabaseExecutor,
{
    let row = query("SELECT used,reserved FROM wallet_storage_accounts WHERE owner=:owner")?
        .bind("owner", owner.to_owned())
        .fetch_one(conn)
        .await?
        .ok_or_else(|| Error::Internal("Storage transfer account is missing".to_owned()))?;
    let [Column::Integer(used), Column::Integer(reserved)] = row.as_slice() else {
        return Err(schema::invalid());
    };
    Ok((*used, *reserved))
}
