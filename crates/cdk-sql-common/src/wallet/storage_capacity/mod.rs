//! Opt-in logical storage accounting for a dedicated SQLite wallet.
//!
//! Callers must size and retain completion reservations before irreversible work.
//! These APIs do not derive those envelopes or reserve filesystem blocks.
use cdk_common::database::Error;
use cdk_common::wallet::WalletSaga;
use uuid::Uuid;

use super::SQLWalletDatabase;
use crate::database::{ConnectionWithTransaction, DatabaseExecutor};
use crate::pool::DatabasePool;
use crate::stmt::{query, Column};

mod acquisition;
mod completion;
mod issue;
mod melt;
mod output_key_sizing;
mod output_keys;
mod preparation;
mod receive;
mod reclaim;
mod reservation;
pub use self::reservation::WalletStorageReservation;
pub(super) mod schema;
mod sizing;
pub(super) use sizing::output as projected_proof;
mod swap;
mod transfer;
pub(super) use acquisition::Acquisition;
pub(super) use completion::{
    completion_record as send_completion_record, token_record as send_token_record,
    CompletionAdmission,
};
pub(super) use issue::insert_initial_issue;
pub(super) use melt::{
    admit_melt_payment, completion_record as melt_completion_record, MeltAcquisition,
};
pub(super) use output_keys::OutputKeys;
pub(super) use preparation::{delete_saga, insert_saga};
pub(super) use receive::completion_record as receive_completion_record;
pub(super) use reclaim::completion_record as reclaim_completion_record;
pub(super) use swap::{completion_record as swap_completion_record, insert_initial_swap};

/// Logical payload and accounting metadata, including retained reservations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalletStorageCapacity {
    /// Configured maximum charged bytes.
    pub maximum_bytes: u64,
    /// Stored bytes or reserved completion bytes, whichever is larger per owner.
    pub charged_bytes: u64,
}

/// An existing or future wallet row assigned to a storage reservation.
///
/// Primary-key parts are in schema order: raw bytes for BLOB keys, UTF-8 for TEXT.
/// Storage ownership is independent of a coin's current financial owner. Callers
/// must coordinate transfers with their financial operation and writer locks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletStorageClaim {
    /// A native wallet table. Internal accounting tables cannot be claimed.
    pub table: String,
    /// Exact primary-key components, without SQL or hex encoding.
    pub primary_key: Vec<Vec<u8>>,
}

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    pub(super) fn require_sqlite_capacity() -> Result<(), Error> {
        if RM::Connection::name() != "sqlite" {
            return Err(Error::Internal(
                "Wallet storage capacity requires SQLite".to_owned(),
            ));
        }
        Ok(())
    }

    /// Enable or resize logical storage accounting, retaining existing records.
    ///
    /// Initialization and resizing are atomic and reject a limit below retained
    /// usage. Accounting stays enabled on reopen. An unknown schema or damaged
    /// accounting definition is rejected. This does not automatically admit
    /// financial operations; callers must reserve their complete storage needs.
    /// Initial activation rejects existing native operations. Applications must
    /// also ensure external payment obligations already have their reservations.
    /// Creating new output secrets then requires an existing operation reservation.
    #[tracing::instrument(skip_all)]
    pub async fn configure_storage_capacity(
        &self,
        maximum_bytes: u64,
    ) -> Result<WalletStorageCapacity, Error> {
        Self::require_sqlite_capacity()?;
        let maximum = i64::try_from(maximum_bytes).map_err(|_| Error::AmountOverflow)?;
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        let tx = ConnectionWithTransaction::new(conn).await?;
        let tables = schema::catalog(&tx).await?;
        if schema::enabled(&tx).await? {
            schema::validate(&tx, &tables).await?;
            query("UPDATE wallet_storage_total SET maximum=:maximum WHERE id=1")?
                .bind("maximum", maximum)
                .execute(&tx)
                .await?;
        } else {
            if query("SELECT 1 FROM wallet_sagas LIMIT 1")?
                .pluck(&tx)
                .await?
                .is_some()
            {
                return Err(Error::Internal(
                    "Enable wallet storage capacity before creating operations".to_owned(),
                ));
            }
            schema::install(&tx, &tables, maximum).await?;
        }
        let capacity = read(&tx).await?;
        tx.commit().await?;
        Ok(capacity)
    }

    /// Read configured capacity; `None` means accounting has never been enabled.
    #[tracing::instrument(skip_all)]
    pub async fn storage_capacity(&self) -> Result<Option<WalletStorageCapacity>, Error> {
        Self::require_sqlite_capacity()?;
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        if !schema::enabled(&*conn).await? {
            return Ok(None);
        }
        read(&*conn).await.map(Some)
    }

    /// Atomically reserve an owner's complete allowance and claim exact rows.
    ///
    /// `bytes` includes retained payload and per-record accounting metadata.
    /// Existing reservations can be resized only while all their usage fits.
    /// A rejected claim or capacity increase preserves every original record.
    /// Use each owner for one financial obligation and fence stale admission or
    /// release retries in the caller's durable operation state.
    #[tracing::instrument(skip_all)]
    pub async fn reserve_storage(
        &self,
        owner: Uuid,
        bytes: u64,
        claims: &[WalletStorageClaim],
    ) -> Result<WalletStorageCapacity, Error> {
        if bytes == 0 {
            return Err(Error::AmountZero);
        }
        self.claim_storage_inner(owner, Some(bytes), claims, None)
            .await
    }

    /// Save a fresh recovery intent and its complete storage allowance atomically.
    ///
    /// The caller supplies the full envelope, including the recovery record, and
    /// must validate the financial intent before admission. No inputs or quotes
    /// are acquired here. Only a version-zero saga with an unused storage owner
    /// and unclaimed recovery record can be admitted. Retries must inspect the
    /// saved intent instead of replacing it. Failure preserves existing claims
    /// and leaves no reservation or saga.
    ///
    /// This SQLite-only primitive requires configured capacity and does not size
    /// the envelope. Removing a cancelled, claimed or reclaimed send, an
    /// unsubmitted/completed receive, or a cancelled/completed standalone swap or
    /// withdrawal, releases unused allowance. Retained records stay charged;
    /// other saga endings retain allowance for external obligations.
    #[tracing::instrument(skip_all)]
    pub async fn reserve_saga_storage(
        &self,
        saga: &WalletSaga,
        bytes: u64,
        claims: &[WalletStorageClaim],
    ) -> Result<WalletStorageCapacity, Error> {
        self.check_saga_read_limit(saga)?;
        if bytes == 0 {
            return Err(Error::AmountZero);
        }
        if saga.version != 0 {
            return Err(Error::Internal(
                "Storage admission requires a fresh saga".to_owned(),
            ));
        }
        self.claim_storage_inner(saga.id, Some(bytes), claims, Some(saga))
            .await
    }

    /// Bind future or existing records to an already-admitted storage allowance.
    ///
    /// This can consume an owner's remaining reservation at full global capacity.
    /// It cannot create or enlarge the reservation. This is storage attribution,
    /// not authority to alter another operation's coins or financial evidence.
    #[tracing::instrument(skip_all)]
    pub async fn claim_storage(
        &self,
        owner: Uuid,
        claims: &[WalletStorageClaim],
    ) -> Result<WalletStorageCapacity, Error> {
        self.claim_storage_inner(owner, None, claims, None).await
    }

    async fn claim_storage_inner(
        &self,
        owner: Uuid,
        bytes: Option<u64>,
        claims: &[WalletStorageClaim],
        saga: Option<&WalletSaga>,
    ) -> Result<WalletStorageCapacity, Error> {
        Self::require_sqlite_capacity()?;
        if owner.is_nil() {
            return Err(Error::Internal("Storage owner must not be nil".to_owned()));
        }
        let owner = owner.to_string();
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        let tx = ConnectionWithTransaction::new(conn).await?;
        if !schema::enabled(&tx).await? {
            return Err(schema::invalid());
        }
        if saga.is_some() {
            require_fresh_owner(&tx, &owner).await?;
        }
        let bytes = bytes
            .map(i64::try_from)
            .transpose()
            .map_err(|_| Error::AmountOverflow)?;
        prepare_claims(&tx, &owner, bytes, claims).await?;
        if let Some(bytes) = bytes {
            set_reservation(&tx, &owner, bytes).await?;
        }
        if let Some(saga) = saga {
            super::sagas::insert(&tx, saga).await?;
        }
        let capacity = read(&tx).await?;
        tx.commit().await?;
        Ok(capacity)
    }

    /// Release unused allowance after the caller verifies all obligations ended.
    ///
    /// Stored rows retain their attribution and stay charged. Only unused future
    /// placeholders are removed. Repeating a release is harmless. This API does
    /// not decide when financial evidence or an admitted payment may be retired.
    #[tracing::instrument(skip_all)]
    pub async fn release_storage(&self, owner: Uuid) -> Result<WalletStorageCapacity, Error> {
        Self::require_sqlite_capacity()?;
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        let tx = ConnectionWithTransaction::new(conn).await?;
        let result = async {
            release(&tx, &owner.to_string()).await?;
            read(&tx).await
        }
        .await;
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

/// Call inside exact terminal retirement after native completion/retry rights end.
pub(super) async fn release_completed<T>(conn: &T, id: &Uuid) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    if schema::enabled(conn).await? {
        release(conn, &id.to_string()).await?;
    }
    Ok(())
}

async fn release<T>(conn: &T, owner: &str) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    query("UPDATE wallet_storage_accounts SET reserved=0 WHERE owner=:owner")?
        .bind("owner", owner.to_owned())
        .execute(conn)
        .await?;
    query("DELETE FROM wallet_storage_objects WHERE owner=:owner AND payload_bytes=0")?
        .bind("owner", owner.to_owned())
        .execute(conn)
        .await?;
    query("DELETE FROM wallet_storage_accounts WHERE owner=:owner AND used=0 AND reserved=0")?
        .bind("owner", owner.to_owned())
        .execute(conn)
        .await?;
    Ok(())
}

/// Restore the prior allowance or admit the measured completion peak, whichever
/// is larger. Call only inside the transaction that withdrew the old allowance.
async fn reserve_growth<T>(conn: &T, owner: &str, reserved: i64, growth: i64) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    let changed = query("UPDATE wallet_storage_accounts SET reserved=max(:reserved,used+:growth) WHERE owner=:owner")?
        .bind("reserved", reserved).bind("growth", growth).bind("owner", owner.to_owned())
        .execute(conn).await?;
    if changed != 1 {
        return Err(Error::InvalidDbResponse);
    }
    Ok(())
}

async fn prepare_claims<T>(
    conn: &T,
    owner: &str,
    bytes: Option<i64>,
    claims: &[WalletStorageClaim],
) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    if bytes.is_some() {
        // Reuse retained payload before charging its replacement allowance.
        // The enclosing transaction restores it if admission fails.
        query("UPDATE wallet_storage_accounts SET reserved=0 WHERE owner=:owner")?
            .bind("owner", owner.to_owned())
            .execute(conn)
            .await?;
    } else {
        require_reservation(conn, owner).await?;
    }
    claim_rows(conn, owner, claims).await?;
    Ok(())
}

async fn set_reservation<T>(conn: &T, owner: &str, bytes: i64) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    query("INSERT INTO wallet_storage_accounts(owner,used,reserved) VALUES(:owner,0,:bytes) ON CONFLICT(owner) DO UPDATE SET reserved=excluded.reserved")?
        .bind("owner", owner.to_owned())
        .bind("bytes", bytes)
        .execute(conn).await?;
    Ok(())
}

async fn require_fresh_owner<T>(conn: &T, owner: &str) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    if query("SELECT 1 FROM wallet_storage_accounts WHERE owner=:owner UNION ALL SELECT 1 FROM wallet_storage_objects WHERE object_key='wallet_sagas:' || hex(:owner) LIMIT 1")?
        .bind("owner", owner.to_owned())
        .pluck(conn)
        .await?
        .is_some()
    {
        return Err(Error::Internal(
            "Storage admission owner or recovery claim already exists".to_owned(),
        ));
    }
    Ok(())
}

async fn require_reservation<T>(conn: &T, owner: &str) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    if !matches!(query("SELECT reserved FROM wallet_storage_accounts WHERE owner=:owner")?
        .bind("owner", owner.to_owned()).pluck(conn).await?, Some(Column::Integer(value)) if value > 0)
    {
        return Err(Error::Internal(
            "Storage owner has no completion reservation".to_owned(),
        ));
    }
    Ok(())
}

async fn claim_rows<T>(
    conn: &T,
    owner: &str,
    claims: &[WalletStorageClaim],
) -> Result<Vec<String>, Error>
where
    T: DatabaseExecutor,
{
    // SQL identifiers come from the bounded catalog; callers provide only bytes.
    let tables = schema::catalog(conn).await?;
    let mut keys = Vec::with_capacity(claims.len());
    for claim in claims {
        let key = schema::claim_key(&tables, claim)?;
        query("INSERT INTO wallet_storage_objects(object_key,owner,payload_bytes) VALUES(:key,:owner,0) ON CONFLICT(object_key) DO UPDATE SET owner=excluded.owner")?
            .bind("key", key.clone()).bind("owner", owner.to_owned()).execute(conn).await?;
        keys.push(key);
    }
    Ok(keys)
}

async fn read<T>(conn: &T) -> Result<WalletStorageCapacity, Error>
where
    T: DatabaseExecutor,
{
    let row = query("SELECT maximum,charged FROM wallet_storage_total WHERE id=1")?
        .fetch_one(conn)
        .await?
        .ok_or_else(schema::invalid)?;
    let [Column::Integer(maximum), Column::Integer(charged)] = row.as_slice() else {
        return Err(schema::invalid());
    };
    Ok(WalletStorageCapacity {
        maximum_bytes: u64::try_from(*maximum).map_err(|_| schema::invalid())?,
        charged_bytes: u64::try_from(*charged).map_err(|_| schema::invalid())?,
    })
}
