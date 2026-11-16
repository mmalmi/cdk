//! Atomic changes to related wallet key-value records.
use std::collections::HashSet;

use cdk_common::database::wallet::ProofChange;
use cdk_common::database::{validate_kvstore_params, Error};

use super::storage_capacity::WalletStorageReservation;
use super::SQLWalletDatabase;
use crate::database::ConnectionWithTransaction;
use crate::keyvalue;
use crate::pool::{DatabasePool, PooledResource};
use crate::stmt::query;

/// One conditional write or deletion in an atomic wallet update.
#[derive(Debug, Clone, Copy)]
pub struct WalletKvChange<'a> {
    /// Primary key-value namespace.
    pub primary_namespace: &'a str,
    /// Secondary key-value namespace.
    pub secondary_namespace: &'a str,
    /// Record key, unique within the batch.
    pub key: &'a str,
    /// Original bytes, or `None` to require an absent record.
    pub expected: Option<&'a [u8]>,
    /// Replacement bytes, or `None` to delete an existing record.
    pub replacement: Option<&'a [u8]>,
}

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    /// Apply all changes together, returning `false` if any original differs.
    ///
    /// Mismatches and database errors roll back every change, including storage
    /// accounting. Keys must be unique and an absent record cannot be deleted.
    /// Deletions run first to release space within the same transaction. Callers
    /// must bound their batch and retain external retry intent until this succeeds.
    #[tracing::instrument(skip_all)]
    pub async fn kv_compare_and_swap_batch(
        &self,
        changes: &[WalletKvChange<'_>],
    ) -> Result<bool, Error> {
        self.apply_wallet_changes(Vec::new(), None, changes, None)
            .await
    }

    /// Compare and change proofs and key-value records in one transaction.
    ///
    /// Proof changes run before key-value changes, so deleted proof storage is
    /// available to the accompanying checkpoint. A mismatch or error rolls back
    /// both records and storage charges. This does not authorize proof release:
    /// callers must check external ownership and bound the batch before calling.
    #[tracing::instrument(skip_all)]
    pub async fn compare_and_swap_proofs_with_kv(
        &self,
        proofs: Vec<ProofChange>,
        changes: &[WalletKvChange<'_>],
    ) -> Result<bool, Error> {
        self.apply_wallet_changes(proofs, None, changes, None).await
    }

    /// Atomically change proofs, key-value records and their storage allowance.
    ///
    /// All comparisons, claims and reservation changes roll back on a mismatch
    /// or error. Deletions run first so replacement records can reuse their space.
    /// The caller supplies the complete envelope and exact claims; unclaimed
    /// records retain their current owner. At least one key-value comparison is
    /// required to fence the caller's saved admission or retirement intent.
    #[tracing::instrument(skip_all)]
    pub async fn compare_and_swap_proofs_with_kv_and_storage(
        &self,
        proofs: Vec<ProofChange>,
        changes: &[WalletKvChange<'_>],
        reservation: WalletStorageReservation<'_>,
    ) -> Result<bool, Error> {
        Self::require_sqlite_capacity()?;
        if changes.is_empty() {
            return Err(Error::Internal(
                "Storage update requires a saved intent".to_owned(),
            ));
        }
        self.apply_wallet_changes(proofs, None, changes, Some(reservation))
            .await
    }
}

pub(super) fn validate(changes: &[WalletKvChange<'_>]) -> Result<(), Error> {
    let mut keys = HashSet::new();
    for change in changes {
        validate_kvstore_params(
            change.primary_namespace,
            change.secondary_namespace,
            Some(change.key),
        )?;
        if !keys.insert((
            change.primary_namespace,
            change.secondary_namespace,
            change.key,
        )) || (change.expected.is_none() && change.replacement.is_none())
        {
            return Err(Error::Internal("Invalid wallet key-value batch".to_owned()));
        }
    }
    Ok(())
}

pub(super) async fn apply<RM>(
    conn: &ConnectionWithTransaction<RM::Connection, PooledResource<RM>>,
    changes: &[WalletKvChange<'_>],
) -> Result<bool, Error>
where
    RM: DatabasePool,
{
    if !apply_part::<RM>(conn, changes, true).await? {
        return Ok(false);
    }
    apply_part::<RM>(conn, changes, false).await
}

pub(super) async fn apply_part<RM>(
    conn: &ConnectionWithTransaction<RM::Connection, PooledResource<RM>>,
    changes: &[WalletKvChange<'_>],
    deletions: bool,
) -> Result<bool, Error>
where
    RM: DatabasePool,
{
    for change in changes
        .iter()
        .filter(|change| change.replacement.is_none() == deletions)
    {
        if !change.apply::<RM>(conn).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

impl WalletKvChange<'_> {
    async fn apply<RM>(
        &self,
        conn: &ConnectionWithTransaction<RM::Connection, PooledResource<RM>>,
    ) -> Result<bool, Error>
    where
        RM: DatabasePool,
    {
        match (self.expected, self.replacement) {
            (None, Some(value)) => {
                keyvalue::kv_write_if_absent_in_transaction::<RM>(
                    conn,
                    self.primary_namespace,
                    self.secondary_namespace,
                    self.key,
                    value,
                )
                .await
            }
            (Some(expected), Some(value)) => {
                keyvalue::kv_write_if_equals_in_transaction::<RM>(
                    conn,
                    self.primary_namespace,
                    self.secondary_namespace,
                    self.key,
                    expected,
                    value,
                )
                .await
            }
            (Some(expected), None) => {
                let affected = query(
                    "DELETE FROM kv_store WHERE primary_namespace=:primary
                    AND secondary_namespace=:secondary AND key=:key AND value=:expected",
                )?
                .bind("primary", self.primary_namespace)
                .bind("secondary", self.secondary_namespace)
                .bind("key", self.key)
                .bind("expected", expected.to_vec())
                .execute(conn)
                .await?;
                Ok(affected == 1)
            }
            (None, None) => Err(Error::Internal("Invalid wallet deletion".to_owned())),
        }
    }
}
