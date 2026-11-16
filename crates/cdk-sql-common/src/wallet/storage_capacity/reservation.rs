use cdk_common::database::Error;
use uuid::Uuid;

use super::{prepare_claims, schema, set_reservation, WalletStorageClaim};
use crate::database::DatabaseExecutor;

/// A complete storage allowance committed with an application's recovery records.
///
/// Callers must size the envelope and fence stale changes with exact record
/// comparisons. This does not authorize retiring financial evidence or reserve
/// physical filesystem blocks. SQLite capacity accounting must be configured.
#[derive(Debug, Clone, Copy)]
pub struct WalletStorageReservation<'a> {
    /// Storage owner for the retained records and future completion writes.
    pub owner: Uuid,
    /// Total nonzero allowance, including retained payload and object metadata.
    /// The owner's account row is charged separately by the database.
    pub bytes: u64,
    /// Exact existing or future records assigned to this owner.
    pub claims: &'a [WalletStorageClaim],
}

impl WalletStorageReservation<'_> {
    pub(in crate::wallet) async fn prepare<T>(&self, conn: &T) -> Result<(), Error>
    where
        T: DatabaseExecutor,
    {
        if self.owner.is_nil() || self.bytes == 0 {
            return Err(Error::Internal(
                "Invalid wallet storage reservation".to_owned(),
            ));
        }
        if !schema::enabled(conn).await? {
            return Err(schema::invalid());
        }
        let bytes = i64::try_from(self.bytes).map_err(|_| Error::AmountOverflow)?;
        prepare_claims(conn, &self.owner.to_string(), Some(bytes), self.claims).await
    }

    pub(in crate::wallet) async fn finish<T>(&self, conn: &T) -> Result<(), Error>
    where
        T: DatabaseExecutor,
    {
        let bytes = i64::try_from(self.bytes).map_err(|_| Error::AmountOverflow)?;
        set_reservation(conn, &self.owner.to_string(), bytes).await
    }
}
