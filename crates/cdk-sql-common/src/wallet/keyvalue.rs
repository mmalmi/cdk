//! Reject oversized records before their payload crosses the SQL driver boundary.
use cdk_common::database::{validate_kvstore_params, Error};

use super::{query_budget, SQLWalletDatabase};
use crate::column_as_number;
use crate::pool::DatabasePool;
use crate::stmt::{query, Column};

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    pub(super) async fn read_bounded_value(
        &self,
        primary_namespace: &str,
        secondary_namespace: &str,
        key: &str,
        max_bytes: u32,
    ) -> Result<Option<Vec<u8>>, Error> {
        validate_kvstore_params(primary_namespace, secondary_namespace, Some(key))?;
        let length = query_budget::payload_lengths::<RM::Connection>(&["value"], &[])?;
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        // The byte count and conditional payload share one statement snapshot.
        let sql = format!(
            "SELECT CASE WHEN {length} <= :max_bytes THEN value END, {length}
             FROM kv_store WHERE primary_namespace = :primary_namespace
             AND secondary_namespace = :secondary_namespace AND key = :key"
        );
        query(&sql)?
            .bind("primary_namespace", primary_namespace)
            .bind("secondary_namespace", secondary_namespace)
            .bind("key", key)
            .bind("max_bytes", i64::from(max_bytes))
            .fetch_one(&*conn)
            .await?
            .map(|row| {
                let [value, length]: [Column; 2] =
                    row.try_into().map_err(|_| Error::InvalidDbResponse)?;
                let bytes: u64 = column_as_number!(length);
                if bytes > u64::from(max_bytes) {
                    return Err(Error::KvValueTooLarge {
                        bytes,
                        limit: max_bytes,
                    });
                }
                match value {
                    Column::Blob(value) => Ok(value),
                    _ => Err(Error::InvalidDbResponse),
                }
            })
            .transpose()
    }
}
