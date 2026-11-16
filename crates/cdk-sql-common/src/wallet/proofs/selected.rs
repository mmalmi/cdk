//! Exact input pages reject oversized payloads before crossing the SQL driver.
use cdk_common::database::wallet::{MAX_PROOF_PAGE_BYTES, MAX_PROOF_PAGE_SIZE};
use cdk_common::database::Error;
use cdk_common::wallet::ProofInfo;
use cdk_common::PublicKey;

use super::{bounded_projection, decode_bounded_row, COLUMNS};
use crate::database::DatabaseExecutor;
use crate::pool::DatabasePool;
use crate::stmt::query;
use crate::wallet::{query_budget, SQLWalletDatabase};

pub(in crate::wallet) async fn load_selected_page<T>(
    conn: &T,
    ys: &[PublicKey],
) -> Result<Vec<ProofInfo>, Error>
where
    T: DatabaseExecutor,
{
    load_with_limit(conn, ys, MAX_PROOF_PAGE_BYTES).await
}

pub(super) async fn load_with_limit<T>(
    conn: &T,
    ys: &[PublicKey],
    max_bytes: u64,
) -> Result<Vec<ProofInfo>, Error>
where
    T: DatabaseExecutor,
{
    if ys.len() > usize::from(MAX_PROOF_PAGE_SIZE) {
        return Err(Error::Internal(
            "Selected proof page exceeds 128 records".to_owned(),
        ));
    }
    if ys.is_empty() {
        return Ok(Vec::new());
    }
    let lengths = query_budget::payload_lengths::<T>(&COLUMNS, &["amount", "derivation_index"])?;
    let projection = bounded_projection();
    let sql = format!(
        "WITH page AS (
            SELECT y, 16 + {lengths} AS payload_bytes FROM proof WHERE y IN (:ys)
        ), budget AS (
            SELECT CAST(COALESCE(SUM(payload_bytes), 0) AS BIGINT) AS bytes FROM page
        )
        SELECT {projection}, budget.bytes
        FROM page LEFT JOIN proof p ON p.y = page.y CROSS JOIN budget
        ORDER BY page.y"
    );
    query(&sql)?
        .bind_vec("ys", ys.iter().map(|y| y.to_bytes().to_vec()).collect())?
        .bind(
            "max_bytes",
            i64::try_from(max_bytes).map_err(|_| Error::AmountOverflow)?,
        )
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(|row| decode_bounded_row(row, max_bytes))
        .collect()
}

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    /// Read up to 128 selected proof identities within the 4-MiB page budget.
    ///
    /// Present rows are returned once each in ascending binary Y order; missing
    /// identities are omitted. Empty input returns no rows. Oversized aggregate
    /// payloads fail before crossing the SQL driver; callers may select fewer
    /// identities and retry. Selected malformed records fail without being
    /// skipped. No rows change, and separate calls do not share a snapshot.
    #[tracing::instrument(skip_all)]
    pub async fn get_proofs_by_ys_bounded(
        &self,
        ys: &[PublicKey],
    ) -> Result<Vec<ProofInfo>, Error> {
        self.get_proofs_by_ys_with_limit(ys, MAX_PROOF_PAGE_BYTES)
            .await
    }

    /// Read up to 128 selected identities within an explicit stored-byte budget.
    ///
    /// This overrides the instance's ordinary proof read policy for this call.
    /// The caller must bound the allowance independently of the stored payload.
    /// Oversized or malformed rows fail without changing evidence; there is no
    /// unbounded retry. Missing identities are omitted, and duplicate identities
    /// return one row. Separate calls do not share a snapshot.
    #[tracing::instrument(skip_all)]
    pub async fn get_proofs_by_ys_with_limit(
        &self,
        ys: &[PublicKey],
        max_bytes: u64,
    ) -> Result<Vec<ProofInfo>, Error> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        load_with_limit(&*conn, ys, max_bytes).await
    }
}
