//! Bounded operation ownership without decoding unrelated proof payloads.
use cdk_common::database::wallet::MAX_PROOF_PAGE_SIZE;
use cdk_common::database::Error;
use cdk_common::PublicKey;
use uuid::Uuid;

use crate::column_as_number;
use crate::pool::DatabasePool;
use crate::stmt::{query, Column};
use crate::wallet::{proofs, query_budget, SQLWalletDatabase};

/// Current operation references on one wallet proof, without its spendable data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProofOperationOwners {
    /// Proof identity.
    pub y: PublicKey,
    /// Operation that acquired the proof.
    pub used_by_operation: Option<Uuid>,
    /// Operation that created the proof.
    pub created_by_operation: Option<Uuid>,
}

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    /// Read operation references for at most 128 selected proof identities.
    ///
    /// Each row is bounded by a compressed public key and two textual UUIDs
    /// before crossing the SQL driver. Missing proofs are omitted; duplicates
    /// are returned once, in binary Y order. Malformed ownership fails the page.
    /// This does not validate proof payloads, establish release authority, or
    /// hold a snapshot across calls. Financial writes still need exact originals.
    #[tracing::instrument(skip_all)]
    pub async fn get_proof_operation_owners(
        &self,
        ys: &[PublicKey],
    ) -> Result<Vec<ProofOperationOwners>, Error> {
        if ys.len() > usize::from(MAX_PROOF_PAGE_SIZE) {
            return Err(Error::Internal(
                "Selected proof page exceeds 128 records".to_owned(),
            ));
        }
        if ys.is_empty() {
            return Ok(Vec::new());
        }
        const LIMIT: u64 = 33 + 2 * 36;
        let columns = ["y", "used_by_operation", "created_by_operation"];
        let lengths = query_budget::payload_lengths::<RM::Connection>(&columns, &[])?;
        let projection = columns
            .iter()
            .map(|column| format!("CASE WHEN {lengths} <= :limit THEN {column} END"))
            .collect::<Vec<_>>()
            .join(", ");
        let conn = self
            .pool
            .get()
            .await
            .map_err(|error| Error::Database(Box::new(error)))?;
        query(&format!(
            "SELECT {projection}, {lengths} FROM proof WHERE y IN (:ys) ORDER BY y"
        ))?
        .bind_vec("ys", ys.iter().map(|y| y.to_bytes().to_vec()).collect())?
        .bind("limit", LIMIT as i64)
        .fetch_all(&*conn)
        .await?
        .into_iter()
        .map(|mut row| {
            let bytes: u64 = column_as_number!(row.pop().ok_or(Error::InvalidDbResponse)?);
            if bytes > LIMIT {
                return Err(Error::ProofPageTooLarge {
                    bytes,
                    limit: LIMIT,
                });
            }
            let created_by_operation =
                proofs::optional_uuid(row.pop().ok_or(Error::InvalidDbResponse)?)?;
            let used_by_operation =
                proofs::optional_uuid(row.pop().ok_or(Error::InvalidDbResponse)?)?;
            let Some(Column::Blob(y)) = row.pop() else {
                return Err(Error::InvalidDbResponse);
            };
            let y = PublicKey::from_slice(&y).map_err(|_| Error::InvalidDbResponse)?;
            Ok(ProofOperationOwners {
                y,
                used_by_operation,
                created_by_operation,
            })
        })
        .collect()
    }
}
