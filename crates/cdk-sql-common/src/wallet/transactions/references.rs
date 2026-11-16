//! Ownership references without decoding unrelated receipt payloads.
use cdk_common::database::wallet::{TransactionCursor, MAX_TRANSACTION_PAGE_BYTES};
use cdk_common::database::Error;

use super::page;
use crate::pool::DatabasePool;
use crate::stmt::Column;
use crate::wallet::SQLWalletDatabase;

/// Stored receipt position and framed proof references, without curve validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionProofReferences {
    /// Exclusive continuation, derived from the last visited stored row.
    pub cursor: TransactionCursor,
    /// Compressed-key encodings; these bytes do not establish valid curve points.
    pub ys: Vec<[u8; 33]>,
}

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    /// Read up to 128 globally ordered receipt references within the whole
    /// receipt page's 4-MiB stored-byte budget, including nonmatching records.
    /// Oversized fields never cross the driver even though only references are
    /// projected. Cursor types and compressed-key framing must be valid.
    ///
    /// This ownership-only read does not validate unrelated metadata, curve
    /// points or canonical receipt IDs. A caller can establish that a valid Y
    /// is absent by comparing its bytes. Any match still requires the unchanged
    /// full receipt read and validation before using its ownership evidence.
    /// Full receipt APIs retain their existing corruption checks. This does not
    /// grant release authority or hold a snapshot across calls; callers must
    /// fence changes through full validation and the eventual financial write.
    #[tracing::instrument(skip_all)]
    pub async fn get_transaction_proof_references_page(
        &self,
        after: Option<TransactionCursor>,
        limit: u16,
    ) -> Result<Vec<TransactionProofReferences>, Error> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|error| Error::Database(Box::new(error)))?;
        page::load_page_rows(
            &*conn,
            None,
            None,
            None,
            after,
            limit,
            &["id", "timestamp", "ys"],
        )
        .await?
        .into_iter()
        .map(decode)
        .collect()
    }
}

fn decode(mut row: Vec<Column>) -> Result<TransactionProofReferences, Error> {
    page::check_budget(&mut row, MAX_TRANSACTION_PAGE_BYTES)?;
    if row.len() != 3 {
        return Err(Error::InvalidDbResponse);
    }
    let cursor = page::stored_cursor(row.first(), row.get(1))?;
    let ys = match row.pop() {
        Some(Column::Blob(ys)) if ys.len() % 33 == 0 => ys,
        _ => {
            return Err(Error::Internal(
                "Invalid transaction proof reference framing".to_owned(),
            ))
        }
    };
    let ys = ys
        .chunks_exact(33)
        .map(|bytes| {
            if !matches!(bytes[0], 2 | 3) {
                return Err(Error::Internal(
                    "Invalid transaction proof reference framing".to_owned(),
                ));
            }
            bytes.try_into().map_err(|_| Error::InvalidDbResponse)
        })
        .collect::<Result<_, _>>()?;
    Ok(TransactionProofReferences { cursor, ys })
}
