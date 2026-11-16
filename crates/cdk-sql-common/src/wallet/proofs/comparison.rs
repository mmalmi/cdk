//! Bound comparison reads by the exact originals already held by the caller.
use std::collections::HashMap;

use cdk_common::database::wallet::{MAX_PROOF_PAGE_BYTES, MAX_PROOF_PAGE_SIZE};
use cdk_common::database::Error;
use cdk_common::wallet::ProofInfo;
use cdk_common::PublicKey;

use super::{selected, statement, COLUMNS};
use crate::database::DatabaseExecutor;
use crate::wallet::query_budget;

/// The caller holds a write transaction across every comparison and subsequent write.
pub(in crate::wallet) async fn matches<T>(
    conn: &T,
    originals: Vec<(PublicKey, Option<&ProofInfo>)>,
) -> Result<bool, Error>
where
    T: DatabaseExecutor,
{
    let mut originals = originals.as_slice();
    while !originals.is_empty() {
        let mut count = 0;
        let mut bytes = 0u64;
        for (_, expected) in originals.iter().take(usize::from(MAX_PROOF_PAGE_SIZE)) {
            let size = expected.map(payload_bytes).transpose()?.unwrap_or(0);
            let next = bytes.checked_add(size).ok_or(Error::AmountOverflow)?;
            if count > 0 && next > MAX_PROOF_PAGE_BYTES {
                break;
            }
            bytes = next;
            count += 1;
        }
        let (page, remaining) = originals.split_at(count);
        let ys: Vec<_> = page.iter().map(|(y, _)| *y).collect();
        // One larger expected row may progress alone. Unexpected stored growth
        // is rejected before driver projection; it cannot enlarge this budget.
        let rows = selected::load_with_limit(conn, &ys, bytes.max(MAX_PROOF_PAGE_BYTES)).await?;
        let existing: HashMap<_, _> = rows.into_iter().map(|coin| (coin.y, coin)).collect();
        if page
            .iter()
            .any(|(y, expected)| existing.get(y) != *expected)
        {
            return Ok(false);
        }
        originals = remaining;
    }
    Ok(true)
}

fn payload_bytes(proof: &ProofInfo) -> Result<u64, Error> {
    // Reuse the production encoder, including derived conditions and mutable
    // witnesses. Match the SQL read budget's byte lengths and fixed overhead.
    query_budget::encoded_payload_bytes(&COLUMNS, &statement(proof.clone(), false)?, 16)
}
