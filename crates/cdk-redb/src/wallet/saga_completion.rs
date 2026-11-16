//! Receipt publication and exact parent transition under one writer lock.
use cdk_common::database;
use cdk_common::database::wallet::{merge_saga_receipt, validate_saga_completion};
use cdk_common::wallet::{Transaction, WalletSaga};
use redb::ReadableTable;

use super::{WalletRedbDatabase, SAGAS_TABLE, TRANSACTIONS_TABLE};
use crate::error::Error;

impl WalletRedbDatabase {
    pub(super) fn commit_saga_receipt(
        &self,
        expected: &WalletSaga,
        next: WalletSaga,
        receipt: Transaction,
    ) -> Result<bool, database::Error> {
        validate_saga_completion(expected, &next, &receipt)?;
        let tx = self.db.begin_write().map_err(Error::from)?;
        {
            let mut sagas = tx.open_table(SAGAS_TABLE).map_err(Error::from)?;
            let id = expected.id.to_string();
            let current: Option<WalletSaga> = sagas
                .get(id.as_str())
                .map_err(Error::from)?
                .map(|v| serde_json::from_str(v.value()))
                .transpose()?;
            if current.as_ref() != Some(expected) {
                return Ok(false);
            }
            if !store_receipt(&tx, receipt)? {
                return Ok(false);
            }
            sagas
                .insert(id.as_str(), serde_json::to_string(&next)?.as_str())
                .map_err(Error::from)?;
        }
        tx.commit().map_err(Error::from)?;
        Ok(true)
    }
}

/// Apply receipt conflict rules under the caller's writer lock.
pub(super) fn store_receipt(
    tx: &redb::WriteTransaction,
    receipt: Transaction,
) -> Result<bool, database::Error> {
    let mut transactions = tx.open_table(TRANSACTIONS_TABLE).map_err(Error::from)?;
    let current: Option<Transaction> = transactions
        .get(receipt.id().as_slice())
        .map_err(Error::from)?
        .map(|v| serde_json::from_str(v.value()))
        .transpose()?;
    let Some(receipt) = merge_saga_receipt(current, receipt) else {
        return Ok(false);
    };
    transactions
        .insert(
            receipt.id().as_slice(),
            serde_json::to_string(&receipt)?.as_str(),
        )
        .map_err(Error::from)?;
    Ok(true)
}
