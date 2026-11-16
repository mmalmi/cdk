//! Issue cleanup under one native write transaction and an exact journal fence.
use cdk_common::database;
use cdk_common::database::wallet::{validate_issue_retirement, ProofChange};
use cdk_common::wallet::{MintQuote, Transaction, WalletSaga};
use redb::ReadableTable;

use super::{
    proof_changes, WalletRedbDatabase, MINT_QUOTES_TABLE, SAGAS_TABLE, TRANSACTIONS_TABLE,
};
use crate::error::Error;

impl WalletRedbDatabase {
    pub(super) fn retire_issue_transaction(
        &self,
        expected: &WalletSaga,
        originals: &[MintQuote],
        changes: Option<Vec<ProofChange>>,
    ) -> Result<bool, database::Error> {
        let plan = validate_issue_retirement(expected, originals, changes.as_deref())?;
        let tx = self.db.begin_write().map_err(Error::from)?;
        {
            let mut sagas = tx.open_table(SAGAS_TABLE).map_err(Error::from)?;
            let mut quotes = tx.open_table(MINT_QUOTES_TABLE).map_err(Error::from)?;
            let mut receipts = tx.open_table(TRANSACTIONS_TABLE).map_err(Error::from)?;
            let id = expected.id.to_string();
            let current: Option<WalletSaga> = sagas
                .get(id.as_str())
                .map_err(Error::from)?
                .map(|v| serde_json::from_str(v.value()))
                .transpose()?;
            if current.as_ref() != Some(expected) {
                return Ok(false);
            }
            for original in originals {
                let current: Option<MintQuote> = quotes
                    .get(original.id.as_str())
                    .map_err(Error::from)?
                    .map(|v| serde_json::from_str(v.value()))
                    .transpose()?;
                if current.as_ref() != Some(original) {
                    return Ok(false);
                }
            }
            let current: Vec<Option<Transaction>> = plan
                .original_receipts()
                .iter()
                .map(|receipt| {
                    receipts
                        .get(receipt.id().as_slice())
                        .map_err(Error::from)?
                        .map(|v| serde_json::from_str(v.value()))
                        .transpose()
                        .map_err(database::Error::from)
                })
                .collect::<Result<_, database::Error>>()?;
            let Some(updates) = plan.receipts(current) else {
                return Ok(false);
            };
            if let Some(changes) = changes {
                if !proof_changes::apply(&tx, changes)? {
                    return Ok(false);
                }
            }
            for receipt in updates {
                receipts
                    .insert(
                        receipt.id().as_slice(),
                        serde_json::to_string(&receipt)?.as_str(),
                    )
                    .map_err(Error::from)?;
            }
            for quote in plan.quotes {
                quotes
                    .insert(quote.id.as_str(), serde_json::to_string(&quote)?.as_str())
                    .map_err(Error::from)?;
            }
            sagas.remove(id.as_str()).map_err(Error::from)?;
        }
        tx.commit().map_err(Error::from)?;
        Ok(true)
    }
}
