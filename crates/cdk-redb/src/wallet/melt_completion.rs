//! Paid publication under one native write transaction.
use cdk_common::database;
use cdk_common::database::wallet::validate_melt_completion;
use cdk_common::wallet::{MeltQuote, Transaction, WalletSaga};
use redb::ReadableTable;

use super::{WalletRedbDatabase, MELT_QUOTES_TABLE, SAGAS_TABLE, TRANSACTIONS_TABLE};
use crate::error::Error;

impl WalletRedbDatabase {
    pub(super) fn complete_melt_transaction(
        &self,
        expected: &WalletSaga,
        next: WalletSaga,
        quote: &MeltQuote,
    ) -> Result<bool, database::Error> {
        let result = validate_melt_completion(expected, &next, quote)?;
        let tx = self.db.begin_write().map_err(Error::from)?;
        {
            let mut sagas = tx.open_table(SAGAS_TABLE).map_err(Error::from)?;
            let mut quotes = tx.open_table(MELT_QUOTES_TABLE).map_err(Error::from)?;
            let mut receipts = tx.open_table(TRANSACTIONS_TABLE).map_err(Error::from)?;
            let id = expected.id.to_string();
            let current_saga: Option<WalletSaga> = sagas
                .get(id.as_str())
                .map_err(Error::from)?
                .map(|v| serde_json::from_str(v.value()))
                .transpose()?;
            let current_quote: Option<MeltQuote> = quotes
                .get(quote.id.as_str())
                .map_err(Error::from)?
                .map(|v| serde_json::from_str(v.value()))
                .transpose()?;
            let current_receipt: Option<Transaction> = receipts
                .get(result.pending.id().as_slice())
                .map_err(Error::from)?
                .map(|v| serde_json::from_str(v.value()))
                .transpose()?;
            if current_saga.as_ref() != Some(expected)
                || current_quote.as_ref() != Some(quote)
                || current_receipt.as_ref() != Some(&result.pending)
            {
                return Ok(false);
            }
            receipts
                .insert(
                    result.receipt.id().as_slice(),
                    serde_json::to_string(&result.receipt)?.as_str(),
                )
                .map_err(Error::from)?;
            quotes
                .insert(
                    quote.id.as_str(),
                    serde_json::to_string(&result.quote)?.as_str(),
                )
                .map_err(Error::from)?;
            sagas
                .insert(id.as_str(), serde_json::to_string(&next)?.as_str())
                .map_err(Error::from)?;
        }
        tx.commit().map_err(Error::from)?;
        Ok(true)
    }
}
