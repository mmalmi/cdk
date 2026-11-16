//! Commit the initial issue intent with every quote reservation.
use cdk_common::database::wallet::{issue_receipts, validate_issue_acquisition};
use cdk_common::wallet::{MintQuote, WalletSaga};
use cdk_common::{database, KeySet};
use redb::ReadableTable;

use super::{WalletRedbDatabase, MINT_QUOTES_TABLE, SAGAS_TABLE, TRANSACTIONS_TABLE};
use crate::error::Error;

impl WalletRedbDatabase {
    pub(super) fn acquire_issue_transaction(
        &self,
        saga: &WalletSaga,
        quotes: &[MintQuote],
        keyset: KeySet,
        counter_start: u32,
        counter_count: u32,
    ) -> Result<bool, database::Error> {
        let reserved = validate_issue_acquisition(saga, quotes)?;
        let receipts = issue_receipts(saga)?;
        let tx = self.db.begin_write().map_err(Error::from)?;
        {
            let mut table = tx.open_table(MINT_QUOTES_TABLE).map_err(Error::from)?;
            let mut sagas = tx.open_table(SAGAS_TABLE).map_err(Error::from)?;
            let mut transactions = tx.open_table(TRANSACTIONS_TABLE).map_err(Error::from)?;
            if sagas
                .get(saga.id.to_string().as_str())
                .map_err(Error::from)?
                .is_some()
            {
                return Ok(false);
            }
            for quote in quotes {
                let current: Option<MintQuote> = table
                    .get(quote.id.as_str())
                    .map_err(Error::from)?
                    .map(|value| serde_json::from_str(value.value()))
                    .transpose()?;
                if current.as_ref() != Some(quote) {
                    return Ok(false);
                }
            }
            for receipt in &receipts {
                if transactions
                    .get(receipt.id().as_slice())
                    .map_err(Error::from)?
                    .is_some()
                {
                    return Ok(false);
                }
            }
            if !super::output_preparation::prepare_range(
                &tx,
                saga,
                keyset,
                counter_start,
                counter_count,
            )? {
                return Ok(false);
            }
            for quote in reserved {
                table
                    .insert(quote.id.as_str(), serde_json::to_string(&quote)?.as_str())
                    .map_err(Error::from)?;
            }
            sagas
                .insert(
                    saga.id.to_string().as_str(),
                    serde_json::to_string(saga)?.as_str(),
                )
                .map_err(Error::from)?;
            for receipt in receipts {
                transactions
                    .insert(
                        receipt.id().as_slice(),
                        serde_json::to_string(&receipt)?.as_str(),
                    )
                    .map_err(Error::from)?;
            }
        }
        tx.commit().map_err(Error::from)?;
        Ok(true)
    }
}
