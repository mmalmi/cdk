//! Native atomic quote, proof and recovery-record acquisition.
use cdk_common::database;
use cdk_common::database::wallet::validate_melt_acquisition;
use cdk_common::wallet::{MeltQuote, WalletSaga};
use redb::ReadableTable;

use super::{proof_changes, WalletRedbDatabase, MELT_QUOTES_TABLE, SAGAS_TABLE};
use crate::error::Error;

impl WalletRedbDatabase {
    pub(super) fn acquire_melt_transaction(
        &self,
        saga: &WalletSaga,
        quote: &MeltQuote,
    ) -> Result<bool, database::Error> {
        let changes = validate_melt_acquisition(saga, quote)?.to_vec();
        let tx = self.db.begin_write().map_err(Error::from)?;
        {
            let mut quotes = tx.open_table(MELT_QUOTES_TABLE).map_err(Error::from)?;
            let mut sagas = tx.open_table(SAGAS_TABLE).map_err(Error::from)?;
            let current: Option<MeltQuote> = quotes
                .get(quote.id.as_str())
                .map_err(Error::from)?
                .map(|v| serde_json::from_str(v.value()))
                .transpose()?;
            if current.as_ref() != Some(quote)
                || sagas
                    .get(saga.id.to_string().as_str())
                    .map_err(Error::from)?
                    .is_some()
            {
                return Ok(false);
            }
            if !proof_changes::apply(&tx, changes)? {
                return Ok(false);
            }
            let mut reserved = quote.clone();
            reserved.used_by_operation = Some(saga.id.to_string());
            reserved.version = quote.version.checked_add(1).ok_or_else(|| {
                database::Error::Internal("Melt quote version exhausted".to_owned())
            })?;
            quotes
                .insert(
                    quote.id.as_str(),
                    serde_json::to_string(&reserved)?.as_str(),
                )
                .map_err(Error::from)?;
            sagas
                .insert(
                    saga.id.to_string().as_str(),
                    serde_json::to_string(saga)?.as_str(),
                )
                .map_err(Error::from)?;
        }
        tx.commit().map_err(Error::from)?;
        Ok(true)
    }
}
