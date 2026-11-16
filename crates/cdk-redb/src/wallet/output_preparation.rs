//! Commit an output proposal, its derivation range and parent under one writer.
use cdk_common::database;
use cdk_common::database::wallet::validate_output_preparation;
use cdk_common::wallet::{Transaction, WalletSaga};
use cdk_common::{KeySet, KeySetInfo, Keys};
use redb::ReadableTable;

use super::{
    WalletRedbDatabase, KEYSETS_TABLE, KEYSET_COUNTER, KEYSET_U32_MAPPING, MINT_KEYSETS_TABLE,
    MINT_KEYS_TABLE, SAGAS_TABLE,
};
use crate::error::Error;

impl WalletRedbDatabase {
    pub(super) fn commit_output_preparation(
        &self,
        expected: Option<&WalletSaga>,
        next: WalletSaga,
        keyset: KeySet,
        counter_start: u32,
        counter_count: u32,
        receipt: Option<Transaction>,
    ) -> Result<bool, database::Error> {
        validate_output_preparation(expected, &next, receipt.as_ref())?;
        let tx = self.db.begin_write().map_err(Error::from)?;
        {
            let id = next.id.to_string();
            let mut sagas = tx.open_table(SAGAS_TABLE).map_err(Error::from)?;
            let current: Option<WalletSaga> = sagas
                .get(id.as_str())
                .map_err(Error::from)?
                .map(|v| serde_json::from_str(v.value()))
                .transpose()?;
            if current.as_ref() != expected {
                return Ok(false);
            }
            if !prepare_range(&tx, &next, keyset, counter_start, counter_count)? {
                return Ok(false);
            }
            if let Some(receipt) = receipt {
                if !super::saga_completion::store_receipt(&tx, receipt)? {
                    return Ok(false);
                }
            }
            sagas
                .insert(id.as_str(), serde_json::to_string(&next)?.as_str())
                .map_err(Error::from)?;
        }
        tx.commit().map_err(Error::from)?;
        Ok(true)
    }
}

pub(super) fn prepare_range(
    tx: &redb::WriteTransaction,
    next: &WalletSaga,
    keyset: KeySet,
    counter_start: u32,
    counter_count: u32,
) -> Result<bool, database::Error> {
    keyset.verify_id()?;
    if keyset.unit != next.unit {
        return Err(database::Error::InvalidDbResponse);
    }
    let end = counter_start
        .checked_add(counter_count)
        .ok_or(database::Error::AmountOverflow)?;
    let key = keyset.id.to_string();
    let mut counters = tx.open_table(KEYSET_COUNTER).map_err(Error::from)?;
    let current = counters
        .get(key.as_str())
        .map_err(Error::from)?
        .map(|v| v.value())
        .unwrap_or(0);
    if current != counter_start {
        return Ok(false);
    }
    let mut keys = tx.open_table(MINT_KEYS_TABLE).map_err(Error::from)?;
    let current: Option<Keys> = keys
        .get(key.as_str())
        .map_err(Error::from)?
        .map(|v| serde_json::from_str(v.value()))
        .transpose()?;
    if current.as_ref().is_some_and(|keys| *keys != keyset.keys) {
        return Err(database::Error::InvalidDbResponse);
    }
    let mut mapping = tx.open_table(KEYSET_U32_MAPPING).map_err(Error::from)?;
    if mapping
        .get(u32::from(keyset.id))
        .map_err(Error::from)?
        .is_some_and(|v| v.value() != key)
    {
        return Err(database::Error::Duplicate);
    }
    mapping
        .insert(u32::from(keyset.id), key.as_str())
        .map_err(Error::from)?;
    let info = KeySetInfo {
        id: keyset.id,
        unit: keyset.unit.clone(),
        active: keyset.active.unwrap_or(false),
        input_fee_ppk: keyset.input_fee_ppk,
        final_expiry: keyset.final_expiry,
    };
    tx.open_table(KEYSETS_TABLE)
        .map_err(Error::from)?
        .insert(
            keyset.id.to_bytes().as_slice(),
            serde_json::to_string(&info)?.as_str(),
        )
        .map_err(Error::from)?;
    tx.open_multimap_table(MINT_KEYSETS_TABLE)
        .map_err(Error::from)?
        .insert(
            next.mint_url.to_string().as_str(),
            keyset.id.to_bytes().as_slice(),
        )
        .map_err(Error::from)?;
    keys.insert(key.as_str(), serde_json::to_string(&keyset.keys)?.as_str())
        .map_err(Error::from)?;
    counters.insert(key.as_str(), end).map_err(Error::from)?;
    Ok(true)
}
