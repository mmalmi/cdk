//! Bound future output metadata using the production row encoders.
use cdk_common::database::Error;
use cdk_common::mint_url::MintUrl;
use cdk_common::{CurrencyUnit, Id, KeySet, KeySetInfo, Keys};
use uuid::Uuid;

use crate::pool::DatabasePool;
use crate::wallet::{keysets, SQLWalletDatabase};

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    /// Maximum charge for one future output keyset, its keys and counter.
    ///
    /// Uses the longest supported keyset ID, all optional scalar fields and
    /// `keys_json_bytes` for the serialized denomination/public-key map. The
    /// caller must enforce that bound on future keysets. This read-only helper
    /// includes row ownership metadata but excludes the owner's account row.
    /// Existing rows or shorter IDs can consume less. It covers one keyset, not
    /// an unbounded sequence of rotations or partially completed preparations.
    #[tracing::instrument(skip_all)]
    pub async fn output_keyset_storage_bytes(
        &self,
        owner: Uuid,
        mint_url: MintUrl,
        unit: CurrencyUnit,
        keys_json_bytes: u64,
    ) -> Result<u64, Error> {
        let size = self.record_sizing(owner).await?;
        let keys = Keys::new(Default::default());
        let empty = serde_json::to_vec(&keys)?.len() as u64;
        let extra = keys_json_bytes
            .checked_sub(empty)
            .ok_or(Error::AmountOverflow)?;
        let expiry = Some(i64::MAX as u64);
        // A sizing-only empty keyset gives a valid full-width ID. It is never
        // persisted, offered as mint metadata or used to derive output secrets.
        let id = Id::v2_from_data(&keys, &unit, 0, expiry);
        let info = KeySetInfo {
            id,
            unit: unit.clone(),
            active: true,
            input_fee_ppk: 0,
            final_expiry: expiry,
        };
        let keyset = KeySet {
            id,
            unit,
            active: Some(true),
            keys,
            input_fee_ppk: 0,
            final_expiry: expiry,
        };
        let key = id.to_string().into_bytes();
        let rows = [
            (
                "keyset",
                keysets::INFO_COLUMNS.as_slice(),
                keysets::info_statement(&mint_url, info)?,
            ),
            (
                "key",
                keysets::KEYS_COLUMNS.as_slice(),
                keysets::keys_statement(keyset)?,
            ),
            (
                "keyset_counter",
                keysets::COUNTER_COLUMNS.as_slice(),
                keysets::counter_statement(id)?,
            ),
        ];
        rows.into_iter()
            .try_fold(extra, |total, (table, columns, statement)| {
                let row = u64::try_from(size.charge(table, key.clone(), columns, statement)?)
                    .map_err(|_| Error::AmountOverflow)?;
                total.checked_add(row).ok_or(Error::AmountOverflow)
            })
    }
}
