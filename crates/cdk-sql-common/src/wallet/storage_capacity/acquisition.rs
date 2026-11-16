//! Claim input storage under the same transaction as its financial acquisition.
use std::collections::HashMap;

use cdk_common::database::wallet::ProofChange;
use cdk_common::database::Error;
use cdk_common::wallet::{ReceiveSagaState, SendSagaState, WalletSaga, WalletSagaState};

use super::{claim_rows, schema, WalletStorageClaim};
use crate::database::DatabaseExecutor;
use crate::stmt::{query, Column};

pub(super) fn cancellation_growth(state: &WalletSagaState) -> Result<Option<i64>, Error> {
    let cancelled = match state {
        WalletSagaState::Send(SendSagaState::PreparedAcquiring) => {
            WalletSagaState::Send(SendSagaState::PreparedCancelling)
        }
        WalletSagaState::Receive(ReceiveSagaState::InputsRecorded) => {
            WalletSagaState::Receive(ReceiveSagaState::Cancelling)
        }
        _ => return Ok(None),
    };
    let growth = serde_json::to_string(&cancelled)?
        .len()
        .saturating_sub(serde_json::to_string(state)?.len());
    Ok(Some(
        i64::try_from(growth).map_err(|_| Error::AmountOverflow)?,
    ))
}

pub(in crate::wallet) struct Acquisition {
    owner: String,
    reserved: i64,
    state_growth: i64,
    inputs: HashMap<String, i64>,
}

impl Acquisition {
    /// Called only after exact saga and proof comparisons, inside a write transaction.
    pub(in crate::wallet) async fn begin<T: DatabaseExecutor>(
        conn: &T,
        saga: Option<&WalletSaga>,
        changes: &[ProofChange],
    ) -> Result<Option<Self>, Error> {
        let Some(saga) = saga else { return Ok(None) };
        let Some(state_growth) = cancellation_growth(&saga.state)? else {
            return Ok(None);
        };
        if !schema::enabled(conn).await? {
            return Ok(None);
        }
        let claims = changes
            .iter()
            .filter(|c| {
                c.after
                    .as_ref()
                    .is_some_and(|p| p.used_by_operation == Some(saga.id))
            })
            .map(|c| WalletStorageClaim {
                table: "proof".to_owned(),
                primary_key: vec![c.y.to_bytes().to_vec()],
            })
            .collect::<Vec<_>>();
        if claims.is_empty() {
            return Ok(None);
        }
        let owner = saga.id.to_string();
        let row = query("SELECT reserved FROM wallet_storage_accounts WHERE owner=:owner")?
            .bind("owner", owner.clone())
            .fetch_one(conn)
            .await?
            .ok_or(Error::InvalidDbResponse)?;
        let [Column::Integer(reserved)] = row.as_slice() else {
            return Err(Error::InvalidDbResponse);
        };
        // Reuse already stored bytes before charging the final allowance. Any
        // failure rolls back every claim, proof write and reservation together.
        query("UPDATE wallet_storage_accounts SET reserved=0 WHERE owner=:owner")?
            .bind("owner", owner.clone())
            .execute(conn)
            .await?;
        let keys = claim_rows(conn, &owner, &claims).await?;
        let inputs = charges(conn, keys).await?;
        Ok(Some(Self {
            owner,
            reserved: *reserved,
            state_growth,
            inputs,
        }))
    }

    pub(in crate::wallet) async fn finish<T: DatabaseExecutor>(
        self,
        conn: &T,
    ) -> Result<(), Error> {
        let current = charges(conn, self.inputs.keys().cloned().collect()).await?;
        let mut growth = self.state_growth;
        for (key, before) in self.inputs {
            let after = current.get(&key).ok_or(Error::InvalidDbResponse)?;
            growth = growth
                .checked_add((before - after).max(0))
                .ok_or(Error::AmountOverflow)?;
        }
        // Keep each input's larger before/after size: rollback can grow one row
        // before shrinking another. The ledger includes exact row and ownership
        // metadata, so this needs no second proof-size or serialization formula.
        super::reserve_growth(conn, &self.owner, self.reserved, growth).await?;
        Ok(())
    }
}

async fn charges<T: DatabaseExecutor>(
    conn: &T,
    keys: Vec<String>,
) -> Result<HashMap<String, i64>, Error> {
    let count = keys.len();
    let rows =
        query("SELECT object_key,charged FROM wallet_storage_objects WHERE object_key IN (:keys)")?
            .bind_vec("keys", keys)?
            .fetch_all(conn)
            .await?;
    let mut result = HashMap::with_capacity(count);
    for row in rows {
        let [Column::Text(key), Column::Integer(bytes)] = row.as_slice() else {
            return Err(Error::InvalidDbResponse);
        };
        result.insert(key.clone(), *bytes);
    }
    if result.len() != count {
        return Err(Error::InvalidDbResponse);
    }
    Ok(result)
}
