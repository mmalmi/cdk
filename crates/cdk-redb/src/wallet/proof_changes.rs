use cdk_common::database;
use cdk_common::database::wallet::{validate_proof_changes, ProofChange};
use cdk_common::wallet::{ProofInfo, WalletSaga};
use redb::ReadableTable;

use super::{WalletRedbDatabase, PROOFS_TABLE, SAGAS_TABLE};
use crate::error::Error;

impl WalletRedbDatabase {
    pub(super) fn apply_proof_changes(
        &self,
        changes: Vec<ProofChange>,
        expected_saga: Option<&WalletSaga>,
    ) -> Result<bool, database::Error> {
        validate_proof_changes(&changes)?;
        if changes.is_empty() && expected_saga.is_none() {
            return Ok(true);
        }
        let tx = self.db.begin_write().map_err(Error::from)?;
        if let Some(expected) = expected_saga {
            let sagas = tx.open_table(SAGAS_TABLE).map_err(Error::from)?;
            let current: Option<WalletSaga> = sagas
                .get(expected.id.to_string().as_str())
                .map_err(Error::from)?
                .map(|value| serde_json::from_str(value.value()))
                .transpose()?;
            if current.as_ref() != Some(expected) {
                return Ok(false);
            }
        }
        if !apply(&tx, changes)? {
            return Ok(false);
        }
        tx.commit().map_err(Error::from)?;
        Ok(true)
    }
}

pub(super) fn apply(
    tx: &redb::WriteTransaction,
    changes: Vec<ProofChange>,
) -> Result<bool, database::Error> {
    {
        let mut table = tx.open_table(PROOFS_TABLE).map_err(Error::from)?;
        for change in &changes {
            let existing: Option<ProofInfo> = table
                .get(change.y.to_bytes().as_slice())
                .map_err(Error::from)?
                .map(|value| serde_json::from_str(value.value()))
                .transpose()?;
            if existing != change.before {
                return Ok(false);
            }
        }
        for change in changes {
            match change.after {
                Some(proof) => {
                    table
                        .insert(
                            change.y.to_bytes().as_slice(),
                            serde_json::to_string(&proof)?.as_str(),
                        )
                        .map_err(Error::from)?;
                }
                None => {
                    table
                        .remove(change.y.to_bytes().as_slice())
                        .map_err(Error::from)?;
                }
            }
        }
    }
    Ok(true)
}
