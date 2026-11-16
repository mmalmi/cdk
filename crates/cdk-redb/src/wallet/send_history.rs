//! Exact token history removal under the database's writer transaction.
use std::collections::HashMap;

use cdk_common::database;
use cdk_common::database::wallet::{token_history_inputs_spent, validate_token_history_retirement};
use cdk_common::wallet::{ProofInfo, Transaction, TransactionDirection};
use redb::ReadableTable;

use super::{WalletRedbDatabase, PROOFS_TABLE, SAGAS_TABLE, TRANSACTIONS_TABLE};
use crate::error::Error;

impl WalletRedbDatabase {
    pub(super) fn retire_token_history(
        &self,
        expected: Vec<Transaction>,
        direction: TransactionDirection,
    ) -> Result<bool, database::Error> {
        validate_token_history_retirement(&expected, direction)?;
        if expected.is_empty() {
            return Ok(true);
        }
        let tx = self.db.begin_write().map_err(Error::from)?;
        {
            let mut transactions = tx.open_table(TRANSACTIONS_TABLE).map_err(Error::from)?;
            let sagas = tx.open_table(SAGAS_TABLE).map_err(Error::from)?;
            let proofs = tx.open_table(PROOFS_TABLE).map_err(Error::from)?;
            let mut inputs = HashMap::new();
            for original in &expected {
                let current: Option<Transaction> = transactions
                    .get(original.id().as_slice())
                    .map_err(Error::from)?
                    .map(|v| serde_json::from_str(v.value()))
                    .transpose()?;
                if current.as_ref() != Some(original) {
                    return Ok(false);
                }
                let operation = original.saga_id.ok_or_else(|| {
                    database::Error::Internal("Token operation missing".to_owned())
                })?;
                if sagas
                    .get(operation.to_string().as_str())
                    .map_err(Error::from)?
                    .is_some()
                {
                    return Ok(false);
                }
                for y in &original.ys {
                    let current: Option<ProofInfo> = proofs
                        .get(y.to_bytes().as_slice())
                        .map_err(Error::from)?
                        .map(|v| serde_json::from_str(v.value()))
                        .transpose()?;
                    if let Some(proof) = current {
                        inputs.insert(*y, proof);
                    }
                }
                if !token_history_inputs_spent(original, &inputs, direction) {
                    return Ok(false);
                }
            }
            for original in expected {
                transactions
                    .remove(original.id().as_slice())
                    .map_err(Error::from)?;
            }
        }
        tx.commit().map_err(Error::from)?;
        Ok(true)
    }
}
