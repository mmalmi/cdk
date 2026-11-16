//! Admit complete recovery batches before their saved requests can spend.
use std::collections::HashMap;

use cdk_common::database::wallet::ProofChange;
use cdk_common::database::Error;
use cdk_common::wallet::{OperationData, ProofInfo, WalletSaga};
use cdk_common::{PreMintSecrets, PreSwap, Proof, PublicKey, State};

use super::{statement, COLUMNS};
use crate::wallet::{query_budget, storage_capacity};

pub(in crate::wallet) fn check(saga: &WalletSaga, limit: u32) -> Result<(), Error> {
    let mut batch = Batch {
        saga,
        limit,
        rows: HashMap::new(),
        bytes: 0,
    };
    match &saga.data {
        OperationData::Send(data) => {
            if let Some(saved) = &data.preparation {
                batch.coins(&saved.inputs)?;
            }
            if let Some(saved) = &data.confirmation {
                batch.proofs(&saved.retained_proofs)?;
                if let Some(swap) = &saved.pre_swap {
                    batch.swap(swap)?;
                }
                if let Some(ready) = &saved.ready {
                    batch.proofs(&ready.send)?;
                    batch.proofs(&ready.change)?;
                }
            }
            if let Some(saved) = &data.reclaim {
                batch.coins(&saved.inputs)?;
                batch.swap(&saved.pre_swap)?;
                if let Some(outputs) = &saved.outputs {
                    batch.coins(outputs)?;
                }
            }
            if let Some(proofs) = &data.proofs {
                batch.proofs(proofs)?;
            }
        }
        OperationData::Receive(data) => {
            if let Some(saved) = &data.checkpoint {
                batch.changes(&saved.inputs)?;
                if let Some(swap) = &saved.pre_swap {
                    batch.swap(swap)?;
                }
                if let Some(outputs) = &saved.outputs {
                    batch.coins(outputs)?;
                }
            }
        }
        OperationData::Swap(data) => {
            if let Some(saved) = &data.checkpoint {
                batch.changes(&saved.acquisition)?;
                batch.swap(&saved.pre_swap)?;
                if let Some(ready) = &saved.ready {
                    batch.proofs(&ready.send)?;
                    batch.proofs(&ready.change)?;
                }
            }
        }
        OperationData::Mint(data) => {
            if let Some(saved) = &data.checkpoint {
                batch.outputs(&saved.premint_secrets)?;
            }
        }
        OperationData::Melt(data) => {
            if let Some(inputs) = &data.acquisition {
                batch.changes(inputs)?;
            }
            if let Some(swap) = &data.swap {
                batch.coins(&swap.inputs)?;
                batch.swap(&swap.pre_swap)?;
                if let Some(outputs) = &swap.outputs {
                    batch.proofs(&outputs.send)?;
                    batch.proofs(&outputs.change)?;
                }
            }
            if let Some(payment) = &data.payment {
                batch.coins(&payment.inputs)?;
                match &payment.change {
                    Some(change) => batch.coins(change)?,
                    None => {
                        for (index, coin) in storage_capacity::melt_completion_record(saga)?
                            .1
                            .into_iter()
                            .enumerate()
                        {
                            // Change sizing reuses one placeholder secret. Each
                            // blinded output still reserves a distinct future row.
                            batch.row(Row::Change(index), coin)?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

struct Batch<'a> {
    saga: &'a WalletSaga,
    limit: u32,
    rows: HashMap<Row, u64>,
    bytes: u64,
}

#[derive(PartialEq, Eq, Hash)]
enum Row {
    Proof(PublicKey),
    Change(usize),
}

impl Batch<'_> {
    fn coin(&mut self, coin: ProofInfo) -> Result<(), Error> {
        self.row(Row::Proof(coin.y), coin)
    }

    fn row(&mut self, key: Row, mut coin: ProofInfo) -> Result<(), Error> {
        // A saved identity may appear in several stages. Charge its largest
        // encoded row once, including later status and fixed-width provenance.
        // These sizing copies are never written or sent to a mint.
        coin.state = State::PendingSpent;
        coin.used_by_operation = Some(self.saga.id);
        coin.created_by_operation = Some(self.saga.id);
        coin.derivation_index = Some(u32::MAX);
        let bytes = query_budget::encoded_payload_bytes(&COLUMNS, &statement(coin, false)?, 16)?;
        let retained = self.rows.entry(key).or_default();
        self.bytes = self
            .bytes
            .checked_add(bytes.saturating_sub(*retained))
            .ok_or(Error::AmountOverflow)?;
        *retained = (*retained).max(bytes);
        if self.bytes > u64::from(self.limit) {
            return Err(Error::ProofInventoryTooLarge {
                proofs: self.rows.len() as u64,
                bytes: self.bytes,
                max_proofs: self.limit / 16,
                max_bytes: self.limit,
            });
        }
        Ok(())
    }

    fn coins(&mut self, coins: &[ProofInfo]) -> Result<(), Error> {
        for coin in coins {
            self.coin(coin.clone())?;
        }
        Ok(())
    }

    fn changes(&mut self, changes: &[ProofChange]) -> Result<(), Error> {
        for change in changes {
            for coin in change.before.iter().chain(&change.after) {
                self.coin(coin.clone())?;
            }
        }
        Ok(())
    }

    fn proofs(&mut self, proofs: &[Proof]) -> Result<(), Error> {
        for proof in proofs {
            self.coin(
                ProofInfo::new(
                    proof.clone(),
                    self.saga.mint_url.clone(),
                    State::Reserved,
                    self.saga.unit.clone(),
                )
                .map_err(|error| Error::Internal(error.to_string()))?,
            )?;
        }
        Ok(())
    }

    fn outputs(&mut self, secrets: &PreMintSecrets) -> Result<(), Error> {
        for secret in &secrets.secrets {
            self.coin(storage_capacity::projected_proof(
                self.saga,
                secret,
                State::Reserved,
            )?)?;
        }
        Ok(())
    }

    fn swap(&mut self, swap: &PreSwap) -> Result<(), Error> {
        self.proofs(swap.swap_request.inputs())?;
        self.outputs(&swap.pre_mint_secrets)
    }
}
