//! Coin identities retained by recovery, independent of mutable row ownership.
use std::collections::HashSet;

use crate::database::wallet::ProofChange;
use crate::nuts::{PreMintSecrets, PreSwap, Proof, PublicKey, Token};
use crate::wallet::{
    OperationData, ProofInfo, SendSagaState, SwapOutputs, WalletSaga, WalletSagaState,
};
use crate::Error;

impl WalletSaga {
    /// All coin identities named by this saved recovery operation.
    ///
    /// This includes original/acquired inputs, output secrets, verified results
    /// and receipts. Missing checkpoints or inconsistent operation kinds fail
    /// closed. It does not validate or release financial authority. Callers must
    /// bound the input record and serialize writers when using it for collection.
    pub fn referenced_proof_ys(&self) -> Result<HashSet<PublicKey>, Error> {
        if self.kind != self.state.kind() || self.kind != self.data.kind() {
            return Err(Error::InvalidOperationState);
        }
        let mut refs = References::default();
        match &self.data {
            OperationData::Send(data) => {
                let complete = match self.state {
                    WalletSagaState::Send(
                        SendSagaState::PreparedAcquiring
                        | SendSagaState::ProofsReserved
                        | SendSagaState::PreparedCancelling
                        | SendSagaState::PreparedCancelled,
                    ) => data
                        .preparation
                        .as_ref()
                        .is_some_and(|p| !p.inputs.is_empty()),
                    WalletSagaState::Send(SendSagaState::Confirming) => data.confirmation.is_some(),
                    WalletSagaState::Send(SendSagaState::TokenCreated | SendSagaState::Claimed) => {
                        data.proofs.as_ref().is_some_and(|p| !p.is_empty()) && data.token.is_some()
                    }
                    WalletSagaState::Send(
                        SendSagaState::ReclaimPreparing
                        | SendSagaState::ReclaimCancelling
                        | SendSagaState::Reclaiming
                        | SendSagaState::Reclaimed,
                    ) => data.reclaim.is_some(),
                    _ => false,
                };
                if !complete {
                    return Err(Error::InvalidOperationState);
                }
                if let Some(preparation) = &data.preparation {
                    refs.coins(&preparation.inputs)?;
                }
                if let Some(confirmation) = &data.confirmation {
                    if let Some(swap) = &confirmation.pre_swap {
                        refs.swap(swap)?;
                    }
                    refs.proofs(&confirmation.retained_proofs)?;
                    if let Some(outputs) = &confirmation.ready {
                        refs.outputs(outputs)?;
                    }
                }
                if let Some(reclaim) = &data.reclaim {
                    refs.coins(&reclaim.inputs)?;
                    refs.swap(&reclaim.pre_swap)?;
                    refs.ys.extend(&reclaim.transaction.ys);
                    if let Some(outputs) = &reclaim.outputs {
                        refs.coins(outputs)?;
                    }
                }
                if let Some(proofs) = &data.proofs {
                    refs.proofs(proofs)?;
                }
                if let Some(token) = &data.token {
                    refs.token(token)?;
                }
            }
            OperationData::Receive(data) => {
                let checkpoint = data
                    .checkpoint
                    .as_ref()
                    .ok_or(Error::InvalidOperationState)?;
                refs.changes(&checkpoint.inputs)?;
                if let Some(swap) = &checkpoint.pre_swap {
                    refs.swap(swap)?;
                }
                refs.ys.extend(&checkpoint.transaction.ys);
                if let Some(outputs) = &checkpoint.outputs {
                    refs.coins(outputs)?;
                }
            }
            OperationData::Swap(data) => {
                let checkpoint = data
                    .checkpoint
                    .as_ref()
                    .ok_or(Error::InvalidOperationState)?;
                refs.changes(&checkpoint.acquisition)?;
                refs.swap(&checkpoint.pre_swap)?;
                if let Some(outputs) = &checkpoint.ready {
                    refs.outputs(outputs)?;
                }
            }
            OperationData::Mint(data) => {
                let checkpoint = data
                    .checkpoint
                    .as_ref()
                    .ok_or(Error::InvalidOperationState)?;
                refs.secrets(&checkpoint.premint_secrets)?;
            }
            OperationData::Melt(data) => {
                refs.changes(
                    data.acquisition
                        .as_ref()
                        .ok_or(Error::InvalidOperationState)?,
                )?;
                if let Some(swap) = &data.swap {
                    refs.coins(&swap.inputs)?;
                    refs.swap(&swap.pre_swap)?;
                    if let Some(outputs) = &swap.outputs {
                        refs.outputs(outputs)?;
                    }
                }
                if let Some(payment) = &data.payment {
                    refs.coins(&payment.inputs)?;
                    refs.ys.extend(&payment.receipt.ys);
                    if let Some(change) = &payment.change {
                        refs.coins(change)?;
                    }
                }
                if let Some(ys) = &data.final_proof_ys {
                    refs.ys.extend(ys);
                }
            }
        }
        Ok(refs.ys)
    }
}

#[derive(Default)]
struct References {
    ys: HashSet<PublicKey>,
}

impl References {
    fn proofs(&mut self, proofs: &[Proof]) -> Result<(), Error> {
        for proof in proofs {
            self.ys.insert(proof.y()?);
        }
        Ok(())
    }

    fn coins(&mut self, coins: &[ProofInfo]) -> Result<(), Error> {
        for coin in coins {
            self.ys.insert(coin.y);
            self.proofs(std::slice::from_ref(&coin.proof))?;
        }
        Ok(())
    }

    fn changes(&mut self, changes: &[ProofChange]) -> Result<(), Error> {
        for change in changes {
            self.ys.insert(change.y);
            for coin in change.before.iter().chain(&change.after) {
                self.coins(std::slice::from_ref(coin))?;
            }
        }
        Ok(())
    }

    fn secrets(&mut self, secrets: &PreMintSecrets) -> Result<(), Error> {
        for secret in &secrets.secrets {
            self.ys
                .insert(cashu::dhke::hash_to_curve(secret.secret.as_bytes())?);
        }
        Ok(())
    }

    fn swap(&mut self, swap: &PreSwap) -> Result<(), Error> {
        self.proofs(swap.swap_request.inputs())?;
        self.secrets(&swap.pre_mint_secrets)
    }

    fn outputs(&mut self, outputs: &SwapOutputs) -> Result<(), Error> {
        self.proofs(&outputs.send)?;
        self.proofs(&outputs.change)
    }

    fn token(&mut self, token: &str) -> Result<(), Error> {
        // Coin identity depends only on the secret, not cached mint keysets.
        match token.parse::<Token>()? {
            Token::TokenV3(token) => {
                for proof in token.token.into_iter().flat_map(|t| t.proofs) {
                    self.ys
                        .insert(cashu::dhke::hash_to_curve(proof.secret.as_bytes())?);
                }
            }
            Token::TokenV4(token) => {
                for proof in token.token.into_iter().flat_map(|t| t.proofs) {
                    self.ys
                        .insert(cashu::dhke::hash_to_curve(proof.secret.as_bytes())?);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
