//! Cancel an exact preparation before any confirmation may spend or expose its coins.
use std::collections::HashMap;

use cdk_common::database::wallet::ProofChange;
use cdk_common::wallet::{
    OperationData, SendPreparation, SendSagaState, WalletSaga, WalletSagaState,
};

use super::state::transition;
use crate::nuts::{Proof, Proofs, State};
use crate::{Amount, Error, Wallet};

fn preparation<'a>(wallet: &Wallet, saga: &'a WalletSaga) -> Result<&'a SendPreparation, Error> {
    let OperationData::Send(data) = &saga.data else {
        return Err(Error::InvalidOperationState);
    };
    let plan = data
        .preparation
        .as_deref()
        .ok_or(Error::InvalidOperationState)?;
    if saga.mint_url != wallet.mint_url
        || saga.unit != wallet.unit
        || data.amount != saga.amount
        || data.confirmation.is_some()
        || data.reclaim.is_some()
        || data.token.is_some()
        || data.proofs.is_some()
        || plan.inputs.is_empty()
        || !matches!(
            saga.state,
            WalletSagaState::Send(
                SendSagaState::PreparedAcquiring
                    | SendSagaState::ProofsReserved
                    | SendSagaState::PreparedCancelling
                    | SendSagaState::PreparedCancelled
            )
        )
    {
        return Err(Error::InvalidOperationState);
    }
    let mut seen = std::collections::HashSet::new();
    for input in &plan.inputs {
        if input.mint_url != wallet.mint_url
            || input.unit != wallet.unit
            || input.used_by_operation != Some(saga.id)
            || input.state != State::Reserved
            || input.y != input.proof.y()?
            || !seen.insert(input.y)
        {
            return Err(Error::InvalidOperationState);
        }
    }
    Ok(plan)
}

pub(in crate::wallet::send) fn validate<'a, 's, I>(
    wallet: &Wallet,
    saga: &'s WalletSaga,
    amount: Amount,
    proofs: I,
) -> Result<&'s SendPreparation, Error>
where
    I: Iterator<Item = &'a Proof>,
{
    let plan = preparation(wallet, saga)?;
    if amount != saga.amount {
        return Err(Error::InvalidOperationState);
    }
    let mut expected: HashMap<_, _> = plan.inputs.iter().map(|p| (p.y, &p.proof)).collect();
    for proof in proofs {
        if expected.remove(&proof.y()?) != Some(proof) {
            return Err(Error::InvalidOperationState);
        }
    }
    if !expected.is_empty() {
        return Err(Error::InvalidOperationState);
    }
    Ok(plan)
}

pub(in crate::wallet::send) async fn complete(
    wallet: &Wallet,
    mut saga: WalletSaga,
) -> Result<(), Error> {
    let retained = preparation(wallet, &saga)?.retain_cancellation;
    wallet.ensure_send_storage(Some(&saga)).await?;
    if matches!(
        saga.state,
        WalletSagaState::Send(SendSagaState::PreparedAcquiring | SendSagaState::ProofsReserved)
    ) {
        // Reserve both transitions before releasing inputs. Confirmation competes
        // for this same version, including sends that need no mint request.
        if saga.version > u32::MAX - 2 {
            return Err(Error::InvalidOperationState);
        }
        if saga.state == WalletSagaState::Send(SendSagaState::ProofsReserved) {
            // Completed acquisition must still own its exact original inputs.
            // Before promotion, acquisition and cancellation instead compete
            // under the saved intent; a fenced acquisition cannot reserve later.
            let originals = preparation(wallet, &saga)?
                .inputs
                .iter()
                .map(|original| ProofChange {
                    y: original.y,
                    before: Some(original.clone()),
                    after: Some(original.clone()),
                })
                .collect();
            if !wallet
                .localstore
                .compare_and_swap_proofs(originals, Some(&saga))
                .await?
            {
                return Err(Error::ConcurrentUpdate);
            }
        }
        transition(wallet, &mut saga, SendSagaState::PreparedCancelling).await?;
    }
    if saga.state == WalletSagaState::Send(SendSagaState::PreparedCancelling) {
        let inputs = &preparation(wallet, &saga)?.inputs;
        let current: HashMap<_, _> = wallet
            .localstore
            .get_proofs_by_ys(inputs.iter().map(|p| p.y).collect())
            .await?
            .into_iter()
            .map(|p| (p.y, p))
            .collect();
        let mut changes = Vec::with_capacity(inputs.len());
        for original in inputs {
            let before = current.get(&original.y);
            let after = if before == Some(original) {
                let mut released = original.clone();
                released.state = State::Unspent;
                released.used_by_operation = None;
                Some(released)
            } else {
                // Release may have committed before a crash. Preserve later use
                // and retirement, but never repair unexplained changes we still own.
                if before.is_some_and(|p| p.used_by_operation == Some(saga.id)) {
                    return Err(Error::ConcurrentUpdate);
                }
                before.cloned()
            };
            changes.push(ProofChange {
                y: original.y,
                before: before.cloned(),
                after,
            });
        }
        if !wallet
            .localstore
            .compare_and_swap_proofs(changes, Some(&saga))
            .await?
        {
            return Err(Error::ConcurrentUpdate);
        }
        transition(wallet, &mut saga, SendSagaState::PreparedCancelled).await?;
    }
    if !retained {
        // This state is terminal. No confirmation or recovery may advance it.
        wallet.localstore.delete_saga(&saga.id).await?;
    }
    Ok(())
}

impl Wallet {
    /// Cancel an exact original preparation and retain proof of zero spend.
    ///
    /// Requires `SendOptions::retain_cancellation` at preparation. Success means
    /// confirmation never acquired the operation: no token, debit or refund exists.
    /// Retries use the retained result without touching subsequently reused coins.
    /// A missing operation is an error, never evidence of successful cancellation.
    #[tracing::instrument(skip_all)]
    pub async fn cancel_prepared_send(
        &self,
        operation_id: uuid::Uuid,
        amount: Amount,
        inputs: Proofs,
    ) -> Result<(), Error> {
        let saga = self
            .localstore
            .get_saga(&operation_id)
            .await?
            .ok_or(Error::InvalidOperationState)?;
        if !validate(self, &saga, amount, inputs.iter())?.retain_cancellation {
            return Err(Error::InvalidOperationState);
        }
        complete(self, saga).await
    }

    /// Release retained cancellation evidence after durably recording its outcome.
    ///
    /// Missing evidence is accepted for acknowledgment retries only; this method
    /// does not establish cancellation. It never releases or changes wallet coins.
    #[tracing::instrument(skip_all)]
    pub async fn acknowledge_prepared_cancellation(
        &self,
        operation_id: uuid::Uuid,
        amount: Amount,
        inputs: Proofs,
    ) -> Result<(), Error> {
        let Some(saga) = self.localstore.get_saga(&operation_id).await? else {
            return Ok(());
        };
        validate(self, &saga, amount, inputs.iter())?;
        if saga.state != WalletSagaState::Send(SendSagaState::PreparedCancelled) {
            return Err(Error::InvalidOperationState);
        }
        self.ensure_send_storage(Some(&saga)).await?;
        self.localstore.delete_saga(&saga.id).await?;
        Ok(())
    }
}
