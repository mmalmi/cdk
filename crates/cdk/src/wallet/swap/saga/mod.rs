//! Save exact swap intent before acquisition; hand off verified outputs atomically.
use cdk_common::wallet::{
    OperationData, SwapCheckpoint, SwapOperationData, SwapSagaState, WalletSaga, WalletSagaState,
};
use tracing::instrument;

mod acquisition;
mod checkpoint;
mod handoff;
pub(crate) mod resume;
pub(crate) mod state;

use self::state::{Finalized, Initial, Prepared};
use crate::amount::SplitTarget;
use crate::nuts::{Proofs, SpendingConditions};
use crate::{Amount, Error, Wallet};

/// Swap typestate; durable state is authoritative after preparation.
pub(crate) struct SwapSaga<'a, S> {
    wallet: &'a Wallet,
    state_data: S,
}

impl<'a> SwapSaga<'a, Initial> {
    pub fn new(wallet: &'a Wallet) -> Self {
        Self {
            wallet,
            state_data: Initial {
                operation_id: uuid::Uuid::now_v7(),
                keyset_policy: Default::default(),
            },
        }
    }

    #[instrument(skip_all)]
    pub async fn prepare(
        self,
        amount: Option<Amount>,
        amount_split_target: SplitTarget,
        input_proofs: Proofs,
        spending_conditions: Option<SpendingConditions>,
        use_p2bk: bool,
        include_fees: bool,
    ) -> Result<SwapSaga<'a, Prepared>, Error> {
        ensure_storage(self.wallet, None).await?;
        let id = self.state_data.operation_id;
        let acquisition = acquisition::plan(self.wallet, id, &input_proofs).await?;
        let policy = self.state_data.keyset_policy;
        let keyset = self.wallet.active_keyset_with_policy(policy).await?.id;
        let fee_and_amounts = self
            .wallet
            .get_keyset_fees_and_amounts_by_id_with_policy(keyset, policy)
            .await?;
        let fee = self.wallet.get_proofs_fee(&input_proofs).await?;
        let counter_start = self
            .wallet
            .localstore
            .increment_keyset_counter(&keyset, 0)
            .await?;
        let pre_swap = self
            .wallet
            .create_swap(
                keyset,
                &fee_and_amounts,
                amount,
                amount_split_target.clone(),
                input_proofs,
                spending_conditions.clone(),
                include_fees,
                use_p2bk,
                &fee,
                counter_start,
            )
            .await?;
        let input_amount = pre_swap.swap_request.input_amount()?;
        let output_amount = pre_swap.swap_request.output_amount()?;
        let saga = WalletSaga::new(
            id,
            WalletSagaState::Swap(SwapSagaState::ProofsReserved),
            input_amount,
            self.wallet.mint_url.clone(),
            self.wallet.unit.clone(),
            OperationData::Swap(SwapOperationData {
                input_amount,
                output_amount,
                checkpoint: Some(Box::new(SwapCheckpoint {
                    acquisition,
                    pre_swap,
                    amount,
                    amount_split_target,
                    spending_conditions,
                    ready: None,
                })),
            }),
        );
        checkpoint::validate(self.wallet, &saga)?;
        self.wallet
            .commit_swap_preparation(
                None,
                &saga,
                keyset,
                counter_start,
                &checkpoint::get(&saga)?.pre_swap,
            )
            .await?;
        acquisition::apply(self.wallet, &saga).await?;
        Ok(SwapSaga {
            wallet: self.wallet,
            state_data: Prepared { saga },
        })
    }
}

impl<'a> SwapSaga<'a, Prepared> {
    #[instrument(skip_all)]
    pub async fn execute(self) -> Result<SwapSaga<'a, Finalized>, Error> {
        let mut saga = self.state_data.saga;
        checkpoint::validate(self.wallet, &saga)?;
        acquisition::check(self.wallet, &saga).await?;
        transition(self.wallet, &mut saga, SwapSagaState::SwapRequested).await?;
        // A final connector error may follow an earlier successful internal retry.
        // Retain the exact request even when the last response rejects its keyset.
        let response = self
            .wallet
            .client
            .post_swap(checkpoint::get(&saga)?.pre_swap.swap_request.clone())
            .await?;
        let ready = checkpoint::resolve(self.wallet, &saga, response).await?;
        let send_proofs = checkpoint::get(&saga)?.amount.map(|_| ready.send.clone());
        checkpoint::finish(self.wallet, &mut saga, ready).await?;
        Ok(SwapSaga {
            wallet: self.wallet,
            state_data: Finalized { send_proofs },
        })
    }
}

impl SwapSaga<'_, Finalized> {
    pub fn into_send_proofs(self) -> Option<Proofs> {
        self.state_data.send_proofs
    }
}

impl<S> std::fmt::Debug for SwapSaga<'_, S>
where
    S: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwapSaga")
            .field("state_data", &self.state_data)
            .finish_non_exhaustive()
    }
}

async fn ensure_storage(wallet: &Wallet, saga: Option<&WalletSaga>) -> Result<(), Error> {
    if !wallet
        .localstore
        .compare_and_swap_proofs(vec![], saga)
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    Ok(())
}

async fn transition(
    wallet: &Wallet,
    saga: &mut WalletSaga,
    state: SwapSagaState,
) -> Result<(), Error> {
    if saga.version == u32::MAX {
        return Err(Error::InvalidOperationState);
    }
    saga.update_state(WalletSagaState::Swap(state));
    if !wallet.localstore.update_saga(saga.clone()).await? {
        return Err(Error::ConcurrentUpdate);
    }
    Ok(())
}

#[cfg(test)]
mod checkpoint_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod acquisition_tests;
#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod integrity_tests;

#[cfg(test)]
mod request_tests;

#[cfg(test)]
mod output_budget_tests;
