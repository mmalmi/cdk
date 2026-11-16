//! Save exact signed mint intent before submitting it.
use cdk_common::wallet::{ReceiveSagaState, WalletSagaState};

use super::state::{Finalized, Prepared};
use super::{admission, finish, receipt, ReceiveSaga};
use crate::nuts::nut10::SpendingConditionVerification;
use crate::nuts::{PreSwap, SigFlag};
use crate::Error;

impl<'a> ReceiveSaga<'a, Prepared> {
    /// Acquire inputs, save the signed request, and receive durable outputs.
    pub async fn execute(self) -> Result<ReceiveSaga<'a, Finalized>, Error> {
        let inputs = admission::plan(self.wallet, &self.state_data).await?;
        let mut saga = receipt::start(self.wallet, &self.state_data, inputs).await?;
        let (pre_swap, counter) = match self.prepare_request().await {
            Ok(request) => request,
            Err(error) => {
                receipt::cancel(self.wallet, saga).await?;
                return Err(error);
            }
        };
        let unsubmitted = saga.clone();
        let saved = receipt::checkpoint_mut(&mut saga)?;
        saved.transaction.amount = pre_swap.swap_request.output_amount()?;
        saved.transaction.fee = pre_swap.fee;
        saved.pre_swap = Some(pre_swap.clone());
        let committed = async {
            if saga.version == u32::MAX {
                return Err(Error::InvalidOperationState);
            }
            saga.update_state(WalletSagaState::Receive(ReceiveSagaState::Redeeming));
            receipt::validate(self.wallet, &saga)?;
            self.wallet
                .commit_swap_preparation(
                    Some(&unsubmitted),
                    &saga,
                    self.state_data.active_keyset_id,
                    counter,
                    &pre_swap,
                )
                .await
        }
        .await;
        if let Err(error) = committed {
            // A failed write may have committed. Release inputs only when a
            // fresh read proves the exact unsubmitted parent is still current.
            if self.wallet.localstore.get_saga(&saga.id).await?.as_ref() == Some(&unsubmitted) {
                receipt::cancel(self.wallet, unsubmitted).await?;
            }
            return Err(error);
        }
        admission::check_owned(self.wallet, &saga).await?;
        // A connector can retry internally; even a final rejection cannot undo
        // the possibility that an earlier request already spent these inputs.
        let response = self
            .wallet
            .client
            .post_swap(pre_swap.swap_request.clone())
            .await?;
        finish::save_response(self.wallet, &mut saga, response).await?;
        let amount = finish::complete(self.wallet, &saga).await?;
        Ok(ReceiveSaga {
            wallet: self.wallet,
            state_data: Finalized { amount },
        })
    }

    async fn prepare_request(&self) -> Result<(PreSwap, u32), Error> {
        let prepared = &self.state_data;
        let fees = self
            .wallet
            .get_keyset_fees_and_amounts_by_id(prepared.active_keyset_id)
            .await?;
        let breakdown = self.wallet.get_proofs_fee(&prepared.proofs).await?;
        let counter = self
            .wallet
            .localstore
            .increment_keyset_counter(&prepared.active_keyset_id, 0)
            .await?;
        let mut request = self
            .wallet
            .create_swap(
                prepared.active_keyset_id,
                &fees,
                None,
                prepared.options.amount_split_target.clone(),
                prepared.proofs.clone(),
                None,
                false,
                false,
                &breakdown,
                counter,
            )
            .await?;
        if self.determine_sig_flag()? == SigFlag::SigAll {
            for key in prepared.p2pk_signing_keys.values() {
                request.swap_request.sign_sig_all(key.clone())?;
            }
            // Persist the same signed output witnesses in both request views.
            for (secret, output) in request
                .pre_mint_secrets
                .secrets
                .iter_mut()
                .zip(request.swap_request.outputs())
            {
                secret.blinded_message = output.clone();
            }
        }
        request.swap_request.verify_spending_conditions()?;
        crate::wallet::swap::outputs::validate_request(&request)?;
        Ok((request, counter))
    }
}
