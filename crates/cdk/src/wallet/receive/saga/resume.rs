//! Resume only complete durable receive checkpoints; never infer success from missing coins.
use cdk_common::wallet::{ReceiveSagaState, WalletSaga, WalletSagaState};

use super::{admission, finish, receipt};
use crate::wallet::recovery::RecoveryAction;
use crate::{Error, Wallet};

impl Wallet {
    pub(crate) async fn resume_receive_saga(
        &self,
        expected: &WalletSaga,
    ) -> Result<RecoveryAction, Error> {
        let mut saga = self
            .localstore
            .get_saga(&expected.id)
            .await?
            .ok_or(Error::ConcurrentUpdate)?;
        if saga != *expected {
            return Err(Error::ConcurrentUpdate);
        }
        receipt::validate(self, &saga)?;
        admission::ensure_storage(self).await?;
        match saga.state {
            WalletSagaState::Receive(
                ReceiveSagaState::InputsRecorded | ReceiveSagaState::Cancelling,
            ) => {
                receipt::cancel(self, saga).await?;
                Ok(RecoveryAction::Compensated)
            }
            WalletSagaState::Receive(ReceiveSagaState::Redeeming) => {
                admission::check_owned(self, &saga).await?;
                let swap = receipt::checkpoint(&saga)?
                    .pre_swap
                    .as_ref()
                    .ok_or(Error::InvalidOperationState)?;
                let response = crate::wallet::swap::outputs::restore(self, swap).await?;
                finish::save_response(self, &mut saga, response).await?;
                finish::complete(self, &saga).await?;
                Ok(RecoveryAction::Recovered)
            }
            WalletSagaState::Receive(ReceiveSagaState::OutputsReady) => {
                finish::complete(self, &saga).await?;
                Ok(RecoveryAction::Recovered)
            }
            WalletSagaState::Receive(ReceiveSagaState::Completed) => {
                // Receipt retirement is permanent: terminal cleanup never imports
                // proofs or recreates a receipt that has already been retired.
                self.localstore.delete_saga(&saga.id).await?;
                Ok(RecoveryAction::Recovered)
            }
            _ => Err(Error::InvalidOperationState),
        }
    }
}
