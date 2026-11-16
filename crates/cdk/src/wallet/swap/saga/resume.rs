//! Resume only complete saved intent; never infer success from partial local outputs.
use cdk_common::wallet::{SwapSagaState, WalletSaga, WalletSagaState};

use super::{acquisition, checkpoint, ensure_storage, transition};
use crate::wallet::recovery::RecoveryAction;
use crate::wallet::swap::outputs;
use crate::{Error, Wallet};

impl Wallet {
    pub(crate) async fn resume_swap_saga(
        &self,
        original: &WalletSaga,
    ) -> Result<RecoveryAction, Error> {
        checkpoint::validate(self, original)?;
        ensure_storage(self, Some(original)).await?;
        let WalletSagaState::Swap(state) = original.state else {
            return Err(Error::InvalidOperationState);
        };
        let mut saga = original.clone();
        if matches!(
            state,
            SwapSagaState::ProofsReserved | SwapSagaState::Cancelling
        ) {
            acquisition::cancel(self, &mut saga).await?;
            return Ok(RecoveryAction::Compensated);
        }
        transition(self, &mut saga, state).await?;
        acquisition::check(self, &saga).await?;
        let ready = match &checkpoint::get(&saga)?.ready {
            Some(ready) => ready.clone(),
            None => {
                let response = outputs::restore(self, &checkpoint::get(&saga)?.pre_swap).await?;
                checkpoint::resolve(self, &saga, response).await?
            }
        };
        checkpoint::finish(self, &mut saga, ready).await?;
        Ok(RecoveryAction::Recovered)
    }
}
