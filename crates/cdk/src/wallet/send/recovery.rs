//! Resume one persisted send confirmation without processing other operations.

use cdk_common::wallet::{OperationData, SendSagaState, WalletSagaState};
use tracing::instrument;
use uuid::Uuid;

use crate::{Error, Wallet};

impl Wallet {
    /// Recover one send whose exact confirmation was already persisted.
    ///
    /// Restores its original mint outputs, retrying only the saved swap if the
    /// mint has no outputs. The token and receipt are persisted on completion.
    /// Missing, foreign and non-confirming operations are rejected; this never
    /// starts a prepared send or recovers unrelated wallet operations.
    #[instrument(skip_all, err)]
    pub async fn recover_send_confirmation(&self, operation_id: Uuid) -> Result<(), Error> {
        let saga = self
            .localstore
            .get_saga(&operation_id)
            .await?
            .ok_or(Error::InvalidOperationState)?;
        if saga.id != operation_id
            || saga.mint_url != self.mint_url
            || saga.unit != self.unit
            || saga.state != WalletSagaState::Send(SendSagaState::Confirming)
            || !matches!(&saga.data, OperationData::Send(data) if data.confirmation.is_some())
        {
            return Err(Error::InvalidOperationState);
        }
        self.complete_send_confirmation(saga, true).await?;
        Ok(())
    }
}
