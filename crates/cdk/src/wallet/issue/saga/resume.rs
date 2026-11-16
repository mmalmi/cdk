//! Resume logic for issue (mint) sagas after crash recovery.
//!
//! This module handles resuming incomplete issue sagas that were interrupted
//! by a crash. It attempts to recover outputs using stored blinded messages.
//!
//! # Recovery Strategy
//!
//! For `MintRequested` state, we use a replay-first strategy:
//! 1. **Replay**: Attempt to replay the original `post_mint` request.
//!    If the mint cached the response (NUT-19), we get signatures immediately.
//! 2. **Fallback**: If replay fails, use `/restore` to recover outputs.

use cdk_common::wallet::{IssueSagaState, MintOperationData, OperationData, WalletSaga};
use tracing::instrument;

use crate::wallet::recovery::RecoveryAction;
use crate::{Error, Wallet};

impl Wallet {
    /// Resume an incomplete issue saga after crash recovery.
    ///
    /// Recovery depends on state:
    /// - SecretsPrepared: No mint request sent, safe to compensate.
    /// - MintRequested: Mint request sent, attempt to recover outputs.
    #[instrument(skip(self, saga))]
    pub(crate) async fn resume_issue_saga(
        &self,
        saga: &WalletSaga,
    ) -> Result<RecoveryAction, Error> {
        if saga.mint_url != self.mint_url || saga.unit != self.unit {
            return Err(Error::InvalidOperationState);
        }
        let state = match &saga.state {
            cdk_common::wallet::WalletSagaState::Issue(s) => s,
            _ => {
                return Err(Error::Custom(format!(
                    "Invalid saga state type for issue saga {}",
                    saga.id
                )))
            }
        };

        let data = match &saga.data {
            OperationData::Mint(d) => d,
            _ => {
                return Err(Error::Custom(format!(
                    "Invalid operation data type for issue saga {}",
                    saga.id
                )))
            }
        };

        match state {
            IssueSagaState::SecretsPrepared => {
                // No mint request was sent - safe to delete saga
                // Counter increments are not reversed (by design)
                tracing::info!(
                    "Issue saga {} in SecretsPrepared state - cleaning up",
                    saga.id
                );
                super::retirement::retire(self, saga, None).await?;
                Ok(RecoveryAction::Compensated)
            }
            IssueSagaState::MintRequested => {
                // Mint request was sent - try to recover outputs
                tracing::info!(
                    "Issue saga {} in MintRequested state - attempting recovery",
                    saga.id
                );
                // Return the result directly (RecoveryAction)
                self.complete_issue_from_restore(saga, data).await
            }
        }
    }

    /// Complete an issue by first trying replay, then falling back to restore.
    /// Replay leverages NUT-19 caching.
    async fn complete_issue_from_restore(
        &self,
        saga: &WalletSaga,
        data: &MintOperationData,
    ) -> Result<RecoveryAction, Error> {
        let original = super::checkpoint::get(data)?;
        if saga.kind != saga.state.kind()
            || saga.amount != data.amount
            || saga.quote_id.as_deref() != Some(data.primary_quote_id())
            || self.localstore.get_saga(&saga.id).await?.as_ref() != Some(saga)
        {
            return Err(Error::ConcurrentUpdate);
        }
        let owner = saga.id.to_string();
        for id in data.quote_ids() {
            let quote = self
                .localstore
                .get_mint_quote(&id)
                .await?
                .ok_or(Error::UnknownQuote)?;
            if quote.used_by_operation.as_deref() != Some(owner.as_str())
                || quote.mint_url != self.mint_url
                || quote.unit != self.unit
                || quote.payment_method != original.payment_method
            {
                return Err(Error::InvalidOperationState);
            }
        }
        cdk_common::database::wallet::issue_receipts(saga)?;
        let proofs = super::checkpoint::recover(self, original, saga.id).await?;
        super::retirement::retire(self, saga, Some(proofs)).await?;
        Ok(RecoveryAction::Recovered)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cdk_common::amount::{FeeAndAmounts, SplitTarget};
    use cdk_common::nuts::{CurrencyUnit, RestoreResponse};
    use cdk_common::wallet::{
        IssueSagaState, MintOperationData, OperationData, TransactionStatus, WalletSaga,
        WalletSagaState,
    };
    use cdk_common::Amount;

    use crate::nuts::PreMintSecrets;
    use crate::wallet::saga::test_utils::{create_test_db, test_mint_url};
    use crate::wallet::test_utils::{
        create_test_wallet_with_mock, test_issue_data, test_keyset_id, test_mint_quote,
        MockMintConnector,
    };

    #[tokio::test]
    async fn test_recover_issue_mint_requested_without_recovery_data_stays_pending() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_mint_quote_{}", uuid::Uuid::new_v4());

        // Create saga in MintRequested state
        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Issue(IssueSagaState::MintRequested),
            Amount::from(1000),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Mint(MintOperationData::new_single(
                quote_id.clone(),
                Amount::from(1000),
            )),
        );
        db.add_saga(saga).await.unwrap();

        // Store mint quote
        let mint_quote = test_mint_quote(mint_url.clone());
        db.add_mint_quote(mint_quote).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_post_mint_response(Ok(crate::nuts::MintResponse { signatures: vec![] }));

        let wallet = create_test_wallet_with_mock(db.clone(), mock_client.clone()).await;
        let result = wallet
            .resume_issue_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await;

        assert!(matches!(result, Err(crate::Error::InvalidOperationState)));
        assert!(mock_client.post_mint_requests().is_empty());
        assert!(db.get_saga(&saga_id).await.unwrap().is_some());

        // No proofs created
        let proofs = db.get_proofs(None, None, None, None).await.unwrap();
        assert!(proofs.is_empty());

        // No transaction can be reconstructed without stable output identifiers.
        let transactions = db.list_transactions(None, None, None).await.unwrap();
        assert!(transactions.is_empty());
    }

    #[tokio::test]
    async fn test_recover_issue_empty_restore_keeps_transaction_pending() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_mint_quote_{}", uuid::Uuid::new_v4());

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_post_mint_response(Err(crate::Error::Custom("Mint failed".to_string())));
        mock_client._set_restore_response(Ok(RestoreResponse {
            signatures: vec![],
            outputs: vec![],
        }));
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        let fee_and_amounts = FeeAndAmounts::from((0, vec![1]));
        let premint_secrets = PreMintSecrets::from_seed(
            test_keyset_id(),
            0,
            &wallet.seed,
            Amount::from(1),
            &SplitTarget::None,
            &fee_and_amounts,
        )
        .unwrap();

        let mut mint_quote = test_mint_quote(mint_url.clone());
        mint_quote.id = quote_id.clone();
        mint_quote.amount = Some(Amount::from(1));
        mint_quote.amount_paid = Amount::from(1);
        let original_quote = mint_quote.clone();
        mint_quote.used_by_operation = Some(saga_id.to_string());
        db.add_mint_quote(mint_quote).await.unwrap();
        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Issue(IssueSagaState::MintRequested),
            Amount::from(1),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Mint(test_issue_data(original_quote, premint_secrets)),
        );
        for receipt in cdk_common::database::wallet::issue_receipts(&saga).unwrap() {
            db.add_transaction(receipt).await.unwrap();
        }
        db.add_saga(saga).await.unwrap();

        let result = wallet
            .resume_issue_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await;

        assert!(matches!(result, Err(crate::Error::InvalidMintResponse(_))));
        assert!(db.get_saga(&saga_id).await.unwrap().is_some());

        // No proofs
        let proofs = db.get_proofs(None, None, None, None).await.unwrap();
        assert!(proofs.is_empty());

        let transactions = db.list_transactions(None, None, None).await.unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].status, TransactionStatus::Pending);
    }

    #[tokio::test]
    async fn test_recover_issue_mint_requested_max_outputs_does_not_restore() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_mint_quote_{}", uuid::Uuid::new_v4());

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client
            .set_post_mint_response(Err(crate::Error::MaxOutputsExceeded { actual: 2, max: 1 }));

        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        let fee_and_amounts = FeeAndAmounts::from((0, vec![1]));
        let premint_secrets = PreMintSecrets::from_seed(
            test_keyset_id(),
            0,
            &wallet.seed,
            Amount::from(1),
            &SplitTarget::None,
            &fee_and_amounts,
        )
        .unwrap();

        let mut mint_quote = test_mint_quote(mint_url.clone());
        mint_quote.id = quote_id.clone();
        mint_quote.amount = Some(Amount::from(1));
        mint_quote.amount_paid = Amount::from(1);
        let original_quote = mint_quote.clone();
        mint_quote.used_by_operation = Some(saga_id.to_string());
        db.add_mint_quote(mint_quote).await.unwrap();
        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Issue(IssueSagaState::MintRequested),
            Amount::from(1),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Mint(test_issue_data(original_quote, premint_secrets)),
        );
        for receipt in cdk_common::database::wallet::issue_receipts(&saga).unwrap() {
            db.add_transaction(receipt).await.unwrap();
        }
        db.add_saga(saga).await.unwrap();

        let result = wallet
            .resume_issue_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await;

        assert!(matches!(
            result,
            Err(crate::Error::MaxOutputsExceeded { actual: 2, max: 1 })
        ));
        assert!(db.get_saga(&saga_id).await.unwrap().is_some());

        let mint_quote = db.get_mint_quote(&quote_id).await.unwrap().unwrap();
        assert_eq!(mint_quote.used_by_operation, Some(saga_id.to_string()));
        let transactions = db.list_transactions(None, None, None).await.unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].status, TransactionStatus::Pending);
    }
}
