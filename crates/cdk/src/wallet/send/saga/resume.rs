//! Resume logic for send sagas after crash recovery.
//!
//! This module handles resuming incomplete send sagas that were interrupted
//! by a crash. It determines the actual state by querying the mint and
//! either completes the operation or compensates.

use cdk_common::wallet::{OperationData, SendSagaState, WalletSaga};
use tracing::instrument;

use crate::nuts::State;
use crate::wallet::recovery::RecoveryAction;
use crate::wallet::util::escape_log_value;
use crate::{Error, Wallet};

impl Wallet {
    /// Resume an incomplete send saga after crash recovery.
    #[instrument(skip(self, saga))]
    pub(crate) async fn resume_send_saga(
        &self,
        saga: &WalletSaga,
    ) -> Result<RecoveryAction, Error> {
        let state = match &saga.state {
            cdk_common::wallet::WalletSagaState::Send(s) => s,
            _ => {
                return Err(Error::Custom(format!(
                    "Invalid saga state type for send saga {}",
                    saga.id
                )))
            }
        };

        let _data = match &saga.data {
            OperationData::Send(d) => d,
            _ => {
                return Err(Error::Custom(format!(
                    "Invalid operation data type for send saga {}",
                    saga.id
                )))
            }
        };

        match state {
            SendSagaState::Confirming => {
                self.complete_send_confirmation(saga.clone(), true).await?;
                Ok(RecoveryAction::Recovered)
            }
            SendSagaState::PreparedAcquiring
            | SendSagaState::ProofsReserved
            | SendSagaState::PreparedCancelling
            | SendSagaState::PreparedCancelled => {
                tracing::info!("Recovering prepared send cancellation for saga {}", saga.id);
                super::cancellation::complete(self, saga.clone()).await?;
                Ok(RecoveryAction::Compensated)
            }
            SendSagaState::TokenCreated => {
                tracing::info!(
                    "Send saga {} in TokenCreated state - checking proof states",
                    saga.id
                );
                self.recover_or_complete_send(saga).await
            }
            SendSagaState::Claimed => {
                super::claim::complete(self, saga.clone()).await?;
                Ok(RecoveryAction::Recovered)
            }
            SendSagaState::ReclaimPreparing
            | SendSagaState::ReclaimCancelling
            | SendSagaState::Reclaiming
            | SendSagaState::Reclaimed => {
                super::reclaim::complete(self, saga.clone(), true).await?;
                Ok(RecoveryAction::Recovered)
            }
            // This state has no exact replacement request or refund evidence.
            SendSagaState::RollingBack => Ok(RecoveryAction::Skipped),
        }
    }

    /// Check the original snapshot; a concurrent reclaim invalidates completion.
    async fn recover_or_complete_send(&self, saga: &WalletSaga) -> Result<RecoveryAction, Error> {
        let OperationData::Send(data) = &saga.data else {
            return Err(Error::InvalidOperationState);
        };
        let proofs = data
            .proofs
            .as_ref()
            .filter(|p| !p.is_empty())
            .ok_or(Error::InvalidOperationState)?;
        match self.check_proofs_state(proofs.clone()).await {
            Ok(states) => {
                if states.iter().all(|s| s.state == State::Spent) {
                    super::claim::complete(self, saga.clone()).await?;
                }
                Ok(RecoveryAction::Recovered)
            }
            Err(e) => {
                tracing::warn!(
                    "Send saga {} - proof check failed: {}",
                    saga.id,
                    escape_log_value(&e)
                );
                Ok(RecoveryAction::Skipped)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use cdk_common::nuts::{CheckStateResponse, CurrencyUnit, ProofState, State};
    use cdk_common::wallet::{
        OperationData, SendOperationData, SendSagaState, Transaction, TransactionDirection,
        TransactionStatus, WalletSaga, WalletSagaState,
    };
    use cdk_common::Amount;

    use crate::wallet::saga::test_utils::{
        create_test_db, test_keyset_id, test_mint_url, test_proof_info,
    };
    use crate::wallet::test_utils::{
        create_test_wallet, create_test_wallet_with_mock, MockMintConnector,
    };

    #[tokio::test]
    async fn test_recover_send_proofs_reserved() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset_id();
        let saga_id = uuid::Uuid::new_v4();

        // Create and store proofs, then reserve them
        let proof_info = test_proof_info(keyset_id, 100, mint_url.clone(), State::Unspent);
        let proof_y = proof_info.y;
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();
        db.reserve_proofs(vec![proof_y], &saga_id).await.unwrap();

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Send(SendSagaState::ProofsReserved),
            Amount::from(100),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Send(SendOperationData {
                preparation: Some(Box::new(cdk_common::wallet::SendPreparation {
                    inputs: db.get_proofs_by_ys(vec![proof_y]).await.unwrap(),
                    metadata: Default::default(),
                    retain_cancellation: false,
                })),
                confirmation: None,
                reclaim: None,
                amount: Amount::from(100),
                memo: None,
                counter_start: None,
                counter_end: None,
                token: None,
                proofs: None,
            }),
        );
        db.add_saga(saga).await.unwrap();

        let wallet = create_test_wallet(db.clone()).await;
        let report = wallet.recover_incomplete_sagas().await.unwrap();

        assert_eq!(report.compensated, 1);

        let proofs = db
            .get_proofs(None, None, Some(vec![State::Unspent]), None)
            .await
            .unwrap();
        assert_eq!(proofs.len(), 1);
        assert_eq!(proofs[0].used_by_operation, None);

        assert!(db.get_saga(&saga_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_recover_send_proofs_reserved_with_changed_owned_proofs_preserves_them() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset_id();
        let saga_id = uuid::Uuid::new_v4();

        let proof_info = test_proof_info(keyset_id, 100, mint_url.clone(), State::Unspent);
        let proof_y = proof_info.y;
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();
        db.reserve_proofs(vec![proof_y], &saga_id).await.unwrap();
        let original = db.get_proofs_by_ys(vec![proof_y]).await.unwrap();
        db.update_proofs_state(vec![proof_y], State::PendingSpent)
            .await
            .unwrap();

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Send(SendSagaState::ProofsReserved),
            Amount::from(100),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Send(SendOperationData {
                preparation: Some(Box::new(cdk_common::wallet::SendPreparation {
                    inputs: original,
                    metadata: Default::default(),
                    retain_cancellation: false,
                })),
                confirmation: None,
                reclaim: None,
                amount: Amount::from(100),
                memo: None,
                counter_start: None,
                counter_end: None,
                token: None,
                proofs: None,
            }),
        );
        db.add_saga(saga).await.unwrap();

        let wallet = create_test_wallet(db.clone()).await;
        let report = wallet.recover_incomplete_sagas().await.unwrap();

        assert_eq!(report.compensated, 0);
        assert_eq!(report.failed, 1);

        let proofs = db.get_proofs_by_ys(vec![proof_y]).await.unwrap();
        assert_eq!(proofs.len(), 1);
        assert_eq!(proofs[0].state, State::PendingSpent);
        assert_eq!(proofs[0].used_by_operation, Some(saga_id));
        assert!(db.get_saga(&saga_id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_recover_send_proofs_reserved_without_operation_link_leaves_reserved_proof() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset_id();
        let saga_id = uuid::Uuid::new_v4();

        let proof_info = test_proof_info(keyset_id, 100, mint_url.clone(), State::Unspent);
        let proof_y = proof_info.y;
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();

        db.update_proofs_state(vec![proof_y], State::Reserved)
            .await
            .unwrap();

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Send(SendSagaState::ProofsReserved),
            Amount::from(100),
            mint_url,
            CurrencyUnit::Sat,
            OperationData::Send(SendOperationData {
                preparation: None,
                confirmation: None,
                reclaim: None,
                amount: Amount::from(100),
                memo: None,
                counter_start: None,
                counter_end: None,
                token: None,
                proofs: None,
            }),
        );
        db.add_saga(saga).await.unwrap();

        let wallet = create_test_wallet(db.clone()).await;
        let report = wallet.recover_incomplete_sagas().await.unwrap();

        assert_eq!(report.recovered, 0);
        assert_eq!(report.compensated, 0);
        assert_eq!(report.failed, 1);

        let reserved = db.get_proofs_by_ys(vec![proof_y]).await.unwrap();
        assert_eq!(reserved.len(), 1);
        assert_eq!(reserved[0].state, State::Reserved);
        assert_eq!(reserved[0].used_by_operation, None);

        assert!(db.get_saga(&saga_id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_recover_send_token_created_proofs_spent() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset_id();
        let saga_id = uuid::Uuid::new_v4();

        let proof_info = test_proof_info(keyset_id, 100, mint_url.clone(), State::Unspent);
        let proof_y = proof_info.y;
        let token_proofs = vec![proof_info.proof.clone()];
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();
        db.reserve_proofs(vec![proof_y], &saga_id).await.unwrap();
        db.update_proofs_state(vec![proof_y], State::PendingSpent)
            .await
            .unwrap();

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Send(SendSagaState::TokenCreated),
            Amount::from(100),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Send(SendOperationData {
                preparation: None,
                confirmation: None,
                reclaim: None,
                amount: Amount::from(100),
                memo: None,
                counter_start: None,
                counter_end: None,
                token: Some("cashuA...".to_string()),
                proofs: Some(token_proofs),
            }),
        );
        let timestamp = saga.created_at;
        db.add_saga(saga).await.unwrap();
        db.add_transaction(Transaction {
            role: Default::default(),
            mint_url: mint_url.clone(),
            direction: TransactionDirection::Outgoing,
            amount: Amount::from(100),
            fee: Amount::ZERO,
            unit: CurrencyUnit::Sat,
            ys: vec![proof_y],
            timestamp,
            memo: None,
            metadata: HashMap::new(),
            quote_id: None,
            payment_request: None,
            payment_proof: None,
            payment_method: None,
            saga_id: Some(saga_id),
            status: TransactionStatus::Pending,
        })
        .await
        .unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_check_state_response(Ok(CheckStateResponse {
            states: vec![ProofState {
                y: proof_y,
                state: State::Spent,
                witness: None,
            }],
        }));

        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        let report = wallet.recover_incomplete_sagas().await.unwrap();

        assert_eq!(report.recovered, 1);
        assert_eq!(report.compensated, 0);

        let proofs = db
            .get_proofs(None, None, Some(vec![State::Spent]), None)
            .await
            .unwrap();
        assert_eq!(proofs.len(), 1);

        assert!(db.get_saga(&saga_id).await.unwrap().is_none());
        let transactions = db.list_transactions(None, None, None).await.unwrap();
        assert_eq!(transactions[0].status, TransactionStatus::Completed);
    }

    #[tokio::test]
    async fn test_recover_send_token_created_proofs_not_spent() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset_id();
        let saga_id = uuid::Uuid::new_v4();

        let proof_info = test_proof_info(keyset_id, 100, mint_url.clone(), State::Unspent);
        let proof_y = proof_info.y;
        let token_proofs = vec![proof_info.proof.clone()];
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();
        db.reserve_proofs(vec![proof_y], &saga_id).await.unwrap();
        db.update_proofs_state(vec![proof_y], State::PendingSpent)
            .await
            .unwrap();

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Send(SendSagaState::TokenCreated),
            Amount::from(100),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Send(SendOperationData {
                preparation: None,
                confirmation: None,
                reclaim: None,
                amount: Amount::from(100),
                memo: None,
                counter_start: None,
                counter_end: None,
                token: Some("cashuA...".to_string()),
                proofs: Some(token_proofs),
            }),
        );
        db.add_saga(saga).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_check_state_response(Ok(CheckStateResponse {
            states: vec![ProofState {
                y: proof_y,
                state: State::Unspent,
                witness: None,
            }],
        }));

        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        let report = wallet.recover_incomplete_sagas().await.unwrap();

        assert_eq!(report.recovered, 1);
        assert_eq!(report.compensated, 0);

        let proofs = db
            .get_proofs(None, None, Some(vec![State::PendingSpent]), None)
            .await
            .unwrap();
        assert_eq!(proofs.len(), 1);

        assert!(db.get_saga(&saga_id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn rollback_without_exact_output_custody_is_retained() {
        for state in [State::PendingSpent, State::Spent] {
            let db = create_test_db().await;
            let id = uuid::Uuid::new_v4();
            let mut proof = test_proof_info(test_keyset_id(), 100, test_mint_url(), state);
            proof.used_by_operation = Some(id);
            db.update_proofs(vec![proof.clone()], vec![]).await.unwrap();
            let saga = WalletSaga::new(
                id,
                WalletSagaState::Send(SendSagaState::RollingBack),
                100.into(),
                test_mint_url(),
                CurrencyUnit::Sat,
                OperationData::Send(SendOperationData {
                    preparation: None,
                    confirmation: None,
                    reclaim: None,
                    amount: 100.into(),
                    memo: None,
                    counter_start: None,
                    counter_end: None,
                    token: None,
                    proofs: Some(vec![proof.proof.clone()]),
                }),
            );
            db.add_saga(saga.clone()).await.unwrap();
            let mint = Arc::new(MockMintConnector::new());
            let wallet = create_test_wallet_with_mock(db.clone(), mint).await;
            let report = wallet.recover_incomplete_sagas().await.unwrap();
            assert_eq!(report.skipped, 1);
            assert_eq!(db.get_saga(&id).await.unwrap(), Some(saga));
            assert_eq!(db.get_proofs_by_ys(vec![proof.y]).await.unwrap(), [proof]);
        }
    }
}
