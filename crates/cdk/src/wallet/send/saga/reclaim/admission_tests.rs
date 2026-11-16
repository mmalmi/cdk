use std::sync::Arc;

use cdk_common::database::wallet::ProofChange;
use cdk_common::wallet::{ReceiveOperationData, ReceiveSagaState};

use super::*;
use crate::nuts::{RestoreResponse, SwapResponse};
use crate::wallet::send::saga::interposed_store::{Action, Interposed};
use crate::wallet::test_utils::{test_keyset, MockMintConnector};

#[tokio::test]
async fn receive_winning_admission_prevents_reclaim_mint_request() {
    for after_read in [false, true] {
        let (mut wallet, mint, send) = super::tests::sent().await;
        let original = wallet
            .localstore
            .get_proofs_by_ys(match &send.data {
                OperationData::Send(data) => data.proofs.as_ref().unwrap().ys().unwrap(),
                _ => panic!("send"),
            })
            .await
            .unwrap()
            .remove(0);
        let receive = WalletSaga::new(
            uuid::Uuid::new_v4(),
            WalletSagaState::Receive(ReceiveSagaState::InputsRecorded),
            send.amount,
            send.mint_url.clone(),
            send.unit.clone(),
            OperationData::Receive(ReceiveOperationData { checkpoint: None }),
        );
        wallet.localstore.add_saga(receive.clone()).await.unwrap();
        let mut acquired = original.clone();
        acquired.state = State::Pending;
        acquired.used_by_operation = Some(receive.id);
        let store = Arc::new(Interposed::new(wallet.localstore.clone()));
        let hook = if after_read {
            &store.before_proofs
        } else {
            &store.after_saga
        };
        *hook.lock().unwrap() = Some(Action::CompareProofs(
            vec![ProofChange {
                y: original.y,
                before: Some(original),
                after: Some(acquired.clone()),
            }],
            receive,
        ));
        wallet.localstore = store;
        mint.set_post_swap_response(Err(Error::Timeout));
        let result = wallet.revoke_send(send.id).await;
        assert!(
            mint.captured_swap_requests().is_empty(),
            "reclaim submitted after receive acquired its input"
        );
        assert!(matches!(result, Err(Error::ConcurrentUpdate)));
        assert_eq!(
            wallet
                .localstore
                .get_proofs_by_ys(vec![acquired.y])
                .await
                .unwrap(),
            [acquired]
        );
        let current = wallet.localstore.get_saga(&send.id).await.unwrap().unwrap();
        assert_eq!(
            current.state,
            WalletSagaState::Send(SendSagaState::TokenCreated)
        );
        let OperationData::Send(data) = current.data else {
            panic!("send");
        };
        assert!(data.reclaim.is_none());
    }
}

async fn prepared(acquire: bool) -> (Wallet, Arc<MockMintConnector>, WalletSaga) {
    let (mut wallet, mint, send) = super::tests::sent().await;
    let store = Arc::new(Interposed::new(wallet.localstore.clone()));
    if acquire {
        *store.after_proofs.lock().unwrap() = Some(Action::Fail);
    } else {
        *store.after_saga.lock().unwrap() = Some(Action::Fail);
    }
    wallet.localstore = store;
    assert!(wallet.revoke_send(send.id).await.is_err());
    assert!(mint.captured_swap_requests().is_empty());
    let saga = wallet.localstore.get_saga(&send.id).await.unwrap().unwrap();
    assert_eq!(
        saga.state,
        WalletSagaState::Send(SendSagaState::ReclaimPreparing)
    );
    let original = &intent(&saga).unwrap().inputs[0];
    assert_eq!(
        wallet
            .localstore
            .get_proofs_by_ys(vec![original.y])
            .await
            .unwrap(),
        [if acquire {
            admission::acquired(original)
        } else {
            original.clone()
        }]
    );
    (wallet, mint, saga)
}

#[tokio::test]
async fn prepared_recovery_keeps_exact_request_and_fee_before_or_after_acquisition() {
    for acquired in [false, true] {
        let (wallet, mint, saved) = prepared(acquired).await;
        let expected = intent(&saved).unwrap().clone();
        let mut changed_keyset = test_keyset();
        changed_keyset.input_fee_ppk = 5000;
        mint.set_active_keyset(changed_keyset);
        wallet
            .keysets(cdk_common::wallet::KeysetLoadPolicy::Refresh)
            .await
            .unwrap();
        mint._set_restore_response(Ok(RestoreResponse {
            outputs: vec![],
            signatures: vec![],
        }));
        mint.set_post_swap_response(Ok(SwapResponse {
            signatures: super::tests::restored(&saved).signatures,
        }));
        assert_eq!(
            wallet.revoke_send(saved.id).await.unwrap(),
            expected.transaction.amount
        );
        assert_eq!(
            mint.captured_swap_requests(),
            [expected.pre_swap.swap_request]
        );
        assert_eq!(
            wallet
                .localstore
                .get_transaction(expected.transaction.id())
                .await
                .unwrap(),
            Some(expected.transaction)
        );
        assert!(wallet
            .localstore
            .get_saga(&saved.id)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn cancellation_fences_late_acquisition_and_submission() {
    for acquired in [false, true] {
        let (wallet, mint, mut cancelling) = prepared(acquired).await;
        let stale = cancelling.clone();
        let original = intent(&stale).unwrap().inputs[0].clone();
        transition(&wallet, &mut cancelling, SendSagaState::ReclaimCancelling)
            .await
            .unwrap();
        let before = if acquired {
            admission::acquired(&original)
        } else {
            original.clone()
        };
        assert!(!wallet
            .localstore
            .compare_and_swap_proofs(
                vec![ProofChange {
                    y: original.y,
                    before: Some(before),
                    after: Some(admission::acquired(&original)),
                }],
                Some(&stale)
            )
            .await
            .unwrap());
        admission::cancel(&wallet, &mut cancelling).await.unwrap();
        let mut late_submitter = stale.clone();
        assert!(matches!(
            advance(&wallet, &mut late_submitter).await,
            Err(Error::ConcurrentUpdate)
        ));
        assert!(matches!(
            complete(&wallet, stale, false).await,
            Err(Error::ConcurrentUpdate)
        ));
        assert_eq!(
            wallet
                .localstore
                .get_proofs_by_ys(vec![original.y])
                .await
                .unwrap(),
            [original]
        );
        assert!(mint.captured_swap_requests().is_empty());
        assert_eq!(
            cancelling.state,
            WalletSagaState::Send(SendSagaState::TokenCreated)
        );
        assert!(intent(&cancelling).is_err());
    }
}

#[tokio::test]
async fn cancellation_preserves_foreign_spent_changed_and_missing_inputs() {
    for case in 0..4 {
        let (wallet, mint, mut saved) = prepared(true).await;
        let original_intent = intent(&saved).unwrap().clone();
        let original = intent(&saved).unwrap().inputs[0].clone();
        let mut changed = admission::acquired(&original);
        match case {
            0 => changed.used_by_operation = Some(uuid::Uuid::new_v4()),
            1 => changed.state = State::Spent,
            2 => changed.derivation_index = Some(u32::MAX),
            _ => {}
        }
        let expected = if case == 3 { None } else { Some(changed) };
        wallet
            .localstore
            .update_proofs(
                expected.clone().into_iter().collect(),
                if expected.is_none() {
                    vec![original.y]
                } else {
                    vec![]
                },
            )
            .await
            .unwrap();
        let outcome = admission::cancel(&wallet, &mut saved).await;
        if case == 2 {
            assert!(matches!(outcome, Err(Error::ConcurrentUpdate)));
            assert_eq!(intent(&saved).unwrap(), &original_intent);
            assert_eq!(
                wallet.localstore.get_saga(&saved.id).await.unwrap(),
                Some(saved.clone())
            );
            assert!(matches!(
                wallet.revoke_send(saved.id).await,
                Err(Error::ConcurrentUpdate)
            ));
        } else {
            outcome.unwrap();
        }
        assert_eq!(
            wallet
                .localstore
                .get_proofs_by_ys(vec![original.y])
                .await
                .unwrap(),
            expected.into_iter().collect::<Vec<_>>()
        );
        assert!(mint.captured_swap_requests().is_empty());
        assert_eq!(
            saved.state,
            WalletSagaState::Send(if case == 2 {
                SendSagaState::ReclaimCancelling
            } else {
                SendSagaState::TokenCreated
            })
        );
    }
}

#[tokio::test]
async fn cancellation_recovers_after_phase_claim_or_input_rollback() {
    for rolled_back in [false, true] {
        let (mut wallet, mint, mut saved) = prepared(true).await;
        let original = intent(&saved).unwrap().inputs[0].clone();
        let store = Arc::new(Interposed::new(wallet.localstore.clone()));
        if rolled_back {
            *store.after_proofs.lock().unwrap() = Some(Action::Fail);
        } else {
            *store.after_saga.lock().unwrap() = Some(Action::Fail);
        }
        wallet.localstore = store;
        assert!(admission::cancel(&wallet, &mut saved).await.is_err());
        let interrupted = wallet
            .localstore
            .get_saga(&saved.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            interrupted.state,
            WalletSagaState::Send(SendSagaState::ReclaimCancelling)
        );
        assert_eq!(
            wallet
                .localstore
                .get_proofs_by_ys(vec![original.y])
                .await
                .unwrap(),
            [if rolled_back {
                original.clone()
            } else {
                admission::acquired(&original)
            }]
        );
        assert!(matches!(
            wallet.revoke_send(saved.id).await,
            Err(Error::ConcurrentUpdate)
        ));
        assert_eq!(
            wallet
                .localstore
                .get_proofs_by_ys(vec![original.y])
                .await
                .unwrap(),
            [original]
        );
        let resumed = wallet
            .localstore
            .get_saga(&saved.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            resumed.state,
            WalletSagaState::Send(SendSagaState::TokenCreated)
        );
        assert!(intent(&resumed).is_err());
        assert!(mint.captured_swap_requests().is_empty());
    }
}

#[tokio::test]
async fn reclaim_admission_rejects_stale_receive_acquisition() {
    let (wallet, mint, saved) = prepared(true).await;
    let original = intent(&saved).unwrap().inputs[0].clone();
    let mut receive_owned = original.clone();
    receive_owned.state = State::Pending;
    receive_owned.used_by_operation = Some(uuid::Uuid::new_v4());
    assert!(!wallet
        .localstore
        .compare_and_swap_proofs(
            vec![ProofChange {
                y: original.y,
                before: Some(original.clone()),
                after: Some(receive_owned),
            }],
            None
        )
        .await
        .unwrap());
    assert_eq!(
        wallet
            .localstore
            .get_proofs_by_ys(vec![original.y])
            .await
            .unwrap(),
        [admission::acquired(&original)]
    );
    assert!(mint.captured_swap_requests().is_empty());
}

#[tokio::test]
async fn submitted_phase_fences_cancellation() {
    let (wallet, mint, mut submitted) = prepared(true).await;
    let mut stale = submitted.clone();
    let original = intent(&submitted).unwrap().inputs[0].clone();
    advance(&wallet, &mut submitted).await.unwrap();
    assert!(matches!(
        admission::cancel(&wallet, &mut stale).await,
        Err(Error::ConcurrentUpdate)
    ));
    assert!(matches!(
        admission::cancel(&wallet, &mut submitted).await,
        Err(Error::InvalidOperationState)
    ));
    assert!(matches!(
        admission::acquire(&wallet, &mut submitted).await,
        Err(Error::InvalidOperationState)
    ));
    assert_eq!(
        wallet.localstore.get_saga(&submitted.id).await.unwrap(),
        Some(submitted)
    );
    assert_eq!(
        wallet
            .localstore
            .get_proofs_by_ys(vec![original.y])
            .await
            .unwrap(),
        [admission::acquired(&original)]
    );
    assert!(mint.captured_swap_requests().is_empty());
}
