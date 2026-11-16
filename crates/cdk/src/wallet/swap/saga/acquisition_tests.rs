use std::sync::Arc;

use super::test_utils::{prepare, submitted, wallet};
use super::*;
use crate::wallet::recovery::RecoveryAction;
use crate::wallet::send::saga::interposed_store::{Action, Interposed};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_keyset_id, test_mint_url, test_proof_info,
    MockMintConnector,
};

#[tokio::test]
async fn rejected_swap_preparation_preserves_original_input() {
    let db = create_test_db().await;
    let original = test_proof_info(test_keyset_id(), 32, test_mint_url());
    db.update_proofs(vec![original.clone()], vec![])
        .await
        .unwrap();
    let mint = Arc::new(MockMintConnector::new());
    mint.reset_default_mint_state();
    let wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
    assert!(wallet
        .swap(
            Some(64.into()),
            Default::default(),
            vec![original.proof.clone()],
            None,
            false,
            false
        )
        .await
        .is_err());
    assert_eq!(
        db.get_proofs_by_ys(vec![original.y]).await.unwrap(),
        [original]
    );
    assert!(db.get_incomplete_sagas().await.unwrap().is_empty());
    assert!(mint.captured_swap_requests().is_empty());
}

#[tokio::test]
async fn prepared_cancellation_restores_exact_records_and_fences_late_submission() {
    for acquired in [false, true] {
        let (mut wallet, mint) = wallet().await;
        let original = wallet
            .localstore
            .get_proofs(None, None, None, None)
            .await
            .unwrap();
        let store = Arc::new(Interposed::new(wallet.localstore.clone()));
        if acquired {
            *store.after_proofs.lock().unwrap() = Some(Action::Fail);
        } else {
            *store.before_proofs.lock().unwrap() = Some(Action::Fail);
        }
        wallet.localstore = store;
        let result = SwapSaga::new(&wallet)
            .prepare(
                None,
                Default::default(),
                original.iter().map(|p| p.proof.clone()).collect(),
                None,
                false,
                false,
            )
            .await;
        assert!(result.is_err());
        let saved = wallet
            .localstore
            .get_incomplete_sagas()
            .await
            .unwrap()
            .remove(0);
        assert!(matches!(
            wallet.resume_swap_saga(&saved).await.unwrap(),
            RecoveryAction::Compensated
        ));
        assert_eq!(
            wallet
                .localstore
                .get_proofs(None, None, None, None)
                .await
                .unwrap(),
            original
        );
        assert!(matches!(
            transition(&wallet, &mut saved.clone(), SwapSagaState::SwapRequested).await,
            Err(Error::ConcurrentUpdate)
        ));
        assert!(matches!(
            acquisition::apply(&wallet, &saved).await,
            Err(Error::ConcurrentUpdate)
        ));
        assert!(mint.captured_swap_requests().is_empty());
    }
}

#[tokio::test]
async fn cancelling_preserves_new_owners_and_submitted_cannot_cancel() {
    let (wallet, mint) = wallet().await;
    let prepared = prepare(&wallet, None, false).await;
    let mut saved = prepared.state_data.saga;
    let mut later = checkpoint::get(&saved).unwrap().acquisition[0]
        .after
        .clone()
        .unwrap();
    later.used_by_operation = Some(uuid::Uuid::new_v4());
    wallet
        .localstore
        .update_proofs(vec![later.clone()], vec![])
        .await
        .unwrap();
    acquisition::cancel(&wallet, &mut saved).await.unwrap();
    assert_eq!(
        wallet
            .localstore
            .get_proofs_by_ys(vec![later.y])
            .await
            .unwrap(),
        [later]
    );
    let (wallet, mint2) = self::wallet().await;
    let mut submitted = submitted(&wallet, &mint2, None, false).await;
    assert!(matches!(
        acquisition::cancel(&wallet, &mut submitted).await,
        Err(Error::InvalidOperationState)
    ));
    assert!(mint.captured_swap_requests().is_empty());
}

#[tokio::test]
async fn cancellation_retains_unexplained_own_record_but_preserves_exact_spent_record() {
    for spent in [false, true] {
        let (wallet, _) = wallet().await;
        let mut saved = prepare(&wallet, None, false).await.state_data.saga;
        let mut changed = checkpoint::get(&saved).unwrap().acquisition[0]
            .after
            .clone()
            .unwrap();
        if spent {
            changed.state = crate::nuts::State::Spent;
        } else {
            changed.derivation_index = Some(u32::MAX);
        }
        wallet
            .localstore
            .update_proofs(vec![changed.clone()], vec![])
            .await
            .unwrap();
        let result = acquisition::cancel(&wallet, &mut saved).await;
        assert_eq!(result.is_ok(), spent);
        assert_eq!(
            wallet
                .localstore
                .get_proofs_by_ys(vec![changed.y])
                .await
                .unwrap(),
            [changed]
        );
        let current = wallet.localstore.get_saga(&saved.id).await.unwrap();
        if spent {
            assert!(current.is_none());
        } else {
            let current = current.unwrap();
            assert_eq!(
                current.state,
                WalletSagaState::Swap(SwapSagaState::Cancelling)
            );
            assert!(checkpoint::get(&current).is_ok());
        }
    }
}

#[tokio::test]
async fn cancelling_phase_fences_paused_admission_and_submission() {
    let (wallet, mint) = wallet().await;
    let mut saved = prepare(&wallet, None, false).await.state_data.saga;
    let stale = saved.clone();
    transition(&wallet, &mut saved, SwapSagaState::Cancelling)
        .await
        .unwrap();
    assert!(matches!(
        acquisition::apply(&wallet, &stale).await,
        Err(Error::ConcurrentUpdate)
    ));
    assert!(matches!(
        acquisition::check(&wallet, &stale).await,
        Err(Error::ConcurrentUpdate)
    ));
    assert!(matches!(
        transition(&wallet, &mut stale.clone(), SwapSagaState::SwapRequested).await,
        Err(Error::ConcurrentUpdate)
    ));
    acquisition::cancel(&wallet, &mut saved).await.unwrap();
    assert!(mint.captured_swap_requests().is_empty());
}
