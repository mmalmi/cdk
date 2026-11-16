//! Receive must admit storage support before acquiring or submitting any value.
use std::sync::Arc;

use super::test_utils::{fixture, options};
use crate::wallet::send::saga::interposed_store::Interposed;
use crate::wallet::test_utils::{test_keyset, test_mint_url, test_proof_info};

async fn assert_unsupported_storage(proof_cas: bool, completion: bool) {
    let (mut wallet, client) = fixture().await;
    let mut original = test_proof_info(test_keyset().id, 8, test_mint_url());
    original.created_by_operation = Some(uuid::Uuid::new_v4());
    original.derivation_index = Some(42);
    wallet
        .localstore
        .update_proofs(vec![original.clone()], vec![])
        .await
        .unwrap();
    let before_counter = wallet
        .localstore
        .increment_keyset_counter(&test_keyset().id, 0)
        .await
        .unwrap();
    let mut store = Interposed::new(wallet.localstore.clone());
    store.proof_cas_supported = proof_cas;
    store.completion_supported = completion;
    wallet.localstore = Arc::new(store);
    assert!(wallet
        .receive_proofs(vec![original.proof.clone()], options(), None, None)
        .await
        .is_err());
    assert_eq!(
        wallet
            .localstore
            .get_proofs_by_ys(vec![original.y])
            .await
            .unwrap(),
        [original]
    );
    assert_eq!(
        wallet
            .localstore
            .increment_keyset_counter(&test_keyset().id, 0)
            .await
            .unwrap(),
        before_counter
    );
    assert!(wallet
        .localstore
        .get_incomplete_sagas()
        .await
        .unwrap()
        .is_empty());
    assert!(wallet
        .localstore
        .list_transactions(None, None, None)
        .await
        .unwrap()
        .is_empty());
    assert!(client.captured_swap_requests().is_empty());
}

#[tokio::test]
async fn receive_without_proof_cas_changes_no_custody_counter_or_mint_state() {
    assert_unsupported_storage(false, true).await;
}

#[tokio::test]
async fn receive_without_atomic_completion_changes_no_custody_counter_or_mint_state() {
    assert_unsupported_storage(true, false).await;
}

#[tokio::test]
async fn receive_completion_limit_rejects_before_spending_and_preserves_retry() {
    use cdk_common::database::{Error as DbError, WalletDatabase};

    let (_, client) = fixture().await;
    let store = Arc::new(
        cdk_sqlite::wallet::memory::empty()
            .await
            .unwrap()
            .with_saga_read_limit(64 * 1024),
    );
    let wallet =
        crate::wallet::test_utils::create_test_wallet_with_mock(store.clone(), client.clone())
            .await;
    let mut original = test_proof_info(test_keyset().id, 64, test_mint_url());
    original.created_by_operation = Some(uuid::Uuid::new_v4());
    original.derivation_index = Some(42);
    store
        .update_proofs(vec![original.clone()], vec![])
        .await
        .unwrap();
    let large = crate::wallet::ReceiveOptions {
        amount_split_target: crate::amount::SplitTarget::Values(vec![1.into(); 64]),
        ..Default::default()
    };
    let result = wallet
        .receive_proofs(vec![original.proof.clone()], large, None, None)
        .await;
    assert!(matches!(
        result,
        Err(crate::Error::Database(DbError::SagaTooLarge { .. }))
    ));
    assert!(client.captured_swap_requests().is_empty());
    assert_eq!(
        store.get_proofs_by_ys(vec![original.y]).await.unwrap(),
        [original.clone()]
    );
    assert_eq!(
        store
            .increment_keyset_counter(&test_keyset().id, 0)
            .await
            .unwrap(),
        0
    );
    assert!(store.get_incomplete_sagas().await.unwrap().is_empty());
    assert!(store
        .list_transactions(None, None, None)
        .await
        .unwrap()
        .is_empty());
    // The same inputs can complete a smaller admitted output plan.
    client.set_post_swap_response(Ok(crate::nuts::SwapResponse {
        signatures: vec![crate::nuts::BlindSignature {
            amount: 64.into(),
            keyset_id: test_keyset().id,
            c: crate::nuts::SecretKey::generate().public_key(),
            dleq: None,
        }],
    }));
    let small = crate::wallet::ReceiveOptions {
        amount_split_target: crate::amount::SplitTarget::Values(vec![64.into()]),
        ..Default::default()
    };
    assert_eq!(
        wallet
            .receive_proofs(vec![original.proof], small, None, None)
            .await
            .unwrap(),
        crate::Amount::from(64)
    );
    assert_eq!(client.captured_swap_requests().len(), 1);
    assert_eq!(
        wallet.total_balance().await.unwrap(),
        crate::Amount::from(64)
    );
    assert!(store.get_incomplete_sagas().await.unwrap().is_empty());
}
