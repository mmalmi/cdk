//! Cleanup must retain exact financial authority on failure or concurrent submission.
use std::sync::Arc;

use super::tests::paid_signed_mint_quote;
use super::*;
use crate::wallet::send::saga::interposed_store::{Action, Interposed};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_mint_url, MockMintConnector,
};

#[tokio::test]
async fn issue_retirement_failed_release_preserves_original_journal() {
    let db = create_test_db().await;
    let store = Arc::new(Interposed::new(db.clone()));
    let mint = Arc::new(MockMintConnector::new());
    let wallet = create_test_wallet_with_mock(store.clone(), mint.clone()).await;
    let quote = paid_signed_mint_quote(test_mint_url(), 8.into(), SecretKey::generate());
    db.add_mint_quote(quote.clone()).await.unwrap();
    let prepared = MintSaga::new(&wallet)
        .prepare(&quote.id, SplitTarget::None, None)
        .await
        .unwrap();
    let original = prepared.state_data.saga;
    let owned = db.get_mint_quote(&quote.id).await.unwrap();
    *store.before_issue_retirement.lock().unwrap() = Some(Action::Fail);
    assert!(wallet.resume_issue_saga(&original).await.is_err());
    assert_eq!(db.get_saga(&original.id).await.unwrap(), Some(original));
    assert_eq!(db.get_mint_quote(&quote.id).await.unwrap(), owned);
    assert!(mint.post_mint_requests().is_empty());
}

#[tokio::test]
async fn issue_retirement_stale_prepared_snapshot_cannot_cancel_submission() {
    let db = create_test_db().await;
    let mint = Arc::new(MockMintConnector::new());
    let wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
    let quote = paid_signed_mint_quote(test_mint_url(), 8.into(), SecretKey::generate());
    db.add_mint_quote(quote.clone()).await.unwrap();
    let prepared = MintSaga::new(&wallet)
        .prepare(&quote.id, SplitTarget::None, None)
        .await
        .unwrap();
    let original = prepared.state_data.saga;
    let mut submitted = original.clone();
    submitted.update_state(WalletSagaState::Issue(IssueSagaState::MintRequested));
    assert!(db.update_saga(submitted.clone()).await.unwrap());
    let owned = db.get_mint_quote(&quote.id).await.unwrap();
    assert!(wallet.resume_issue_saga(&original).await.is_err());
    assert_eq!(db.get_saga(&original.id).await.unwrap(), Some(submitted));
    assert_eq!(db.get_mint_quote(&quote.id).await.unwrap(), owned);
    assert!(mint.post_mint_requests().is_empty());
}

#[tokio::test]
async fn issue_retirement_failed_completion_recovers_original_proofs() {
    let db = create_test_db().await;
    let store = Arc::new(Interposed::new(db.clone()));
    let mint = Arc::new(MockMintConnector::new());
    mint.enable_mint_signing();
    let wallet = create_test_wallet_with_mock(store.clone(), mint.clone()).await;
    let quote = paid_signed_mint_quote(test_mint_url(), 8.into(), SecretKey::generate());
    db.add_mint_quote(quote.clone()).await.unwrap();
    let prepared = MintSaga::new(&wallet)
        .prepare(&quote.id, SplitTarget::None, None)
        .await
        .unwrap();
    let id = prepared.state_data.saga.id;
    let originals = prepared
        .state_data
        .checkpoint()
        .unwrap()
        .premint_secrets
        .clone();
    *store.before_issue_retirement.lock().unwrap() = Some(Action::Fail);
    assert!(prepared.execute().await.is_err());
    let saved = db.get_saga(&id).await.unwrap().unwrap();
    assert_eq!(
        saved.state,
        WalletSagaState::Issue(IssueSagaState::MintRequested)
    );
    assert!(db
        .get_proofs(None, None, None, None)
        .await
        .unwrap()
        .is_empty());
    let mut receipts = db.list_transactions(None, None, None).await.unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].status, TransactionStatus::Pending);
    assert_eq!(
        db.get_mint_quote(&quote.id)
            .await
            .unwrap()
            .unwrap()
            .used_by_operation,
        Some(id.to_string())
    );
    assert_eq!(
        wallet.resume_issue_saga(&saved).await.unwrap(),
        crate::wallet::recovery::RecoveryAction::Recovered
    );
    assert!(db.get_saga(&id).await.unwrap().is_none());
    assert!(db
        .get_mint_quote(&quote.id)
        .await
        .unwrap()
        .unwrap()
        .used_by_operation
        .is_none());
    let recovered = db.get_proofs(None, None, None, None).await.unwrap();
    let expected: std::collections::HashMap<_, _> = originals
        .secrets
        .iter()
        .map(|secret| (&secret.secret, secret.derivation_index))
        .collect();
    assert_eq!(recovered.len(), expected.len());
    assert!(recovered
        .iter()
        .all(|proof| proof.created_by_operation == Some(id)
            && expected.get(&proof.proof.secret) == Some(&proof.derivation_index)));
    receipts[0].status = TransactionStatus::Completed;
    assert_eq!(
        db.list_transactions(None, None, None).await.unwrap(),
        receipts
    );
}

#[tokio::test]
async fn issue_retirement_unsupported_store_never_acquires_or_submits() {
    let db = create_test_db().await;
    let mut store = Interposed::new(db.clone());
    store.completion_supported = false;
    let mint = Arc::new(MockMintConnector::new());
    let wallet = create_test_wallet_with_mock(Arc::new(store), mint.clone()).await;
    let quote = paid_signed_mint_quote(test_mint_url(), 8.into(), SecretKey::generate());
    db.add_mint_quote(quote.clone()).await.unwrap();
    assert!(MintSaga::new(&wallet)
        .prepare(&quote.id, SplitTarget::None, None)
        .await
        .is_err());
    assert_eq!(db.get_mint_quote(&quote.id).await.unwrap(), Some(quote));
    assert!(db.get_incomplete_sagas().await.unwrap().is_empty());
    assert!(mint.post_mint_requests().is_empty());
}

#[tokio::test]
async fn issue_retirement_quote_status_cannot_release_missing_original_authority() {
    let db = create_test_db().await;
    let mint = Arc::new(MockMintConnector::new());
    let wallet = create_test_wallet_with_mock(db.clone(), mint).await;
    let mut quote = paid_signed_mint_quote(test_mint_url(), 8.into(), SecretKey::generate());
    quote.used_by_operation = Some(uuid::Uuid::new_v4().to_string());
    db.add_mint_quote(quote.clone()).await.unwrap();
    assert!(wallet.check_mint_quote_status(&quote.id).await.is_err());
    assert_eq!(db.get_mint_quote(&quote.id).await.unwrap(), Some(quote));
    assert!(db.get_incomplete_sagas().await.unwrap().is_empty());
}
