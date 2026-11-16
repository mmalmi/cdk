use std::sync::Arc;

use cdk_common::nut00::KnownMethod;
use cdk_common::{MeltQuoteState, PaymentMethod};

use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_melt_quote, MockMintConnector,
};

#[tokio::test]
async fn stale_status_preserves_newer_paid_quote() {
    let db = create_test_db().await;
    let wallet = create_test_wallet_with_mock(db.clone(), Arc::new(MockMintConnector::new())).await;
    let mut quote = test_melt_quote();
    db.add_melt_quote(quote.clone()).await.unwrap();
    quote = db.get_melt_quote(&quote.id).await.unwrap().unwrap();
    let stale = quote.clone();
    quote.state = MeltQuoteState::Paid;
    quote.payment_proof = Some("verified payment".to_string());
    db.add_melt_quote(quote.clone()).await.unwrap();
    let completed = db.get_melt_quote(&quote.id).await.unwrap().unwrap();

    for state in [MeltQuoteState::Unpaid, MeltQuoteState::Pending] {
        let mut observed = stale.clone();
        wallet
            .update_melt_quote_state(&mut observed, state, None)
            .await
            .unwrap();
        assert_eq!(observed, completed);
        assert_eq!(
            db.get_melt_quote(&quote.id).await.unwrap(),
            Some(completed.clone())
        );
    }
}

#[tokio::test]
async fn stale_status_preserves_newer_operation_owner() {
    let db = create_test_db().await;
    let wallet = create_test_wallet_with_mock(db.clone(), Arc::new(MockMintConnector::new())).await;
    let mut quote = test_melt_quote();
    db.add_melt_quote(quote.clone()).await.unwrap();
    quote = db.get_melt_quote(&quote.id).await.unwrap().unwrap();
    let mut stale = quote.clone();
    quote.state = MeltQuoteState::Pending;
    quote.used_by_operation = Some(uuid::Uuid::new_v4().to_string());
    db.add_melt_quote(quote.clone()).await.unwrap();
    let owned = db.get_melt_quote(&quote.id).await.unwrap().unwrap();
    wallet
        .update_melt_quote_state(&mut stale, MeltQuoteState::Unpaid, None)
        .await
        .unwrap();
    assert_eq!(stale, owned);
    assert_eq!(db.get_melt_quote(&quote.id).await.unwrap(), Some(owned));
}

#[tokio::test]
async fn completed_status_is_read_only_for_every_method() {
    let db = create_test_db().await;
    // No response is configured: completed local payments need no mint request.
    let wallet = create_test_wallet_with_mock(db.clone(), Arc::new(MockMintConnector::new())).await;
    for method in [
        PaymentMethod::BOLT11,
        PaymentMethod::BOLT12,
        PaymentMethod::Known(KnownMethod::Onchain),
        PaymentMethod::Custom("test".to_string()),
    ] {
        let mut quote = test_melt_quote();
        quote.payment_method = method;
        quote.state = MeltQuoteState::Paid;
        quote.payment_proof = Some("verified payment".to_string());
        db.add_melt_quote(quote.clone()).await.unwrap();
        let completed = db.get_melt_quote(&quote.id).await.unwrap().unwrap();
        assert_eq!(
            wallet.check_melt_quote_status(&quote.id).await.unwrap(),
            completed
        );
        assert_eq!(db.get_melt_quote(&quote.id).await.unwrap(), Some(completed));
    }
}
