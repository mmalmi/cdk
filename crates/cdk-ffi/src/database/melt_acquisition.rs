//! Verify the native/foreign database bridge preserves atomic acquisition.
use cdk_common::database::wallet::test;

use super::*;

async fn bridge() -> WalletDatabaseBridge {
    WalletDatabaseBridge::new(FfiWalletDatabaseWrapper::new(
        cdk_sqlite::wallet::memory::empty().await.unwrap(),
    ))
}

#[tokio::test]
async fn acquire_melt_bridge_batch_roundtrip() {
    test::acquire_melt_batch(bridge().await).await;
}

#[tokio::test]
async fn acquire_melt_bridge_invalid_intent() {
    test::acquire_melt_invalid(bridge().await).await;
}

#[tokio::test]
async fn acquire_melt_bridge_competing_writers() {
    test::acquire_melt_competing(bridge().await).await;
}

#[tokio::test]
async fn acquire_melt_bridge_rejects_malformed_saga() {
    let db = FfiWalletDatabaseWrapper::new(cdk_sqlite::wallet::memory::empty().await.unwrap());
    let (quote, _, _) = test::melt_acquisition_fixture();
    assert!(db
        .acquire_melt("{}".to_owned(), quote.into())
        .await
        .is_err());
}

#[tokio::test]
async fn acquire_melt_bridge_preserves_payment_evidence() {
    test::acquire_melt_payment_evidence(bridge().await).await;
}
