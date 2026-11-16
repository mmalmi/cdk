//! Foreign database callbacks preserve exact terminal ownership checks.
use super::*;
use cdk_common::database::wallet::test;

async fn bridge() -> WalletDatabaseBridge {
    WalletDatabaseBridge::new(FfiWalletDatabaseWrapper::new(
        cdk_sqlite::wallet::memory::empty().await.unwrap(),
    ))
}
#[tokio::test]
async fn issue_retirement_bridge_contract() {
    test::issue_retirement_contract(bridge().await).await;
}
#[tokio::test]
async fn issue_retirement_bridge_concurrent() {
    test::issue_retirement_concurrent(bridge().await).await;
}
#[tokio::test]
async fn issue_retirement_bridge_invalid() {
    test::issue_retirement_invalid(bridge().await).await;
}
