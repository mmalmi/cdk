//! Exercise paid publication through both foreign-database adapters.
use cdk_common::database::wallet::test;

use super::*;

async fn bridge() -> WalletDatabaseBridge {
    WalletDatabaseBridge::new(FfiWalletDatabaseWrapper::new(
        cdk_sqlite::wallet::memory::empty().await.unwrap(),
    ))
}

#[tokio::test]
async fn complete_melt_bridge_contract() {
    test::complete_melt_contract(bridge().await).await;
}

#[tokio::test]
async fn complete_melt_bridge_exact_snapshots() {
    test::complete_melt_exact_snapshots(bridge().await).await;
}

#[tokio::test]
async fn complete_melt_bridge_concurrent() {
    test::complete_melt_concurrent(bridge().await).await;
}

#[tokio::test]
async fn complete_melt_bridge_invalid() {
    test::complete_melt_invalid(bridge().await).await;
}

#[tokio::test]
async fn complete_melt_bridge_fee_budget() {
    test::complete_melt_fee_budget(bridge().await).await;
}
