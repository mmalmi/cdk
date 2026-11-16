//! The foreign bridge retains the same all-or-nothing issue contract.
use cdk_common::database::wallet::test;

use super::*;

async fn bridge() -> WalletDatabaseBridge {
    WalletDatabaseBridge::new(FfiWalletDatabaseWrapper::new(
        cdk_sqlite::wallet::memory::empty().await.unwrap(),
    ))
}

#[tokio::test]
async fn issue_acquisition_bridge_contract() {
    test::issue_acquisition_contract(bridge().await).await;
}

#[tokio::test]
async fn issue_acquisition_bridge_invalid() {
    test::issue_acquisition_invalid(bridge().await).await;
}

#[tokio::test]
async fn issue_acquisition_bridge_concurrent() {
    test::issue_acquisition_concurrent(bridge().await).await;
}
