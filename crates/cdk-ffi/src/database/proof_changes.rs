//! Exercise both directions of the foreign database callback bridge.
use cdk_common::database::wallet::test;

use super::*;

async fn bridge() -> WalletDatabaseBridge {
    WalletDatabaseBridge::new(FfiWalletDatabaseWrapper::new(
        cdk_sqlite::wallet::memory::empty().await.unwrap(),
    ))
}

#[tokio::test]
async fn conditional_proofs_bridge_is_atomic() {
    test::compare_and_swap_proofs(bridge().await).await;
}

#[tokio::test]
async fn conditional_proofs_bridge_preserves_all_fields() {
    test::compare_and_swap_proofs_fields(bridge().await).await;
}

#[tokio::test]
async fn conditional_proofs_bridge_preserves_saga_fence() {
    test::compare_and_swap_proofs_saga_guard(bridge().await).await;
}

#[tokio::test]
async fn conditional_proofs_bridge_has_one_concurrent_winner() {
    test::compare_and_swap_proofs_concurrent(bridge().await).await;
}

#[tokio::test]
async fn conditional_proofs_bridge_rejects_malformed_saga() {
    let db = FfiWalletDatabaseWrapper::new(cdk_sqlite::wallet::memory::empty().await.unwrap());
    assert!(db
        .compare_and_swap_proofs(vec![], Some("{}".to_owned()))
        .await
        .is_err());
}

#[tokio::test]
async fn complete_saga_bridge_preserves_atomic_contract() {
    test::complete_saga_contract(bridge().await).await;
}

#[tokio::test]
async fn complete_saga_bridge_has_one_winner() {
    test::complete_saga_concurrent(bridge().await).await;
}

#[tokio::test]
async fn proof_inventory_budget_survives_both_ffi_storage_directions() {
    use std::str::FromStr;
    let db = bridge().await;
    let mint: cdk::mint_url::MintUrl = "https://inventory.example".parse().unwrap();
    let mut original = cdk::types::ProofInfo::new(
        cdk::nuts::Proof::new(
            8.into(),
            cdk::nuts::Id::from_str("00916bbf7ef91a36").unwrap(),
            cdk::secret::Secret::generate(),
            cdk::nuts::SecretKey::generate().public_key(),
        ),
        mint.clone(),
        cdk::nuts::State::Unspent,
        cdk::nuts::CurrencyUnit::Sat,
    )
    .unwrap()
    .with_derivation_index(7);
    original.created_by_operation = Some(uuid::Uuid::new_v4());
    db.update_proofs(vec![original.clone()], vec![])
        .await
        .unwrap();
    for (count, bytes) in [(0, 4096), (1, 0)] {
        assert!(db
            .get_unspent_proofs_bounded(mint.clone(), cdk::nuts::CurrencyUnit::Sat, count, bytes)
            .await
            .is_err());
    }
    assert_eq!(
        db.get_unspent_proofs_bounded(mint, cdk::nuts::CurrencyUnit::Sat, 1, 4096)
            .await
            .unwrap(),
        vec![original]
    );
}

#[tokio::test]
async fn incomplete_saga_pages_survive_both_ffi_storage_directions() {
    test::incomplete_saga_pages(bridge().await).await;
}

#[tokio::test]
async fn kv_read_bounded_survives_both_ffi_storage_directions() {
    let db = bridge().await;
    assert_eq!(
        db.kv_read_bounded("bounded", "", "key", 0).await.unwrap(),
        None
    );
    db.kv_write("bounded", "", "key", b"kept").await.unwrap();
    assert!(db.kv_read_bounded("bounded", "", "key", 3).await.is_err());
    assert_eq!(
        db.kv_read_bounded("bounded", "", "key", 4).await.unwrap(),
        Some(b"kept".to_vec())
    );
}

#[tokio::test]
async fn output_preparation_bridge_preserves_receipt_and_counter_atomicity() {
    test::output_preparation_contract(bridge().await).await;
    test::output_preparation_concurrent(bridge().await).await;
}
