use std::future::Future;
use std::task::Poll;

use super::*;

#[tokio::test]
async fn repository_entry_limit_rejects_zero() {
    assert!(matches!(
        WalletRepositoryBuilder::new().with_max_wallets(0).build().await,
        Err(Error::Custom(message)) if message == "maximum wallet count must be positive"
    ));
}

#[tokio::test]
async fn concurrent_creation_shares_capacity_and_existing_entries_remain_usable() {
    let mut repo = create_test_repository().await;
    repo.max_wallets = Some(1);
    let first: MintUrl = "https://first.example".parse().unwrap();
    let second: MintUrl = "https://second.example".parse().unwrap();
    let guard = repo.wallets.write().await;
    let mut create = Box::pin(repo.create_wallet(first.clone(), CurrencyUnit::Sat, None));
    let mut get = Box::pin(repo.get_or_create_wallet(second.clone(), CurrencyUnit::Sat, None));
    std::future::poll_fn(|cx| {
        assert!(create.as_mut().poll(cx).is_pending());
        assert!(get.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(guard);
    let (created, refused) = tokio::join!(create, get);
    created.unwrap();
    assert!(matches!(
        refused,
        Err(Error::WalletRepositoryTooLarge {
            wallets: 2,
            max_wallets: 1
        })
    ));
    assert!(!repo.has_mint(&second).await);
    assert!(repo.localstore.get_mints().await.unwrap().is_empty());

    repo.set_mint_config(
        first.clone(),
        CurrencyUnit::Sat,
        WalletConfig::new().with_target_proof_count(7),
    )
    .await
    .unwrap();
    let existing = repo
        .get_or_create_wallet(
            first.clone(),
            CurrencyUnit::Sat,
            Some(WalletConfig::new().with_target_proof_count(99)),
        )
        .await
        .unwrap();
    assert_eq!(existing.target_proof_count, 7);

    // A second unit is another entry, and refusal precedes client construction.
    repo.proxy_config = Some(unsupported_proxy_url());
    assert!(matches!(
        repo.create_wallet(first, CurrencyUnit::Usd, None).await,
        Err(Error::WalletRepositoryTooLarge {
            wallets: 2,
            max_wallets: 1
        })
    ));
    assert_eq!(repo.get_wallets().await.len(), 1);
}

#[tokio::test]
async fn startup_preflights_all_units_and_preserves_the_published_repository() {
    let localstore = Arc::new(cdk_sqlite::wallet::memory::empty().await.unwrap());
    let original: MintUrl = "https://original.example".parse().unwrap();
    localstore.add_mint(original.clone(), None).await.unwrap();
    let mut repo = WalletRepositoryBuilder::new()
        .localstore(localstore.clone())
        .seed([0; 64])
        .with_max_wallets(2)
        .build()
        .await
        .unwrap();
    let extra: MintUrl = "https://extra.example".parse().unwrap();
    localstore
        .add_mint(
            extra.clone(),
            Some(mint_info_with_units(vec![
                CurrencyUnit::Sat,
                CurrencyUnit::Usd,
            ])),
        )
        .await
        .unwrap();
    // Any wallet construction would fail with a different error.
    repo.proxy_config = Some(unsupported_proxy_url());
    assert!(matches!(
        repo.load_wallets().await,
        Err(Error::WalletRepositoryTooLarge {
            wallets: 3,
            max_wallets: 2
        })
    ));
    assert!(repo.has_wallet(&original, &CurrencyUnit::Sat).await);
    assert!(!repo.has_mint(&extra).await);
    assert_eq!(repo.get_wallets().await.len(), 1);
    assert_eq!(localstore.get_mints().await.unwrap().len(), 2);
}
