use std::collections::BTreeMap;
use std::future::{poll_fn, Future};
use std::sync::atomic::Ordering;
use std::task::Poll;

use super::*;
use crate::nuts::SecretKey;
use crate::wallet::test_utils::{test_keyset, test_mint_info, test_mint_url, MockMintConnector};
use crate::Amount;

#[tokio::test]
async fn database_metadata_read_refusal_does_not_fall_back_to_http() {
    let database = cdk_sqlite::wallet::memory::empty().await.unwrap();
    let info = MintInfo::new().description("x".repeat(2048));
    database
        .add_mint(test_mint_url(), Some(info.clone()))
        .await
        .unwrap();
    let database = Arc::new(database.with_mint_metadata_read_limits(2, 2, 1024));
    let store: Arc<dyn WalletDatabase<database::Error> + Send + Sync> = database.clone();
    let mock = Arc::new(MockMintConnector::new());
    let client: Arc<dyn MintConnector + Send + Sync> = mock.clone();
    let cache = MintMetadataCache::with_limits(
        test_mint_url(),
        Some(MintMetadataLimits {
            max_keysets: 2,
            max_bytes: 64 * 1024,
        }),
    );
    let original = cache.metadata.load().clone();
    for _ in 0..2 {
        for result in [
            cache.load(&store, &client).await,
            cache.load_from_mint(&store, &client).await,
        ] {
            let error = result.unwrap_err();
            assert!(matches!(
                error,
                Error::Database(database::Error::MintMetadataReadTooLarge { .. })
            ));
            assert!(!error.is_definitive_failure());
            assert!(cache.get_cached().is_none());
            assert!(Arc::ptr_eq(&cache.metadata.load().clone(), &original));
        }
    }
    assert_eq!(mock.metadata_requests.load(Ordering::SeqCst), 0);
    drop(store);
    let database = Arc::try_unwrap(database)
        .unwrap()
        .with_mint_metadata_read_limits(2, 2, 64 * 1024);
    assert_eq!(
        database.get_mint(test_mint_url()).await.unwrap(),
        Some(info)
    );
}

#[tokio::test]
async fn queued_database_load_and_http_refresh_preserve_the_newest_snapshot() {
    let store: Arc<dyn WalletDatabase<database::Error> + Send + Sync> =
        Arc::new(cdk_sqlite::wallet::memory::empty().await.unwrap());
    let original = test_keyset();
    let info = KeySetInfo {
        id: original.id,
        unit: original.unit.clone(),
        active: original.active.unwrap_or(true),
        input_fee_ppk: original.input_fee_ppk,
        final_expiry: original.final_expiry,
    };
    store
        .add_mint(test_mint_url(), Some(test_mint_info()))
        .await
        .unwrap();
    store
        .add_mint_keysets(test_mint_url(), vec![info])
        .await
        .unwrap();
    store.add_keys(original.clone()).await.unwrap();
    let mut newer = original.clone();
    newer.keys = Keys::new(BTreeMap::from([(
        Amount::from(1_u64),
        SecretKey::from_slice(&[42; 32]).unwrap().public_key(),
    )]));
    newer.id = Id::v1_from_keys(&newer.keys);
    let mock = Arc::new(MockMintConnector::new());
    *mock.keysets.lock().unwrap() = vec![original, newer];
    let client: Arc<dyn MintConnector + Send + Sync> = mock.clone();
    let limits = MintMetadataLimits {
        max_keysets: 2,
        max_bytes: 64 * 1024,
    };
    let cache = MintMetadataCache::with_limits(test_mint_url(), Some(limits));

    // Pause a real initial DB load behind the same lock held by HTTP refreshes.
    let guard = cache.fetch_lock.lock().await;
    let mut reading = Box::pin(cache.load_cached(&store));
    poll_fn(|cx| {
        assert!(reading.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;

    // Exercise refresh admission/publication, leaving persistence behind as it
    // can be after a failed best-effort write. No database implementation is mocked.
    let admitted = cache
        .fetch_from_http(Some(&client), None, Arc::new(MintMetadata::default()))
        .await
        .unwrap();
    assert_eq!(admitted.keysets.len(), 2);
    drop(guard);
    let observed = reading.await.unwrap();
    assert!(Arc::ptr_eq(&observed, &admitted));
    assert!(Arc::ptr_eq(&cache.get_cached().unwrap(), &admitted));
    assert_eq!(
        store
            .get_mint_keysets(test_mint_url())
            .await
            .unwrap()
            .unwrap()
            .len(),
        1
    );

    // Queue the opposite order: initial DB publication must not satisfy an
    // explicit HTTP refresh that was already waiting on the same mutex.
    let cold = MintMetadataCache::with_limits(test_mint_url(), Some(limits));
    let guard = cold.fetch_lock.lock().await;
    let mut database_read = Box::pin(cold.load_cached(&store));
    let mut refresh = Box::pin(cold.load_from_mint(&store, &client));
    poll_fn(|cx| {
        assert!(database_read.as_mut().poll(cx).is_pending());
        assert!(refresh.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(guard);
    let from_database = database_read.await.unwrap();
    assert_eq!(from_database.keysets.len(), 1);
    let refreshed = refresh.await.unwrap();
    assert_eq!(refreshed.keysets.len(), 2);
    assert!(!Arc::ptr_eq(&refreshed, &from_database));

    mock.mint_info.lock().unwrap().description = Some("x".repeat(limits.max_bytes as usize));
    assert!(matches!(
        cache.load_from_mint(&store, &client).await,
        Err(Error::MintMetadataTooLarge { .. })
    ));
    let refused = cache.get_cached().unwrap();
    assert!(Arc::ptr_eq(&refused, &admitted));
    assert_eq!(refused.status.version, admitted.status.version);
    assert_eq!(refused.status.updated_at, admitted.status.updated_at);
}
