use cdk_common::database::WalletDatabase;

use super::test_db::{open, Fixture};
use crate::WalletKvChange;

fn change<'a>(
    key: &'a str,
    expected: Option<&'a [u8]>,
    replacement: Option<&'a [u8]>,
) -> WalletKvChange<'a> {
    WalletKvChange {
        primary_namespace: "atomic",
        secondary_namespace: "",
        key,
        expected,
        replacement,
    }
}

#[tokio::test]
async fn batch_mismatch_failure_and_capacity_preserve_original_records() {
    let f = Fixture::new().await;
    f.db.kv_write("atomic", "", "old", &[7; 4096])
        .await
        .unwrap();
    f.db.kv_write("atomic", "", "cursor", b"before")
        .await
        .unwrap();
    let capacity = f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    f.db.configure_storage_capacity(capacity.charged_bytes)
        .await
        .unwrap();
    let before = f.db.storage_capacity().await.unwrap();
    let next = vec![9; 2048];
    let changes = [
        change("cursor", Some(b"before"), Some(b"after")),
        change("new", None, Some(&next)),
        change("old", Some(&[7; 4096]), None),
    ];
    f.raw
        .execute_batch(
            "CREATE TRIGGER fail_batch BEFORE INSERT ON kv_store
        WHEN NEW.primary_namespace='atomic' AND NEW.key='new'
        BEGIN SELECT RAISE(ABORT,'test atomic write interrupted'); END;",
        )
        .unwrap();
    assert!(f.db.kv_compare_and_swap_batch(&changes).await.is_err());
    f.raw.execute_batch("DROP TRIGGER fail_batch;").unwrap();
    let oversized = vec![1; 8192];
    let mut rejected = changes;
    rejected[1].replacement = Some(&oversized);
    assert!(f.db.kv_compare_and_swap_batch(&rejected).await.is_err());
    rejected = changes;
    rejected[0].expected = Some(b"changed");
    assert!(!f.db.kv_compare_and_swap_batch(&rejected).await.unwrap());
    assert!(f
        .db
        .kv_compare_and_swap_batch(&[changes[0], changes[0]])
        .await
        .is_err());
    let reopened = open(&f.path).await;
    assert_eq!(reopened.storage_capacity().await.unwrap(), before);
    assert_eq!(
        reopened.kv_read("atomic", "", "old").await.unwrap(),
        Some(vec![7; 4096])
    );
    assert_eq!(
        reopened.kv_read("atomic", "", "cursor").await.unwrap(),
        Some(b"before".to_vec())
    );
    assert_eq!(reopened.kv_read("atomic", "", "new").await.unwrap(), None);
    assert!(reopened.kv_compare_and_swap_batch(&changes).await.unwrap());
    assert!(!reopened.kv_compare_and_swap_batch(&changes).await.unwrap());
    assert_eq!(reopened.kv_read("atomic", "", "old").await.unwrap(), None);
    assert_eq!(
        reopened.kv_read("atomic", "", "cursor").await.unwrap(),
        Some(b"after".to_vec())
    );
    assert_eq!(
        reopened.kv_read("atomic", "", "new").await.unwrap(),
        Some(next)
    );
    assert!(
        reopened
            .storage_capacity()
            .await
            .unwrap()
            .unwrap()
            .charged_bytes
            < before.unwrap().charged_bytes
    );
}

#[tokio::test]
async fn competing_batches_cannot_both_replace_the_same_original() {
    let f = Fixture::new().await;
    f.db.kv_write("atomic", "", "cursor", b"before")
        .await
        .unwrap();
    let other = open(&f.path).await;
    let first = [
        change("cursor", Some(b"before"), Some(b"first")),
        change("first", None, Some(b"owned")),
    ];
    let second = [
        change("cursor", Some(b"before"), Some(b"second")),
        change("second", None, Some(b"owned")),
    ];
    let (a, b) = tokio::join!(
        f.db.kv_compare_and_swap_batch(&first),
        other.kv_compare_and_swap_batch(&second)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_ne!(a, b);
    let winner: &[u8] = if a { b"first" } else { b"second" };
    assert_eq!(
        f.db.kv_read("atomic", "", "cursor")
            .await
            .unwrap()
            .as_deref(),
        Some(winner)
    );
    for (key, present) in [("first", a), ("second", b)] {
        assert_eq!(
            f.db.kv_read("atomic", "", key).await.unwrap().is_some(),
            present
        );
    }
}
