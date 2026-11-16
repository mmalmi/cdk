use super::*;

fn account(f: &Fixture, owner: Uuid) -> (u64, u64) {
    f.raw
        .query_row(
            "SELECT used,reserved FROM wallet_storage_accounts WHERE owner=?1",
            [owner.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
}

#[tokio::test]
async fn transfer_is_atomic_at_capacity_and_retries_after_reopen_and_release() {
    let f = Fixture::new().await;
    f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    let source = Uuid::new_v4();
    let destination = Uuid::new_v4();
    let value = vec![0xab; 4096];
    let target_bytes =
        f.db.kv_storage_bytes(
            destination,
            "capacity-test",
            "",
            "refund",
            value.len() as u64,
        )
        .await
        .unwrap();
    let marker_bytes =
        f.db.storage_reservation_transfer_bytes(source)
            .await
            .unwrap();
    f.db.reserve_storage(source, 8192, &[kv("original")])
        .await
        .unwrap();
    f.db.kv_write("capacity-test", "", "original", b"retained authority")
        .await
        .unwrap();
    f.db.reserve_storage(destination, 1024, &[kv("refund")])
        .await
        .unwrap();
    let source_before = account(&f, source);
    let target_before = account(&f, destination);
    let capacity = f.db.storage_capacity().await.unwrap().unwrap();
    let full =
        f.db.configure_storage_capacity(capacity.charged_bytes)
            .await
            .unwrap();
    // Fail after the source withdrawal and marker write, at destination credit.
    f.raw.execute_batch(&format!("CREATE TRIGGER reject_transfer BEFORE UPDATE OF reserved ON wallet_storage_accounts WHEN NEW.owner='{destination}' BEGIN SELECT RAISE(ABORT,'injected transfer failure'); END")).unwrap();
    let error =
        f.db.transfer_storage_reservation(source, destination)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("injected transfer failure"),
        "{error}"
    );
    assert_eq!(f.db.storage_capacity().await.unwrap(), Some(full));
    assert_eq!(account(&f, source), source_before);
    assert_eq!(account(&f, destination), target_before);
    assert!(f
        .db
        .kv_read("cdk_storage_transfer", "", &source.to_string())
        .await
        .unwrap()
        .is_none());
    f.raw.execute_batch("DROP TRIGGER reject_transfer").unwrap();

    assert_eq!(
        f.db.transfer_storage_reservation(source, destination)
            .await
            .unwrap(),
        full
    );
    assert_eq!(account(&f, source), (source_before.0 + marker_bytes, 0));
    let transferred = source_before.1 - source_before.0 - marker_bytes;
    assert_eq!(
        account(&f, destination),
        (target_before.0, target_before.1 + transferred)
    );
    assert!(target_bytes > target_before.1);
    let reopened = open(&f.path).await;
    assert_eq!(
        reopened
            .transfer_storage_reservation(source, destination)
            .await
            .unwrap(),
        full
    );
    assert_eq!(account(&f, destination).1, target_before.1 + transferred);
    assert!(reopened
        .transfer_storage_reservation(source, Uuid::new_v4())
        .await
        .is_err());
    reopened
        .kv_write("capacity-test", "", "refund", &value)
        .await
        .unwrap();
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(full));
    assert_eq!(
        reopened
            .kv_read("capacity-test", "", "original")
            .await
            .unwrap(),
        Some(b"retained authority".to_vec())
    );
    assert_eq!(
        reopened
            .kv_read("capacity-test", "", "refund")
            .await
            .unwrap(),
        Some(value)
    );

    let released = reopened.release_storage(destination).await.unwrap();
    assert_eq!(
        reopened
            .transfer_storage_reservation(source, destination)
            .await
            .unwrap(),
        released
    );
    assert_eq!(account(&f, destination).1, 0);
    reopened
        .kv_remove("capacity-test", "", "refund")
        .await
        .unwrap();
    let retired = reopened.release_storage(destination).await.unwrap();
    let reopened = open(&f.path).await;
    assert_eq!(
        reopened
            .transfer_storage_reservation(source, destination)
            .await
            .unwrap(),
        retired
    );
    assert!(reopened
        .transfer_storage_reservation(source, Uuid::new_v4())
        .await
        .is_err());
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(retired));
    assert!(reopened
        .retire_storage_reservation_transfer(source, Uuid::new_v4())
        .await
        .is_err());
    f.raw.execute_batch(&format!("CREATE TRIGGER reject_retirement BEFORE UPDATE OF reserved ON wallet_storage_accounts WHEN NEW.owner='{source}' BEGIN SELECT RAISE(ABORT,'injected retirement failure'); END")).unwrap();
    assert!(reopened
        .retire_storage_reservation_transfer(source, destination)
        .await
        .is_err());
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(retired));
    assert_eq!(
        reopened
            .kv_read("cdk_storage_transfer", "", &source.to_string())
            .await
            .unwrap(),
        Some(destination.as_bytes().to_vec())
    );
    f.raw
        .execute_batch("DROP TRIGGER reject_retirement")
        .unwrap();
    let retired = reopened
        .retire_storage_reservation_transfer(source, destination)
        .await
        .unwrap();
    assert_eq!(account(&f, source), (source_before.0, 0));
    assert!(reopened
        .kv_read("cdk_storage_transfer", "", &source.to_string())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        reopened
            .retire_storage_reservation_transfer(source, destination)
            .await
            .unwrap(),
        retired
    );
    assert!(reopened
        .transfer_storage_reservation(source, destination)
        .await
        .is_err());
}

#[tokio::test]
async fn transfer_rejects_insufficient_space_and_conflicting_evidence() {
    let f = Fixture::new().await;
    f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    let source = Uuid::new_v4();
    let destination = Uuid::new_v4();
    let bytes =
        f.db.storage_reservation_transfer_bytes(source)
            .await
            .unwrap();
    f.db.reserve_storage(source, bytes, &[]).await.unwrap();
    f.db.reserve_storage(destination, 1024, &[]).await.unwrap();
    let before = f.db.storage_capacity().await.unwrap();
    // Even with global headroom, marker storage cannot consume another owner's allowance.
    assert!(f
        .db
        .transfer_storage_reservation(source, destination)
        .await
        .is_err());
    assert_eq!(f.db.storage_capacity().await.unwrap(), before);
    assert_eq!(account(&f, source), (0, bytes));
    assert_eq!(account(&f, destination), (0, 1024));
    assert!(f
        .db
        .kv_read("cdk_storage_transfer", "", &source.to_string())
        .await
        .unwrap()
        .is_none());
    for (from, to) in [
        (source, source),
        (Uuid::nil(), destination),
        (source, Uuid::nil()),
        (source, Uuid::new_v4()),
    ] {
        assert!(f.db.transfer_storage_reservation(from, to).await.is_err());
        assert_eq!(f.db.storage_capacity().await.unwrap(), before);
    }
    f.db.reserve_storage(source, bytes + 2048, &[])
        .await
        .unwrap();
    f.db.transfer_storage_reservation(source, destination)
        .await
        .unwrap();
    let claim = WalletStorageClaim {
        table: "kv_store".to_owned(),
        primary_key: vec![
            b"cdk_storage_transfer".to_vec(),
            vec![],
            source.to_string().into_bytes(),
        ],
    };
    f.db.claim_storage(destination, std::slice::from_ref(&claim))
        .await
        .unwrap();
    let changed = f.db.storage_capacity().await.unwrap();
    assert!(f
        .db
        .transfer_storage_reservation(source, destination)
        .await
        .is_err());
    assert_eq!(f.db.storage_capacity().await.unwrap(), changed);
    f.db.reserve_storage(source, bytes + 2048, &[claim])
        .await
        .unwrap();
    let readmitted = f.db.storage_capacity().await.unwrap();
    assert!(f
        .db
        .transfer_storage_reservation(source, destination)
        .await
        .is_err());
    assert_eq!(f.db.storage_capacity().await.unwrap(), readmitted);
    assert!(f
        .db
        .retire_storage_reservation_transfer(source, destination)
        .await
        .is_err());
    assert_eq!(f.db.storage_capacity().await.unwrap(), readmitted);
}
