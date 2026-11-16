mod admission;
mod output_keys;
mod reclaim;
mod reservation;
mod sizing;
mod transfer;

use cdk_common::database::WalletDatabase;
use cdk_common::wallet::{
    OperationData, SendOperationData, SendPreparation, SendSagaState, SwapOperationData,
    SwapSagaState, WalletSaga, WalletSagaState,
};
use uuid::Uuid;

use super::test_db::{open, Fixture};
use crate::{WalletSqliteDatabase, WalletStorageClaim};

fn kv(key: &str) -> WalletStorageClaim {
    WalletStorageClaim {
        table: "kv_store".to_owned(),
        primary_key: vec![
            b"capacity-test".to_vec(),
            Vec::new(),
            key.as_bytes().to_vec(),
        ],
    }
}

// These records exercise storage admission only, without mint-signed money.
fn saga(id: u128) -> WalletSaga {
    WalletSaga::new(
        Uuid::from_u128(id),
        WalletSagaState::Swap(SwapSagaState::ProofsReserved),
        1.into(),
        "https://mint.example".parse().unwrap(),
        cdk_common::CurrencyUnit::Sat,
        OperationData::Swap(SwapOperationData {
            checkpoint: None,
            input_amount: 1.into(),
            output_amount: 1.into(),
        }),
    )
}

#[tokio::test]
async fn send_preparation_reserves_exact_cancellation_growth_atomically() {
    let f = Fixture::new().await;
    let initial = f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    let mut intent = saga(20);
    intent.state = WalletSagaState::Send(SendSagaState::PreparedAcquiring);
    intent.kind = intent.state.kind();
    intent.data = OperationData::Send(SendOperationData {
        preparation: Some(Box::new(SendPreparation {
            inputs: vec![],
            metadata: [("caller".to_owned(), "\"雪\u{0}".repeat(128))].into(),
            retain_cancellation: true,
        })),
        confirmation: None,
        reclaim: None,
        amount: 1.into(),
        memo: Some("arbitrary caller data".to_owned()),
        counter_start: None,
        counter_end: None,
        token: None,
        proofs: None,
    });
    f.db.add_saga(intent.clone()).await.unwrap();
    let admitted = f.db.storage_capacity().await.unwrap().unwrap();
    f.db.delete_saga(&intent.id).await.unwrap();
    f.db.release_storage(intent.id).await.unwrap();
    assert_eq!(f.db.storage_capacity().await.unwrap(), Some(initial));

    // One byte short must reject the whole admission, including its new parent.
    f.db.configure_storage_capacity(admitted.charged_bytes - 1)
        .await
        .unwrap();
    assert!(f.db.add_saga(intent.clone()).await.is_err());
    assert!(f.db.get_saga(&intent.id).await.unwrap().is_none());
    assert_eq!(
        f.db.storage_capacity()
            .await
            .unwrap()
            .unwrap()
            .charged_bytes,
        initial.charged_bytes
    );
    let full =
        f.db.configure_storage_capacity(admitted.charged_bytes)
            .await
            .unwrap();
    f.db.add_saga(intent.clone()).await.unwrap();
    let reopened = open(&f.path).await;
    for state in [
        SendSagaState::ProofsReserved,
        SendSagaState::PreparedCancelling,
        SendSagaState::PreparedCancelled,
    ] {
        intent.update_state(WalletSagaState::Send(state));
        assert!(reopened.update_saga(intent.clone()).await.unwrap());
        let current = reopened.storage_capacity().await.unwrap().unwrap();
        assert_eq!(current.charged_bytes, full.maximum_bytes);
    }
    f.raw.execute_batch("CREATE TRIGGER reject_cancel_release BEFORE UPDATE OF reserved ON wallet_storage_accounts WHEN OLD.reserved>0 AND NEW.reserved=0 BEGIN SELECT RAISE(ABORT,'injected release failure'); END").unwrap();
    assert!(reopened.delete_saga(&intent.id).await.is_err());
    assert_eq!(
        reopened.get_saga(&intent.id).await.unwrap(),
        Some(intent.clone())
    );
    assert_eq!(
        reopened
            .storage_capacity()
            .await
            .unwrap()
            .unwrap()
            .charged_bytes,
        full.maximum_bytes
    );
    f.raw
        .execute_batch("DROP TRIGGER reject_cancel_release")
        .unwrap();
    reopened.delete_saga(&intent.id).await.unwrap();
    assert_eq!(
        reopened
            .storage_capacity()
            .await
            .unwrap()
            .unwrap()
            .charged_bytes,
        initial.charged_bytes,
        "acknowledged cancellation must release its unused allowance"
    );
    reopened.delete_saga(&intent.id).await.unwrap();
    assert_eq!(
        reopened
            .storage_capacity()
            .await
            .unwrap()
            .unwrap()
            .charged_bytes,
        initial.charged_bytes
    );
}

#[tokio::test]
async fn reservations_complete_at_capacity_and_release_only_unused_storage() {
    let f = Fixture::new().await;
    let owner = Uuid::from_u128(1);
    let future = kv("counter");
    assert!(f.db.storage_capacity().await.unwrap().is_none());
    f.db.kv_write("capacity-test", "", "protected", b"original")
        .await
        .unwrap();
    assert!(f.db.configure_storage_capacity(1).await.is_err());
    assert_eq!(f.db.storage_capacity().await.unwrap(), None);
    let enabled = f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    assert!(f
        .db
        .claim_storage(owner, std::slice::from_ref(&future))
        .await
        .is_err());
    assert_eq!(f.db.storage_capacity().await.unwrap(), Some(enabled));
    f.db.reserve_storage(owner, 1, &[]).await.unwrap();
    let small = f.db.storage_capacity().await.unwrap();
    assert!(f
        .db
        .claim_storage(owner, std::slice::from_ref(&future))
        .await
        .is_err());
    assert_eq!(f.db.storage_capacity().await.unwrap(), small);
    f.db.reserve_storage(
        owner,
        131072,
        &[kv("protected"), kv("future"), kv("unused")],
    )
    .await
    .unwrap();
    let before = f.db.storage_capacity().await.unwrap();
    let invalid = WalletStorageClaim {
        table: "wallet_storage_total".to_owned(),
        primary_key: vec![b"1".to_vec()],
    };
    assert!(f
        .db
        .reserve_storage(Uuid::from_u128(2), 2048, &[invalid])
        .await
        .is_err());
    assert_eq!(f.db.storage_capacity().await.unwrap(), before);
    f.db.kv_write("capacity-test", "", "fill", &[])
        .await
        .unwrap();
    let capacity = f.db.storage_capacity().await.unwrap().unwrap();
    f.db.kv_write(
        "capacity-test",
        "",
        "fill",
        &vec![0; (capacity.maximum_bytes - capacity.charged_bytes) as usize],
    )
    .await
    .unwrap();
    let full = f.db.storage_capacity().await.unwrap().unwrap();
    assert_eq!(full.maximum_bytes, full.charged_bytes);
    assert!(f
        .db
        .configure_storage_capacity(full.maximum_bytes - 1)
        .await
        .is_err());
    assert!(f
        .db
        .kv_write("capacity-test", "", "unrelated", &[1])
        .await
        .is_err());
    assert!(f
        .db
        .kv_read("capacity-test", "", "unrelated")
        .await
        .unwrap()
        .is_none());
    let reopened = open(&f.path).await;
    reopened
        .claim_storage(owner, std::slice::from_ref(&future))
        .await
        .unwrap();
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(full));
    reopened
        .claim_storage(owner, std::slice::from_ref(&future))
        .await
        .unwrap();
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(full));
    reopened
        .kv_write("capacity-test", "", "protected", &vec![1; 65536])
        .await
        .unwrap();
    reopened
        .kv_write("capacity-test", "", "future", &vec![2; 1024])
        .await
        .unwrap();
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(full));
    assert!(reopened
        .kv_write("capacity-test", "", "protected", &vec![3; 200000])
        .await
        .is_err());
    assert_eq!(
        reopened
            .kv_read("capacity-test", "", "protected")
            .await
            .unwrap()
            .unwrap(),
        vec![1; 65536]
    );
    let detail: String = f.raw.query_row(
        "EXPLAIN QUERY PLAN DELETE FROM wallet_storage_objects WHERE owner=?1 AND payload_bytes=0",
        [owner.to_string()], |row| row.get(3),
    ).unwrap();
    assert!(
        detail.contains("wallet_storage_future_by_owner"),
        "{detail}"
    );
    let released = reopened.release_storage(owner).await.unwrap();
    assert!(released.charged_bytes < full.charged_bytes);
    assert_eq!(reopened.release_storage(owner).await.unwrap(), released);
    assert!(reopened.claim_storage(owner, &[kv("late")]).await.is_err());
    let unused: u64 = f
        .raw
        .query_row(
            "SELECT count(*) FROM wallet_storage_objects WHERE payload_bytes=0",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(unused, 0);
    assert!(reopened
        .kv_read("capacity-test", "", "future")
        .await
        .unwrap()
        .is_some());
    reopened
        .kv_write("capacity-test", "", "reused", &[3; 1024])
        .await
        .unwrap();
}

#[tokio::test]
async fn concurrent_admissions_cannot_both_consume_the_last_allowance() {
    let f = Fixture::new().await;
    let status = f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    // 64 bytes of account metadata and the UUID's 36 bytes are charged as well.
    f.db.configure_storage_capacity(status.charged_bytes + 5100)
        .await
        .unwrap();
    let first = saga(1);
    let second = saga(2);
    let (a, b) = tokio::join!(
        f.db.reserve_saga_storage(&first, 5000, &[]),
        f.db.reserve_saga_storage(&second, 5000, &[]),
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let status = f.db.storage_capacity().await.unwrap().unwrap();
    assert_eq!(status.maximum_bytes, status.charged_bytes);
    let reopened = open(&f.path).await;
    for (intent, admitted) in [(first, a.is_ok()), (second, b.is_ok())] {
        assert_eq!(
            reopened.get_saga(&intent.id).await.unwrap(),
            admitted.then_some(intent)
        );
    }
    let accounts: u64 = f
        .raw
        .query_row(
            "SELECT count(*) FROM wallet_storage_accounts WHERE reserved>0",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(accounts, 1);
}

#[tokio::test]
async fn saga_admission_rolls_back_claims_and_recovers_at_full_capacity() {
    let f = Fixture::new().await;
    let intent = saga(1);
    assert!(f.db.reserve_saga_storage(&intent, 5000, &[]).await.is_err());
    f.db.kv_write("capacity-test", "", "owned", &[7; 256])
        .await
        .unwrap();
    let original = f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    let object_bytes: u64 = f
        .raw
        .query_row(
            "SELECT charged FROM wallet_storage_objects WHERE object_key LIKE 'kv_store:%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    // This fits the transferred record (including its new UUID), but not the
    // recovery intent. A failed insert must roll back the earlier reservation.
    assert!(f
        .db
        .reserve_saga_storage(&intent, object_bytes + 36, &[kv("owned")])
        .await
        .is_err());
    assert_eq!(f.db.storage_capacity().await.unwrap(), Some(original));
    assert!(f.db.get_saga(&intent.id).await.unwrap().is_none());
    let owner: String = f
        .raw
        .query_row(
            "SELECT owner FROM wallet_storage_objects WHERE object_key LIKE 'kv_store:%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(owner.is_empty());
    let accounts: u64 = f
        .raw
        .query_row(
            "SELECT count(*) FROM wallet_storage_accounts WHERE owner=?1",
            [intent.id.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(accounts, 0);

    let mut stale = intent.clone();
    stale.version = 1;
    assert!(f.db.reserve_saga_storage(&stale, 5000, &[]).await.is_err());
    assert!(f
        .db
        .reserve_saga_storage(&saga(0), 5000, &[])
        .await
        .is_err());
    assert!(f.db.reserve_saga_storage(&intent, 0, &[]).await.is_err());
    assert_eq!(f.db.storage_capacity().await.unwrap(), Some(original));

    let admitted =
        f.db.reserve_saga_storage(&intent, 5000, &[kv("owned"), kv("future")])
            .await
            .unwrap();
    // Neither a duplicate saga nor a separately reserved external owner may be
    // silently replaced by an admission retry with new terms or claims.
    assert!(f
        .db
        .reserve_saga_storage(&intent, 10000, &[kv("retry")])
        .await
        .is_err());
    assert_eq!(f.db.storage_capacity().await.unwrap(), Some(admitted));
    let external = saga(2);
    let future = saga(3);
    let reserved =
        f.db.reserve_storage(
            external.id,
            1000,
            &[WalletStorageClaim {
                table: "wallet_sagas".to_owned(),
                primary_key: vec![future.id.to_string().into_bytes()],
            }],
        )
        .await
        .unwrap();
    assert!(f
        .db
        .reserve_saga_storage(&external, 5000, &[])
        .await
        .is_err());
    assert!(f.db.reserve_saga_storage(&future, 5000, &[]).await.is_err());
    assert_eq!(f.db.storage_capacity().await.unwrap(), Some(reserved));
    assert!(f.db.get_saga(&external.id).await.unwrap().is_none());
    assert!(f.db.get_saga(&future.id).await.unwrap().is_none());
    f.db.release_storage(external.id).await.unwrap();

    let full =
        f.db.configure_storage_capacity(admitted.charged_bytes)
            .await
            .unwrap();
    let reopened = open(&f.path).await;
    let mut saved = reopened.get_saga(&intent.id).await.unwrap().unwrap();
    assert_eq!(saved, intent);
    saved.update_state(WalletSagaState::Swap(SwapSagaState::SwapRequested));
    assert!(reopened.update_saga(saved).await.unwrap());
    reopened
        .kv_write("capacity-test", "", "future", &[9; 1024])
        .await
        .unwrap();
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(full));
    assert!(reopened
        .kv_write("capacity-test", "", "unrelated", &[1])
        .await
        .is_err());
    reopened.delete_saga(&intent.id).await.unwrap();
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(full));
    assert!(
        reopened
            .release_storage(intent.id)
            .await
            .unwrap()
            .charged_bytes
            < full.charged_bytes
    );
}

#[tokio::test]
async fn reopen_rejects_missing_accounting_and_changed_native_schema() {
    for alteration in [
        "DROP TRIGGER wallet_storage_proof_INSERT",
        "DROP INDEX wallet_storage_future_by_owner",
        "ALTER TABLE proof ADD COLUMN unaccounted TEXT",
        "ALTER TABLE proof ADD COLUMN unaccounted TEXT GENERATED ALWAYS AS(secret || secret) VIRTUAL",
        "CREATE TABLE unaccounted(id TEXT PRIMARY KEY)",
        "CREATE TABLE sqlitex_unaccounted(id TEXT PRIMARY KEY)",
        "DROP TABLE wallet_storage_total",
        "DROP TABLE wallet_storage_objects; DROP TABLE wallet_storage_accounts; DROP TABLE wallet_storage_total;",
    ] {
        let f=Fixture::new().await;
        f.db.configure_storage_capacity(1024*1024).await.unwrap();
        f.raw.execute_batch(alteration).unwrap();
        #[cfg(feature = "sqlcipher")]
        let config=(f.path.clone(),"page-test".to_owned());
        #[cfg(not(feature = "sqlcipher"))]
        let config=f.path.clone();
        assert!(WalletSqliteDatabase::new(config).await.is_err(),"{alteration}");
    }
}

#[tokio::test]
async fn admission_reuses_claimed_bytes_and_transfers_the_last_record_at_capacity() {
    let f = Fixture::new().await;
    f.db.kv_write("capacity-test", "", "owned", &[7; 4096])
        .await
        .unwrap();
    let start = f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    let original: u64 = f
        .raw
        .query_row(
            "SELECT charged FROM wallet_storage_objects WHERE object_key LIKE 'kv_store:%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let a = Uuid::from_u128(1);
    let b = Uuid::from_u128(2);
    let c = Uuid::from_u128(3);
    // Reassigning a shared record adds a UUID, its account and completion space.
    // The record's existing payload must not be charged again during admission.
    let allowance = original + 36 + 1024;
    let maximum = start.charged_bytes + 36 + 100 + 1024;
    f.db.configure_storage_capacity(maximum).await.unwrap();
    let full =
        f.db.reserve_storage(a, allowance, &[kv("owned")])
            .await
            .unwrap();
    assert_eq!(full.charged_bytes, maximum);

    let retained = f.db.release_storage(a).await.unwrap();
    f.db.configure_storage_capacity(retained.charged_bytes)
        .await
        .unwrap();
    let used = allowance - 1024;
    let moved = f.db.reserve_storage(b, used, &[kv("owned")]).await.unwrap();
    assert_eq!(moved.charged_bytes, retained.charged_bytes);
    assert_eq!(moved.charged_bytes, moved.maximum_bytes);
    let old: u64 = f
        .raw
        .query_row(
            "SELECT count(*) FROM wallet_storage_accounts WHERE owner=?1",
            [a.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(old, 0);

    // The receiving owner can already have a smaller reservation. Replacing it
    // must not reject the transfer against that obsolete allowance mid-transaction.
    f.db.configure_storage_capacity(moved.maximum_bytes + 101)
        .await
        .unwrap();
    let admitted = f.db.reserve_storage(c, 1, &[]).await.unwrap();
    assert_eq!(admitted.charged_bytes, admitted.maximum_bytes);
    f.db.release_storage(b).await.unwrap();
    let resized = f.db.reserve_storage(c, used, &[kv("owned")]).await.unwrap();
    assert_eq!(resized.charged_bytes, moved.charged_bytes);

    // A rejected resize restores the previous allowance and all exact claims.
    let before_rejection =
        f.db.configure_storage_capacity(resized.charged_bytes + 1024)
            .await
            .unwrap();
    assert!(f
        .db
        .reserve_storage(c, 1, &[kv("owned"), kv("rejected")])
        .await
        .is_err());
    assert_eq!(
        f.db.storage_capacity().await.unwrap(),
        Some(before_rejection)
    );
    let reservation: u64 = f
        .raw
        .query_row(
            "SELECT reserved FROM wallet_storage_accounts WHERE owner=?1",
            [c.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(reservation, used);
    let placeholders: u64 = f
        .raw
        .query_row(
            "SELECT count(*) FROM wallet_storage_objects WHERE payload_bytes=0",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(placeholders, 0);
    assert_eq!(
        f.db.kv_read("capacity-test", "", "owned")
            .await
            .unwrap()
            .unwrap(),
        vec![7; 4096]
    );
}
