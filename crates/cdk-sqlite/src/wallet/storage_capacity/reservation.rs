use cdk_common::database::wallet::ProofChange;
use cdk_common::database::WalletDatabase;
use uuid::Uuid;

use super::{kv, open, Fixture};
use crate::{WalletKvChange, WalletStorageReservation};

fn change<'a>(
    key: &'a str,
    expected: Option<&'a [u8]>,
    replacement: Option<&'a [u8]>,
) -> WalletKvChange<'a> {
    WalletKvChange {
        primary_namespace: "capacity-test",
        secondary_namespace: "",
        key,
        expected,
        replacement,
    }
}

fn ledger(raw: &rusqlite::Connection) -> Vec<(String, String, i64, i64)> {
    raw.prepare("SELECT 'account',owner,used,reserved FROM wallet_storage_accounts UNION ALL SELECT object_key,owner,payload_bytes,charged FROM wallet_storage_objects ORDER BY 1,2")
        .unwrap().query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))
        .unwrap().collect::<Result<_, _>>().unwrap()
}

#[tokio::test]
async fn custody_admission_rolls_back_proofs_claims_and_reservation() {
    let f = Fixture::new().await;
    let proof = super::super::proof_changes::proof();
    f.db.update_proofs(vec![proof.clone()], vec![])
        .await
        .unwrap();
    f.db.kv_write("capacity-test", "", "intent", b"before")
        .await
        .unwrap();
    let before = f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    let original = ledger(&f.raw);
    let claims = [kv("intent"), kv("page")];
    let reservation = WalletStorageReservation {
        owner: Uuid::from_u128(701),
        bytes: 16384,
        claims: &claims,
    };
    let deletion = || {
        vec![ProofChange {
            y: proof.y,
            before: Some(proof.clone()),
            after: None,
        }]
    };
    let changes = [change("intent", Some(b"before"), Some(b"admitted"))];
    f.raw.execute_batch("CREATE TRIGGER reject_admission BEFORE UPDATE ON kv_store WHEN NEW.key='intent' BEGIN SELECT RAISE(ABORT,'injected admission failure'); END").unwrap();
    assert!(f
        .db
        .compare_and_swap_proofs_with_kv_and_storage(deletion(), &changes, reservation)
        .await
        .is_err());
    f.raw
        .execute_batch("DROP TRIGGER reject_admission")
        .unwrap();
    assert_eq!(ledger(&f.raw), original);
    for bytes in [1, 2 * 1024 * 1024] {
        assert!(f
            .db
            .compare_and_swap_proofs_with_kv_and_storage(
                deletion(),
                &changes,
                WalletStorageReservation {
                    bytes,
                    ..reservation
                }
            )
            .await
            .is_err());
        assert_eq!(ledger(&f.raw), original);
    }
    let stale = [change("intent", Some(b"stale"), Some(b"admitted"))];
    assert!(!f
        .db
        .compare_and_swap_proofs_with_kv_and_storage(deletion(), &stale, reservation)
        .await
        .unwrap());
    assert_eq!(ledger(&f.raw), original);
    let reopened = open(&f.path).await;
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(before));
    assert_eq!(
        reopened.get_proofs_by_ys(vec![proof.y]).await.unwrap(),
        std::slice::from_ref(&proof)
    );
    assert_eq!(
        reopened
            .kv_read("capacity-test", "", "intent")
            .await
            .unwrap(),
        Some(b"before".to_vec())
    );
    assert!(reopened
        .compare_and_swap_proofs_with_kv_and_storage(deletion(), &changes, reservation)
        .await
        .unwrap());
    assert!(!reopened
        .compare_and_swap_proofs_with_kv_and_storage(deletion(), &changes, reservation)
        .await
        .unwrap());
    assert!(reopened
        .get_proofs_by_ys(vec![proof.y])
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn admitted_custody_completes_and_rotates_at_full_capacity() {
    let f = Fixture::new().await;
    f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    let owner = Uuid::from_u128(702);
    let page = vec![42; 4096];
    let page_bytes =
        f.db.kv_storage_bytes(owner, "capacity-test", "", "page1", page.len() as u64)
            .await
            .unwrap();
    let intent_bytes =
        f.db.kv_storage_bytes(owner, "capacity-test", "", "intent", 256)
            .await
            .unwrap();
    let claims = [kv("intent")];
    let reservation = WalletStorageReservation {
        owner,
        bytes: page_bytes + intent_bytes,
        claims: &claims,
    };
    assert!(f
        .db
        .compare_and_swap_proofs_with_kv_and_storage(
            vec![],
            &[change("intent", None, Some(b"admitted"))],
            reservation
        )
        .await
        .unwrap());
    f.db.kv_write("capacity-test", "", "unrelated", &[])
        .await
        .unwrap();
    let capacity = f.db.storage_capacity().await.unwrap().unwrap();
    let filler = vec![0; (capacity.maximum_bytes - capacity.charged_bytes) as usize];
    f.db.kv_write("capacity-test", "", "unrelated", &filler)
        .await
        .unwrap();
    let full = f.db.storage_capacity().await.unwrap().unwrap();
    assert_eq!(full.charged_bytes, full.maximum_bytes);
    let reopened = open(&f.path).await;
    let claims = [kv("page1")];
    let saved = WalletStorageReservation {
        claims: &claims,
        ..reservation
    };
    assert!(reopened
        .compare_and_swap_proofs_with_kv_and_storage(
            vec![],
            &[
                change("page1", None, Some(&page)),
                change("intent", Some(b"admitted"), Some(b"captured"))
            ],
            saved
        )
        .await
        .unwrap());
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(full));
    // Deleting the old page before claiming its replacement must reuse the same
    // exact allowance; even a temporary placeholder cannot require extra space.
    let claims = [kv("page2")];
    let rotated = WalletStorageReservation {
        claims: &claims,
        ..reservation
    };
    assert!(reopened
        .compare_and_swap_proofs_with_kv_and_storage(
            vec![],
            &[
                change("page1", Some(&page), None),
                change("page2", None, Some(&page)),
                change("intent", Some(b"captured"), Some(b"rotated"))
            ],
            rotated
        )
        .await
        .unwrap());
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(full));
    assert_eq!(
        reopened
            .kv_read("capacity-test", "", "page1")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        reopened
            .kv_read("capacity-test", "", "page2")
            .await
            .unwrap(),
        Some(page.clone())
    );
    let smaller = WalletStorageReservation {
        bytes: intent_bytes,
        claims: &[],
        ..reservation
    };
    assert!(reopened
        .compare_and_swap_proofs_with_kv_and_storage(
            vec![],
            &[
                change("page2", Some(&page), None),
                change("intent", Some(b"rotated"), Some(b"retired"))
            ],
            smaller
        )
        .await
        .unwrap());
    assert_eq!(
        reopened
            .storage_capacity()
            .await
            .unwrap()
            .unwrap()
            .charged_bytes,
        full.charged_bytes - page_bytes
    );
    assert_eq!(
        reopened
            .kv_read("capacity-test", "", "unrelated")
            .await
            .unwrap(),
        Some(filler)
    );
}
