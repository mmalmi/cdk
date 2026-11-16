use super::*;
use crate::wallet::test_db::{open, Fixture};
use crate::WalletKvChange;

#[tokio::test]
async fn proof_deletion_and_checkpoint_rollback_together_and_reclaim_full_capacity() {
    let f = Fixture::new().await;
    let mut original = proof();
    original.state = State::Spent;
    f.db.update_proofs(vec![original.clone()], vec![])
        .await
        .unwrap();
    f.db.kv_write("history", "", "cursor", b"before")
        .await
        .unwrap();
    let capacity = f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    f.db.configure_storage_capacity(capacity.charged_bytes)
        .await
        .unwrap();
    let before = f.db.storage_capacity().await.unwrap();
    let proof_change = ProofChange {
        y: original.y,
        before: Some(original.clone()),
        after: None,
    };
    let kv = WalletKvChange {
        primary_namespace: "history",
        secondary_namespace: "",
        key: "cursor",
        expected: Some(b"before"),
        replacement: Some(b"after"),
    };
    for fault in ["proof", "cursor", "write", "capacity"] {
        let mut change = proof_change.clone();
        let mut edit = kv;
        let oversized = vec![1; 4096];
        match fault {
            "proof" => change.before.as_mut().unwrap().derivation_index = Some(17),
            "cursor" => edit.expected = Some(b"changed"),
            "capacity" => edit.replacement = Some(&oversized),
            "write" => f
                .raw
                .execute_batch(
                    "CREATE TRIGGER fail_checkpoint BEFORE UPDATE ON kv_store
                WHEN NEW.primary_namespace='history'
                BEGIN SELECT RAISE(ABORT,'test checkpoint interrupted'); END;",
                )
                .unwrap(),
            _ => unreachable!(),
        }
        let result =
            f.db.compare_and_swap_proofs_with_kv(vec![change], &[edit])
                .await;
        if fault == "proof" || fault == "cursor" {
            assert!(!result.unwrap());
        } else {
            assert!(result.is_err());
        }
        if fault == "write" {
            f.raw
                .execute_batch("DROP TRIGGER fail_checkpoint;")
                .unwrap();
        }
        let reopened = open(&f.path).await;
        assert_eq!(
            reopened.get_proofs_by_ys(vec![original.y]).await.unwrap(),
            vec![original.clone()]
        );
        assert_eq!(
            reopened.kv_read("history", "", "cursor").await.unwrap(),
            Some(b"before".to_vec())
        );
        assert_eq!(reopened.storage_capacity().await.unwrap(), before);
    }
    assert!(f
        .db
        .compare_and_swap_proofs_with_kv(vec![proof_change.clone()], &[kv])
        .await
        .unwrap());
    assert!(!f
        .db
        .compare_and_swap_proofs_with_kv(vec![proof_change], &[kv])
        .await
        .unwrap());
    let reopened = open(&f.path).await;
    assert!(reopened
        .get_proofs_by_ys(vec![original.y])
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        reopened.kv_read("history", "", "cursor").await.unwrap(),
        Some(b"after".to_vec())
    );
    let capacity = reopened.storage_capacity().await.unwrap().unwrap();
    assert_eq!(capacity.maximum_bytes, before.unwrap().maximum_bytes);
    assert!(capacity.charged_bytes < capacity.maximum_bytes);
}
