use super::*;
use cdk_common::database::wallet::MAX_PROOF_PAGE_BYTES;
use cdk_common::database::Error;

use crate::wallet::test_db::Fixture;
use crate::WalletKvChange;

fn deletion(coin: &ProofInfo) -> ProofChange {
    ProofChange {
        y: coin.y,
        before: Some(coin.clone()),
        after: None,
    }
}

#[tokio::test]
async fn proof_comparison_bounds_unexpected_payload_before_decoding() {
    let f = Fixture::new().await;
    let coins: Vec<_> = (0..129).map(|_| proof()).collect();
    f.db.update_proofs(coins.clone(), vec![]).await.unwrap();
    f.db.kv_write("history", "", "cursor", b"before")
        .await
        .unwrap();
    let oversized = "x".repeat(MAX_PROOF_PAGE_BYTES as usize);
    let last = coins.last().unwrap();
    f.raw
        .execute(
            "UPDATE proof SET witness=?1 WHERE y=?2",
            rusqlite::params![oversized, last.y.to_bytes().to_vec()],
        )
        .unwrap();
    let edit = WalletKvChange {
        primary_namespace: "history",
        secondary_namespace: "",
        key: "cursor",
        expected: Some(b"before"),
        replacement: Some(b"after"),
    };
    for expected_present in [true, false] {
        let mut changes: Vec<_> = coins.iter().map(deletion).collect();
        if !expected_present {
            changes.last_mut().unwrap().before = None;
        }
        let error =
            f.db.compare_and_swap_proofs_with_kv(changes, &[edit])
                .await
                .unwrap_err();
        assert!(
            matches!(error, Error::ProofPageTooLarge { bytes, limit }
                if bytes > limit && limit == MAX_PROOF_PAGE_BYTES),
            "atomic comparison must bound unexpected evidence before decoding: {error}"
        );
        assert_eq!(
            f.db.get_proofs_by_ys(vec![coins[0].y]).await.unwrap(),
            vec![coins[0].clone()]
        );
        assert_eq!(
            f.db.kv_read("history", "", "cursor").await.unwrap(),
            Some(b"before".to_vec())
        );
        let retained: String = f
            .raw
            .query_row(
                "SELECT witness FROM proof WHERE y=?1",
                [last.y.to_bytes().to_vec()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained, oversized);
    }
}

#[tokio::test]
async fn proof_comparison_preserves_large_expected_rows_and_typed_equality() {
    let f = Fixture::new().await;
    let mut coins = Vec::new();
    for size in [
        MAX_PROOF_PAGE_BYTES,
        MAX_PROOF_PAGE_BYTES / 2,
        MAX_PROOF_PAGE_BYTES / 2,
        0,
    ] {
        let mut coin = proof();
        coin.proof.witness = Some(cdk_common::nuts::Witness::P2PKWitness(
            cdk_common::nuts::P2PKWitness {
                signatures: vec!["a".repeat(size as usize)],
            },
        ));
        coins.push(coin);
    }
    f.db.update_proofs(coins.clone(), vec![]).await.unwrap();
    // Whitespace is not financial evidence: retain typed, not raw JSON, comparison.
    f.raw
        .execute(
            "UPDATE proof SET witness=?1 WHERE y=?2",
            rusqlite::params![
                format!(
                    " {} ",
                    serde_json::to_string(&coins[3].proof.witness).unwrap()
                ),
                coins[3].y.to_bytes().to_vec()
            ],
        )
        .unwrap();
    let changes = coins.iter().map(deletion).collect();
    let edit = WalletKvChange {
        primary_namespace: "history",
        secondary_namespace: "",
        key: "cursor",
        expected: None,
        replacement: Some(b"after"),
    };
    assert!(f
        .db
        .compare_and_swap_proofs_with_kv(changes, &[edit])
        .await
        .unwrap());
    assert!(f
        .db
        .get_proofs_by_ys(coins.iter().map(|c| c.y).collect())
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        f.db.kv_read("history", "", "cursor").await.unwrap(),
        Some(b"after".to_vec())
    );
}
