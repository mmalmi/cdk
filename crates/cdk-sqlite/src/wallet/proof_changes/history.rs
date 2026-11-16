use super::*;
use crate::wallet::test_db::{open, Fixture};
use crate::{WalletKvChange, WalletStorageClaim};
use cdk_common::wallet::{Transaction, TransactionDirection, TransactionRole, TransactionStatus};

#[tokio::test]
async fn outgoing_retirement_releases_allowance_atomically_and_retains_refund_evidence() {
    let f = Fixture::new().await;
    let operation = uuid::Uuid::new_v4();
    let unrelated = uuid::Uuid::new_v4();
    let mut coin = proof();
    coin.state = State::Spent;
    coin.used_by_operation = Some(operation);
    let mut outgoing = receipt(
        &coin,
        operation,
        TransactionDirection::Outgoing,
        TransactionRole::Primary,
    );
    outgoing.status = TransactionStatus::Failed;
    let refund = receipt(
        &coin,
        operation,
        TransactionDirection::Incoming,
        TransactionRole::SendRefund,
    );
    f.db.update_proofs(vec![coin.clone()], vec![])
        .await
        .unwrap();
    f.db.add_transaction(outgoing.clone()).await.unwrap();
    f.db.add_transaction(refund.clone()).await.unwrap();
    f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    f.db.reserve_storage(operation, 4096, &[]).await.unwrap();
    f.db.reserve_storage(unrelated, 2048, &[]).await.unwrap();
    let full =
        f.db.storage_capacity()
            .await
            .unwrap()
            .unwrap()
            .charged_bytes;
    let before = f.db.configure_storage_capacity(full).await.unwrap();
    f.raw
        .execute_batch(&format!(
        "CREATE TRIGGER fail_outgoing_release BEFORE UPDATE OF reserved ON wallet_storage_accounts
        WHEN OLD.owner='{operation}' AND NEW.reserved=0
        BEGIN SELECT RAISE(ABORT,'test outgoing release interrupted'); END;"
    ))
        .unwrap();
    assert!(f
        .db
        .retire_send_transactions(vec![outgoing.clone()])
        .await
        .is_err());
    assert_eq!(
        f.db.get_transaction(outgoing.id()).await.unwrap(),
        Some(outgoing.clone())
    );
    assert_eq!(f.db.storage_capacity().await.unwrap(), Some(before));
    f.raw
        .execute_batch("DROP TRIGGER fail_outgoing_release")
        .unwrap();
    let reopened = open(&f.path).await;
    assert!(reopened
        .retire_send_transactions(vec![outgoing.clone()])
        .await
        .unwrap());
    assert!(reopened
        .get_transaction(outgoing.id())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        reopened.get_transaction(refund.id()).await.unwrap(),
        Some(refund)
    );
    assert_eq!(
        reopened.get_proofs_by_ys(vec![coin.y]).await.unwrap(),
        vec![coin]
    );
    let capacity = reopened.storage_capacity().await.unwrap().unwrap();
    assert_eq!(capacity.maximum_bytes, full);
    assert!(capacity.charged_bytes < full);
    for (owner, expected) in [(operation, 0), (unrelated, 2048)] {
        let reserved: u64 = f
            .raw
            .query_row(
                "SELECT reserved FROM wallet_storage_accounts WHERE owner=?1",
                [owner.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reserved, expected);
    }
}

#[tokio::test]
async fn receipt_retirement_checkpoint_and_allowance_release_are_atomic_at_capacity() {
    for role in [TransactionRole::Primary, TransactionRole::SendRefund] {
        let f = Fixture::new().await;
        let operation = uuid::Uuid::new_v4();
        let unrelated = uuid::Uuid::new_v4();
        let mut coin = proof();
        coin.state = State::Spent;
        coin.used_by_operation = Some(operation);
        let receipt = receipt(&coin, operation, TransactionDirection::Incoming, role);
        f.db.update_proofs(vec![coin.clone()], vec![])
            .await
            .unwrap();
        f.db.add_transaction(receipt.clone()).await.unwrap();
        f.db.kv_write("history", "", "totals", b"before")
            .await
            .unwrap();
        f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
        f.db.reserve_storage(
            operation,
            4096,
            &[
                WalletStorageClaim {
                    table: "transactions".to_owned(),
                    primary_key: vec![receipt.id().as_slice().to_vec()],
                },
                WalletStorageClaim {
                    table: "proof".to_owned(),
                    primary_key: vec![coin.y.to_bytes().to_vec()],
                },
            ],
        )
        .await
        .unwrap();
        f.db.reserve_storage(unrelated, 2048, &[]).await.unwrap();
        let full =
            f.db.storage_capacity()
                .await
                .unwrap()
                .unwrap()
                .charged_bytes;
        let before = f.db.configure_storage_capacity(full).await.unwrap();
        let checkpoint = WalletKvChange {
            primary_namespace: "history",
            secondary_namespace: "",
            key: "totals",
            expected: Some(b"before"),
            replacement: Some(b"after"),
        };
        for fault in [
            "receipt",
            "input",
            "checkpoint",
            "write",
            "release",
            "capacity",
        ] {
            let mut expected = receipt.clone();
            let mut edit = checkpoint;
            let oversized = vec![1; 8192];
            match fault {
                "receipt" => expected.memo = Some("stale receipt".to_owned()),
                "input" => {
                    let mut unspent = coin.clone();
                    unspent.state = State::Unspent;
                    f.db.update_proofs(vec![unspent], vec![]).await.unwrap();
                }
                "checkpoint" => edit.expected = Some(b"stale"),
                "capacity" => edit.replacement = Some(&oversized),
                "write" => f
                    .raw
                    .execute_batch(
                        "CREATE TRIGGER fail_retirement BEFORE UPDATE ON kv_store
                    WHEN NEW.primary_namespace='history'
                    BEGIN SELECT RAISE(ABORT,'test checkpoint interrupted'); END;",
                    )
                    .unwrap(),
                "release" => f
                    .raw
                    .execute_batch(&format!(
                        "CREATE TRIGGER fail_retirement BEFORE UPDATE ON wallet_storage_accounts
                    WHEN OLD.owner='{operation}' AND NEW.reserved=0
                    BEGIN SELECT RAISE(ABORT,'test allowance release interrupted'); END;"
                    ))
                    .unwrap(),
                _ => unreachable!(),
            }
            let result =
                f.db.retire_incoming_transactions_with_kv(vec![expected], &[edit])
                    .await;
            if matches!(fault, "receipt" | "input" | "checkpoint") {
                assert!(!result.unwrap(), "{fault}");
            } else {
                assert!(result.is_err(), "{fault}");
            }
            if matches!(fault, "write" | "release") {
                f.raw
                    .execute_batch("DROP TRIGGER fail_retirement;")
                    .unwrap();
            }
            if fault == "input" {
                f.db.update_proofs(vec![coin.clone()], vec![])
                    .await
                    .unwrap();
            }
            let reopened = open(&f.path).await;
            assert_eq!(
                reopened.get_transaction(receipt.id()).await.unwrap(),
                Some(receipt.clone())
            );
            assert_eq!(
                reopened.get_proofs_by_ys(vec![coin.y]).await.unwrap(),
                vec![coin.clone()]
            );
            assert_eq!(
                reopened.kv_read("history", "", "totals").await.unwrap(),
                Some(b"before".to_vec())
            );
            assert_eq!(reopened.storage_capacity().await.unwrap(), Some(before));
        }
        assert!(f
            .db
            .retire_incoming_transactions_with_kv(vec![receipt.clone()], &[checkpoint])
            .await
            .unwrap());
        assert!(!f
            .db
            .retire_incoming_transactions_with_kv(vec![receipt.clone()], &[checkpoint])
            .await
            .unwrap());
        let reopened = open(&f.path).await;
        assert!(reopened
            .get_transaction(receipt.id())
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            reopened.get_proofs_by_ys(vec![coin.y]).await.unwrap(),
            vec![coin]
        );
        assert_eq!(
            reopened.kv_read("history", "", "totals").await.unwrap(),
            Some(b"after".to_vec())
        );
        let capacity = reopened.storage_capacity().await.unwrap().unwrap();
        assert_eq!(capacity.maximum_bytes, full);
        assert!(capacity.charged_bytes < full);
        for (owner, reserved) in [(operation, 0), (unrelated, 2048)] {
            let saved: u64 = f
                .raw
                .query_row(
                    "SELECT reserved FROM wallet_storage_accounts WHERE owner=?1",
                    [owner.to_string()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(saved, reserved);
        }
    }
}

fn receipt(
    coin: &ProofInfo,
    operation: uuid::Uuid,
    direction: TransactionDirection,
    role: TransactionRole,
) -> Transaction {
    Transaction {
        mint_url: coin.mint_url.clone(),
        direction,
        unit: coin.unit.clone(),
        amount: coin.proof.amount,
        fee: Amount::from(0),
        ys: vec![coin.y],
        timestamp: 1,
        memo: None,
        metadata: Default::default(),
        quote_id: None,
        payment_request: None,
        payment_proof: None,
        payment_method: None,
        saga_id: Some(operation),
        status: TransactionStatus::Completed,
        role,
    }
}

#[tokio::test]
async fn history_retirement_bounds_selected_proof_payload_before_decoding() {
    use cdk_common::database::wallet::MAX_PROOF_PAGE_BYTES;
    use cdk_common::database::Error;
    use cdk_common::nuts::{P2PKWitness, Witness};

    for direction in [
        TransactionDirection::Incoming,
        TransactionDirection::Outgoing,
    ] {
        let f = Fixture::new().await;
        let operation = uuid::Uuid::new_v4();
        let mut selected = proof();
        selected.state = State::Spent;
        selected.used_by_operation = Some(operation);
        selected.proof.witness = Some(Witness::P2PKWitness(P2PKWitness {
            signatures: vec!["a".repeat(MAX_PROOF_PAGE_BYTES as usize)],
        }));
        let mut unrelated = proof();
        unrelated.proof.witness = selected.proof.witness.clone();
        let original = receipt(&selected, operation, direction, TransactionRole::Primary);
        f.db.update_proofs(vec![selected.clone(), unrelated.clone()], vec![])
            .await
            .unwrap();
        f.db.add_transaction(original.clone()).await.unwrap();
        // Existence alone protects an active operation, even when its recovery
        // payload cannot be decoded. Eligibility must not read that payload.
        f.raw
            .execute(
                "INSERT INTO wallet_sagas
                 (id, kind, state, amount, mint_url, unit, created_at, updated_at, data)
                 VALUES (?1, 'send', 'token_created', 8, ?2, 'sat', 1, 1, ?3)",
                rusqlite::params![
                    operation.to_string(),
                    selected.mint_url.to_string(),
                    "x".repeat(MAX_PROOF_PAGE_BYTES as usize)
                ],
            )
            .unwrap();
        assert!(!f.db.token_transaction_retirable(&original).await.unwrap());
        f.db.delete_saga(&operation).await.unwrap();
        assert!(matches!(
            f.db.token_transaction_retirable(&original).await,
            Err(Error::ProofPageTooLarge { .. })
        ));
        let result = match direction {
            TransactionDirection::Incoming => {
                f.db.retire_incoming_transactions(vec![original.clone()])
                    .await
            }
            TransactionDirection::Outgoing => {
                f.db.retire_send_transactions(vec![original.clone()]).await
            }
        };
        let error = result
            .expect_err("oversized selected input must be rejected before receipt retirement");
        assert!(
            matches!(error, Error::ProofPageTooLarge { bytes, limit } if bytes > limit && limit == MAX_PROOF_PAGE_BYTES)
        );
        let reopened = open(&f.path).await;
        assert_eq!(
            reopened.get_transaction(original.id()).await.unwrap(),
            Some(original.clone())
        );
        let bytes: u64 = f
            .raw
            .query_row(
                "SELECT length(witness) FROM proof WHERE y=?1",
                [selected.y.to_bytes().to_vec()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(bytes > MAX_PROOF_PAGE_BYTES);
        selected.proof.witness = None;
        f.db.update_proofs(vec![selected.clone()], vec![])
            .await
            .unwrap();
        assert!(f.db.token_transaction_retirable(&original).await.unwrap());
        let mut changed = original.clone();
        changed.fee = Amount::from(1);
        assert!(!f.db.token_transaction_retirable(&changed).await.unwrap());
        let retired = match direction {
            TransactionDirection::Incoming => {
                f.db.retire_incoming_transactions(vec![original.clone()])
                    .await
            }
            TransactionDirection::Outgoing => {
                f.db.retire_send_transactions(vec![original.clone()]).await
            }
        };
        assert!(
            retired.unwrap(),
            "unrelated oversized proof must not block exact retirement"
        );
        assert_eq!(
            reopened.get_proofs_by_ys(vec![selected.y]).await.unwrap(),
            [selected]
        );
        let bytes: u64 = f
            .raw
            .query_row(
                "SELECT length(witness) FROM proof WHERE y=?1",
                [unrelated.y.to_bytes().to_vec()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(bytes > MAX_PROOF_PAGE_BYTES);
    }
}

#[tokio::test]
async fn exact_receipt_batches_bound_payload_and_preserve_request_order() {
    use cdk_common::database::wallet::MAX_TRANSACTION_PAGE_BYTES;
    use cdk_common::database::Error;

    let f = Fixture::new().await;
    let make = || {
        receipt(
            &proof(),
            uuid::Uuid::new_v4(),
            TransactionDirection::Incoming,
            TransactionRole::Primary,
        )
    };
    let mut first = make();
    let mut second = make();
    let mut unrelated = make();
    first.memo = Some("a".repeat(MAX_TRANSACTION_PAGE_BYTES as usize / 2));
    second.memo = first.memo.clone();
    unrelated.memo = Some("b".repeat(MAX_TRANSACTION_PAGE_BYTES as usize));
    for tx in [&first, &second, &unrelated] {
        f.db.add_transaction(tx.clone()).await.unwrap();
    }
    for tx in [&first, &second] {
        assert_eq!(
            f.db.get_transactions_by_ids_bounded(&[tx.id()])
                .await
                .unwrap(),
            std::slice::from_ref(tx)
        );
    }
    for ids in [vec![first.id(), second.id()], vec![unrelated.id()]] {
        assert!(matches!(
            f.db.get_transactions_by_ids_bounded(&ids).await,
            Err(Error::TransactionPageTooLarge { bytes, limit })
                if bytes > limit && limit == MAX_TRANSACTION_PAGE_BYTES
        ));
    }
    assert!(matches!(
        f.db.retire_incoming_transactions(vec![first.clone(), second.clone()])
            .await,
        Err(Error::TransactionPageTooLarge { .. })
    ));
    let reopened = open(&f.path).await;
    for tx in [&first, &second] {
        assert_eq!(
            reopened.get_transaction(tx.id()).await.unwrap(),
            Some(tx.clone())
        );
    }
    first.memo = None;
    second.memo = None;
    for tx in [&first, &second] {
        f.db.add_transaction(tx.clone()).await.unwrap();
    }
    let missing = make().id();
    assert_eq!(
        f.db.get_transactions_by_ids_bounded(&[second.id(), missing, first.id(), second.id()])
            .await
            .unwrap(),
        [second.clone(), first.clone()]
    );
    assert!(f
        .db
        .get_transactions_by_ids_bounded(&[])
        .await
        .unwrap()
        .is_empty());
    assert!(f
        .db
        .get_transactions_by_ids_bounded(&vec![missing; 129])
        .await
        .is_err());
    f.raw
        .execute(
            "UPDATE transactions SET metadata='{' WHERE id=?1",
            [second.id().as_slice()],
        )
        .unwrap();
    assert!(f
        .db
        .get_transactions_by_ids_bounded(&[second.id()])
        .await
        .is_err());
    assert_eq!(
        f.db.get_transactions_by_ids_bounded(&[first.id()])
            .await
            .unwrap(),
        [first]
    );
}

#[tokio::test]
async fn paged_history_checks_late_inputs_without_loading_the_whole_token() {
    use cdk_common::nuts::{P2PKWitness, Witness};

    let f = Fixture::new().await;
    let operation = uuid::Uuid::new_v4();
    // Individual inputs fit, but 128 together require a smaller bounded page.
    let coins: Vec<_> = (0..257)
        .map(|_| {
            let mut coin = proof();
            coin.state = State::Spent;
            coin.used_by_operation = Some(operation);
            coin.proof.witness = Some(Witness::P2PKWitness(P2PKWitness {
                signatures: vec!["a".repeat(33 * 1024)],
            }));
            coin
        })
        .collect();
    let last = coins.last().unwrap().clone();
    let mut original = receipt(
        &coins[0],
        operation,
        TransactionDirection::Incoming,
        TransactionRole::SendRefund,
    );
    original.ys = coins.iter().map(|coin| coin.y).collect();
    original.amount = Amount::from(8 * coins.len() as u64);
    f.db.update_proofs(coins, vec![]).await.unwrap();
    f.db.add_transaction(original.clone()).await.unwrap();
    assert!(f
        .db
        .token_transaction_retirable(&original)
        .await
        .expect("individually bounded inputs must split into smaller pages"));
    f.db.kv_write("history", "", "totals", b"before")
        .await
        .unwrap();
    let checkpoint = WalletKvChange {
        primary_namespace: "history",
        secondary_namespace: "",
        key: "totals",
        expected: Some(b"before"),
        replacement: Some(b"after"),
    };
    for fault in ["state", "owner", "amount", "mint", "missing", "corrupt"] {
        let mut changed = last.clone();
        match fault {
            "state" => changed.state = State::Unspent,
            "owner" => changed.used_by_operation = Some(uuid::Uuid::new_v4()),
            "amount" => changed.proof.amount = Amount::from(4),
            "mint" => changed.mint_url = MintUrl::from_str("https://other.example").unwrap(),
            _ => (),
        }
        if fault == "missing" {
            f.db.update_proofs(vec![], vec![last.y]).await.unwrap();
        } else {
            f.db.update_proofs(vec![changed], vec![]).await.unwrap();
            if fault == "corrupt" {
                f.raw
                    .execute(
                        "UPDATE proof SET witness='{' WHERE y=?1",
                        [last.y.to_bytes().to_vec()],
                    )
                    .unwrap();
            }
        }
        let advisory = f.db.token_transaction_retirable(&original).await;
        let result =
            f.db.retire_incoming_transactions_with_kv(vec![original.clone()], &[checkpoint])
                .await;
        if fault == "corrupt" {
            assert!(advisory.is_err());
            assert!(result.is_err());
        } else {
            assert!(!advisory.unwrap(), "{fault}");
            assert!(!result.unwrap(), "{fault}");
        }
        let reopened = open(&f.path).await;
        assert_eq!(
            reopened.get_transaction(original.id()).await.unwrap(),
            Some(original.clone())
        );
        assert_eq!(
            reopened.kv_read("history", "", "totals").await.unwrap(),
            Some(b"before".to_vec())
        );
        f.db.update_proofs(vec![last.clone()], vec![])
            .await
            .unwrap();
    }
    assert!(f.db.token_transaction_retirable(&original).await.unwrap());
    assert!(f
        .db
        .retire_incoming_transactions_with_kv(vec![original.clone()], &[checkpoint])
        .await
        .unwrap());
    let reopened = open(&f.path).await;
    assert!(reopened
        .get_transaction(original.id())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        reopened.kv_read("history", "", "totals").await.unwrap(),
        Some(b"after".to_vec())
    );
    assert_eq!(
        f.raw
            .query_row("SELECT count(*) FROM proof", [], |row| row.get::<_, u64>(0))
            .unwrap(),
        257
    );
}
