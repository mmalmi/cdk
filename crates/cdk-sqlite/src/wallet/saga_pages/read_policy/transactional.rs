use cdk_common::{Id, KeySet, Keys};

use super::*;

#[derive(Clone, Copy)]
enum Transition {
    OutputKeys,
    Completion,
    Proofs,
}

async fn check_internal_read(transition: Transition) {
    let f = Fixture::new().await;
    let limit = 8192;
    let db = open(&f.path).await.with_saga_read_limit(limit);
    let (original, prepared, _, input) = receive_states(1);
    db.add_mint(original.mint_url.clone(), None).await.unwrap();
    db.configure_storage_capacity(1024 * 1024).await.unwrap();
    db.reserve_saga_storage(&original, 256 * 1024, &[])
        .await
        .unwrap();
    db.update_proofs(vec![input.clone()], vec![]).await.unwrap();
    let keys: Keys = serde_json::from_value(serde_json::json!({
        "1": SecretKey::generate().public_key().to_string()
    }))
    .unwrap();
    let keyset = KeySet {
        id: Id::v1_from_keys(&keys),
        keys,
        unit: original.unit.clone(),
        active: Some(true),
        input_fee_ppk: 0,
        final_expiry: None,
    };
    let mut spent = input.clone();
    spent.state = State::Spent;
    let apply = || async {
        match transition {
            Transition::OutputKeys => db
                .prepare_output_keyset(original.id, original.mint_url.clone(), keyset.clone())
                .await
                .map(|()| true),
            Transition::Completion => db.update_saga(prepared.clone()).await,
            Transition::Proofs => {
                db.compare_and_swap_proofs(
                    vec![ProofChange {
                        y: input.y,
                        before: Some(input.clone()),
                        after: Some(spent.clone()),
                    }],
                    Some(&original),
                )
                .await
            }
        }
    };
    let mut enlarged = original.clone();
    let OperationData::Receive(data) = &mut enlarged.data else {
        unreachable!()
    };
    data.checkpoint.as_mut().unwrap().transaction.memo = Some("雪\0".repeat(limit as usize));
    for payload in [
        serde_json::to_string(&enlarged.data).unwrap(),
        "!".repeat(limit as usize + 1),
    ] {
        f.raw
            .execute(
                "UPDATE wallet_sagas SET data=?1 WHERE id=?2",
                rusqlite::params![payload, original.id.to_string()],
            )
            .unwrap();
        let capacity = db.storage_capacity().await.unwrap();
        let accounts = || {
            f.raw
                .prepare("SELECT owner, used, reserved FROM wallet_storage_accounts ORDER BY owner")
                .unwrap()
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        let before = accounts();
        let result = apply().await;
        assert!(
            matches!(result, Err(Error::SagaTooLarge { limit: actual, .. }) if actual == limit),
            "internal parent reads must enforce the configured limit before decoding: {result:?}"
        );
        assert_eq!(accounts(), before);
        assert_eq!(db.storage_capacity().await.unwrap(), capacity);
        let saved: (String, u32) = f
            .raw
            .query_row(
                "SELECT data, version FROM wallet_sagas WHERE id=?1",
                [original.id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(saved, (payload, original.version));
        assert_eq!(
            db.get_proofs_by_ys(vec![input.y]).await.unwrap(),
            vec![input.clone()]
        );
        assert!(db.get_keys(&keyset.id).await.unwrap().is_none());
        assert!(db.get_keyset_by_id(&keyset.id).await.unwrap().is_none());
        let counters: u64 = f
            .raw
            .query_row("SELECT count(*) FROM keyset_counter", [], |row| row.get(0))
            .unwrap();
        assert_eq!(counters, 0);
    }
    f.raw
        .execute(
            "UPDATE wallet_sagas SET data=?1 WHERE id=?2",
            rusqlite::params![
                serde_json::to_string(&original.data).unwrap(),
                original.id.to_string()
            ],
        )
        .unwrap();
    assert!(apply().await.unwrap());
    assert_eq!(
        db.get_saga(&original.id).await.unwrap(),
        Some(match transition {
            Transition::Completion => prepared,
            _ => original.clone(),
        })
    );
    assert_eq!(
        db.get_proofs_by_ys(vec![input.y]).await.unwrap(),
        vec![match transition {
            Transition::Proofs => spent,
            _ => input,
        }]
    );
    if matches!(transition, Transition::OutputKeys) {
        assert_eq!(db.get_keys(&keyset.id).await.unwrap(), Some(keyset.keys));
    }
}

#[tokio::test]
async fn output_key_admission_bounds_its_saved_parent() {
    check_internal_read(Transition::OutputKeys).await;
}

#[tokio::test]
async fn completion_admission_bounds_its_saved_parent() {
    check_internal_read(Transition::Completion).await;
}

#[tokio::test]
async fn proof_fence_respects_the_configured_parent_limit() {
    check_internal_read(Transition::Proofs).await;
}

#[tokio::test]
async fn completed_receive_releases_only_unused_allowance_atomically_at_capacity() {
    let f = Fixture::new().await;
    let (original, prepared, ready, mut input) = receive_states(1);
    let OperationData::Receive(data) = &ready.data else {
        unreachable!()
    };
    let saved = data.checkpoint.as_ref().unwrap();
    let receipt = saved.transaction.clone();
    let output = saved.outputs.as_ref().unwrap()[0].clone();
    f.db.add_mint(original.mint_url.clone(), None)
        .await
        .unwrap();
    f.db.configure_storage_capacity(20 * 1024 * 1024)
        .await
        .unwrap();
    f.db.add_saga(original.clone()).await.unwrap();
    assert!(f
        .db
        .compare_and_swap_proofs(saved.inputs.clone(), Some(&original))
        .await
        .unwrap());
    assert!(f.db.update_saga(prepared).await.unwrap());
    f.db.kv_write("capacity-test", "", "fill", &[])
        .await
        .unwrap();
    let admitted = f.db.storage_capacity().await.unwrap().unwrap();
    let filler = vec![0; (admitted.maximum_bytes - admitted.charged_bytes) as usize];
    f.db.kv_write("capacity-test", "", "fill", &filler)
        .await
        .unwrap();
    let full = f.db.storage_capacity().await.unwrap().unwrap();
    assert_eq!(full.charged_bytes, full.maximum_bytes);

    let reopened = open(&f.path).await;
    assert!(reopened.update_saga(ready.clone()).await.unwrap());
    let acquired = input.clone();
    input.state = State::Spent;
    assert!(reopened
        .compare_and_swap_proofs(
            vec![
                ProofChange {
                    y: input.y,
                    before: Some(acquired),
                    after: Some(input.clone()),
                },
                ProofChange {
                    y: output.y,
                    before: None,
                    after: Some(output.clone()),
                },
            ],
            Some(&ready),
        )
        .await
        .unwrap());
    let mut completed = ready.clone();
    completed.update_state(WalletSagaState::Receive(ReceiveSagaState::Completed));
    assert!(reopened
        .complete_saga(&ready, completed.clone(), receipt.clone())
        .await
        .unwrap());
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(full));
    let account = || {
        f.raw
            .query_row(
                "SELECT used,reserved FROM wallet_storage_accounts WHERE owner=?1",
                [original.id.to_string()],
                |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)),
            )
            .unwrap()
    };
    let before = account();
    assert!(before.1 > before.0);
    f.raw.execute_batch("CREATE TRIGGER reject_receive_release BEFORE UPDATE OF reserved ON wallet_storage_accounts WHEN OLD.reserved>0 AND NEW.reserved=0 BEGIN SELECT RAISE(ABORT,'injected release failure'); END").unwrap();
    let error = reopened.delete_saga(&original.id).await.unwrap_err();
    assert!(error.to_string().contains("injected release failure"));
    assert_eq!(account(), before);
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(full));
    assert_eq!(
        reopened.get_saga(&original.id).await.unwrap(),
        Some(completed)
    );
    f.raw
        .execute_batch("DROP TRIGGER reject_receive_release")
        .unwrap();

    let reopened = open(&f.path).await;
    for _ in 0..2 {
        reopened.delete_saga(&original.id).await.unwrap();
        assert!(reopened.get_saga(&original.id).await.unwrap().is_none());
        let (used, reserved) = account();
        assert!(used > 0 && used < before.0);
        assert_eq!(reserved, 0);
        assert_eq!(
            reopened
                .storage_capacity()
                .await
                .unwrap()
                .unwrap()
                .charged_bytes,
            full.charged_bytes - before.1 + used
        );
        assert_eq!(
            reopened.get_transaction(receipt.id()).await.unwrap(),
            Some(receipt.clone())
        );
        for expected in [&input, &output] {
            assert_eq!(
                reopened.get_proofs_by_ys(vec![expected.y]).await.unwrap(),
                std::slice::from_ref(expected)
            );
        }
        assert_eq!(
            reopened.kv_read("capacity-test", "", "fill").await.unwrap(),
            Some(filler.clone())
        );
    }
}

#[tokio::test]
async fn terminal_send_releases_only_unused_allowance() {
    for state in [
        SendSagaState::Claimed,
        SendSagaState::Reclaimed,
        SendSagaState::TokenCreated,
    ] {
        let f = Fixture::new().await;
        let (mut send, _, reclaimed, mut input) = reclaim_states(receive_states(1));
        let OperationData::Send(data) = &reclaimed.data else {
            unreachable!()
        };
        let refund = data.reclaim.as_ref().unwrap();
        let terminal = state != SendSagaState::TokenCreated;
        input.state = if terminal {
            State::Spent
        } else {
            State::PendingSpent
        };
        let mut proofs = vec![input.clone()];
        let mut receipts = vec![Transaction {
            role: TransactionRole::Primary,
            direction: TransactionDirection::Outgoing,
            status: match state {
                SendSagaState::Claimed => TransactionStatus::Completed,
                SendSagaState::Reclaimed => TransactionStatus::Failed,
                _ => TransactionStatus::Pending,
            },
            timestamp: send.created_at,
            ..refund.transaction.clone()
        }];
        if state == SendSagaState::Reclaimed {
            proofs.extend(refund.outputs.as_ref().unwrap().iter().cloned());
            receipts.push(refund.transaction.clone());
            send = reclaimed;
        }
        // Seed exact retained records at the deletion boundary. No mint or
        // financial transition is performed by this storage regression.
        send.state = WalletSagaState::Send(state);
        send.version = 0;
        f.db.add_mint(send.mint_url.clone(), None).await.unwrap();
        f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
        f.db.reserve_saga_storage(&send, 64 * 1024, &[])
            .await
            .unwrap();
        f.db.update_proofs(proofs.clone(), vec![]).await.unwrap();
        for receipt in &receipts {
            f.db.add_transaction(receipt.clone()).await.unwrap();
        }
        let admitted = f.db.storage_capacity().await.unwrap().unwrap();
        let full =
            f.db.configure_storage_capacity(admitted.charged_bytes)
                .await
                .unwrap();
        let account = || {
            f.raw
                .query_row(
                    "SELECT used,reserved FROM wallet_storage_accounts WHERE owner=?1",
                    [send.id.to_string()],
                    |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)),
                )
                .unwrap()
        };
        let before = account();
        assert!(before.1 > before.0);
        let reopened = open(&f.path).await;
        if terminal {
            f.raw.execute_batch("CREATE TRIGGER reject_send_release BEFORE UPDATE OF reserved ON wallet_storage_accounts WHEN OLD.reserved>0 AND NEW.reserved=0 BEGIN SELECT RAISE(ABORT,'injected release failure'); END").unwrap();
            let error = reopened.delete_saga(&send.id).await.unwrap_err();
            assert!(error.to_string().contains("injected release failure"));
            assert_eq!(account(), before);
            assert_eq!(reopened.storage_capacity().await.unwrap(), Some(full));
            assert_eq!(
                reopened.get_saga(&send.id).await.unwrap(),
                Some(send.clone())
            );
            f.raw
                .execute_batch("DROP TRIGGER reject_send_release")
                .unwrap();
        }
        for _ in 0..2 {
            reopened.delete_saga(&send.id).await.unwrap();
            assert!(reopened.get_saga(&send.id).await.unwrap().is_none());
            let (used, reserved) = account();
            assert!(used > 0 && used < before.0);
            assert_eq!(reserved, if terminal { 0 } else { before.1 });
            assert_eq!(
                reopened
                    .storage_capacity()
                    .await
                    .unwrap()
                    .unwrap()
                    .charged_bytes,
                if terminal {
                    full.charged_bytes - before.1 + used
                } else {
                    full.charged_bytes
                }
            );
            for receipt in &receipts {
                assert_eq!(
                    reopened
                        .get_transaction(receipt.id())
                        .await
                        .unwrap()
                        .as_ref(),
                    Some(receipt)
                );
            }
            for proof in &proofs {
                assert_eq!(
                    reopened.get_proofs_by_ys(vec![proof.y]).await.unwrap(),
                    std::slice::from_ref(proof)
                );
            }
        }
    }
}
