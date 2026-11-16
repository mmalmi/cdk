//! Storage ownership follows only a successful, fenced input acquisition.
use cdk_common::wallet::{
    OperationData, ReceiveCheckpoint, ReceiveOperationData, ReceiveSagaState, Transaction,
    TransactionDirection, TransactionRole, TransactionStatus, WalletSaga, WalletSagaState,
};
use uuid::Uuid;

use super::*;
use crate::wallet::test_db::{open, Fixture};

fn receive(changes: Vec<ProofChange>, owner: Uuid) -> WalletSaga {
    let amount = (changes.len() as u64 * 8).into();
    let mint_url: MintUrl = "https://mint.example".parse().unwrap();
    WalletSaga::new(
        owner,
        WalletSagaState::Receive(ReceiveSagaState::InputsRecorded),
        amount,
        mint_url.clone(),
        CurrencyUnit::Sat,
        OperationData::Receive(ReceiveOperationData {
            checkpoint: Some(Box::new(ReceiveCheckpoint {
                transaction: Transaction {
                    role: TransactionRole::Primary,
                    mint_url,
                    direction: TransactionDirection::Incoming,
                    amount,
                    fee: Amount::ZERO,
                    unit: CurrencyUnit::Sat,
                    ys: changes.iter().map(|p| p.y).collect(),
                    timestamp: 1,
                    memo: None,
                    metadata: Default::default(),
                    quote_id: None,
                    payment_request: None,
                    payment_proof: None,
                    payment_method: None,
                    saga_id: Some(owner),
                    status: TransactionStatus::Completed,
                },
                inputs: changes,
                pre_swap: None,
                outputs: None,
            })),
        }),
    )
}

fn ledger(f: &Fixture) -> Vec<(String, String, i64)> {
    f.raw
        .prepare(
            "SELECT object_key,owner,payload_bytes FROM wallet_storage_objects ORDER BY object_key",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[tokio::test]
async fn acquired_inputs_restore_at_capacity_without_claiming_a_losing_batch() {
    let f = Fixture::new().await;
    let previous = Uuid::from_u128(1);
    let owner = Uuid::from_u128(2);
    let mut sent = proof();
    sent.state = State::PendingSpent;
    sent.used_by_operation = Some(previous);
    let unspent = proof();
    let unknown = proof();
    let originals = vec![sent.clone(), unspent.clone()];
    f.db.update_proofs(originals.clone(), vec![]).await.unwrap();
    f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    let changes: Vec<_> = [sent, unspent, unknown]
        .into_iter()
        .enumerate()
        .map(|(i, mut record)| {
            let before = (i < 2).then(|| record.clone());
            record.state = State::Pending;
            record.used_by_operation = Some(owner);
            ProofChange {
                y: record.y,
                before,
                after: Some(record),
            }
        })
        .collect();
    let mut intent = receive(changes.clone(), owner);
    f.db.add_saga(intent.clone()).await.unwrap();
    let before = f.db.storage_capacity().await.unwrap().unwrap();
    let original_ledger = ledger(&f);
    f.db.configure_storage_capacity(before.charged_bytes)
        .await
        .unwrap();
    assert!(f
        .db
        .compare_and_swap_proofs(changes.clone(), Some(&intent))
        .await
        .is_err());
    assert_eq!(ledger(&f), original_ledger);
    assert_eq!(f.db.get_saga(&owner).await.unwrap(), Some(intent.clone()));
    f.db.configure_storage_capacity(before.maximum_bytes)
        .await
        .unwrap();

    let mut stale = changes.clone();
    stale[1].before.as_mut().unwrap().state = State::Spent;
    assert!(!f
        .db
        .compare_and_swap_proofs(stale, Some(&intent))
        .await
        .unwrap());
    let mut stale = intent.clone();
    stale.version += 1;
    assert!(!f
        .db
        .compare_and_swap_proofs(changes.clone(), Some(&stale))
        .await
        .unwrap());
    assert_eq!(ledger(&f), original_ledger);
    assert_eq!(f.db.storage_capacity().await.unwrap(), Some(before));

    f.raw.execute_batch("CREATE TRIGGER reject_acquisition_reserve BEFORE UPDATE OF reserved ON wallet_storage_accounts WHEN OLD.reserved=0 AND NEW.reserved>0 BEGIN SELECT RAISE(ABORT,'injected reservation failure'); END").unwrap();
    assert!(f
        .db
        .compare_and_swap_proofs(changes.clone(), Some(&intent))
        .await
        .is_err());
    assert_eq!(ledger(&f), original_ledger);
    assert_eq!(f.db.storage_capacity().await.unwrap(), Some(before));
    for change in &changes {
        assert_eq!(
            f.db.get_proofs_by_ys(vec![change.y]).await.unwrap(),
            change.before.iter().cloned().collect::<Vec<_>>()
        );
    }
    f.raw
        .execute_batch("DROP TRIGGER reject_acquisition_reserve")
        .unwrap();

    assert!(f
        .db
        .compare_and_swap_proofs(changes.clone(), Some(&intent))
        .await
        .unwrap());
    f.db.kv_write("capacity-test", "", "fill", &[])
        .await
        .unwrap();
    let charged =
        f.db.storage_capacity()
            .await
            .unwrap()
            .unwrap()
            .charged_bytes;
    f.db.kv_write(
        "capacity-test",
        "",
        "fill",
        &vec![0; (before.maximum_bytes - charged) as usize],
    )
    .await
    .unwrap();
    let full = f.db.storage_capacity().await.unwrap();
    let reopened = open(&f.path).await;
    intent.update_state(WalletSagaState::Receive(ReceiveSagaState::Cancelling));
    assert!(reopened.update_saga(intent.clone()).await.unwrap());
    let rollback = changes
        .into_iter()
        .map(|change| ProofChange {
            y: change.y,
            before: change.after,
            after: change.before,
        })
        .collect();
    assert!(reopened
        .compare_and_swap_proofs(rollback, Some(&intent))
        .await
        .expect("admitted cancellation must finish at capacity"));
    assert_eq!(reopened.storage_capacity().await.unwrap(), full);
    for original in &originals {
        assert_eq!(
            reopened.get_proofs_by_ys(vec![original.y]).await.unwrap(),
            vec![original.clone()]
        );
    }
    let proof_owners: Vec<_> = ledger(&f)
        .into_iter()
        .filter(|(key, _, _)| key.starts_with("proof:"))
        .collect();
    assert_eq!(proof_owners.len(), 2);
    assert!(proof_owners
        .iter()
        .all(|(_, storage_owner, _)| storage_owner == &owner.to_string()));
    reopened.delete_saga(&owner).await.unwrap();
    let released = reopened.storage_capacity().await.unwrap().unwrap();
    assert!(released.charged_bytes < released.maximum_bytes);
    reopened.delete_saga(&owner).await.unwrap();
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(released));
}

#[tokio::test]
async fn acquisition_consumes_an_existing_allowance_without_increasing_it() {
    let f = Fixture::new().await;
    let owner = Uuid::from_u128(3);
    let mut acquired = proof();
    acquired.state = State::Pending;
    acquired.used_by_operation = Some(owner);
    let changes = vec![ProofChange {
        y: acquired.y,
        before: None,
        after: Some(acquired),
    }];
    let intent = receive(changes.clone(), owner);
    f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    f.db.reserve_saga_storage(&intent, 65536, &[])
        .await
        .unwrap();
    let before = f.db.storage_capacity().await.unwrap();
    assert!(f
        .db
        .compare_and_swap_proofs(changes, Some(&intent))
        .await
        .unwrap());
    assert_eq!(f.db.storage_capacity().await.unwrap(), before);
    let reserved: i64 = f
        .raw
        .query_row(
            "SELECT reserved FROM wallet_storage_accounts WHERE owner=?1",
            [owner.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reserved, 65536);
}
