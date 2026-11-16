//! Real SQLite acquisition boundaries, without manufacturing successful send states.
//! Dropping preparation at a post-commit pause models interruption before its reply.
//! The second wallet shares the database and exercises ordinary recovery/cancellation.

use std::sync::Mutex;
use std::time::Duration;

use cdk_common::wallet::{OperationData, ProofInfo, WalletSaga};
use tokio::sync::oneshot;

use super::interposed_store::Action;
use super::prepared_cancel_tests::{
    assert_cancelled, fixture, retained_options, Fixture, Snapshot,
};
use crate::nuts::{Proofs, State};
use crate::wallet::test_utils::{test_keyset, test_mint_url, test_proof_info};
use crate::wallet::SendOptions;
use crate::Amount;

#[derive(Clone, Copy, Debug)]
enum Boundary {
    Saved,
    Acquired,
    Promoted,
}

fn hook(f: &Fixture, boundary: Boundary) -> &Mutex<Option<Action>> {
    match boundary {
        Boundary::Saved => &f.store.after_add_saga,
        Boundary::Acquired => &f.store.after_proofs,
        Boundary::Promoted => &f.store.after_saga,
    }
}

fn pause(f: &Fixture, boundary: Boundary) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (reached, arrival) = oneshot::channel();
    let (release, resumed) = oneshot::channel();
    *hook(f, boundary).lock().unwrap() = Some(Action::Pause {
        reached,
        release: resumed,
    });
    (arrival, release)
}

fn options(amount: u64) -> SendOptions {
    let mut options = retained_options(amount);
    options
        .metadata
        .insert("request".to_owned(), "acquisition-original".to_owned());
    options
}

async fn proofs(f: &Fixture) -> Vec<ProofInfo> {
    let mut proofs = f
        .store
        .inner
        .get_proofs(None, None, None, None)
        .await
        .unwrap();
    proofs.sort_by_key(|proof| proof.y);
    proofs
}

async fn only_saga(f: &Fixture) -> WalletSaga {
    let mut sagas = f.store.inner.get_incomplete_sagas().await.unwrap();
    assert_eq!(
        sagas.len(),
        1,
        "one original operation remains discoverable"
    );
    sagas.remove(0)
}

fn original_inputs(saga: &WalletSaga) -> Proofs {
    let OperationData::Send(data) = &saga.data else {
        panic!("expected original send");
    };
    let preparation = data.preparation.as_ref().unwrap();
    assert_eq!(preparation.metadata, options(saga.amount.to_u64()).metadata);
    assert!(preparation.retain_cancellation);
    preparation
        .inputs
        .iter()
        .map(|row| row.proof.clone())
        .collect()
}

async fn no_mint_spend(f: &Fixture) {
    assert!(f.mint.captured_swap_requests().is_empty());
    assert!(f
        .store
        .inner
        .list_transactions(None, None, None)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn failed_initial_save_leaves_no_reserved_inputs() {
    let f = fixture().await;
    let before = proofs(&f).await;
    *f.store.before_add_saga.lock().unwrap() = Some(Action::Fail);
    assert!(f.wallet.prepare_send(32.into(), options(32)).await.is_err());
    assert!(f
        .store
        .inner
        .get_incomplete_sagas()
        .await
        .unwrap()
        .is_empty());
    assert!(
        proofs(&f).await == before,
        "failed intent save must not acquire coins"
    );
    assert_eq!(f.winner.total_balance().await.unwrap(), Amount::from(64));
    no_mint_spend(&f).await;
}

#[tokio::test]
async fn committed_initial_save_error_retains_recoverable_original() {
    let f = fixture().await;
    let before = proofs(&f).await;
    *f.store.after_add_saga.lock().unwrap() = Some(Action::Fail);
    assert!(f.wallet.prepare_send(32.into(), options(32)).await.is_err());
    let original = only_saga(&f).await;
    let _inputs = original_inputs(&original);
    // A lost save reply is not retried by reinserting or replacing the saga.
    let report = f.winner.recover_incomplete_sagas().await.unwrap();
    assert_eq!(report.failed, 0);
    let cancelled = assert_cancelled(&f, &original).await;
    assert!(proofs(&f).await == before);
    f.winner.recover_incomplete_sagas().await.unwrap();
    cancelled.assert_unchanged(&f).await;
}

#[tokio::test]
async fn interrupted_preparation_recovers_after_each_durable_boundary() {
    for boundary in [Boundary::Saved, Boundary::Acquired, Boundary::Promoted] {
        let f = fixture().await;
        let before = proofs(&f).await;
        let (reached, release) = pause(&f, boundary);
        let original;
        {
            let preparing = f.wallet.prepare_send(32.into(), options(32));
            tokio::pin!(preparing);
            tokio::select! {
                result = &mut preparing => panic!("preparation missed {boundary:?} pause: {}", result.is_ok()),
                result = tokio::time::timeout(Duration::from_secs(5), reached) => result.unwrap().unwrap(),
            }
            original = only_saga(&f).await;
            let _inputs = original_inputs(&original);
            assert_eq!(
                original.state.state_str(),
                match boundary {
                    Boundary::Saved | Boundary::Acquired => "prepared_acquiring",
                    Boundary::Promoted => "proofs_reserved",
                }
            );
            let current = proofs(&f).await;
            match boundary {
                Boundary::Saved => assert!(current == before, "intent precedes custody"),
                Boundary::Acquired | Boundary::Promoted => {
                    assert_eq!(current.len(), before.len());
                    assert!(current.iter().all(|row| row.state == State::Reserved
                        && row.used_by_operation == Some(original.id)));
                }
            }
            // Drop the actual preparation future before storage returns to it.
        }
        drop(release);
        no_mint_spend(&f).await;
        let report = f.winner.recover_incomplete_sagas().await.unwrap();
        assert_eq!(report.failed, 0, "{boundary:?}");
        let cancelled = assert_cancelled(&f, &original).await;
        assert!(
            proofs(&f).await == before,
            "recover exact original input rows"
        );
        assert_eq!(f.winner.total_balance().await.unwrap(), Amount::from(64));
        f.winner.recover_incomplete_sagas().await.unwrap();
        cancelled.assert_unchanged(&f).await;
    }
}

#[tokio::test]
async fn cancellation_fences_paused_acquisition_and_promotion() {
    for boundary in [Boundary::Saved, Boundary::Acquired] {
        let f = fixture().await;
        let before = proofs(&f).await;
        let (reached, release) = pause(&f, boundary);
        let preparing = f.wallet.prepare_send(32.into(), options(32));
        tokio::pin!(preparing);
        tokio::select! {
            result = &mut preparing => panic!("preparation missed {boundary:?} pause: {}", result.is_ok()),
            result = tokio::time::timeout(Duration::from_secs(5), reached) => result.unwrap().unwrap(),
        }
        let original = only_saga(&f).await;
        assert_eq!(original.state.state_str(), "prepared_acquiring");
        f.winner
            .cancel_prepared_send(original.id, original.amount, original_inputs(&original))
            .await
            .unwrap();
        let cancelled = assert_cancelled(&f, &original).await;
        assert!(proofs(&f).await == before);
        release.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), preparing)
            .await
            .unwrap();
        assert!(
            result.is_err(),
            "a stale preparation cannot return a usable handle"
        );
        cancelled.assert_unchanged(&f).await;
    }
}

#[tokio::test]
async fn foreign_preparation_prevents_partial_acquisition_and_survives_cancellation() {
    let f = fixture().await;
    // Two fixture coins force the original 96-sat preparation to select both.
    f.store
        .inner
        .update_proofs(
            vec![test_proof_info(test_keyset().id, 64, test_mint_url())],
            vec![],
        )
        .await
        .unwrap();
    let before = proofs(&f).await;
    assert_eq!(before.len(), 2);
    let (reached, release) = pause(&f, Boundary::Saved);
    let preparing = f.wallet.prepare_send(96.into(), options(96));
    tokio::pin!(preparing);
    tokio::select! {
        result = &mut preparing => panic!("preparation missed saved-intent pause: {}", result.is_ok()),
        result = tokio::time::timeout(Duration::from_secs(5), reached) => result.unwrap().unwrap(),
    }
    let original = only_saga(&f).await;
    let inputs = original_inputs(&original);
    assert_eq!(inputs.len(), 2);
    assert_eq!(original.state.state_str(), "prepared_acquiring");
    assert!(proofs(&f).await == before, "intent alone owns no coins");

    let winner = f
        .winner
        .prepare_send(32.into(), retained_options(32))
        .await
        .unwrap();
    let winner_id = winner.operation_id();
    let captured = Snapshot::capture(&f, winner_id).await;
    let owned = proofs(&f).await;
    assert_eq!(
        owned
            .iter()
            .filter(|row| row.used_by_operation == Some(winner_id))
            .count(),
        1
    );
    assert_eq!(
        owned
            .iter()
            .filter(|row| row.state == State::Unspent && row.used_by_operation.is_none())
            .count(),
        1
    );
    release.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), preparing)
        .await
        .unwrap();
    assert!(
        result.is_err(),
        "one foreign row must reject the whole acquisition"
    );
    captured.assert_unchanged(&f).await;
    f.winner
        .cancel_prepared_send(original.id, original.amount, inputs)
        .await
        .unwrap();
    captured.assert_unchanged(&f).await;
    assert_cancelled(&f, &original).await;
    assert_eq!(f.winner.total_balance().await.unwrap(), Amount::from(64));
    no_mint_spend(&f).await;
    // Keep the genuine unrelated preparation alive throughout all comparisons.
    drop(winner);
}
