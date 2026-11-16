//! Prepared cleanup must not undo a newer owner of the same send operation.
//! The mock mint and SQLite fixture are the existing send-confirmation fixtures;
//! all newer saga states below are produced by the public confirmation APIs.

use std::sync::Arc;
use std::time::Duration;

use cdk_common::wallet::{
    OperationData, ProofInfo, SendSagaState, Transaction, WalletSaga, WalletSagaState,
};
use tokio::sync::oneshot;

use super::confirmation_tests::response;
use super::interposed_store::{Action, Interposed};
use super::state::Prepared;
use super::SendSaga;
use crate::nuts::{Proofs, State};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock_seed, test_keyset, test_mint_url, test_proof_info,
    MockMintConnector,
};
use crate::wallet::SendOptions;
use crate::{Amount, Error, Wallet};

pub(super) struct Fixture {
    pub(super) wallet: Wallet,
    pub(super) winner: Wallet,
    pub(super) mint: Arc<MockMintConnector>,
    pub(super) store: Arc<Interposed>,
}

pub(super) async fn fixture() -> Fixture {
    let inner = create_test_db().await;
    let mut keyset = test_keyset();
    keyset.input_fee_ppk = 0;
    inner
        .update_proofs(
            vec![test_proof_info(keyset.id, 64, test_mint_url())],
            vec![],
        )
        .await
        .unwrap();
    let mint = Arc::new(MockMintConnector::new());
    mint.reset_default_mint_state();
    mint.set_active_keyset(keyset);
    mint.set_post_swap_response(Err(Error::Timeout));
    let store = Arc::new(Interposed::new(inner.clone()));
    let mut wallet = create_test_wallet_with_mock_seed(store.clone(), mint.clone(), [7; 64]).await;
    let mut winner = create_test_wallet_with_mock_seed(inner, mint.clone(), [7; 64]).await;
    wallet.set_target_proof_count(0);
    winner.set_target_proof_count(0);
    Fixture {
        wallet,
        winner,
        mint,
        store,
    }
}

fn options() -> SendOptions {
    SendOptions {
        max_total_amount: Some(32.into()),
        required_keyset_id: Some(test_keyset().id),
        ..Default::default()
    }
}

pub(super) struct Snapshot {
    saga: WalletSaga,
    proofs: Vec<ProofInfo>,
    transactions: Vec<Transaction>,
    spendable: Amount,
    requests: usize,
}

impl Snapshot {
    pub(super) async fn capture(f: &Fixture, id: uuid::Uuid) -> Self {
        let mut proofs = f
            .store
            .inner
            .get_proofs(None, None, None, None)
            .await
            .unwrap();
        proofs.sort_by_key(|proof| proof.y);
        let mut transactions = f
            .store
            .inner
            .list_transactions(None, None, None)
            .await
            .unwrap();
        transactions.sort();
        Self {
            saga: f.store.inner.get_saga(&id).await.unwrap().unwrap(),
            proofs,
            transactions,
            spendable: f.winner.total_balance().await.unwrap(),
            requests: f.mint.captured_swap_requests().len(),
        }
    }

    pub(super) async fn assert_unchanged(&self, f: &Fixture) {
        assert!(
            f.store
                .inner
                .get_saga(&self.saga.id)
                .await
                .unwrap()
                .as_ref()
                == Some(&self.saga),
            "stale prepared cleanup must retain the newer exact saga"
        );
        let after = Self::capture(f, self.saga.id).await;
        assert!(
            after.proofs == self.proofs,
            "stale cleanup must preserve every proof owner and state"
        );
        assert!(
            after.transactions == self.transactions,
            "stale cleanup must preserve the winner's receipt"
        );
        assert_eq!(after.spendable, self.spendable, "no coin resurrection");
        assert_eq!(
            after.requests, self.requests,
            "cleanup cannot submit another mint request"
        );
    }
}

async fn advance(f: &Fixture, prepared: &SendSaga<'_, Prepared>, token_created: bool) -> Snapshot {
    let error = f
        .winner
        .confirm_send(
            prepared.operation_id(),
            prepared.amount(),
            prepared.options().clone(),
            prepared.proofs_to_swap().clone(),
            prepared.proofs_to_send().clone(),
            prepared.swap_fee(),
            prepared.send_fee(),
            None,
        )
        .await
        .err()
        .unwrap();
    assert!(
        matches!(error, Error::Timeout),
        "original confirmation reaches the mock mint"
    );
    let saved = f
        .store
        .inner
        .get_saga(&prepared.operation_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        saved.state,
        WalletSagaState::Send(SendSagaState::Confirming)
    );
    assert_eq!(f.mint.captured_swap_requests().len(), 1);
    if token_created {
        f.mint._set_restore_response(Ok(response(&saved)));
        f.winner.recover_send_confirmation(saved.id).await.unwrap();
    }
    let snapshot = Snapshot::capture(f, saved.id).await;
    assert_eq!(
        snapshot.saga.state,
        WalletSagaState::Send(if token_created {
            SendSagaState::TokenCreated
        } else {
            SendSagaState::Confirming
        })
    );
    assert_eq!(
        snapshot.spendable,
        Amount::from(if token_created { 32 } else { 0 })
    );
    snapshot
}

#[tokio::test]
async fn stale_prepared_cancel_preserves_confirming_operation() {
    stale_cancel(false).await;
}

#[tokio::test]
async fn stale_prepared_cancel_preserves_token_created_operation() {
    stale_cancel(true).await;
}

async fn stale_cancel(token_created: bool) {
    let f = fixture().await;
    // Retaining the production Prepared saga isolates the public cancel_send
    // window after it loads the original record and before its first write.
    let prepared = SendSaga::new(&f.wallet)
        .prepare(32.into(), options())
        .await
        .unwrap();
    let snapshot = advance(&f, &prepared, token_created).await;
    let cancelled = prepared.cancel().await;
    snapshot.assert_unchanged(&f).await;
    assert!(
        cancelled.is_err(),
        "a newer confirmation prevents prepared cancellation"
    );
}

#[tokio::test]
async fn stale_definitive_compensation_preserves_confirming_operation() {
    stale_compensation(false).await;
}

#[tokio::test]
async fn stale_definitive_compensation_preserves_token_created_operation() {
    stale_compensation(true).await;
}

async fn stale_compensation(token_created: bool) {
    let f = fixture().await;
    let prepared = SendSaga::new(&f.wallet)
        .prepare(32.into(), options())
        .await
        .unwrap();
    // The stale worker passes its real storage ownership preflight. Suspend it
    // before a second actor confirms the same operation; no saga is fabricated.
    let (reached, arrived) = oneshot::channel();
    let (release, proceed) = oneshot::channel();
    *f.store.after_empty_proof_check.lock().unwrap() = Some((reached, proceed));
    let id = prepared.operation_id();
    let mut tightened = prepared.options().clone();
    tightened.max_total_amount = Some(31.into());
    let mut stale = Box::pin(f.wallet.confirm_send(
        id,
        prepared.amount(),
        tightened,
        prepared.proofs_to_swap().clone(),
        prepared.proofs_to_send().clone(),
        prepared.swap_fee(),
        prepared.send_fee(),
        None,
    ));
    tokio::select! {
        result = &mut stale => panic!("stale confirmation completed before preflight pause: {}", result.is_ok()),
        result = arrived => result.unwrap(),
        _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("storage preflight pause did not arrive"),
    }
    let snapshot = advance(&f, &prepared, token_created).await;
    // A tightened spend allowance fails definitively after the successful
    // storage preflight. Its cleanup must still respect the newer winner.
    release.send(()).unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), stale)
        .await
        .unwrap()
        .err()
        .unwrap();
    assert!(
        matches!(error, Error::MaxAmountExceeded { .. }),
        "exercise the definitive-failure cleanup: {error}"
    );
    snapshot.assert_unchanged(&f).await;
}

pub(super) fn retained_options(amount: u64) -> SendOptions {
    SendOptions {
        retain_cancellation: true,
        max_total_amount: Some(amount.into()),
        ..options()
    }
}

fn inputs(prepared: &SendSaga<'_, Prepared>) -> Proofs {
    prepared
        .proofs_to_swap()
        .iter()
        .chain(prepared.proofs_to_send())
        .cloned()
        .collect()
}

pub(super) async fn assert_cancelled(f: &Fixture, original: &WalletSaga) -> Snapshot {
    let snapshot = Snapshot::capture(f, original.id).await;
    assert_eq!(
        snapshot.saga.state,
        WalletSagaState::Send(SendSagaState::PreparedCancelled)
    );
    assert_eq!(snapshot.saga.amount, original.amount);
    assert_eq!(snapshot.saga.mint_url, original.mint_url);
    assert_eq!(snapshot.saga.unit, original.unit);
    assert_eq!(snapshot.saga.created_at, original.created_at);
    // Comparing privately avoids putting bearer secrets into a panic message.
    assert!(
        snapshot.saga.data == original.data,
        "original preparation retained"
    );
    assert!(
        snapshot.transactions.is_empty(),
        "no fabricated send or refund"
    );
    assert_eq!(snapshot.requests, 0, "cancellation submits no swap");
    snapshot
}

#[tokio::test]
async fn cancellation_fences_stale_reconstruction_with_and_without_swap() {
    for amount in [32, 64] {
        let f = fixture().await;
        let prepared = SendSaga::new(&f.wallet)
            .prepare(amount.into(), retained_options(amount))
            .await
            .unwrap();
        assert_eq!(prepared.proofs_to_swap().is_empty(), amount == 64);
        let original = Snapshot::capture(&f, prepared.operation_id()).await;
        f.winner
            .cancel_prepared_send(
                prepared.operation_id(),
                prepared.amount(),
                inputs(&prepared),
            )
            .await
            .unwrap();
        let cancelled = assert_cancelled(&f, &original.saga).await;
        assert_eq!(cancelled.spendable, 64.into());
        assert!(cancelled
            .proofs
            .iter()
            .all(|p| { p.state == State::Unspent && p.used_by_operation.is_none() }));

        // Reconstruct from the real pre-cancellation snapshot, as a paused
        // caller can. Its cached retention option cannot override the record.
        let mut stale_options = prepared.options().clone();
        stale_options.retain_cancellation = false;
        let stale = SendSaga::from_prepared(
            &f.wallet,
            prepared.operation_id(),
            prepared.amount(),
            stale_options,
            prepared.proofs_to_swap().clone(),
            prepared.proofs_to_send().clone(),
            prepared.swap_fee(),
            prepared.send_fee(),
            original.saga,
        )
        .unwrap();
        assert!(stale.confirm(None).await.is_err());
        cancelled.assert_unchanged(&f).await;
    }
}

#[tokio::test]
async fn direct_token_confirmation_prevents_prepared_cancellation_without_mint_swap() {
    let f = fixture().await;
    let prepared = SendSaga::new(&f.wallet)
        .prepare(64.into(), retained_options(64))
        .await
        .unwrap();
    assert!(prepared.proofs_to_swap().is_empty());
    let id = prepared.operation_id();
    let original_inputs = inputs(&prepared);
    let token = f
        .winner
        .confirm_send(
            id,
            prepared.amount(),
            prepared.options().clone(),
            prepared.proofs_to_swap().clone(),
            prepared.proofs_to_send().clone(),
            prepared.swap_fee(),
            prepared.send_fee(),
            None,
        )
        .await
        .unwrap();
    let snapshot = Snapshot::capture(&f, id).await;
    assert_eq!(
        snapshot.saga.state,
        WalletSagaState::Send(SendSagaState::TokenCreated)
    );
    let OperationData::Send(data) = &snapshot.saga.data else {
        panic!("send")
    };
    assert!(data.token.as_deref() == Some(token.to_string().as_str()));
    assert!(data.preparation.is_none());
    assert_eq!(
        snapshot.requests, 0,
        "a real direct token needs no mint swap"
    );
    assert_eq!(snapshot.spendable, Amount::ZERO);
    assert_eq!(snapshot.transactions.len(), 1);
    assert!(snapshot
        .proofs
        .iter()
        .all(|p| { p.state == State::PendingSpent && p.used_by_operation == Some(id) }));
    assert!(prepared.cancel().await.is_err());
    assert!(f
        .wallet
        .cancel_prepared_send(id, 64.into(), original_inputs)
        .await
        .is_err());
    snapshot.assert_unchanged(&f).await;
}

#[tokio::test]
async fn confirmation_write_error_cancels_only_uncommitted_preparation() {
    for committed in [false, true] {
        let f = fixture().await;
        let prepared = SendSaga::new(&f.wallet)
            .prepare(64.into(), retained_options(64))
            .await
            .unwrap();
        let id = prepared.operation_id();
        let original = Snapshot::capture(&f, id).await;
        let hook = if committed {
            &f.store.after_saga
        } else {
            &f.store.before_saga
        };
        *hook.lock().unwrap() = Some(Action::Fail);
        assert!(matches!(
            prepared.confirm(None).await,
            Err(Error::Database(_))
        ));

        if committed {
            let saved = Snapshot::capture(&f, id).await;
            assert_eq!(
                saved.saga.state,
                WalletSagaState::Send(SendSagaState::Confirming)
            );
            assert!(saved.proofs == original.proofs);
            assert_eq!(saved.spendable, Amount::ZERO);
            assert!(saved.transactions.is_empty());
            f.winner.recover_send_confirmation(id).await.unwrap();
            let recovered = Snapshot::capture(&f, id).await;
            assert_eq!(
                recovered.saga.state,
                WalletSagaState::Send(SendSagaState::TokenCreated)
            );
            assert_eq!(recovered.transactions.len(), 1);
            assert_eq!(f.winner.total_balance().await.unwrap(), Amount::ZERO);
        } else {
            let cancelled = assert_cancelled(&f, &original.saga).await;
            assert_eq!(cancelled.spendable, 64.into());
            assert!(cancelled.proofs.iter().all(|proof| {
                proof.state == State::Unspent && proof.used_by_operation.is_none()
            }));
        }
        assert!(f.mint.captured_swap_requests().is_empty());
    }
}

#[tokio::test]
async fn prepared_metadata_survives_recovery_and_rejects_rebinding() {
    let f = fixture().await;
    let mut opts = retained_options(32);
    opts.metadata
        .insert("request".to_owned(), "original".to_owned());
    let prepared = SendSaga::new(&f.wallet)
        .prepare(32.into(), opts.clone())
        .await
        .unwrap();
    let original = Snapshot::capture(&f, prepared.operation_id()).await;
    let OperationData::Send(data) = &original.saga.data else {
        panic!("expected send");
    };
    let saved = serde_json::to_value(data.preparation.as_ref().unwrap()).unwrap();
    assert!(saved["metadata"] == serde_json::to_value(&opts.metadata).unwrap());

    opts.metadata
        .insert("request".to_owned(), "replacement".to_owned());
    assert!(matches!(
        f.winner
            .confirm_send(
                prepared.operation_id(),
                prepared.amount(),
                opts,
                prepared.proofs_to_swap().clone(),
                prepared.proofs_to_send().clone(),
                prepared.swap_fee(),
                prepared.send_fee(),
                None,
            )
            .await,
        Err(Error::InvalidOperationState)
    ));
    original.assert_unchanged(&f).await;
    drop(prepared);
    // Another wallet instance recovers from SQLite, without the original handle.
    f.winner.recover_incomplete_sagas().await.unwrap();
    assert_cancelled(&f, &original.saga).await;
}

#[tokio::test]
async fn retained_cancellation_replays_until_exact_acknowledgment() {
    let f = fixture().await;
    let prepared = SendSaga::new(&f.wallet)
        .prepare(32.into(), retained_options(32))
        .await
        .unwrap();
    let id = prepared.operation_id();
    let original_inputs = inputs(&prepared);
    let original = Snapshot::capture(&f, id).await;
    assert!(
        f.wallet
            .acknowledge_prepared_cancellation(id, 32.into(), original_inputs.clone())
            .await
            .is_err(),
        "cannot acknowledge an unfinished cancellation"
    );
    original.assert_unchanged(&f).await;
    f.wallet
        .cancel_prepared_send(id, 32.into(), original_inputs.clone())
        .await
        .unwrap();
    let terminal = assert_cancelled(&f, &original.saga).await;
    f.wallet
        .cancel_prepared_send(id, 32.into(), original_inputs.clone())
        .await
        .unwrap();
    terminal.assert_unchanged(&f).await;

    *f.store.before_delete_saga.lock().unwrap() = Some(Action::Fail);
    assert!(f
        .wallet
        .acknowledge_prepared_cancellation(id, 32.into(), original_inputs.clone())
        .await
        .is_err());
    terminal.assert_unchanged(&f).await;

    // Acknowledgment A passes its exact storage check; B acknowledges while A
    // is paused. A must not recreate evidence or mutate released wallet coins.
    let (reached, arrived) = oneshot::channel();
    let (release, proceed) = oneshot::channel();
    *f.store.after_empty_proof_check.lock().unwrap() = Some((reached, proceed));
    let mut first = Box::pin(f.wallet.acknowledge_prepared_cancellation(
        id,
        32.into(),
        original_inputs.clone(),
    ));
    tokio::select! {
        result = &mut first => panic!("ack completed before pause: {}", result.is_ok()),
        result = arrived => result.unwrap(),
        _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("ack pause did not arrive"),
    }
    f.winner
        .acknowledge_prepared_cancellation(id, 32.into(), original_inputs.clone())
        .await
        .unwrap();
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), first)
        .await
        .unwrap()
        .unwrap();
    assert!(f.store.inner.get_saga(&id).await.unwrap().is_none());
    assert!(
        f.wallet
            .cancel_prepared_send(id, 32.into(), original_inputs.clone())
            .await
            .is_err(),
        "missing evidence does not establish zero-spend cancellation"
    );
    f.wallet
        .acknowledge_prepared_cancellation(id, 32.into(), original_inputs)
        .await
        .unwrap();
    let mut rows = f
        .store
        .inner
        .get_proofs(None, None, None, None)
        .await
        .unwrap();
    rows.sort_by_key(|p| p.y);
    assert!(rows == terminal.proofs);
    assert!(f
        .store
        .inner
        .list_transactions(None, None, None)
        .await
        .unwrap()
        .is_empty());
    assert!(f.mint.captured_swap_requests().is_empty());
}

#[tokio::test]
async fn cancellation_release_failure_preserves_later_legitimate_coin_owner() {
    let f = fixture().await;
    let prepared = SendSaga::new(&f.wallet)
        .prepare(32.into(), retained_options(32))
        .await
        .unwrap();
    let id = prepared.operation_id();
    let original_inputs = inputs(&prepared);
    let original = Snapshot::capture(&f, id).await;
    // First interrupt after the cancellation fence. On retry, the next proof
    // transaction is the actual release, rather than the admission check.
    *f.store.after_saga.lock().unwrap() = Some(Action::Fail);
    assert!(f
        .wallet
        .cancel_prepared_send(id, 32.into(), original_inputs.clone())
        .await
        .is_err());
    let fenced = Snapshot::capture(&f, id).await;
    assert_eq!(
        fenced.saga.state,
        WalletSagaState::Send(SendSagaState::PreparedCancelling)
    );
    assert!(fenced.proofs == original.proofs);
    *f.store.after_proofs.lock().unwrap() = Some(Action::Fail);
    assert!(f
        .wallet
        .cancel_prepared_send(id, 32.into(), original_inputs.clone())
        .await
        .is_err());
    let released = Snapshot::capture(&f, id).await;
    assert_eq!(
        released.saga.state,
        WalletSagaState::Send(SendSagaState::PreparedCancelling)
    );
    assert_eq!(
        released.spendable,
        64.into(),
        "release really committed before failure"
    );
    assert!(released
        .proofs
        .iter()
        .all(|p| p.state == State::Unspent && p.used_by_operation.is_none()));

    let next = f
        .winner
        .prepare_send(64.into(), retained_options(64))
        .await
        .unwrap();
    let next_id = next.operation_id();
    let owner = Snapshot::capture(&f, next_id).await;
    assert_eq!(owner.proofs.len(), original.proofs.len());
    assert!(owner.proofs.iter().all(|p| {
        p.state == State::Reserved
            && p.used_by_operation == Some(next_id)
            && original
                .proofs
                .iter()
                .any(|old| old.y == p.y && old.proof == p.proof)
    }));
    f.wallet
        .cancel_prepared_send(id, 32.into(), original_inputs.clone())
        .await
        .unwrap();
    assert_cancelled(&f, &original.saga).await;
    owner.assert_unchanged(&f).await;
    f.wallet
        .cancel_prepared_send(id, 32.into(), original_inputs.clone())
        .await
        .unwrap();
    f.wallet
        .acknowledge_prepared_cancellation(id, 32.into(), original_inputs)
        .await
        .unwrap();
    owner.assert_unchanged(&f).await;
    assert!(f.store.inner.get_saga(&id).await.unwrap().is_none());
}

#[tokio::test]
async fn definitive_failure_uses_original_cancellation_retention() {
    for retained in [false, true] {
        let f = fixture().await;
        let mut opts = retained_options(32);
        opts.retain_cancellation = retained;
        let prepared = SendSaga::new(&f.wallet)
            .prepare(32.into(), opts)
            .await
            .unwrap();
        let id = prepared.operation_id();
        let original = Snapshot::capture(&f, id).await;
        let mut reconstructed = prepared.options().clone();
        reconstructed.retain_cancellation = !retained;
        reconstructed.max_total_amount = Some(31.into());
        let error = f
            .wallet
            .confirm_send(
                id,
                prepared.amount(),
                reconstructed,
                prepared.proofs_to_swap().clone(),
                prepared.proofs_to_send().clone(),
                prepared.swap_fee(),
                prepared.send_fee(),
                None,
            )
            .await
            .err()
            .unwrap();
        assert!(matches!(error, Error::MaxAmountExceeded { .. }));
        assert_eq!(f.winner.total_balance().await.unwrap(), 64.into());
        assert!(f.mint.captured_swap_requests().is_empty());
        assert!(f
            .store
            .inner
            .list_transactions(None, None, None)
            .await
            .unwrap()
            .is_empty());
        if retained {
            assert_cancelled(&f, &original.saga).await;
        } else {
            assert!(
                f.store.inner.get_saga(&id).await.unwrap().is_none(),
                "untracked compensation must not leak a retained tombstone"
            );
        }
    }
}

#[tokio::test]
async fn cancellation_rejects_changed_original_terms_without_mutation() {
    let f = fixture().await;
    let prepared = SendSaga::new(&f.wallet)
        .prepare(32.into(), retained_options(32))
        .await
        .unwrap();
    let id = prepared.operation_id();
    let original_inputs = inputs(&prepared);
    let snapshot = Snapshot::capture(&f, id).await;
    let mut changed = original_inputs.clone();
    changed[0].amount = 1.into();
    let mut duplicated = original_inputs.clone();
    duplicated.push(original_inputs[0].clone());
    for (amount, proposed) in [
        (31_u64, original_inputs.clone()),
        (32, vec![]),
        (32, changed),
        (32, duplicated),
    ] {
        assert!(f
            .wallet
            .cancel_prepared_send(id, amount.into(), proposed)
            .await
            .is_err());
        snapshot.assert_unchanged(&f).await;
    }
    f.wallet
        .cancel_prepared_send(id, 32.into(), original_inputs.clone())
        .await
        .unwrap();
    let terminal = assert_cancelled(&f, &snapshot.saga).await;
    assert!(f
        .wallet
        .acknowledge_prepared_cancellation(id, 31.into(), original_inputs)
        .await
        .is_err());
    assert!(f
        .wallet
        .acknowledge_prepared_cancellation(id, 32.into(), vec![])
        .await
        .is_err());
    terminal.assert_unchanged(&f).await;
}

#[tokio::test]
async fn cancellation_write_failures_retain_release_obligation() {
    for after_transition in [false, true] {
        let f = fixture().await;
        let prepared = SendSaga::new(&f.wallet)
            .prepare(32.into(), retained_options(32))
            .await
            .unwrap();
        let id = prepared.operation_id();
        let original_inputs = inputs(&prepared);
        let original = Snapshot::capture(&f, id).await;
        let hook = if after_transition {
            &f.store.after_saga
        } else {
            &f.store.before_proofs
        };
        *hook.lock().unwrap() = Some(Action::Fail);
        assert!(f
            .wallet
            .cancel_prepared_send(id, 32.into(), original_inputs.clone())
            .await
            .is_err());
        let interrupted = Snapshot::capture(&f, id).await;
        assert_eq!(
            interrupted.saga.state,
            WalletSagaState::Send(if after_transition {
                SendSagaState::PreparedCancelling
            } else {
                SendSagaState::ProofsReserved
            })
        );
        assert!(interrupted.proofs == original.proofs);
        assert_eq!(interrupted.spendable, Amount::ZERO);
        assert!(f
            .wallet
            .acknowledge_prepared_cancellation(id, 32.into(), original_inputs.clone())
            .await
            .is_err());
        interrupted.assert_unchanged(&f).await;
        f.winner
            .cancel_prepared_send(id, 32.into(), original_inputs)
            .await
            .unwrap();
        assert_cancelled(&f, &original.saga).await;
        assert_eq!(f.winner.total_balance().await.unwrap(), 64.into());
    }
}

#[tokio::test]
async fn exhausted_prepared_version_cannot_release_inputs() {
    for version in [u32::MAX - 1, u32::MAX] {
        let f = fixture().await;
        let prepared = SendSaga::new(&f.wallet)
            .prepare(32.into(), retained_options(32))
            .await
            .unwrap();
        let id = prepared.operation_id();
        // State-driven overflow control: retain every real preparation/input
        // field, changing only the stored version instead of billions of writes.
        let mut saved = f.store.inner.get_saga(&id).await.unwrap().unwrap();
        saved.version = version;
        f.store.inner.delete_saga(&id).await.unwrap();
        f.store.inner.add_saga(saved).await.unwrap();
        let snapshot = Snapshot::capture(&f, id).await;
        assert!(f
            .wallet
            .cancel_prepared_send(id, 32.into(), inputs(&prepared))
            .await
            .is_err());
        snapshot.assert_unchanged(&f).await;
    }
}

#[tokio::test]
async fn missing_or_foreign_inputs_before_cancellation_leave_original_evidence() {
    for missing in [true, false] {
        let f = fixture().await;
        let prepared = SendSaga::new(&f.wallet)
            .prepare(32.into(), retained_options(32))
            .await
            .unwrap();
        let id = prepared.operation_id();
        let original = Snapshot::capture(&f, id).await;
        // State-driven custody corruption: no cancellation has released this
        // preparation. It cannot be mistaken for legitimate post-release reuse.
        let mut row = original.proofs[0].clone();
        if missing {
            f.store
                .inner
                .update_proofs(vec![], vec![row.y])
                .await
                .unwrap();
        } else {
            row.used_by_operation = Some(uuid::Uuid::new_v4());
            f.store
                .inner
                .update_proofs(vec![row], vec![])
                .await
                .unwrap();
        }
        let changed = Snapshot::capture(&f, id).await;
        for _ in 0..2 {
            assert!(
                f.wallet
                    .cancel_prepared_send(id, 32.into(), inputs(&prepared))
                    .await
                    .is_err(),
                "missing original custody cannot prove completed release"
            );
            changed.assert_unchanged(&f).await;
        }
    }
}
