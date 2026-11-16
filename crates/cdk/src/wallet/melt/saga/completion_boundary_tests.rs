//! Paid completion retries preserve the request and its original change allocation.
use std::collections::HashMap;
use std::sync::Arc;

use cdk_common::wallet::{MeltSagaState, OperationData, TransactionStatus, WalletSagaState};

use super::swap_fixture::Fixture;
use crate::nuts::{MeltQuoteState, PreMintSecrets, ProofsMethods, State};
use crate::wallet::send::saga::interposed_store::{Action, Interposed};
use crate::wallet::test_utils::{test_keyset, MockMintConnector};
use crate::{Amount, Error};

#[tokio::test]
async fn request_preserves_allocated_change_range_when_counter_moves() {
    let mut f = Fixture::new().await;
    let keyset = test_keyset().id;
    let initial = f
        .wallet
        .localstore
        .increment_keyset_counter(&keyset, 7)
        .await
        .unwrap();
    let store = Arc::new(Interposed::new(f.wallet.localstore.clone()));
    *store.after_counter_increment.lock().unwrap() = Some(19);
    f.wallet.localstore = store.clone();
    let proofs = f.wallet.get_unspent_proofs().await.unwrap();
    let prepared = super::MeltSaga::new(&f.wallet)
        .prepare_with_proofs(&f.quote.id, proofs.clone(), HashMap::new())
        .await
        .unwrap();
    let id = prepared.operation_id();
    let requested = prepared
        .request_melt_with_options(Default::default())
        .await
        .unwrap();
    let saga = f.wallet.localstore.get_saga(&id).await.unwrap().unwrap();
    assert_eq!(
        saga.state,
        WalletSagaState::Melt(MeltSagaState::MeltRequested)
    );
    let OperationData::Melt(data) = &saga.data else {
        panic!("expected melt");
    };
    let start = data.counter_start.unwrap();
    let end = data.counter_end.unwrap();
    let messages = data.change_blinded_messages.as_ref().unwrap();
    assert_eq!(start, initial);
    assert_eq!(end - start, messages.len() as u32);
    assert_eq!(
        PreMintSecrets::restore_batch(keyset, &f.wallet.seed, start, end)
            .unwrap()
            .blinded_messages(),
        *messages
    );
    assert_eq!(
        requested.state_data.premint_secrets.blinded_messages(),
        *messages
    );
    assert!(store.after_counter_increment.lock().unwrap().is_none());
    assert_eq!(
        store
            .inner
            .increment_keyset_counter(&keyset, 0)
            .await
            .unwrap(),
        end + 19
    );
    let payment = data.payment.as_ref().unwrap();
    assert_eq!(payment.receipt.status, TransactionStatus::Pending);
    assert_eq!(payment.receipt.ys, proofs.ys().unwrap());
    assert_eq!(
        f.wallet
            .localstore
            .get_transaction(payment.receipt.id())
            .await
            .unwrap(),
        Some(payment.receipt.clone())
    );
    assert!(payment
        .inputs
        .iter()
        .all(|input| { input.state == State::Pending && input.used_by_operation == Some(id) }));
    assert!(f.client.last_post_melt_request().is_none());
}

fn require_local_retry(client: &MockMintConnector) {
    client.set_melt_quote_status_response(Err(Error::Timeout));
    client._set_restore_response(Err(Error::Timeout));
    client.set_post_melt_response(Err(Error::Timeout));
}

fn assert_no_mint_request(client: &MockMintConnector) {
    assert!(client.melt_quote_status_response.lock().unwrap().is_some());
    assert!(client.restore_response.lock().unwrap().is_some());
    assert!(client.post_melt_response.lock().unwrap().is_some());
    assert!(client.last_post_melt_request().is_none());
}

#[tokio::test]
async fn paid_ready_retries_locally_after_final_publication_fails() {
    let mut f = super::paid_change_fixture::Fixture::new().await;
    let store = Arc::new(Interposed::new(f.wallet.localstore.clone()));
    f.wallet.localstore = store.clone();
    let original = super::custody::data(&f.wallet, &f.saga)
        .unwrap()
        .payment
        .as_ref()
        .unwrap()
        .receipt
        .clone();
    let quote = store
        .inner
        .get_melt_quote(&f.quote.id)
        .await
        .unwrap()
        .unwrap();
    *store.before_melt_completion.lock().unwrap() = Some(Action::Fail);
    assert!(f.restore(&[16, 8]).await.is_err());
    let ready = store.inner.get_saga(&f.saga.id).await.unwrap().unwrap();
    assert_eq!(ready.state, WalletSagaState::Melt(MeltSagaState::PaidReady));
    let saved = super::custody::data(&f.wallet, &ready)
        .unwrap()
        .payment
        .as_ref()
        .unwrap();
    assert_eq!(saved.receipt.status, TransactionStatus::Completed);
    assert_eq!(saved.receipt.payment_proof.as_deref(), Some("preimage"));
    assert_eq!(saved.receipt.fee, Amount::ZERO);
    assert_eq!(
        store.inner.get_melt_quote(&f.quote.id).await.unwrap(),
        Some(quote.clone())
    );
    assert_eq!(
        store.inner.get_transaction(original.id()).await.unwrap(),
        Some(original)
    );
    let inputs = store
        .inner
        .get_proofs_by_ys(f.inputs.ys().unwrap())
        .await
        .unwrap();
    assert!(inputs.iter().all(|input| input.state == State::Spent));

    let mut later = saved.change.as_ref().unwrap()[0].clone();
    later.state = State::Reserved;
    later.used_by_operation = Some(uuid::Uuid::new_v4());
    store
        .inner
        .update_proofs(vec![later.clone()], vec![])
        .await
        .unwrap();
    require_local_retry(&f.client);
    let result = f.wallet.resume_melt_saga(&ready).await.unwrap().unwrap();
    assert_eq!(result.state(), MeltQuoteState::Paid);
    assert_eq!(result.payment_proof(), Some("preimage"));
    assert_eq!(
        result.change().unwrap().total_amount().unwrap(),
        Amount::from(24)
    );
    assert_eq!(
        store
            .inner
            .get_transaction(saved.receipt.id())
            .await
            .unwrap(),
        Some(saved.receipt.clone())
    );
    assert_eq!(
        store.inner.get_proofs_by_ys(vec![later.y]).await.unwrap(),
        vec![later]
    );
    let completed_quote = store
        .inner
        .get_melt_quote(&f.quote.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed_quote.state, MeltQuoteState::Paid);
    assert_eq!(completed_quote.used_by_operation, None);
    assert_eq!(completed_quote.payment_proof.as_deref(), Some("preimage"));
    assert_eq!(completed_quote.version, quote.version + 1);
    assert!(store.inner.get_saga(&ready.id).await.unwrap().is_none());
    assert_no_mint_request(&f.client);
}

#[tokio::test]
async fn completed_retries_do_not_recreate_removed_history_or_revive_change() {
    for lost_completion_reply in [false, true] {
        let mut f = super::paid_change_fixture::Fixture::new().await;
        let store = Arc::new(Interposed::new(f.wallet.localstore.clone()));
        f.wallet.localstore = store.clone();
        if lost_completion_reply {
            *store.after_melt_completion.lock().unwrap() = Some(Action::Fail);
        } else {
            *store.before_delete_saga.lock().unwrap() = Some(Action::Fail);
        }
        let result = f.restore(&[16, 8]).await;
        if lost_completion_reply {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap().unwrap().state(), MeltQuoteState::Paid);
        }
        let completed = store.inner.get_saga(&f.saga.id).await.unwrap().unwrap();
        assert_eq!(
            completed.state,
            WalletSagaState::Melt(MeltSagaState::Completed)
        );
        let saved = super::custody::data(&f.wallet, &completed)
            .unwrap()
            .payment
            .as_ref()
            .unwrap();
        assert_eq!(
            store
                .inner
                .get_transaction(saved.receipt.id())
                .await
                .unwrap(),
            Some(saved.receipt.clone())
        );
        let quote = store
            .inner
            .get_melt_quote(&f.quote.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(quote.state, MeltQuoteState::Paid);
        assert_eq!(quote.used_by_operation, None);
        store
            .inner
            .remove_transaction(saved.receipt.id())
            .await
            .unwrap();
        let mut spent = saved.change.clone().unwrap();
        for output in &mut spent {
            output.state = State::Spent;
            output.used_by_operation = Some(uuid::Uuid::new_v4());
        }
        store
            .inner
            .update_proofs(spent.clone(), vec![])
            .await
            .unwrap();
        let ys = spent.iter().map(|output| output.y).collect();
        let before = store.inner.get_proofs_by_ys(ys).await.unwrap();
        require_local_retry(&f.client);
        let result = f
            .wallet
            .resume_melt_saga(&completed)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.state(), MeltQuoteState::Paid);
        assert_eq!(result.payment_proof(), Some("preimage"));
        assert_eq!(
            result.change().unwrap().total_amount().unwrap(),
            Amount::from(24)
        );
        assert!(store
            .inner
            .get_transaction(saved.receipt.id())
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            store.inner.get_melt_quote(&f.quote.id).await.unwrap(),
            Some(quote)
        );
        assert_eq!(
            store
                .inner
                .get_proofs_by_ys(before.iter().map(|output| output.y).collect())
                .await
                .unwrap(),
            before
        );
        assert!(store.inner.get_saga(&completed.id).await.unwrap().is_none());
        assert!(f
            .wallet
            .resume_melt_saga(&completed)
            .await
            .unwrap()
            .is_none());
        assert_no_mint_request(&f.client);
    }
}

#[tokio::test]
async fn oversized_payment_evidence_preserves_pending_custody() {
    let f = super::paid_change_fixture::Fixture::new().await;
    let saved = f.wallet.localstore.get_saga(&f.saga.id).await.unwrap();
    let ys = f.inputs.ys().unwrap();
    let coins = f
        .wallet
        .localstore
        .get_proofs_by_ys(ys.clone())
        .await
        .unwrap();
    let quote = f
        .wallet
        .localstore
        .get_melt_quote(&f.quote.id)
        .await
        .unwrap();
    let receipts = f
        .wallet
        .localstore
        .list_transactions(None, None, None)
        .await
        .unwrap();
    let result = super::completion::record_paid(
        &f.wallet,
        &f.saga,
        Some("x".repeat(cdk_common::database::wallet::MAX_MELT_PAYMENT_PROOF_BYTES + 1)),
        Some(f.response(&[16, 8]).signatures),
        cdk_common::wallet::KeysetLoadPolicy::CacheOnly,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        f.wallet.localstore.get_saga(&f.saga.id).await.unwrap(),
        saved
    );
    assert_eq!(
        f.wallet.localstore.get_proofs_by_ys(ys).await.unwrap(),
        coins
    );
    assert_eq!(
        f.wallet
            .localstore
            .get_melt_quote(&f.quote.id)
            .await
            .unwrap(),
        quote
    );
    assert_eq!(
        f.wallet
            .localstore
            .list_transactions(None, None, None)
            .await
            .unwrap(),
        receipts
    );
    assert_eq!(
        f.restore(&[16, 8]).await.unwrap().unwrap().state(),
        MeltQuoteState::Paid
    );
}
