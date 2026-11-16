use std::sync::Arc;

use cdk_common::wallet::{KeysetLoadPolicy, TransactionStatus};

use super::*;
use crate::nuts::{BlindSignature, CheckStateResponse, ProofState, RestoreResponse};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_keyset, test_mint_url, test_proof_info,
    MockMintConnector,
};
use crate::wallet::{SendKind, SendOptions};

pub(super) async fn sent() -> (Wallet, Arc<MockMintConnector>, WalletSaga) {
    let db = create_test_db().await;
    let mut keyset = test_keyset();
    keyset.input_fee_ppk = 1000;
    db.update_proofs(
        vec![test_proof_info(keyset.id, 32, test_mint_url())],
        vec![],
    )
    .await
    .unwrap();
    let mint = Arc::new(MockMintConnector::new());
    mint.reset_default_mint_state();
    mint.set_active_keyset(keyset);
    let mut wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
    wallet.set_target_proof_count(0);
    wallet.keysets(KeysetLoadPolicy::Refresh).await.unwrap();
    let prepared = wallet
        .prepare_send(
            32.into(),
            SendOptions {
                send_kind: SendKind::OfflineExact,
                include_fee: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let id = prepared.operation_id();
    prepared.confirm(None).await.unwrap();
    let original = db.get_saga(&id).await.unwrap().unwrap();
    let OperationData::Send(data) = &original.data else {
        panic!("send");
    };
    mint.set_check_state_response(Ok(CheckStateResponse {
        states: data
            .proofs
            .as_ref()
            .unwrap()
            .ys()
            .unwrap()
            .into_iter()
            .map(|y| ProofState {
                y,
                state: State::Unspent,
                witness: None,
            })
            .collect(),
    }));
    (wallet, mint, original)
}

async fn interrupted() -> (Wallet, Arc<MockMintConnector>, WalletSaga, WalletSaga) {
    let (wallet, mint, original) = sent().await;
    let db = wallet.localstore.clone();
    let id = original.id;
    mint.set_post_swap_response(Err(Error::Timeout));
    assert!(matches!(wallet.revoke_send(id).await, Err(Error::Timeout)));
    let saga = db.get_saga(&id).await.unwrap().unwrap();
    assert_eq!(saga.state, WalletSagaState::Send(SendSagaState::Reclaiming));
    assert_eq!(db.get_incomplete_sagas().await.unwrap().len(), 1);
    assert_eq!(
        mint.captured_swap_requests()[0],
        intent(&saga).unwrap().pre_swap.swap_request
    );
    assert_eq!(
        serde_json::from_str::<WalletSaga>(&serde_json::to_string(&saga).unwrap()).unwrap(),
        saga
    );
    (wallet, mint, original, saga)
}

pub(super) fn restored(saga: &WalletSaga) -> RestoreResponse {
    let outputs = intent(saga)
        .unwrap()
        .pre_swap
        .swap_request
        .outputs()
        .clone();
    let signatures = outputs
        .iter()
        .map(|output| BlindSignature {
            amount: output.amount,
            keyset_id: output.keyset_id,
            c: output.blinded_secret,
            dleq: None,
        })
        .collect();
    RestoreResponse {
        outputs,
        signatures,
    }
}

#[tokio::test]
async fn reclaim_keeps_exact_fee_and_receipt_through_partial_restore() {
    let (wallet, mint, _, saved) = interrupted().await;
    let expected = intent(&saved).unwrap().transaction.clone();
    assert_eq!(expected.amount, 31.into());
    assert_eq!(expected.fee, 1.into());
    let mut partial = restored(&saved);
    partial.outputs.pop();
    partial.signatures.pop();
    mint._set_restore_response(Ok(partial));
    assert!(wallet.revoke_send(saved.id).await.is_err());
    assert_eq!(wallet.total_balance().await.unwrap(), Amount::ZERO);
    let current = wallet
        .localstore
        .get_saga(&saved.id)
        .await
        .unwrap()
        .unwrap();
    assert!(intent(&current).unwrap().outputs.is_none());
    assert_eq!(intent(&current).unwrap().transaction, expected);
    let mut keyset = test_keyset();
    keyset.input_fee_ppk = 5000;
    mint.set_active_keyset(keyset);
    wallet.keysets(KeysetLoadPolicy::Refresh).await.unwrap();
    mint._set_restore_response(Ok(restored(&saved)));
    assert_eq!(wallet.revoke_send(saved.id).await.unwrap(), expected.amount);
    assert_eq!(wallet.revoke_send(saved.id).await.unwrap(), expected.amount);
    assert_eq!(
        wallet
            .localstore
            .get_transaction(expected.id())
            .await
            .unwrap(),
        Some(expected.clone())
    );
    let outgoing = wallet
        .localstore
        .get_transaction(TransactionId::from_saga_id(saved.id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outgoing.status, TransactionStatus::Failed);
    assert_eq!(outgoing.role, TransactionRole::Primary);
    assert_eq!(expected.role, TransactionRole::SendRefund);
    assert_ne!(outgoing.id(), expected.id());
    assert_eq!(wallet.total_balance().await.unwrap(), expected.amount);
    assert_eq!(mint.captured_swap_requests().len(), 1);
    let coins = wallet
        .localstore
        .get_proofs(None, None, Some(vec![State::Unspent]), None)
        .await
        .unwrap();
    assert!(coins
        .iter()
        .all(|p| p.derivation_index.is_some() && p.created_by_operation == Some(saved.id)));
    assert!(wallet
        .retire_incoming_transactions(vec![expected.clone()])
        .await
        .unwrap());
    assert!(wallet
        .retire_send_transactions(vec![outgoing])
        .await
        .unwrap());
    assert!(complete(&wallet, current, true).await.is_err());
    assert!(wallet
        .localstore
        .get_transaction(expected.id())
        .await
        .unwrap()
        .is_none());
    assert!(wallet.revoke_send(saved.id).await.is_err());
}

#[tokio::test]
async fn stale_recipient_observation_cannot_delete_reclaim() {
    let (wallet, _, original, saved) = interrupted().await;
    assert!(matches!(
        super::super::claim::complete(&wallet, original).await,
        Err(Error::ConcurrentUpdate)
    ));
    assert_eq!(
        wallet.localstore.get_saga(&saved.id).await.unwrap(),
        Some(saved.clone())
    );
    assert!(wallet
        .localstore
        .get_proofs_by_ys(intent(&saved).unwrap().transaction.ys.clone())
        .await
        .unwrap()
        .iter()
        .all(|p| p.state == State::Pending && p.used_by_operation == Some(saved.id)));
}

#[tokio::test]
async fn resumed_handoff_preserves_later_output_spending_and_ownership() {
    for state in [State::Spent, State::Reserved] {
        let (wallet, _, _, mut saved) = interrupted().await;
        let infos = save_outputs(&wallet, &mut saved).await;
        let later = uuid::Uuid::new_v4();
        let later_coins = infos
            .into_iter()
            .map(|mut p| {
                p.state = state;
                p.used_by_operation = Some(later);
                p
            })
            .collect::<Vec<_>>();
        wallet
            .localstore
            .update_proofs(later_coins.clone(), vec![])
            .await
            .unwrap();
        let amount = complete(&wallet, saved.clone(), true).await.unwrap();
        assert_eq!(amount, intent(&saved).unwrap().transaction.amount);
        for coin in later_coins {
            assert_eq!(
                wallet
                    .localstore
                    .get_proofs_by_ys(vec![coin.y])
                    .await
                    .unwrap(),
                [coin]
            );
        }
        assert!(matches!(
            complete(&wallet, saved, true).await,
            Err(Error::ConcurrentUpdate)
        ));
    }
}

async fn save_outputs(wallet: &Wallet, saved: &mut WalletSaga) -> Vec<ProofInfo> {
    let swap = &intent(saved).unwrap().pre_swap;
    let response = restored(saved);
    let resolved = outputs::resolve(
        wallet,
        swap,
        None,
        &Default::default(),
        None,
        crate::nuts::SwapResponse {
            signatures: response.signatures,
        },
    )
    .await
    .unwrap();
    let infos = resolved
        .change
        .into_iter()
        .map(|proof| {
            let mut info = ProofInfo::new(
                proof,
                wallet.mint_url.clone(),
                State::Unspent,
                wallet.unit.clone(),
            )
            .unwrap();
            info.created_by_operation = Some(saved.id);
            info.derivation_index = swap
                .pre_mint_secrets
                .secrets
                .iter()
                .find(|s| s.secret == info.proof.secret)
                .unwrap()
                .derivation_index;
            info
        })
        .collect::<Vec<_>>();
    let OperationData::Send(data) = &mut saved.data else {
        panic!("send");
    };
    data.reclaim.as_mut().unwrap().outputs = Some(infos.clone());
    advance(wallet, saved).await.unwrap();
    handoff::apply(
        wallet,
        saved,
        &intent(saved)
            .unwrap()
            .inputs
            .iter()
            .map(admission::acquired)
            .collect::<Vec<_>>(),
        infos.clone(),
    )
    .await
    .unwrap();
    infos
}

#[tokio::test]
async fn refund_publication_and_primary_completion_resume_without_a_mint_request() {
    for primary_published in [false, true] {
        let (wallet, mint, _, mut saved) = interrupted().await;
        let outputs = save_outputs(&wallet, &mut saved).await;
        let refund = intent(&saved).unwrap().transaction.clone();
        terminal::publish(
            &wallet,
            &mut saved,
            refund.clone(),
            SendSagaState::Reclaimed,
        )
        .await
        .unwrap();
        let mut primary = wallet
            .localstore
            .get_transaction(TransactionId::from_saga_id(saved.id))
            .await
            .unwrap()
            .unwrap();
        if primary_published {
            primary.status = TransactionStatus::Failed;
            terminal::publish(
                &wallet,
                &mut saved,
                primary.clone(),
                SendSagaState::Reclaimed,
            )
            .await
            .unwrap();
        }
        assert!(!wallet
            .retire_incoming_transactions(vec![refund.clone()])
            .await
            .unwrap());
        let mut terminal_primary = primary.clone();
        terminal_primary.status = TransactionStatus::Failed;
        assert!(!wallet
            .retire_send_transactions(vec![terminal_primary])
            .await
            .unwrap());
        mint._set_restore_response(Err(Error::Timeout));
        assert_eq!(wallet.revoke_send(saved.id).await.unwrap(), refund.amount);
        assert_eq!(mint.captured_swap_requests().len(), 1);
        for output in outputs {
            assert_eq!(
                wallet
                    .localstore
                    .get_proofs_by_ys(vec![output.y])
                    .await
                    .unwrap(),
                [output]
            );
        }
        assert!(wallet
            .localstore
            .get_saga(&saved.id)
            .await
            .unwrap()
            .is_none());
        assert!(wallet
            .retire_incoming_transactions(vec![refund])
            .await
            .unwrap());
        primary.status = TransactionStatus::Failed;
        assert!(wallet
            .retire_send_transactions(vec![primary])
            .await
            .unwrap());
        assert!(complete(&wallet, saved, true).await.is_err());
    }
}
