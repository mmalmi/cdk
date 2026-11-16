//! Receive custody and recovery regressions.
use super::test_utils::ready;
use crate::nuts::State;

#[tokio::test]
async fn stale_receive_completion_cannot_recreate_a_retired_receipt() {
    let (wallet, _, saga) = ready().await;
    let receipt = super::receipt::checkpoint(&saga)
        .unwrap()
        .transaction
        .clone();
    super::finish::complete(&wallet, &saga).await.unwrap();
    wallet
        .localstore
        .remove_transaction(receipt.id())
        .await
        .unwrap();
    let before = wallet
        .localstore
        .get_proofs(None, None, None, None)
        .await
        .unwrap();
    assert!(super::finish::complete(&wallet, &saga).await.is_err());
    assert!(wallet
        .localstore
        .get_transaction(receipt.id())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        wallet
            .localstore
            .get_proofs(None, None, None, None)
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn receive_handoff_preserves_later_spent_or_reserved_outputs() {
    for state in [State::Spent, State::Reserved] {
        let (wallet, _, saga) = ready().await;
        let saved = super::receipt::checkpoint(&saga).unwrap();
        let mut input = saved.inputs[0].after.clone().unwrap();
        input.state = State::Spent;
        let mut output = saved.outputs.as_ref().unwrap()[0].clone();
        output.state = state;
        output.used_by_operation = Some(uuid::Uuid::new_v4());
        wallet
            .localstore
            .update_proofs(vec![input.clone(), output.clone()], vec![])
            .await
            .unwrap();
        wallet.resume_receive_saga(&saga).await.unwrap();
        assert_eq!(
            wallet
                .localstore
                .get_proofs_by_ys(vec![output.y])
                .await
                .unwrap(),
            [output]
        );
        assert_eq!(
            wallet
                .localstore
                .get_proofs_by_ys(vec![input.y])
                .await
                .unwrap(),
            [input]
        );
        assert_eq!(
            wallet
                .localstore
                .get_transaction(saved.transaction.id())
                .await
                .unwrap(),
            Some(saved.transaction.clone())
        );
    }
}

#[tokio::test]
async fn receive_handoff_rejects_missing_or_reowned_input_without_partial_writes() {
    for missing in [true, false] {
        let (wallet, _, saga) = ready().await;
        let saved = super::receipt::checkpoint(&saga).unwrap();
        let input = saved.inputs[0].after.clone().unwrap();
        if missing {
            wallet
                .localstore
                .update_proofs(vec![], vec![input.y])
                .await
                .unwrap();
        } else {
            let mut changed = input.clone();
            changed.used_by_operation = Some(uuid::Uuid::new_v4());
            wallet
                .localstore
                .update_proofs(vec![changed], vec![])
                .await
                .unwrap();
        }
        let before = wallet
            .localstore
            .get_proofs(None, None, None, None)
            .await
            .unwrap();
        assert!(wallet.resume_receive_saga(&saga).await.is_err());
        assert_eq!(
            wallet
                .localstore
                .get_proofs(None, None, None, None)
                .await
                .unwrap(),
            before
        );
        assert!(wallet
            .localstore
            .get_transaction(saved.transaction.id())
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            wallet.localstore.get_saga(&saga.id).await.unwrap(),
            Some(saga)
        );
    }
}

#[tokio::test]
async fn conflicting_receive_receipt_preserves_recoverable_outputs_ready_intent() {
    let (wallet, _, saga) = ready().await;
    let mut receipt = super::receipt::checkpoint(&saga)
        .unwrap()
        .transaction
        .clone();
    receipt.memo = Some("different receipt".to_owned());
    wallet
        .localstore
        .add_transaction(receipt.clone())
        .await
        .unwrap();
    assert!(wallet.resume_receive_saga(&saga).await.is_err());
    assert_eq!(
        wallet
            .localstore
            .get_transaction(receipt.id())
            .await
            .unwrap(),
        Some(receipt)
    );
    assert_eq!(
        wallet.localstore.get_saga(&saga.id).await.unwrap(),
        Some(saga)
    );
}

#[tokio::test]
async fn receive_handoff_preserves_newer_proof_evidence_but_rejects_changed_coin() {
    for same_coin in [true, false] {
        let (wallet, _, saga) = ready().await;
        let saved = super::receipt::checkpoint(&saga).unwrap();
        let mut output = saved.outputs.as_ref().unwrap()[0].clone();
        output.proof.p2pk_e = Some(crate::nuts::SecretKey::generate().public_key());
        if !same_coin {
            output.proof.c = crate::nuts::SecretKey::generate().public_key();
        }
        wallet
            .localstore
            .update_proofs(vec![output.clone()], vec![])
            .await
            .unwrap();
        let result = wallet.resume_receive_saga(&saga).await;
        assert_eq!(result.is_ok(), same_coin);
        assert_eq!(
            wallet
                .localstore
                .get_proofs_by_ys(vec![output.y])
                .await
                .unwrap(),
            [output]
        );
        if !same_coin {
            assert_eq!(
                wallet
                    .localstore
                    .get_proofs_by_ys(vec![saved.inputs[0].y])
                    .await
                    .unwrap(),
                [saved.inputs[0].after.clone().unwrap()]
            );
        }
    }
}
