//! The mint reply is not permission to overwrite newer local custody.
use std::sync::Arc;

use cdk_common::wallet::OperationData;
use tokio::sync::Notify;

use super::paid_change_fixture::Fixture;
use crate::nuts::State;
use crate::Error;

#[tokio::test]
async fn paid_change_recovery_preserves_complete_input_and_new_output_records() {
    let f = Fixture::new().await;
    let original = f
        .wallet
        .localstore
        .get_reserved_proofs(&f.saga.id)
        .await
        .unwrap();
    f.restore(&[16, 8]).await.unwrap().unwrap();
    let mut expected = original[0].clone();
    expected.state = State::Spent;
    assert_eq!(
        f.wallet
            .localstore
            .get_proofs_by_ys(vec![expected.y])
            .await
            .unwrap(),
        [expected]
    );
    let outputs = f
        .wallet
        .localstore
        .get_proofs(None, None, Some(vec![State::Unspent]), None)
        .await
        .unwrap();
    assert_eq!(outputs.len(), 2);
    for output in outputs {
        assert_eq!(output.created_by_operation, Some(f.saga.id));
        assert_eq!(output.used_by_operation, None);
        assert!(output.derivation_index.is_some());
    }
}

#[tokio::test]
async fn paid_change_requires_every_persisted_input_with_its_owner() {
    for case in [
        "missing_set",
        "empty_set",
        "missing_input",
        "wrong_owner",
        "wrong_state",
    ] {
        let f = Fixture::new().await;
        let mut current = f.saga.clone();
        let OperationData::Melt(ref mut data) = current.data else {
            unreachable!()
        };
        let mut input = f
            .wallet
            .localstore
            .get_reserved_proofs(&current.id)
            .await
            .unwrap()[0]
            .clone();
        match case {
            "missing_set" => data.final_proof_ys = None,
            "empty_set" => data.final_proof_ys = Some(vec![]),
            "missing_input" => data
                .final_proof_ys
                .as_mut()
                .unwrap()
                .push(crate::nuts::SecretKey::generate().public_key()),
            "wrong_owner" => input.used_by_operation = Some(uuid::Uuid::new_v4()),
            "wrong_state" => input.state = State::Unspent,
            _ => unreachable!(),
        }
        current.update_state(current.state);
        assert!(f.wallet.localstore.update_saga(current).await.unwrap());
        f.wallet
            .localstore
            .update_proofs(vec![input.clone()], vec![])
            .await
            .unwrap();
        assert!(f.restore(&[16, 8]).await.is_err(), "{case}");
        assert_eq!(
            f.wallet
                .localstore
                .get_proofs_by_ys(vec![input.y])
                .await
                .unwrap(),
            [input]
        );
        assert!(f
            .wallet
            .localstore
            .get_saga(&f.saga.id)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            f.wallet
                .localstore
                .get_proofs(None, None, None, None)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn paid_change_saga_and_inputs_are_fenced_across_restore() {
    for case in [
        "saga_advanced",
        "saga_deleted",
        "input_removed",
        "input_reassigned",
    ] {
        let f = Fixture::new().await;
        let entered = Arc::new(Notify::new());
        let proceed = Arc::new(Notify::new());
        *f.client.restore_gate.lock().unwrap() = Some((entered.clone(), proceed.clone()));
        let input = f
            .wallet
            .localstore
            .get_reserved_proofs(&f.saga.id)
            .await
            .unwrap()[0]
            .clone();
        let intervene = async {
            entered.notified().await;
            match case {
                "saga_advanced" => {
                    let mut saga = f.saga.clone();
                    saga.update_state(saga.state);
                    assert!(f.wallet.localstore.update_saga(saga).await.unwrap());
                }
                "saga_deleted" => f.wallet.localstore.delete_saga(&f.saga.id).await.unwrap(),
                "input_removed" => f
                    .wallet
                    .localstore
                    .update_proofs(vec![], vec![input.y])
                    .await
                    .unwrap(),
                "input_reassigned" => {
                    let mut changed = input.clone();
                    changed.used_by_operation = Some(uuid::Uuid::new_v4());
                    f.wallet
                        .localstore
                        .update_proofs(vec![changed], vec![])
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let before = f
                .wallet
                .localstore
                .get_proofs(None, None, None, None)
                .await
                .unwrap();
            proceed.notify_one();
            before
        };
        let (result, before) = tokio::join!(f.restore(&[16, 8]), intervene);
        assert!(
            matches!(result, Err(Error::ConcurrentUpdate)),
            "{case}: {result:?}"
        );
        assert_eq!(
            f.wallet
                .localstore
                .get_proofs(None, None, None, None)
                .await
                .unwrap(),
            before,
            "{case}"
        );
    }
}

#[tokio::test]
async fn paid_change_conflicting_existing_output_rejects_whole_handoff() {
    for case in [
        "amount",
        "signature",
        "keyset",
        "mint",
        "unit",
        "encryption",
        "origin",
        "derivation",
    ] {
        let f = Fixture::new().await;
        let mut outputs = f.existing_change(State::Spent).await;
        match case {
            "amount" => outputs[0].proof.amount = 1.into(),
            "signature" => outputs[0].proof.c = crate::nuts::SecretKey::generate().public_key(),
            "keyset" => outputs[0].proof.keyset_id = "00916bbf7ef91a36".parse().unwrap(),
            "mint" => outputs[0].mint_url = "https://other-mint.example.com".parse().unwrap(),
            "unit" => outputs[0].unit = crate::nuts::CurrencyUnit::Msat,
            "encryption" => {
                outputs[0].proof.p2pk_e = Some(crate::nuts::SecretKey::generate().public_key())
            }
            "origin" => outputs[0].created_by_operation = Some(uuid::Uuid::new_v4()),
            "derivation" => outputs[0].derivation_index = Some(99),
            _ => unreachable!(),
        }
        f.wallet
            .localstore
            .update_proofs(outputs, vec![])
            .await
            .unwrap();
        let before = f
            .wallet
            .localstore
            .get_proofs(None, None, None, None)
            .await
            .unwrap();
        assert!(
            matches!(f.restore(&[16, 8]).await, Err(Error::ConcurrentUpdate)),
            "{case}"
        );
        assert_eq!(
            f.wallet
                .localstore
                .get_proofs(None, None, None, None)
                .await
                .unwrap(),
            before,
            "{case}"
        );
        assert!(f
            .wallet
            .localstore
            .get_saga(&f.saga.id)
            .await
            .unwrap()
            .is_some());
    }
}
