//! Admission must preserve custody and counters until the parent owns the swap.
use std::collections::HashMap;
use std::sync::Arc;

use cdk_common::wallet::OperationData;

use super::swap_fixture::Fixture;
use crate::Amount;

#[tokio::test]
async fn parent_swap_admission_rejects_changed_input_before_counter_or_post() {
    for corruption in ["missing", "owner", "origin", "derivation"] {
        let f = Fixture::new().await;
        let prepared = super::MeltSaga::new(&f.wallet)
            .prepare(&f.quote.id, HashMap::new())
            .await
            .unwrap();
        let mut original = f
            .wallet
            .localstore
            .get_reserved_proofs(&prepared.operation_id())
            .await
            .unwrap()
            .pop()
            .unwrap();
        let keyset = original.proof.keyset_id;
        match corruption {
            "missing" => f
                .wallet
                .localstore
                .update_proofs(vec![], vec![original.y])
                .await
                .unwrap(),
            _ => {
                match corruption {
                    "owner" => original.used_by_operation = Some(uuid::Uuid::new_v4()),
                    "origin" => original.created_by_operation = Some(uuid::Uuid::new_v4()),
                    _ => original.derivation_index = Some(57),
                }
                f.wallet
                    .localstore
                    .update_proofs(vec![original.clone()], vec![])
                    .await
                    .unwrap();
            }
        }
        let before = f
            .wallet
            .localstore
            .get_proofs_by_ys(vec![original.y])
            .await
            .unwrap();
        let counter = f
            .wallet
            .localstore
            .increment_keyset_counter(&keyset, 0)
            .await
            .unwrap();
        assert!(prepared
            .request_melt_with_options(Default::default())
            .await
            .is_err());
        assert!(f.client.captured_swap_requests().is_empty());
        assert_eq!(
            f.wallet
                .localstore
                .increment_keyset_counter(&keyset, 0)
                .await
                .unwrap(),
            counter
        );
        assert_eq!(
            f.wallet
                .localstore
                .get_proofs_by_ys(vec![original.y])
                .await
                .unwrap(),
            before
        );
    }
}

#[tokio::test]
async fn prepared_cancellation_fences_first_swap_submission() {
    let f = Fixture::new().await;
    let prepared = super::MeltSaga::new(&f.wallet)
        .prepare(&f.quote.id, HashMap::new())
        .await
        .unwrap();
    f.wallet
        .cancel_prepared_melt(prepared.operation_id(), vec![], vec![])
        .await
        .unwrap();
    assert!(prepared
        .request_melt_with_options(Default::default())
        .await
        .is_err());
    assert!(f.client.captured_swap_requests().is_empty());
    assert_eq!(f.wallet.total_balance().await.unwrap(), Amount::from(32));
}

#[tokio::test]
async fn unsupported_parent_swap_storage_keeps_counter_custody_and_mint_unchanged() {
    let mut f = Fixture::new().await;
    let mut store =
        crate::wallet::send::saga::interposed_store::Interposed::new(f.wallet.localstore.clone());
    store.proof_cas_supported = false;
    f.wallet.localstore = Arc::new(store);
    let originals = f
        .wallet
        .localstore
        .get_proofs(
            Some(f.wallet.mint_url.clone()),
            Some(f.wallet.unit.clone()),
            Some(vec![crate::nuts::State::Unspent]),
            None,
        )
        .await
        .unwrap();
    let keyset = originals[0].proof.keyset_id;
    let counter = f
        .wallet
        .localstore
        .increment_keyset_counter(&keyset, 0)
        .await
        .unwrap();
    assert!(super::MeltSaga::new(&f.wallet)
        .prepare(&f.quote.id, HashMap::new())
        .await
        .is_err());
    assert_eq!(
        f.wallet
            .localstore
            .get_melt_quote(&f.quote.id)
            .await
            .unwrap(),
        Some(f.quote)
    );
    assert!(f
        .wallet
        .localstore
        .get_incomplete_sagas()
        .await
        .unwrap()
        .is_empty());
    assert!(f.client.captured_swap_requests().is_empty());
    assert_eq!(
        f.wallet
            .localstore
            .increment_keyset_counter(&keyset, 0)
            .await
            .unwrap(),
        counter
    );
    assert_eq!(
        f.wallet
            .localstore
            .get_proofs_by_ys(originals.iter().map(|p| p.y).collect())
            .await
            .unwrap(),
        originals
    );
}

#[tokio::test]
async fn saved_parent_cannot_recover_changed_request_witness() {
    let f = Fixture::new().await;
    let mut saga = f.interrupted().await;
    let OperationData::Melt(data) = &mut saga.data else {
        panic!("expected melt");
    };
    data.swap.as_mut().unwrap().inputs[0].proof.witness = Some(crate::nuts::Witness::P2PKWitness(
        crate::nuts::nut11::P2PKWitness {
            signatures: vec!["different-witness".to_owned()],
        },
    ));
    saga.update_state(saga.state);
    assert!(f.wallet.localstore.update_saga(saga.clone()).await.unwrap());
    assert!(f.wallet.resume_melt_saga(&saga).await.is_err());
    assert_eq!(f.client.captured_swap_requests().len(), 1);
    assert_eq!(
        f.wallet.localstore.get_saga(&saga.id).await.unwrap(),
        Some(saga)
    );
}

#[tokio::test]
async fn signed_parent_request_survives_restart_but_rejects_matching_invalid_witness() {
    use crate::nuts::nut10::SpendingConditionVerification;
    use crate::nuts::{SecretKey, SpendingConditions, State};
    for corrupt in [false, true] {
        let f = Fixture::new().await;
        let mut input = f
            .wallet
            .localstore
            .get_proofs(
                Some(f.wallet.mint_url.clone()),
                Some(f.wallet.unit.clone()),
                Some(vec![State::Unspent]),
                None,
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
        let previous = input.y;
        let key = SecretKey::generate();
        let conditions = SpendingConditions::new_p2pk(key.public_key(), None);
        input.proof.secret = conditions.clone().try_into().unwrap();
        input.proof.sign_p2pk(key).unwrap();
        input.y = input.proof.y().unwrap();
        input.spending_condition = Some(conditions);
        f.wallet
            .localstore
            .update_proofs(vec![input], vec![previous])
            .await
            .unwrap();
        let mut saga = f.interrupted().await;
        let original = super::swap::saved(&saga).unwrap().inputs[0].clone();
        super::swap::saved(&saga)
            .unwrap()
            .pre_swap
            .swap_request
            .verify_spending_conditions()
            .unwrap();
        if corrupt {
            let OperationData::Melt(data) = &mut saga.data else {
                panic!("expected melt");
            };
            let saved = data.swap.as_mut().unwrap();
            let witness = Some(crate::nuts::Witness::P2PKWitness(
                crate::nuts::nut11::P2PKWitness {
                    signatures: vec!["invalid".to_owned()],
                },
            ));
            saved.inputs[0].proof.witness = witness.clone();
            saved.pre_swap.swap_request.inputs_mut()[0].witness = witness;
            f.wallet
                .localstore
                .update_proofs(saved.inputs.clone(), vec![])
                .await
                .unwrap();
            saga.update_state(saga.state);
            assert!(f.wallet.localstore.update_saga(saga.clone()).await.unwrap());
        }
        f.client._set_restore_response(Ok(f.restore(&saga)));
        let result = f.wallet.resume_melt_saga(&saga).await;
        if corrupt {
            assert!(
                result.is_err(),
                "matching malformed authorization must not be replayed"
            );
            assert_eq!(
                f.wallet.localstore.get_saga(&saga.id).await.unwrap(),
                Some(saga)
            );
        } else {
            result.unwrap().unwrap();
            let mut spent = original;
            spent.state = State::Spent;
            assert_eq!(
                f.wallet
                    .localstore
                    .get_proofs_by_ys(vec![spent.y])
                    .await
                    .unwrap(),
                vec![spent]
            );
            assert_eq!(f.wallet.total_balance().await.unwrap(), Amount::from(32));
        }
        assert_eq!(f.client.captured_swap_requests().len(), 1);
    }
}
