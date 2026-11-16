//! Direct confirmation and cancellation cannot operate on partial or stale ownership.
use std::collections::HashMap;
use std::sync::Arc;

use cdk_common::wallet::{MeltSagaState, OperationData, WalletSagaState};

use super::swap_fixture::Fixture;
use crate::nuts::{ProofsMethods, State};
use crate::wallet::send::saga::interposed_store::{Action, Interposed};
use crate::wallet::test_utils::{test_keyset, test_mint_url, test_proof_info};

#[tokio::test]
async fn pending_evidence_preserves_full_records_in_payment_order() {
    for swapped in [false, true] {
        let f = Fixture::new().await;
        let (saga, mut proofs) = if swapped {
            let saga = f.ready().await;
            let proofs = super::swap::saved(&saga)
                .unwrap()
                .outputs
                .as_ref()
                .unwrap()
                .send
                .clone();
            (saga, proofs)
        } else {
            let second = test_proof_info(test_keyset().id, 16, test_mint_url());
            f.wallet
                .localstore
                .update_proofs(vec![second], vec![])
                .await
                .unwrap();
            let proofs = f.wallet.get_unspent_proofs().await.unwrap();
            let prepared = super::MeltSaga::new(&f.wallet)
                .prepare_with_proofs(&f.quote.id, proofs.clone(), HashMap::new())
                .await
                .unwrap();
            (prepared.state_data.saga, proofs)
        };
        assert!(proofs.len() > 1);
        proofs.reverse();
        let mut expected: HashMap<_, _> = f
            .wallet
            .localstore
            .get_proofs_by_ys(proofs.ys().unwrap())
            .await
            .unwrap()
            .into_iter()
            .map(|mut p| {
                p.state = State::Pending;
                (p.y, p)
            })
            .collect();
        let evidence = if swapped {
            super::swap_custody::pending(&f.wallet, &saga, &proofs)
                .await
                .unwrap()
        } else {
            super::admission::pending(&f.wallet, &saga, &proofs)
                .await
                .unwrap()
        };
        assert_eq!(
            evidence,
            proofs
                .iter()
                .map(|p| expected.remove(&p.y().unwrap()).unwrap())
                .collect::<Vec<_>>()
        );
        for record in evidence {
            assert_eq!(
                f.wallet
                    .localstore
                    .get_proofs_by_ys(vec![record.y])
                    .await
                    .unwrap(),
                vec![record]
            );
        }
    }
}

#[tokio::test]
async fn pending_handoff_has_no_partial_writes_after_ownership_changes() {
    for stale_saga in [false, true] {
        let mut f = Fixture::new().await;
        let second = test_proof_info(test_keyset().id, 16, test_mint_url());
        f.wallet
            .localstore
            .update_proofs(vec![second], vec![])
            .await
            .unwrap();
        let proofs = f.wallet.get_unspent_proofs().await.unwrap();
        let store = Arc::new(Interposed::new(f.wallet.localstore.clone()));
        f.wallet.localstore = store.clone();
        let prepared = super::MeltSaga::new(&f.wallet)
            .prepare_with_proofs(&f.quote.id, proofs.clone(), HashMap::new())
            .await
            .unwrap();
        let mut expected = f
            .wallet
            .localstore
            .get_proofs_by_ys(proofs.ys().unwrap())
            .await
            .unwrap();
        let action = if stale_saga {
            let mut saga = prepared.state_data.saga.clone();
            saga.update_state(WalletSagaState::Melt(MeltSagaState::SwapCancelling));
            Action::ReplaceSaga(saga)
        } else {
            expected[0].state = State::Spent;
            Action::ReplaceProof(Box::new(expected[0].clone()))
        };
        *store.before_proofs.lock().unwrap() = Some(action);
        assert!(prepared
            .request_melt_with_options(Default::default())
            .await
            .is_err());
        assert_eq!(
            f.wallet
                .localstore
                .get_proofs_by_ys(proofs.ys().unwrap())
                .await
                .unwrap(),
            expected
        );
        assert!(f.client.captured_swap_requests().is_empty());
    }
}

#[tokio::test]
async fn cancellation_requires_complete_present_unchanged_evidence() {
    for corruption in ["partial", "missing", "changed", "absent"] {
        let f = Fixture::new().await;
        let second = test_proof_info(test_keyset().id, 16, test_mint_url());
        f.wallet
            .localstore
            .update_proofs(vec![second], vec![])
            .await
            .unwrap();
        let proofs = f.wallet.get_unspent_proofs().await.unwrap();
        let prepared = f
            .wallet
            .prepare_melt_proofs(&f.quote.id, proofs.clone(), HashMap::new())
            .await
            .unwrap();
        let mut saga = f
            .wallet
            .localstore
            .get_saga(&prepared.operation_id())
            .await
            .unwrap()
            .unwrap();
        let OperationData::Melt(data) = &mut saga.data else {
            panic!("expected melt");
        };
        match corruption {
            "partial" => {
                data.acquisition.as_mut().unwrap().pop();
            }
            "absent" => {
                data.acquisition = None;
            }
            "missing" => {
                f.wallet
                    .localstore
                    .update_proofs(vec![], vec![proofs[0].y().unwrap()])
                    .await
                    .unwrap();
            }
            _ => {
                let mut changed = data.acquisition.as_ref().unwrap()[0].after.clone().unwrap();
                changed.proof.c = crate::nuts::SecretKey::generate().public_key();
                f.wallet
                    .localstore
                    .update_proofs(vec![changed], vec![])
                    .await
                    .unwrap();
            }
        }
        saga.update_state(saga.state);
        assert!(f.wallet.localstore.update_saga(saga.clone()).await.unwrap());
        let before = f
            .wallet
            .localstore
            .get_proofs_by_ys(proofs.ys().unwrap())
            .await
            .unwrap();
        let quote = f
            .wallet
            .localstore
            .get_melt_quote(&f.quote.id)
            .await
            .unwrap();
        assert!(
            f.wallet.resume_melt_saga(&saga).await.is_err(),
            "{corruption}"
        );
        assert_eq!(
            f.wallet
                .localstore
                .get_proofs_by_ys(proofs.ys().unwrap())
                .await
                .unwrap(),
            before
        );
        assert_eq!(
            f.wallet
                .localstore
                .get_melt_quote(&f.quote.id)
                .await
                .unwrap(),
            quote
        );
        assert!(f
            .wallet
            .localstore
            .get_saga(&saga.id)
            .await
            .unwrap()
            .is_some());
    }
}

#[tokio::test]
async fn retried_cancellation_preserves_later_owners_and_fences_cached_confirmation() {
    let f = Fixture::new().await;
    let proofs = f.wallet.get_unspent_proofs().await.unwrap();
    let prepared = super::MeltSaga::new(&f.wallet)
        .prepare_with_proofs(&f.quote.id, proofs.clone(), HashMap::new())
        .await
        .unwrap();
    let mut saga = prepared.state_data.saga.clone();
    saga.update_state(WalletSagaState::Melt(MeltSagaState::SwapCancelling));
    assert!(f.wallet.localstore.update_saga(saga.clone()).await.unwrap());
    super::admission::release(&f.wallet, &saga).await.unwrap();
    f.wallet
        .localstore
        .reserve_proofs(proofs.ys().unwrap(), &uuid::Uuid::new_v4())
        .await
        .unwrap();
    let later = f
        .wallet
        .localstore
        .get_proofs_by_ys(proofs.ys().unwrap())
        .await
        .unwrap();
    assert!(prepared
        .request_melt_with_options(Default::default())
        .await
        .is_err());
    super::swap::cancel(&f.wallet, &mut saga).await.unwrap();
    assert_eq!(
        f.wallet
            .localstore
            .get_proofs_by_ys(proofs.ys().unwrap())
            .await
            .unwrap(),
        later
    );
    assert!(f
        .wallet
        .localstore
        .get_saga(&saga.id)
        .await
        .unwrap()
        .is_none());
}
