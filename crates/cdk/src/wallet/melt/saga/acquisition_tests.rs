//! Admission must commit the quote, original proof batch and journal together.
use std::collections::HashMap;

use super::swap_fixture::Fixture;
use crate::nuts::ProofsMethods;
use crate::wallet::test_utils::{test_keyset, test_mint_url, test_proof_info};
use crate::Amount;

#[tokio::test]
async fn insufficient_preparation_leaves_quote_unlocked() {
    for supplied in [false, true] {
        let mut f = Fixture::new().await;
        f.quote.amount = Amount::from(64);
        f.wallet
            .localstore
            .add_melt_quote(f.quote.clone())
            .await
            .unwrap();
        f.quote = f
            .wallet
            .localstore
            .get_melt_quote(&f.quote.id)
            .await
            .unwrap()
            .unwrap();
        let proofs = f.wallet.get_unspent_proofs().await.unwrap();
        let result = if supplied {
            f.wallet
                .prepare_melt_proofs(&f.quote.id, proofs, HashMap::new())
                .await
        } else {
            f.wallet.prepare_melt(&f.quote.id, HashMap::new()).await
        };
        assert!(result.is_err());
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
        assert_eq!(f.wallet.total_balance().await.unwrap(), Amount::from(32));
    }
}

#[tokio::test]
async fn supplied_inputs_cannot_steal_another_operation_reservation() {
    let f = Fixture::new().await;
    let proofs = f.wallet.get_unspent_proofs().await.unwrap();
    let ys = proofs.ys().unwrap();
    f.wallet
        .localstore
        .reserve_proofs(ys.clone(), &uuid::Uuid::new_v4())
        .await
        .unwrap();
    let original = f
        .wallet
        .localstore
        .get_proofs_by_ys(ys.clone())
        .await
        .unwrap();
    assert!(f
        .wallet
        .prepare_melt_proofs(&f.quote.id, proofs, HashMap::new())
        .await
        .is_err());
    assert_eq!(
        f.wallet.localstore.get_proofs_by_ys(ys).await.unwrap(),
        original
    );
    assert_eq!(
        f.wallet
            .localstore
            .get_melt_quote(&f.quote.id)
            .await
            .unwrap(),
        Some(f.quote)
    );
}

#[tokio::test]
async fn restored_direct_confirmation_rejects_extra_inputs_before_counters() {
    let f = Fixture::new().await;
    let proofs = f.wallet.get_unspent_proofs().await.unwrap();
    let mut prepared = super::MeltSaga::new(&f.wallet)
        .prepare_with_proofs(&f.quote.id, proofs, HashMap::new())
        .await
        .unwrap();
    let extra = test_proof_info(test_keyset().id, 8, test_mint_url());
    f.wallet
        .localstore
        .update_proofs(vec![extra.clone()], vec![])
        .await
        .unwrap();
    prepared.state_data.proofs.push(extra.proof.clone());
    let ys = prepared.state_data.proofs.ys().unwrap();
    let before = f
        .wallet
        .localstore
        .get_proofs_by_ys(ys.clone())
        .await
        .unwrap();
    let counter = f
        .wallet
        .localstore
        .increment_keyset_counter(&test_keyset().id, 0)
        .await
        .unwrap();
    assert!(prepared
        .request_melt_with_options(Default::default())
        .await
        .is_err());
    assert_eq!(
        f.wallet.localstore.get_proofs_by_ys(ys).await.unwrap(),
        before
    );
    assert_eq!(
        f.wallet
            .localstore
            .increment_keyset_counter(&test_keyset().id, 0)
            .await
            .unwrap(),
        counter
    );
}

#[tokio::test]
async fn acquisition_failure_preserves_coins_quote_and_journal() {
    use std::sync::Arc;

    use crate::wallet::send::saga::interposed_store::{Action, Interposed};
    for supplied in [false, true] {
        let mut f = Fixture::new().await;
        let proofs = f.wallet.get_unspent_proofs().await.unwrap();
        let ys = proofs.ys().unwrap();
        let before = f
            .wallet
            .localstore
            .get_proofs_by_ys(ys.clone())
            .await
            .unwrap();
        let store = Arc::new(Interposed::new(f.wallet.localstore.clone()));
        *store.before_acquisition.lock().unwrap() = Some(Action::Fail);
        f.wallet.localstore = store;
        let result = if supplied {
            f.wallet
                .prepare_melt_proofs(&f.quote.id, proofs, HashMap::new())
                .await
        } else {
            f.wallet.prepare_melt(&f.quote.id, HashMap::new()).await
        };
        assert!(result.is_err());
        assert_eq!(
            f.wallet.localstore.get_proofs_by_ys(ys).await.unwrap(),
            before
        );
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
    }
}

#[tokio::test]
async fn acquisition_reloads_quote_version_and_preserves_full_originals() {
    use cdk_common::wallet::OperationData;
    for external in [false, true] {
        let f = Fixture::new().await;
        let proofs = f.wallet.get_unspent_proofs().await.unwrap();
        let ys = proofs.ys().unwrap();
        let original = f
            .wallet
            .localstore
            .get_proofs_by_ys(ys.clone())
            .await
            .unwrap();
        if external {
            f.wallet
                .localstore
                .update_proofs(vec![], ys.clone())
                .await
                .unwrap();
        }
        let prepared = f
            .wallet
            .prepare_melt_proofs(
                &f.quote.id,
                proofs,
                HashMap::from([("purpose".to_owned(), "test".to_owned())]),
            )
            .await
            .unwrap();
        assert_eq!(prepared.quote().version, f.quote.version + 1);
        assert_eq!(
            prepared.quote().used_by_operation,
            Some(prepared.operation_id().to_string())
        );
        let saved = f
            .wallet
            .localstore
            .get_saga(&prepared.operation_id())
            .await
            .unwrap()
            .unwrap();
        let OperationData::Melt(data) = &saved.data else {
            panic!("expected melt");
        };
        let acquisition = data.acquisition.as_ref().unwrap();
        assert_eq!(acquisition.len(), 1);
        let reserved = acquisition[0].after.as_ref().unwrap();
        if external {
            assert!(acquisition[0].before.is_none());
            assert!(reserved.created_by_operation.is_none());
            assert!(reserved.derivation_index.is_none());
        } else {
            assert_eq!(acquisition[0].before.as_ref(), Some(&original[0]));
            assert_eq!(
                reserved.created_by_operation,
                original[0].created_by_operation
            );
            assert_eq!(reserved.derivation_index, original[0].derivation_index);
        }
        assert!(!format!("{saved:?}").contains(&original[0].proof.secret.to_string()));
        prepared.cancel().await.unwrap();
        let released = f.wallet.localstore.get_proofs_by_ys(ys).await.unwrap();
        assert_eq!(released[0].state, crate::nuts::State::Unspent);
        assert!(released[0].used_by_operation.is_none());
        if !external {
            assert_eq!(released, original);
        }
    }
}

#[tokio::test]
async fn concurrent_preparation_has_one_quote_and_proof_owner() {
    let f = Fixture::new().await;
    let proofs = f.wallet.get_unspent_proofs().await.unwrap();
    let (a, b) = tokio::join!(
        f.wallet
            .prepare_melt_proofs(&f.quote.id, proofs.clone(), HashMap::new()),
        f.wallet
            .prepare_melt_proofs(&f.quote.id, proofs, HashMap::new()),
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let sagas = f.wallet.localstore.get_incomplete_sagas().await.unwrap();
    assert_eq!(sagas.len(), 1);
    let quote = f
        .wallet
        .localstore
        .get_melt_quote(&f.quote.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(quote.used_by_operation, Some(sagas[0].id.to_string()));
    assert_eq!(quote.version, f.quote.version + 1);
}

#[tokio::test]
async fn reserved_metadata_and_unsupported_completion_fail_before_acquisition() {
    use std::sync::Arc;

    use crate::wallet::send::saga::interposed_store::Interposed;
    for case in ["metadata", "receipt_completion", "melt_completion"] {
        let mut f = Fixture::new().await;
        let proofs = f.wallet.get_unspent_proofs().await.unwrap();
        let before = f
            .wallet
            .localstore
            .get_proofs_by_ys(proofs.ys().unwrap())
            .await
            .unwrap();
        let mut store = Interposed::new(f.wallet.localstore.clone());
        store.completion_supported = case != "receipt_completion";
        store.melt_completion_supported = case != "melt_completion";
        f.wallet.localstore = Arc::new(store);
        let metadata = if case == "metadata" {
            HashMap::from([("batch_quote_id".to_owned(), "invalid".to_owned())])
        } else {
            HashMap::new()
        };
        assert!(f
            .wallet
            .prepare_melt_proofs(&f.quote.id, proofs.clone(), metadata)
            .await
            .is_err());
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
            Some(f.quote)
        );
        assert!(f
            .wallet
            .localstore
            .get_incomplete_sagas()
            .await
            .unwrap()
            .is_empty());
    }
}
