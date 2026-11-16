//! Quote ownership must never outlive the operation that acquired it.
use std::sync::Arc;

use super::tests::paid_signed_mint_quote;
use super::*;
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_mint_url, MockMintConnector,
};

#[tokio::test]
async fn issue_acquisition_batch_conflict_leaves_every_original_quote_unchanged() {
    let db = create_test_db().await;
    let mint = Arc::new(MockMintConnector::new());
    let wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
    let mut quotes = Vec::new();
    for _ in 0..2 {
        let quote = paid_signed_mint_quote(test_mint_url(), 8.into(), SecretKey::generate());
        db.add_mint_quote(quote.clone()).await.unwrap();
        quotes.push(quote);
    }
    let competing = uuid::Uuid::new_v4();
    db.reserve_mint_quote(&quotes[1].id, &competing)
        .await
        .unwrap();
    quotes[1] = db.get_mint_quote(&quotes[1].id).await.unwrap().unwrap();
    let names: Vec<_> = quotes.iter().map(|quote| quote.id.as_str()).collect();
    assert!(MintSaga::new(&wallet)
        .prepare_batch(&names, SplitTarget::None, None, None)
        .await
        .is_err());
    for quote in quotes {
        assert_eq!(db.get_mint_quote(&quote.id).await.unwrap(), Some(quote));
    }
    assert!(db.get_incomplete_sagas().await.unwrap().is_empty());
    assert!(mint.post_mint_requests().is_empty());
    assert!(mint.post_batch_mint_requests().is_empty());
}

#[tokio::test]
async fn issue_acquisition_lost_commit_reply_keeps_complete_original_intent() {
    use crate::wallet::send::saga::interposed_store::{Action, Interposed};
    for batch in [false, true] {
        let db = create_test_db().await;
        let store = Arc::new(Interposed::new(db.clone()));
        let mint = Arc::new(MockMintConnector::new());
        let wallet = create_test_wallet_with_mock(store.clone(), mint.clone()).await;
        let mut quotes = Vec::new();
        for _ in 0..if batch { 2 } else { 1 } {
            let quote = paid_signed_mint_quote(test_mint_url(), 8.into(), SecretKey::generate());
            db.add_mint_quote(quote.clone()).await.unwrap();
            quotes.push(quote);
        }
        *store.after_add_saga.lock().unwrap() = Some(Action::Fail);
        let initial = MintSaga::new(&wallet);
        let operation = initial.state_data.operation_id;
        let names: Vec<_> = quotes.iter().map(|quote| quote.id.as_str()).collect();
        let result = if batch {
            initial
                .prepare_batch(&names, SplitTarget::None, None, None)
                .await
        } else {
            initial.prepare(names[0], SplitTarget::None, None).await
        };
        assert!(result.is_err());
        let saved = db.get_saga(&operation).await.unwrap().unwrap();
        assert_eq!(
            saved.state,
            WalletSagaState::Issue(IssueSagaState::SecretsPrepared)
        );
        let OperationData::Mint(data) = &saved.data else {
            panic!("original issue intent")
        };
        assert_eq!(data.quote_ids(), names);
        let secrets = &data.checkpoint.as_ref().unwrap().premint_secrets;
        let indices: Vec<_> = secrets
            .secrets
            .iter()
            .map(|output| output.derivation_index.unwrap())
            .collect();
        let counter = db
            .increment_keyset_counter(&secrets.keyset_id, 0)
            .await
            .unwrap();
        assert_eq!(indices, (0..counter).collect::<Vec<_>>());
        assert_eq!(counter as usize, secrets.len());
        assert!(db.get_keys(&secrets.keyset_id).await.unwrap().is_some());
        assert_eq!(
            data.blinded_messages()
                .unwrap()
                .iter()
                .map(|output| u64::from(output.amount))
                .sum::<u64>(),
            if batch { 16 } else { 8 }
        );
        for mut quote in quotes {
            quote.used_by_operation = Some(operation.to_string());
            quote.version += 1;
            assert_eq!(db.get_mint_quote(&quote.id).await.unwrap(), Some(quote));
        }
        assert!(mint.post_mint_requests().is_empty());
        assert!(mint.post_batch_mint_requests().is_empty());
    }
}

#[tokio::test]
async fn issue_acquisition_rejected_single_preserves_output_counter() {
    rejected_preparation(false).await;
}

#[tokio::test]
async fn issue_acquisition_rejected_batch_preserves_output_counter() {
    rejected_preparation(true).await;
}

async fn rejected_preparation(batch: bool) {
    use crate::wallet::send::saga::interposed_store::{Action, Interposed};

    let db = create_test_db().await;
    let store = Arc::new(Interposed::new(db.clone()));
    let mint = Arc::new(MockMintConnector::new());
    let wallet = create_test_wallet_with_mock(store.clone(), mint.clone()).await;
    let keyset = wallet.active_keyset().await.unwrap().id;
    let counter = db.increment_keyset_counter(&keyset, 0).await.unwrap();
    let mut quotes = Vec::new();
    for _ in 0..if batch { 2 } else { 1 } {
        let quote = paid_signed_mint_quote(test_mint_url(), 8.into(), SecretKey::generate());
        db.add_mint_quote(quote.clone()).await.unwrap();
        quotes.push(quote);
    }
    *store.before_acquisition.lock().unwrap() = Some(Action::Fail);
    let initial = MintSaga::new(&wallet);
    let names: Vec<_> = quotes.iter().map(|quote| quote.id.as_str()).collect();
    let result = if batch {
        initial
            .prepare_batch(&names, SplitTarget::None, None, None)
            .await
    } else {
        initial.prepare(names[0], SplitTarget::None, None).await
    };
    assert!(result.is_err());
    for quote in quotes {
        assert_eq!(db.get_mint_quote(&quote.id).await.unwrap(), Some(quote));
    }
    assert!(db.get_incomplete_sagas().await.unwrap().is_empty());
    assert!(mint.post_mint_requests().is_empty());
    assert!(mint.post_batch_mint_requests().is_empty());
    assert_eq!(
        db.increment_keyset_counter(&keyset, 0).await.unwrap(),
        counter,
        "interrupted issuance must not consume output counters"
    );
}
