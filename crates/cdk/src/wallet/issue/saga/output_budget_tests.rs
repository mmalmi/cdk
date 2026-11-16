use std::sync::Arc;

use super::tests::paid_signed_mint_quote;
use super::*;
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_keyset_id, test_mint_url, MockMintConnector,
};

#[tokio::test]
async fn mint_output_budget_rejects_whole_requests_before_creating_secrets() {
    for batch in [false, true] {
        for locked in [false, true] {
            let db = create_test_db().await;
            let mint = Arc::new(MockMintConnector::new());
            let mut wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
            wallet.target_proof_count = 0;
            let count = if batch { 4 } else { 2 };
            wallet.max_outputs = Some(count - 1);
            let mut ids = Vec::new();
            for _ in 0..if batch { 2 } else { 1 } {
                let quote =
                    paid_signed_mint_quote(test_mint_url(), 6.into(), SecretKey::generate());
                ids.push(quote.id.clone());
                db.add_mint_quote(quote).await.unwrap();
            }
            let names: Vec<_> = ids.iter().map(String::as_str).collect();
            let conditions = locked
                .then(|| SpendingConditions::new_p2pk(SecretKey::generate().public_key(), None));
            let saga = MintSaga::new(&wallet);
            let result = if batch {
                saga.prepare_batch(&names, SplitTarget::None, conditions.clone(), None)
                    .await
            } else {
                saga.prepare(names[0], SplitTarget::None, conditions.clone())
                    .await
            };
            assert!(
                matches!(result, Err(Error::MaxOutputsExceeded { actual, max }) if actual == count as usize && max == (count - 1) as usize)
            );
            assert_eq!(
                db.increment_keyset_counter(&test_keyset_id(), 0)
                    .await
                    .unwrap(),
                0
            );
            assert!(db.get_incomplete_sagas().await.unwrap().is_empty());
            assert!(mint.post_mint_requests().is_empty());
            assert!(mint.post_batch_mint_requests().is_empty());
            for id in &ids {
                assert!(db
                    .get_mint_quote(id)
                    .await
                    .unwrap()
                    .unwrap()
                    .used_by_operation
                    .is_none());
            }
            wallet.max_outputs = Some(count);
            let saga = MintSaga::new(&wallet);
            let prepared = if batch {
                saga.prepare_batch(&names, SplitTarget::None, conditions, None)
                    .await
            } else {
                saga.prepare(names[0], SplitTarget::None, conditions).await
            }
            .unwrap();
            let actual = prepared
                .state_data
                .checkpoint()
                .unwrap()
                .request
                .outputs()
                .len();
            assert_eq!(actual, count as usize);
        }
    }
}

#[tokio::test]
async fn batch_amount_overflow_rejects_before_reserving_quotes() {
    let db = create_test_db().await;
    let mint = Arc::new(MockMintConnector::new());
    let wallet = create_test_wallet_with_mock(db.clone(), mint).await;
    let mut ids = Vec::new();
    for _ in 0..4 {
        let quote =
            paid_signed_mint_quote(test_mint_url(), (1u64 << 62).into(), SecretKey::generate());
        ids.push(quote.id.clone());
        db.add_mint_quote(quote).await.unwrap();
    }
    let names: Vec<_> = ids.iter().map(String::as_str).collect();
    let result = MintSaga::new(&wallet)
        .prepare_batch(&names, SplitTarget::None, None, None)
        .await;
    assert!(matches!(result, Err(Error::AmountOverflow)));
    for id in ids {
        assert!(db
            .get_mint_quote(&id)
            .await
            .unwrap()
            .unwrap()
            .used_by_operation
            .is_none());
    }
}
