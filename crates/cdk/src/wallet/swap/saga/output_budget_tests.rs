use super::test_utils::{prepare, response, submitted, wallet};
use super::*;
use crate::nuts::{ProofsMethods, SecretKey};
use crate::wallet::recovery::RecoveryAction;
use crate::wallet::test_utils::test_keyset_id;

#[tokio::test]
async fn output_budget_counts_payment_and_change_before_reservation() {
    for locked in [false, true] {
        let (mut wallet, mint) = wallet().await;
        wallet.max_outputs = Some(4);
        let original = wallet.get_unspent_proofs().await.unwrap();
        let conditions =
            locked.then(|| SpendingConditions::new_p2pk(SecretKey::generate().public_key(), None));
        let result = SwapSaga::new(&wallet)
            .prepare(
                Some(12.into()),
                Default::default(),
                original.clone(),
                conditions.clone(),
                false,
                false,
            )
            .await;
        // 12 = 8+4 and 19 change = 16+2+1, after the one-sat input fee.
        assert!(matches!(
            result,
            Err(Error::MaxOutputsExceeded { actual: 5, max: 4 })
        ));
        assert_eq!(wallet.get_unspent_proofs().await.unwrap(), original);
        assert!(wallet
            .localstore
            .get_incomplete_sagas()
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            wallet
                .localstore
                .increment_keyset_counter(&test_keyset_id(), 0)
                .await
                .unwrap(),
            0
        );
        assert!(mint.captured_swap_requests().is_empty());
        wallet.max_outputs = Some(5);
        let accepted = prepare(&wallet, conditions, false).await;
        assert_eq!(
            checkpoint::get(&accepted.state_data.saga)
                .unwrap()
                .pre_swap
                .swap_request
                .outputs()
                .len(),
            5
        );
    }
}

#[tokio::test]
async fn output_budget_does_not_replace_a_submitted_request() {
    let (mut wallet, mint) = wallet().await;
    let saga = submitted(&wallet, &mint, None, false).await;
    let original = mint.captured_swap_requests();
    wallet.max_outputs = Some(0);
    wallet.proof_inventory_limits = Some((0, 0));
    mint._set_restore_response(Ok(response(&saga)));
    assert!(matches!(
        wallet.resume_swap_saga(&saga).await.unwrap(),
        RecoveryAction::Recovered
    ));
    assert_eq!(mint.captured_swap_requests(), original);
    assert!(wallet
        .localstore
        .get_incomplete_sagas()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(wallet.total_balance().await.unwrap(), Amount::from(19));
    assert_eq!(
        wallet
            .get_reserved_proofs()
            .await
            .unwrap()
            .total_amount()
            .unwrap(),
        Amount::from(12)
    );
}

#[tokio::test]
async fn output_budget_rejects_large_refills_before_expanding_them() {
    let (mut wallet, _) = wallet().await;
    wallet.max_outputs = Some(4);
    wallet.target_proof_count = usize::MAX;
    let result = wallet
        .determine_split_target_values((1u64 << 30).into(), &(0, vec![1]).into())
        .await;
    assert!(matches!(
        result,
        Err(Error::MaxOutputsExceeded {
            actual: 1_073_741_824,
            max: 4
        })
    ));
}
