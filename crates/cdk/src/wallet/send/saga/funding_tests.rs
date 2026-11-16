use std::sync::Arc;

use crate::nuts::{Id, ProofsMethods};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_keyset, test_mint_url, test_proof_info,
    MockMintConnector,
};
use crate::wallet::{SendKind, SendOptions};
use crate::Amount;

async fn wallet(amounts: &[u64]) -> crate::Wallet {
    let db = create_test_db().await;
    let mut keyset = test_keyset();
    keyset.input_fee_ppk = 500;
    db.update_proofs(
        amounts
            .iter()
            .map(|a| test_proof_info(keyset.id, *a, test_mint_url()))
            .collect(),
        vec![],
    )
    .await
    .unwrap();
    let mint = Arc::new(MockMintConnector::new());
    mint.set_active_keyset(keyset);
    create_test_wallet_with_mock(db, mint).await
}

#[tokio::test]
async fn debit_bound_covers_swap_and_redemption_rounding_before_reservation() {
    let wallet = wallet(&[64]).await;
    let result = wallet
        .prepare_send(
            Amount::from(38),
            SendOptions {
                include_fee: true,
                max_total_amount: Some(Amount::from(40)),
                ..Default::default()
            },
        )
        .await;
    assert!(
        result.is_err(),
        "40-sat token plus 1-sat swap exceeds the cap"
    );
    assert_eq!(wallet.total_balance().await.unwrap(), Amount::from(64));
    assert!(wallet.get_reserved_proofs().await.unwrap().is_empty());
}

#[tokio::test]
async fn debit_bound_allows_exact_cost_with_or_without_swap() {
    for (coins, maximum, swap_fee) in [(vec![64], 41, 1), (vec![32, 8], 40, 0)] {
        let wallet = wallet(&coins).await;
        let prepared = wallet
            .prepare_send(
                Amount::from(38),
                SendOptions {
                    include_fee: true,
                    max_total_amount: Some(Amount::from(maximum)),
                    required_keyset_id: Some(test_keyset().id),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(prepared.swap_fee(), Amount::from(swap_fee));
        assert_eq!(prepared.amount() + prepared.fee(), Amount::from(maximum));
        prepared.cancel().await.unwrap();
    }
}

#[tokio::test]
async fn wrong_required_keyset_rejects_before_reservation() {
    let wallet = wallet(&[64]).await;
    let result = wallet
        .prepare_send(
            Amount::from(38),
            SendOptions {
                required_keyset_id: Some("009a1f293253e41e".parse::<Id>().unwrap()),
                ..Default::default()
            },
        )
        .await;
    assert!(result.is_err(), "must not silently use a different keyset");
    assert_eq!(wallet.total_balance().await.unwrap(), Amount::from(64));
    assert!(wallet.get_reserved_proofs().await.unwrap().is_empty());
}

#[tokio::test]
async fn debit_bound_counts_tolerated_extra_without_redemption_reserve() {
    let wallet = wallet(&[32]).await;
    let result = wallet
        .prepare_send(
            Amount::from(30),
            SendOptions {
                send_kind: SendKind::OnlineTolerance(Amount::from(2)),
                max_total_amount: Some(Amount::from(31)),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(crate::Error::MaxAmountExceeded { .. })
    ));
    assert_eq!(wallet.total_balance().await.unwrap(), Amount::from(32));
}

#[tokio::test]
async fn reconstructed_confirmation_rechecks_debit_bound_and_releases_inputs() {
    let wallet = wallet(&[64]).await;
    let prepared = wallet
        .prepare_send(
            Amount::from(38),
            SendOptions {
                include_fee: true,
                max_total_amount: Some(Amount::from(41)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut options = prepared.options().clone();
    options.max_total_amount = Some(Amount::from(40));
    let result = wallet
        .confirm_send(
            prepared.operation_id(),
            prepared.amount(),
            options,
            prepared.proofs_to_swap().clone(),
            prepared.proofs_to_send().clone(),
            prepared.swap_fee(),
            prepared.send_fee(),
            None,
        )
        .await;
    assert!(matches!(
        result,
        Err(crate::Error::MaxAmountExceeded { .. })
    ));
    assert_eq!(wallet.total_balance().await.unwrap(), Amount::from(64));
    assert!(wallet.get_reserved_proofs().await.unwrap().is_empty());
}

async fn rotated_wallet() -> (crate::Wallet, Arc<MockMintConnector>, Id) {
    let db = create_test_db().await;
    let mut old = test_keyset();
    old.active = Some(false);
    old.input_fee_ppk = 500;
    let mut active = old.clone();
    active.id = Id::v2_from_data(
        &active.keys,
        &active.unit,
        active.input_fee_ppk,
        active.final_expiry,
    );
    active.active = Some(true);
    db.update_proofs(
        vec![
            test_proof_info(old.id, 64, test_mint_url()),
            test_proof_info(old.id, 128, test_mint_url()),
        ],
        vec![],
    )
    .await
    .unwrap();
    let mint = Arc::new(MockMintConnector::new());
    *mint.keysets.lock().unwrap() = vec![old, active.clone()];
    (
        create_test_wallet_with_mock(db, mint.clone()).await,
        mint,
        active.id,
    )
}

#[tokio::test]
async fn keyset_constraint_converts_only_selected_inputs_with_one_priced_swap() {
    for include_fee in [false, true] {
        let (wallet, _, active) = rotated_wallet().await;
        let prepared = wallet
            .prepare_send(
                Amount::from(38),
                SendOptions {
                    include_fee,
                    required_keyset_id: Some(active),
                    max_total_amount: Some(Amount::from(41)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(prepared.proofs_to_send().is_empty());
        assert_eq!(
            prepared.proofs_to_swap().total_amount().unwrap(),
            Amount::from(64)
        );
        assert_eq!(prepared.swap_fee(), Amount::from(1));
        assert_eq!(wallet.total_balance().await.unwrap(), Amount::from(128));
        prepared.cancel().await.unwrap();
        assert_eq!(wallet.total_balance().await.unwrap(), Amount::from(192));
    }
}

#[tokio::test]
async fn required_keyset_conversion_is_not_possible_offline() {
    let (wallet, _, active) = rotated_wallet().await;
    wallet
        .keysets(cdk_common::wallet::KeysetLoadPolicy::Refresh)
        .await
        .unwrap();
    let result = wallet
        .prepare_send(
            Amount::from(38),
            SendOptions {
                send_kind: SendKind::OfflineExact,
                required_keyset_id: Some(active),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(result, Err(crate::Error::InsufficientFunds)));
    assert_eq!(wallet.total_balance().await.unwrap(), Amount::from(192));
    assert!(wallet.get_reserved_proofs().await.unwrap().is_empty());
}

#[tokio::test]
async fn keyset_rotation_after_prepare_rejects_before_swap_and_releases_inputs() {
    let (wallet, mint, active) = rotated_wallet().await;
    let prepared = wallet
        .prepare_send(
            Amount::from(38),
            SendOptions {
                include_fee: true,
                required_keyset_id: Some(active),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    {
        let mut keysets = mint.keysets.lock().unwrap();
        for keyset in keysets.iter_mut() {
            keyset.active = Some(keyset.id != active);
        }
    }
    wallet
        .keysets(cdk_common::wallet::KeysetLoadPolicy::Refresh)
        .await
        .unwrap();
    // No mock swap response exists: spending before rejecting would panic.
    assert!(matches!(
        prepared.confirm(None).await,
        Err(crate::Error::InactiveKeyset)
    ));
    assert_eq!(wallet.total_balance().await.unwrap(), Amount::from(192));
    assert!(wallet.get_reserved_proofs().await.unwrap().is_empty());
}

#[tokio::test]
async fn prepared_send_retains_exact_request_after_mint_rejection() {
    let (wallet, mint, active) = rotated_wallet().await;
    let prepared = wallet
        .prepare_send(
            Amount::from(38),
            SendOptions {
                include_fee: true,
                required_keyset_id: Some(active),
                max_total_amount: Some(Amount::from(41)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let id = prepared.operation_id();
    // This baseline refreshes before confirmation; exercise a mint-side rejection
    // after that refresh. A second request would exhaust the single mock response.
    mint.set_post_swap_response(Err(crate::Error::InactiveKeyset));
    assert!(matches!(
        prepared.confirm(None).await,
        Err(crate::Error::InactiveKeyset)
    ));
    assert!(mint.post_swap_response.lock().unwrap().is_none());
    assert_eq!(wallet.total_balance().await.unwrap(), Amount::from(128));
    assert_eq!(
        wallet
            .get_reserved_proofs()
            .await
            .unwrap()
            .total_amount()
            .unwrap(),
        Amount::from(64)
    );
    let saga = wallet.localstore.get_saga(&id).await.unwrap().unwrap();
    assert_eq!(
        saga.state,
        cdk_common::wallet::WalletSagaState::Send(cdk_common::wallet::SendSagaState::Confirming)
    );
    let cdk_common::wallet::OperationData::Send(data) = saga.data else {
        panic!("send data")
    };
    let intent = data.confirmation.unwrap();
    assert_eq!(intent.required_keyset_id, Some(active));
    assert_eq!(intent.max_total_amount, Some(Amount::from(41)));
    assert!(intent.pre_swap.unwrap().swap_request == mint.captured_swap_requests()[0]);
}

#[tokio::test]
async fn debit_bound_counts_retained_overpayment_alongside_a_locked_proof_swap() {
    let db = create_test_db().await;
    let mut keyset = test_keyset();
    keyset.input_fee_ppk = 500;
    let mut locked = test_proof_info(keyset.id, 4, test_mint_url()).proof;
    let conditions = crate::nuts::SpendingConditions::new_p2pk(
        crate::nuts::SecretKey::generate().public_key(),
        None,
    );
    let secret: crate::nuts::nut10::Secret = conditions.try_into().unwrap();
    locked.secret = secret.try_into().unwrap();
    let locked_info = cdk_common::wallet::ProofInfo::new(
        locked,
        test_mint_url(),
        crate::nuts::State::Unspent,
        crate::nuts::CurrencyUnit::Sat,
    )
    .unwrap();
    db.update_proofs(
        vec![test_proof_info(keyset.id, 32, test_mint_url()), locked_info],
        vec![],
    )
    .await
    .unwrap();
    let mint = Arc::new(MockMintConnector::new());
    mint.set_active_keyset(keyset);
    let wallet = create_test_wallet_with_mock(db, mint).await;
    let proofs = wallet.get_unspent_proofs().await.unwrap();
    let result = super::SendSaga::new(&wallet)
        .internal_prepare(
            Amount::from(30),
            SendOptions {
                send_kind: SendKind::OnlineTolerance(Amount::from(6)),
                max_total_amount: Some(Amount::from(31)),
                ..Default::default()
            },
            proofs,
            false,
            Default::default(),
        )
        .await;
    // The plan retains 32 sats for the recipient and pays 1 sat to swap the
    // locked input. Its wallet debit is 33, even though amount + swap_fee is 31.
    assert!(matches!(
        result,
        Err(crate::Error::MaxAmountExceeded { .. })
    ));
    assert_eq!(wallet.total_balance().await.unwrap(), Amount::from(36));
    assert!(wallet.get_reserved_proofs().await.unwrap().is_empty());
}

#[tokio::test]
async fn tolerant_confirmation_caps_actual_token_value() {
    let wallet = wallet(&[32]).await;
    let prepared = wallet
        .prepare_send(
            Amount::from(30),
            SendOptions {
                send_kind: SendKind::OnlineTolerance(Amount::from(2)),
                max_total_amount: Some(Amount::from(32)),
                required_keyset_id: Some(test_keyset().id),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(prepared.proofs_to_swap().is_empty());
    let token = prepared.confirm(None).await.unwrap();
    assert_eq!(token.value().unwrap(), Amount::from(32));
    assert_eq!(wallet.total_balance().await.unwrap(), Amount::ZERO);
}
