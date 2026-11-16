use std::sync::Arc;

use super::SendSaga;
use crate::nuts::ProofsMethods;
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_keyset, test_mint_url, test_proof_info,
    MockMintConnector,
};
use crate::wallet::SendOptions;
use crate::{Amount, Error};

#[test]
fn sender_denomination_plan_counts_full_width_output_sets() {
    for (requested, fee, total) in [(u64::MAX, 0, u64::MAX), (u64::MAX / 2, 500, u64::MAX - 1)] {
        let keyset = (fee, vec![1]).into();
        let (amounts, reserve) =
            super::fee_plan::amounts_and_reserve(Amount::from(requested), true, &keyset).unwrap();
        assert_eq!(amounts, vec![(Amount::ONE, total)]);
        assert_eq!(
            Amount::from(requested).checked_add(reserve),
            Some(Amount::from(total))
        );
    }
}

#[tokio::test]
async fn direct_send_does_not_expand_the_active_keysets_hypothetical_outputs() {
    let amount = 1u64 << 40;
    let mut old = test_keyset();
    old.input_fee_ppk = 0;
    let key = *old.keys.iter().next().unwrap().1;
    old.keys = crate::nuts::Keys::new([(Amount::from(amount), key)].into());
    old.id = crate::nuts::Id::v2_from_data(&old.keys, &old.unit, 0, None);
    old.active = Some(false);
    let mut active = test_keyset();
    active.input_fee_ppk = 0;
    active.keys = crate::nuts::Keys::new([(Amount::ONE, key)].into());
    active.id = crate::nuts::Id::v2_from_data(&active.keys, &active.unit, 0, None);
    let original = test_proof_info(old.id, amount, test_mint_url());
    let db = create_test_db().await;
    db.update_proofs(vec![original.clone()], vec![])
        .await
        .unwrap();
    let mint = Arc::new(MockMintConnector::new());
    mint.set_mint_keys_response(Ok(vec![old, active]));
    let mut wallet = create_test_wallet_with_mock(db, mint).await;
    wallet.max_outputs = Some(0);
    wallet.proof_inventory_limits = Some((1, 4096));
    for send_kind in [
        crate::wallet::SendKind::OnlineExact,
        crate::wallet::SendKind::OfflineExact,
    ] {
        let prepared = wallet
            .prepare_send(
                Amount::from(amount),
                SendOptions {
                    include_fee: true,
                    max_proofs: Some(1),
                    send_kind,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(prepared.proofs_to_send(), &vec![original.proof.clone()]);
        assert!(prepared.proofs_to_swap().is_empty());
        assert_eq!(prepared.send_fee(), Amount::ZERO);
        prepared.cancel().await.unwrap();
        assert_eq!(
            wallet.get_unspent_proofs().await.unwrap(),
            vec![original.proof.clone()]
        );
    }
}

#[test]
fn inactive_proof_selection_covers_redemption_fee() {
    let keyset = test_keyset();
    let proofs = [8, 4]
        .into_iter()
        .map(|amount| test_proof_info(keyset.id, amount, test_mint_url()).proof)
        .collect();
    let fees = [(keyset.id, (500, vec![1, 2, 4, 8, 16]).into())]
        .into_iter()
        .collect();
    let selected =
        crate::Wallet::select_proofs(Amount::from(8), proofs, &vec![], &fees, true).unwrap();
    let fee = Amount::from((selected.len() as u64 * 500).div_ceil(1000));
    assert!(selected.total_amount().unwrap() - fee >= Amount::from(8));
}

async fn wallet(amounts: &[u64]) -> crate::Wallet {
    let db = create_test_db().await;
    let mut keyset = test_keyset();
    keyset.input_fee_ppk = 500;
    let proofs = amounts
        .iter()
        .map(|amount| test_proof_info(keyset.id, *amount, test_mint_url()))
        .collect();
    db.update_proofs(proofs, vec![]).await.unwrap();
    let mint = Arc::new(MockMintConnector::new());
    mint.set_active_keyset(keyset);
    create_test_wallet_with_mock(db, mint).await
}

#[tokio::test]
async fn fee_reserve_includes_denomination_rounding() {
    let wallet = wallet(&[64]).await;
    let prepared = wallet
        .prepare_send(
            Amount::from(38),
            SendOptions {
                include_fee: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // 39 split as 32+4+2+1 costs 2 sats to redeem and nets only 37.
    // 40 split as 32+8 costs 1 sat, so the send needs a 2-sat reserve.
    assert_eq!(prepared.amount(), Amount::from(38));
    assert_eq!(prepared.send_fee(), Amount::from(2));
    prepared.cancel().await.unwrap();
}

#[tokio::test]
async fn token_proof_limit_applies_before_reserving_inputs() {
    for (coins, swaps) in [(vec![64], true), (vec![32, 16], true), (vec![32, 8], false)] {
        for include_fee in [false, true] {
            for maximum in [0, 1, 2] {
                let wallet = wallet(&coins).await;
                let before = wallet
                    .localstore
                    .get_proofs(None, None, None, None)
                    .await
                    .unwrap();
                let prepared = wallet
                    .prepare_send(
                        Amount::from(if include_fee { 38 } else { 40 }),
                        SendOptions {
                            include_fee,
                            max_proofs: Some(maximum),
                            ..Default::default()
                        },
                    )
                    .await;
                let accepted = match prepared {
                    Ok(prepared) => {
                        // Both requests produce a 40-sat token with outputs 32+8.
                        let actual_swaps = !prepared.proofs_to_swap().is_empty();
                        prepared.cancel().await.unwrap();
                        assert_eq!(actual_swaps, swaps);
                        true
                    }
                    Err(Error::MaxOutputsExceeded { actual, max }) => {
                        assert_eq!(actual, 2);
                        assert_eq!(max, maximum);
                        false
                    }
                    // An exact direct balance cannot pay for a replacement swap
                    // when its existing proofs exceed the requested limit.
                    Err(Error::InsufficientFunds) if !swaps && maximum < 2 => false,
                    Err(error) => panic!(
                        "unexpected proof-limit error: {error}; coins={coins:?}, \
                         include_fee={include_fee}, maximum={maximum}"
                    ),
                };
                assert_eq!(accepted, maximum >= 2, "maximum={maximum}");
                assert!(wallet.get_reserved_proofs().await.unwrap().is_empty());
                assert!(wallet
                    .localstore
                    .get_incomplete_sagas()
                    .await
                    .unwrap()
                    .is_empty());
                assert!(
                    wallet
                        .localstore
                        .get_proofs(None, None, None, None)
                        .await
                        .unwrap()
                        == before
                );
            }
        }
    }
}

#[tokio::test]
async fn token_proof_limit_is_rechecked_before_confirming_a_swap() {
    for coins in [vec![64], vec![32, 16], vec![32, 8]] {
        for include_fee in [false, true] {
            let balance = coins.iter().sum::<u64>();
            let wallet = wallet(&coins).await;
            let prepared = wallet
                .prepare_send(
                    Amount::from(if include_fee { 38 } else { 40 }),
                    SendOptions {
                        include_fee,
                        max_proofs: Some(2),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let mut options = prepared.options().clone();
            options.max_proofs = Some(1);
            // Reconstructed confirmation must enforce its supplied limit before the mint
            // request. The mock has no swap response: reaching it would fail the test.
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
                Err(Error::MaxOutputsExceeded { actual: 2, max: 1 })
            ));
            assert_eq!(wallet.total_balance().await.unwrap(), Amount::from(balance));
            assert!(wallet.get_reserved_proofs().await.unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn input_byte_limit_rejects_before_saving_or_reserving_preparation() {
    for coins in [vec![64], vec![32, 16]] {
        let wallet = wallet(&coins).await;
        let before = wallet
            .localstore
            .get_proofs(None, None, None, None)
            .await
            .unwrap();
        let operations =
            serde_json::to_value(wallet.localstore.get_incomplete_sagas().await.unwrap()).unwrap();
        let bytes = serde_json::to_vec(&wallet.get_unspent_proofs().await.unwrap())
            .unwrap()
            .len();
        for maximum in [Some(0), Some((bytes - 1) as u64), Some(bytes as u64), None] {
            let result = wallet
                .prepare_send(
                    38.into(),
                    SendOptions {
                        include_fee: true,
                        max_input_bytes: maximum,
                        ..Default::default()
                    },
                )
                .await;
            let accepted = match result {
                Ok(prepared) => {
                    prepared.cancel().await.unwrap();
                    true
                }
                Err(Error::RequestFieldTooLarge { field, actual, max }) => {
                    assert_eq!(field, "send_inputs");
                    assert_eq!(actual, bytes);
                    assert_eq!(Some(max as u64), maximum);
                    false
                }
                Err(error) => panic!("unexpected input-byte error: {error}"),
            };
            assert_eq!(accepted, maximum.is_none_or(|max| max >= bytes as u64));
            assert!(
                wallet
                    .localstore
                    .get_proofs(None, None, None, None)
                    .await
                    .unwrap()
                    == before
            );
            assert!(
                serde_json::to_value(wallet.localstore.get_incomplete_sagas().await.unwrap())
                    .unwrap()
                    == operations
            );
        }
    }
}

#[tokio::test]
async fn input_byte_limit_is_rechecked_before_confirming() {
    let wallet = wallet(&[64]).await;
    let before = wallet
        .localstore
        .get_proofs(None, None, None, None)
        .await
        .unwrap();
    let prepared = wallet
        .prepare_send(
            38.into(),
            SendOptions {
                include_fee: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut options = prepared.options().clone();
    options.max_input_bytes = Some(0);
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
        Err(Error::RequestFieldTooLarge { max: 0, .. })
    ));
    assert!(
        wallet
            .localstore
            .get_proofs(None, None, None, None)
            .await
            .unwrap()
            == before
    );
    assert!(wallet
        .localstore
        .get_incomplete_sagas()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn fragmented_exact_send_uses_actual_redemption_fee() {
    let wallet = wallet(&[8, 4, 2, 1, 1]).await;
    let proofs = wallet.get_unspent_proofs().await.unwrap();
    let prepared = SendSaga::new(&wallet)
        .internal_prepare(
            Amount::from(13),
            SendOptions {
                include_fee: true,
                ..Default::default()
            },
            proofs,
            false,
            Default::default(),
        )
        .await
        .unwrap();
    assert!(prepared.proofs_to_swap().is_empty());
    assert_eq!(prepared.send_fee(), Amount::from(3));
    let (token, _) = prepared.confirm(None).await.unwrap();
    let keysets = vec![crate::nuts::KeySetInfo {
        id: test_keyset().id,
        unit: crate::nuts::CurrencyUnit::Sat,
        active: true,
        input_fee_ppk: 500,
        final_expiry: None,
    }];
    let sent = token.proofs(&keysets).unwrap();
    let fee = wallet.get_proofs_fee(&sent).await.unwrap().total;
    assert_eq!(sent.total_amount().unwrap() - fee, Amount::from(13));
}

#[tokio::test]
async fn fragmented_nominal_match_cannot_underpay_recipient() {
    let wallet = wallet(&[8, 4, 2, 1, 1]).await;
    let proofs = wallet.get_unspent_proofs().await.unwrap();
    let prepared = SendSaga::new(&wallet)
        .internal_prepare(
            Amount::from(15),
            SendOptions {
                include_fee: true,
                ..Default::default()
            },
            proofs,
            false,
            Default::default(),
        )
        .await;
    assert!(matches!(prepared, Err(crate::Error::InsufficientFunds)));
}

#[tokio::test]
async fn fee_inclusive_public_send_reuses_sufficient_existing_proofs() {
    for (coins, amount) in [(vec![8, 4, 2, 1, 1], 13), (vec![32, 8], 38)] {
        let wallet = wallet(&coins).await;
        let prepared = wallet
            .prepare_send(
                Amount::from(amount),
                SendOptions {
                    include_fee: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(prepared.proofs_to_swap().is_empty());
        let token = prepared.confirm(None).await.unwrap();
        let keysets = vec![crate::nuts::KeySetInfo {
            id: test_keyset().id,
            unit: crate::nuts::CurrencyUnit::Sat,
            active: true,
            input_fee_ppk: 500,
            final_expiry: None,
        }];
        let sent = token.proofs(&keysets).unwrap();
        let fee = wallet.get_proofs_fee(&sent).await.unwrap().total;
        assert!(sent.total_amount().unwrap() - fee >= Amount::from(amount));
    }
}

#[tokio::test]
async fn fee_increase_after_preparation_rejects_before_swap() {
    let db = create_test_db().await;
    let mut keyset = test_keyset();
    keyset.input_fee_ppk = 0;
    db.update_proofs(
        vec![test_proof_info(keyset.id, 64, test_mint_url())],
        vec![],
    )
    .await
    .unwrap();
    let mint = Arc::new(MockMintConnector::new());
    mint.set_active_keyset(keyset.clone());
    let wallet = create_test_wallet_with_mock(db, mint.clone()).await;
    let prepared = wallet
        .prepare_send(
            Amount::from(38),
            SendOptions {
                include_fee: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    keyset.input_fee_ppk = 500;
    mint.set_active_keyset(keyset);
    wallet
        .keysets(cdk_common::wallet::KeysetLoadPolicy::Refresh)
        .await
        .unwrap();
    // The mock deliberately has no swap response: attempting a swap would panic.
    let result = prepared.confirm(None).await;
    assert!(matches!(result, Err(crate::Error::MaxFeeExceeded)));
    assert_eq!(wallet.total_balance().await.unwrap(), Amount::from(64));
    assert!(wallet.get_reserved_proofs().await.unwrap().is_empty());
}

#[tokio::test]
async fn proof_inventory_limits_reject_send_and_refill_before_reservation() {
    let mut wallet = wallet(&[32, 8]).await;
    let original = wallet
        .localstore
        .get_proofs(None, None, None, None)
        .await
        .unwrap();
    for limits in [(1, 4096), (2, 1)] {
        wallet.proof_inventory_limits = Some(limits);
        assert!(matches!(
            wallet.prepare_send(8.into(), SendOptions::default()).await,
            Err(Error::Database(
                cdk_common::database::Error::ProofInventoryTooLarge { .. }
            ))
        ));
        assert!(matches!(
            wallet
                .determine_split_target_values(8.into(), &(0, vec![1, 2, 4, 8]).into())
                .await,
            Err(Error::Database(
                cdk_common::database::Error::ProofInventoryTooLarge { .. }
            ))
        ));
        assert_eq!(
            wallet
                .localstore
                .get_proofs(None, None, None, None)
                .await
                .unwrap(),
            original
        );
        assert!(wallet
            .localstore
            .get_incomplete_sagas()
            .await
            .unwrap()
            .is_empty());
    }
    wallet.proof_inventory_limits = Some((2, 4096));
    let prepared = wallet
        .prepare_send(8.into(), SendOptions::default())
        .await
        .unwrap();
    assert!(prepared.proofs_to_swap().is_empty());
    prepared.cancel().await.unwrap();
}
