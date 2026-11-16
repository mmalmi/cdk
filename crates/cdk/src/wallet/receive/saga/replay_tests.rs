//! Retry the original payment without relying on a proof's later owner.
use cdk_common::wallet::receive_identity;

use super::test_utils::{fixture, options};
use crate::nuts::{CurrencyUnit, State};
use crate::wallet::test_utils::{test_keyset, test_mint_url, test_proof_info};
use crate::{Amount, Error};

#[tokio::test]
async fn completed_receive_retries_exact_coins_until_receipt_retirement() {
    let (wallet, client) = fixture().await;
    let mut first = test_proof_info(test_keyset().id, 4, test_mint_url());
    first.created_by_operation = Some(uuid::Uuid::new_v4());
    // This input already belongs to the wallet, so its prior ownership survives.
    wallet
        .localstore
        .update_proofs(vec![first.clone()], vec![])
        .await
        .unwrap();
    let second = test_proof_info(test_keyset().id, 4, test_mint_url());
    let mut proofs = vec![first.proof.clone(), second.proof];
    let id = receive_identity::transaction_id(&wallet.mint_url, &wallet.unit, &proofs).unwrap();
    let mut forged = options();
    forged
        .metadata
        .insert(receive_identity::METADATA_KEY.to_owned(), "00".repeat(32));
    assert!(wallet
        .receive_proofs(proofs.clone(), forged, None, None)
        .await
        .is_err());
    assert!(client.captured_swap_requests().is_empty());
    assert_eq!(
        wallet
            .receive_proofs(proofs.clone(), options(), None, None)
            .await
            .unwrap(),
        Amount::from(8)
    );
    let original = wallet
        .localstore
        .get_transaction(id)
        .await
        .unwrap()
        .unwrap();
    let before = wallet
        .localstore
        .get_proofs(None, None, None, None)
        .await
        .unwrap();
    proofs.reverse();
    assert_eq!(
        receive_identity::transaction_id(&wallet.mint_url, &wallet.unit, &proofs).unwrap(),
        id
    );
    assert!(matches!(
        wallet
            .receive_proofs(proofs.clone(), options(), None, None)
            .await,
        Err(Error::TokenAlreadySpent)
    ));
    let mut replay = options();
    replay.replay_completed = true;
    assert_eq!(
        wallet
            .receive_proofs(
                proofs.clone(),
                replay.clone(),
                Some("retry".to_owned()),
                None
            )
            .await
            .unwrap(),
        original.amount
    );
    assert_eq!(
        wallet.localstore.get_transaction(id).await.unwrap(),
        Some(original.clone())
    );
    assert_eq!(client.captured_swap_requests().len(), 1);

    let mut corrupt = original.clone();
    corrupt.direction = cdk_common::wallet::TransactionDirection::Outgoing;
    assert!(wallet.localstore.add_transaction(corrupt).await.is_err());
    let mut corrupt = original.clone();
    corrupt.amount = Amount::from(9);
    wallet.localstore.add_transaction(corrupt).await.unwrap();
    assert!(wallet
        .receive_proofs(proofs.clone(), replay.clone(), None, None)
        .await
        .is_err());
    wallet.localstore.add_transaction(original).await.unwrap();
    wallet.localstore.remove_transaction(id).await.unwrap();
    assert!(wallet
        .receive_proofs(proofs, replay, None, None)
        .await
        .is_err());
    assert!(wallet
        .localstore
        .get_transaction(id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(client.captured_swap_requests().len(), 1);
    assert_eq!(
        wallet
            .localstore
            .get_proofs(None, None, None, None)
            .await
            .unwrap(),
        before
    );
    first.state = State::Spent;
    assert_eq!(
        wallet
            .localstore
            .get_proofs_by_ys(vec![first.y])
            .await
            .unwrap(),
        [first]
    );
}

#[test]
fn receive_identity_binds_mint_unit_and_coin_fields() {
    let mint = "https://mint.example".parse().unwrap();
    let proof = crate::nuts::Proof {
        amount: Amount::from(8),
        keyset_id: "009a1f293253e41e".parse().unwrap(),
        secret: crate::secret::Secret::new("receive-id-fixture"),
        c: "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
            .parse()
            .unwrap(),
        witness: None,
        dleq: None,
        p2pk_e: None,
    };
    let proofs = vec![proof.clone()];
    let id = receive_identity::transaction_id(&mint, &CurrencyUnit::Sat, &proofs).unwrap();
    assert_eq!(
        id.to_string(),
        "c0f019817971b1c460077f001c70ae34eb373bdea87c07327183c155776524f7"
    );
    assert_ne!(
        receive_identity::transaction_id(&mint, &CurrencyUnit::Msat, &proofs).unwrap(),
        id
    );
    assert_ne!(
        receive_identity::transaction_id(
            &"https://other.example".parse().unwrap(),
            &CurrencyUnit::Sat,
            &proofs
        )
        .unwrap(),
        id
    );
    let mut changed = proof.clone();
    changed.p2pk_e = Some(proof.c);
    assert_eq!(
        receive_identity::transaction_id(&mint, &CurrencyUnit::Sat, &[changed]).unwrap(),
        id
    );
    let mut changed = proof.clone();
    changed.amount = Amount::from(9);
    assert_ne!(
        receive_identity::transaction_id(&mint, &CurrencyUnit::Sat, &[changed]).unwrap(),
        id
    );
    assert!(
        receive_identity::transaction_id(&mint, &CurrencyUnit::Sat, &[proof.clone(), proof])
            .is_err()
    );
    assert!(receive_identity::transaction_id(&mint, &CurrencyUnit::Sat, &[]).is_err());
}
