//! Production receive admission and handoff regressions.
use std::sync::Arc;

use crate::nuts::{BlindSignature, SwapResponse};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_keyset, test_mint_url, test_proof_info,
    MockMintConnector,
};
use crate::Wallet;

pub(super) fn options() -> crate::wallet::ReceiveOptions {
    crate::wallet::ReceiveOptions {
        amount_split_target: crate::amount::SplitTarget::Values(vec![8.into()]),
        ..Default::default()
    }
}

pub(super) async fn fixture() -> (Wallet, Arc<MockMintConnector>) {
    let mut client = MockMintConnector::new();
    client.unspent_check_fallback = true;
    let client = Arc::new(client);
    let mut keys = test_keyset();
    keys.input_fee_ppk = 0;
    client.set_active_keyset(keys.clone());
    client.set_post_swap_response(Ok(SwapResponse {
        signatures: vec![BlindSignature {
            amount: 8.into(),
            keyset_id: keys.id,
            c: crate::nuts::SecretKey::generate().public_key(),
            dleq: None,
        }],
    }));
    let wallet = create_test_wallet_with_mock(create_test_db().await, client.clone()).await;
    wallet.active_keyset().await.unwrap();
    (wallet, client)
}

pub(super) async fn ready() -> (
    Wallet,
    Arc<MockMintConnector>,
    cdk_common::wallet::WalletSaga,
) {
    let (wallet, client) = fixture().await;
    client.set_post_swap_response(Err(crate::Error::Custom("lost reply".to_owned())));
    let input = test_proof_info(test_keyset().id, 8, test_mint_url());
    assert!(wallet
        .receive_proofs(
            vec![input.proof],
            options(),
            Some("original memo".to_owned()),
            None
        )
        .await
        .is_err());
    let mut saga = wallet
        .localstore
        .get_incomplete_sagas()
        .await
        .unwrap()
        .pop()
        .unwrap();
    super::finish::save_response(
        &wallet,
        &mut saga,
        SwapResponse {
            signatures: vec![BlindSignature {
                amount: 8.into(),
                keyset_id: test_keyset().id,
                c: crate::nuts::SecretKey::generate().public_key(),
                dleq: None,
            }],
        },
    )
    .await
    .unwrap();
    (wallet, client, saga)
}
