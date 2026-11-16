use std::sync::Arc;

use cdk_common::wallet::WalletSaga;

use super::*;
use crate::nuts::{BlindSignature, RestoreResponse};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_keyset_id, test_mint_url, test_proof_info,
    MockMintConnector,
};

pub(super) async fn wallet() -> (Wallet, Arc<MockMintConnector>) {
    let db = create_test_db().await;
    db.update_proofs(
        vec![test_proof_info(test_keyset_id(), 32, test_mint_url())],
        vec![],
    )
    .await
    .unwrap();
    let mint = Arc::new(MockMintConnector::new());
    mint.reset_default_mint_state();
    let mut wallet = create_test_wallet_with_mock(db, mint.clone()).await;
    wallet.set_target_proof_count(0);
    (wallet, mint)
}

pub(super) async fn prepare(
    wallet: &Wallet,
    conditions: Option<SpendingConditions>,
    p2bk: bool,
) -> SwapSaga<'_, state::Prepared> {
    SwapSaga::new(wallet)
        .prepare(
            Some(12.into()),
            Default::default(),
            wallet.get_unspent_proofs().await.unwrap(),
            conditions,
            p2bk,
            false,
        )
        .await
        .unwrap()
}

pub(super) fn response(saga: &WalletSaga) -> RestoreResponse {
    let outputs = checkpoint::get(saga)
        .unwrap()
        .pre_swap
        .swap_request
        .outputs()
        .clone();
    let signatures = outputs
        .iter()
        .map(|p| BlindSignature {
            amount: p.amount,
            keyset_id: p.keyset_id,
            c: p.blinded_secret,
            dleq: None,
        })
        .collect();
    RestoreResponse {
        outputs,
        signatures,
    }
}

pub(super) async fn submitted(
    wallet: &Wallet,
    mint: &MockMintConnector,
    conditions: Option<SpendingConditions>,
    p2bk: bool,
) -> WalletSaga {
    let prepared = prepare(wallet, conditions, p2bk).await;
    let id = prepared.state_data.saga.id;
    mint.set_post_swap_response(Err(Error::Timeout));
    assert!(matches!(prepared.execute().await, Err(Error::Timeout)));
    wallet.localstore.get_saga(&id).await.unwrap().unwrap()
}
