//! Parent-owned denomination swaps survive a lost reply without a child saga.
use std::collections::HashMap;
use std::sync::Arc;

use cdk_common::wallet::{MeltQuote, WalletSaga};

use crate::nuts::{BlindSignature, KeySetInfo, RestoreResponse};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_keyset, test_melt_quote, test_mint_url,
    test_proof_info, MockMintConnector,
};
use crate::{Amount, Error, Wallet};

pub(super) struct Fixture {
    pub(super) wallet: Wallet,
    pub(super) client: Arc<MockMintConnector>,
    pub(super) quote: MeltQuote,
}

impl Fixture {
    pub(super) async fn new() -> Self {
        let db = create_test_db().await;
        let client = Arc::new(MockMintConnector::new());
        let wallet = create_test_wallet_with_mock(db.clone(), client.clone()).await;
        let mut keyset = test_keyset();
        keyset.input_fee_ppk = 0;
        db.add_mint(test_mint_url(), None).await.unwrap();
        db.add_mint_keysets(
            test_mint_url(),
            vec![KeySetInfo {
                id: keyset.id,
                unit: keyset.unit.clone(),
                active: true,
                input_fee_ppk: 0,
                final_expiry: None,
            }],
        )
        .await
        .unwrap();
        db.add_keys(keyset.clone()).await.unwrap();
        let mut quote = test_melt_quote();
        quote.id = uuid::Uuid::new_v4().to_string();
        quote.amount = Amount::from(8);
        quote.fee_reserve = Amount::from(1);
        db.add_melt_quote(quote.clone()).await.unwrap();
        let mut input = test_proof_info(keyset.id, 32, test_mint_url());
        input.created_by_operation = Some(uuid::Uuid::new_v4());
        input.derivation_index = Some(42);
        db.update_proofs(vec![input], vec![]).await.unwrap();
        Self {
            wallet,
            client,
            quote,
        }
    }
}

impl Fixture {
    pub(super) async fn interrupted(&self) -> WalletSaga {
        let prepared = super::MeltSaga::new(&self.wallet)
            .prepare(&self.quote.id, HashMap::new())
            .await
            .unwrap();
        let id = prepared.operation_id();
        self.client.set_post_swap_response(Err(Error::Timeout));
        assert!(prepared
            .request_melt_with_options(Default::default())
            .await
            .is_err());
        self.wallet.localstore.get_saga(&id).await.unwrap().unwrap()
    }

    pub(super) fn restore(&self, saga: &WalletSaga) -> RestoreResponse {
        let swap = super::swap::saved(saga).unwrap();
        let outputs = swap.pre_swap.swap_request.outputs().clone();
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

    pub(super) async fn ready(&self) -> WalletSaga {
        let mut saga = self.interrupted().await;
        self.client._set_restore_response(Ok(self.restore(&saga)));
        super::swap::finish(&self.wallet, &mut saga, true)
            .await
            .unwrap();
        saga
    }
}
