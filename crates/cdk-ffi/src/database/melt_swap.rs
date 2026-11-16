//! Parent swap evidence survives the foreign storage bridge without a child operation.
use std::collections::HashMap;
use std::str::FromStr;

use cdk::amount::{FeeAndAmounts, SplitTarget};
use cdk::nuts::{CurrencyUnit, Id, PreMintSecrets, PreSwap, Proof, SecretKey, State, SwapRequest};
use cdk_common::wallet::{
    MeltOperationData, MeltSagaState, MeltSwap, OperationData, ProofInfo, SwapOutputs, WalletSaga,
    WalletSagaState,
};
use cdk_common::Amount;

use super::{CdkWalletDatabase, FfiWalletDatabaseWrapper, WalletDatabaseBridge};

#[tokio::test]
async fn parent_melt_swap_roundtrips_with_cancellation_fence() {
    let db = WalletDatabaseBridge::new(FfiWalletDatabaseWrapper::new(
        cdk_sqlite::wallet::memory::empty().await.unwrap(),
    ));
    let id = uuid::Uuid::new_v4();
    let mint = "https://example.com".parse().unwrap();
    let keyset = Id::from_str("0094d5a774c40a32").unwrap();
    let mut input = ProofInfo::new_with_operations(
        Proof {
            amount: 8.into(),
            keyset_id: keyset,
            secret: cdk::secret::Secret::new("parent-input"),
            c: SecretKey::generate().public_key(),
            witness: None,
            dleq: None,
            p2pk_e: None,
        },
        mint,
        State::Reserved,
        CurrencyUnit::Sat,
        Some(id),
        Some(uuid::Uuid::new_v4()),
    )
    .unwrap();
    input.derivation_index = Some(7);
    let secrets = PreMintSecrets::random(
        keyset,
        Amount::from(8),
        &SplitTarget::None,
        &FeeAndAmounts::from((0, vec![1, 2, 4, 8])),
    )
    .unwrap();
    let intent = MeltSwap {
        inputs: vec![input.clone()],
        amount: 8.into(),
        pre_swap: PreSwap {
            swap_request: SwapRequest::new(vec![input.proof.clone()], secrets.blinded_messages()),
            pre_mint_secrets: secrets,
            derived_secret_count: 0,
            fee: Amount::ZERO,
            p2bk_secret_keys: None,
        },
        outputs: Some(SwapOutputs {
            send: vec![input.proof.clone()],
            change: vec![],
        }),
    };
    let saga = WalletSaga::new(
        id,
        WalletSagaState::Melt(MeltSagaState::ProofsReserved),
        8.into(),
        input.mint_url.clone(),
        CurrencyUnit::Sat,
        OperationData::Melt(MeltOperationData {
            payment: None,
            acquisition: None,
            quote_id: "quote".to_owned(),
            amount: 8.into(),
            fee_reserve: Amount::ZERO,
            swap: Some(Box::new(intent)),
            counter_start: None,
            counter_end: None,
            change_amount: None,
            metadata: HashMap::from([("purpose".to_owned(), "test".to_owned())]),
            final_proof_ys: Some(vec![input.y]),
            change_blinded_messages: None,
        }),
    );
    db.add_saga(saga.clone()).await.unwrap();
    assert_eq!(db.get_saga(&id).await.unwrap(), Some(saga.clone()));
    let mut cancelling = saga.clone();
    cancelling.update_state(WalletSagaState::Melt(MeltSagaState::SwapCancelling));
    assert!(db.update_saga(cancelling.clone()).await.unwrap());
    assert_eq!(db.get_saga(&id).await.unwrap(), Some(cancelling.clone()));
    assert!(!db
        .compare_and_swap_proofs(vec![], Some(&saga))
        .await
        .unwrap());
    assert!(db
        .compare_and_swap_proofs(vec![], Some(&cancelling))
        .await
        .unwrap());
}
