//! Recovery and enumeration must not materialize every saved operation.
use std::sync::Arc;

use cdk_common::wallet::{
    OperationData, ReceiveOperationData, ReceiveSagaState, WalletSaga, WalletSagaState,
};
use cdk_common::CurrencyUnit;

use super::interposed_store::Interposed;
use crate::wallet::test_utils::{create_test_db, create_test_wallet, test_mint_url};

#[tokio::test]
async fn saga_pages_enumeration_uses_bounded_reads() {
    let db = create_test_db().await;
    let mut originals = Vec::new();
    for i in 0..259u128 {
        let mut saga = WalletSaga::new(
            uuid::Uuid::from_u128(i + 1),
            WalletSagaState::Receive(ReceiveSagaState::InputsRecorded),
            1.into(),
            test_mint_url(),
            CurrencyUnit::Sat,
            OperationData::Receive(ReceiveOperationData { checkpoint: None }),
        );
        saga.created_at = (i / 4) as u64;
        db.add_saga(saga.clone()).await.unwrap();
        originals.push(saga);
    }
    let mut store = Interposed::new(db.clone());
    store.reject_bulk_history = true;
    let wallet = create_test_wallet(Arc::new(store)).await;
    let pending = wallet.get_pending_sends().await;
    let melts = wallet.finalize_pending_melts().await;
    let recovery = wallet.recover_incomplete_sagas().await;
    assert!(pending.is_ok(), "pending sends: {pending:?}");
    assert!(melts.is_ok(), "pending melts: {melts:?}");
    let report = recovery.unwrap();
    assert_eq!(report.failed, 259);
    assert_eq!(report.recovered + report.skipped + report.compensated, 0);
    // Missing checkpoint evidence is retained on every page.
    for original in originals {
        assert_eq!(db.get_saga(&original.id).await.unwrap(), Some(original));
    }
}
