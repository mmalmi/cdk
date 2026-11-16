//! Missing original authority must not turn an uncertain payment into a new one.
use std::collections::HashMap;

use super::paid_change_fixture::Fixture;
use crate::nuts::{MeltQuoteState, ProofsMethods};
use crate::Error;

#[tokio::test]
async fn missing_melt_journal_blocks_status_without_changing_custody() {
    for malformed_owner in [false, true] {
        let f = Fixture::new().await;
        let db = &f.wallet.localstore;
        let mut quote = db.get_melt_quote(&f.quote.id).await.unwrap().unwrap();
        db.delete_saga(&f.saga.id).await.unwrap();
        if malformed_owner {
            quote.used_by_operation = Some("damaged-owner".to_owned());
            db.add_melt_quote(quote).await.unwrap();
            quote = db.get_melt_quote(&f.quote.id).await.unwrap().unwrap();
        }
        let ys = f.inputs.ys().unwrap();
        let proofs = db.get_proofs_by_ys(ys.clone()).await.unwrap();
        let receipts = db.list_transactions(None, None, None).await.unwrap();
        f.status(Some(f.response(&[16, 8]).signatures));

        let status = f.wallet.check_melt_quote_status(&quote.id).await;
        assert!(
            matches!(status, Err(Error::InvalidOperationState)),
            "missing melt journal must reject status reconciliation: {status:?}"
        );
        assert!(f
            .wallet
            .prepare_melt(&quote.id, HashMap::new())
            .await
            .is_err());
        assert_eq!(
            db.get_melt_quote(&quote.id).await.unwrap(),
            Some(quote.clone())
        );
        assert_eq!(db.get_proofs_by_ys(ys).await.unwrap(), proofs);
        assert_eq!(
            db.list_transactions(None, None, None).await.unwrap(),
            receipts
        );
        assert!(f
            .client
            .melt_quote_status_response
            .lock()
            .unwrap()
            .is_some());

        // Only restoring the original journal/owner authorizes completion.
        if malformed_owner {
            quote.used_by_operation = Some(f.saga.id.to_string());
            db.add_melt_quote(quote).await.unwrap();
        }
        db.add_saga(f.saga.clone()).await.unwrap();
        let recovered = f.wallet.check_melt_quote_status(&f.quote.id).await.unwrap();
        assert_eq!(recovered.state, MeltQuoteState::Paid);
        assert!(recovered.used_by_operation.is_none());
        assert!(db.get_saga(&f.saga.id).await.unwrap().is_none());
        assert_eq!(f.wallet.total_balance().await.unwrap().to_u64(), 24);
        assert_eq!(
            db.list_transactions(None, None, None).await.unwrap().len(),
            1
        );
    }
}
