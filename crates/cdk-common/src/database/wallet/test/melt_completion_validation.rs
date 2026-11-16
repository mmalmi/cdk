use super::*;
use crate::wallet::{MeltSagaState, TransactionRole};

/// Invalid identity, custody and amount evidence is rejected before every write.
pub async fn complete_melt_invalid<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (saga, next, quote, pending) = melt_completion_fixture();
    db.add_melt_quote(quote.clone()).await.unwrap();
    db.add_saga(saga.clone()).await.unwrap();
    db.add_transaction(pending.clone()).await.unwrap();
    for field in [
        "phase",
        "kind",
        "next_data",
        "next_version",
        "quote_owner",
        "quote_proof",
        "large_proof",
        "quote_version",
        "fee",
        "receipt_role",
        "receipt_status",
        "timestamp",
        "metadata",
        "batch_quote",
        "request",
        "method",
        "receipt_order",
        "input_order",
        "input_owner",
        "input_state",
        "input_scope",
        "output_owner",
        "output_origin",
        "output_state",
        "output_scope",
        "output_collision",
        "unverified",
        "missing_payment",
        "missing_change",
        "too_much_change",
        "insufficient_change",
        "identity",
        "sum_overflow",
    ] {
        let mut invalid = saga.clone();
        let mut invalid_quote = quote.clone();
        let OperationData::Melt(data) = &mut invalid.data else {
            unreachable!()
        };
        let payment = data.payment.as_mut().unwrap();
        match field {
            "phase" => invalid.state = WalletSagaState::Melt(MeltSagaState::PaymentPending),
            "kind" => invalid.kind = crate::wallet::OperationKind::Send,
            "quote_owner" => {
                invalid_quote.used_by_operation = Some(uuid::Uuid::new_v4().to_string())
            }
            "quote_proof" => invalid_quote.payment_proof = Some("conflicting-proof".to_owned()),
            "quote_version" => invalid_quote.version = u32::MAX,
            "large_proof" => {
                payment.receipt.payment_proof = Some("x".repeat(MAX_MELT_PAYMENT_PROOF_BYTES + 1));
                invalid_quote.payment_proof = payment.receipt.payment_proof.clone();
            }
            "fee" => payment.receipt.fee += Amount::from(1),
            "receipt_role" => payment.receipt.role = TransactionRole::SendRefund,
            "receipt_status" => payment.receipt.status = TransactionStatus::Pending,
            "timestamp" => payment.receipt.timestamp += 1,
            "metadata" => {
                payment
                    .receipt
                    .metadata
                    .insert("altered".to_owned(), "value".to_owned());
            }
            "batch_quote" => {
                data.metadata
                    .insert("batch_quote_id".to_owned(), "foreign-identity".to_owned());
                payment.receipt.metadata = data.metadata.clone();
            }
            "request" => payment.receipt.payment_request = None,
            "method" => payment.receipt.payment_method = None,
            "receipt_order" => payment.receipt.ys.reverse(),
            "input_order" => data.final_proof_ys.as_mut().unwrap().reverse(),
            "input_owner" => payment.inputs[0].used_by_operation = None,
            "input_state" => payment.inputs[0].state = State::Spent,
            "input_scope" => payment.inputs[0].mint_url = test_mint_url_2(),
            "output_owner" => {
                payment.change.as_mut().unwrap()[0].used_by_operation = Some(uuid::Uuid::new_v4())
            }
            "output_origin" => payment.change.as_mut().unwrap()[0].created_by_operation = None,
            "output_state" => payment.change.as_mut().unwrap()[0].state = State::Spent,
            "output_scope" => payment.change.as_mut().unwrap()[0].unit = CurrencyUnit::Msat,
            "output_collision" => payment
                .change
                .as_mut()
                .unwrap()
                .push(payment.inputs[0].clone()),
            "unverified" => payment.change = None,
            "missing_payment" => data.payment = None,
            "missing_change" => payment.change.as_mut().unwrap().clear(),
            "too_much_change" => payment.change.as_mut().unwrap()[0].proof.amount = 16.into(),
            "insufficient_change" => {
                payment.change.as_mut().unwrap().pop();
                payment.receipt.fee = 4.into();
            }
            "identity" => payment.inputs[0].y = SecretKey::generate().public_key(),
            "sum_overflow" => payment.inputs[0].proof.amount = u64::MAX.into(),
            "next_data" | "next_version" => (),
            _ => unreachable!(),
        }
        let mut invalid_next = invalid.clone();
        invalid_next.update_state(next.state);
        if field == "next_version" {
            invalid_next.version += 1;
        }
        if field == "next_data" {
            let OperationData::Melt(data) = &mut invalid_next.data else {
                unreachable!()
            };
            data.counter_end = None;
        }
        assert!(
            db.complete_melt(&invalid, invalid_next, &invalid_quote)
                .await
                .is_err(),
            "{field}"
        );
        assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
        assert_eq!(
            db.get_melt_quote(&quote.id).await.unwrap(),
            Some(quote.clone())
        );
        assert_eq!(
            db.get_transaction(pending.id()).await.unwrap(),
            Some(pending.clone())
        );
    }
}

/// Input fees and an exactly exhausted network fee budget remain valid paid outcomes.
pub async fn complete_melt_fee_budget<D>(db: D)
where
    D: Database<Error> + Sync,
{
    for empty_change in [false, true] {
        let (mut saga, mut next, mut quote, _) = melt_completion_fixture();
        let OperationData::Melt(data) = &mut saga.data else {
            unreachable!()
        };
        let payment = data.payment.as_mut().unwrap();
        if empty_change {
            payment.change = Some(vec![]);
            payment.receipt.fee = 8.into();
            data.fee_reserve = 8.into();
            quote.fee_reserve = 8.into();
            quote.payment_proof = payment.receipt.payment_proof.clone();
        } else {
            data.change_amount = Some(6.into());
            payment.change.as_mut().unwrap().pop();
            payment.receipt.fee = 4.into();
        }
        next.data = saga.data.clone();
        let result = validate_melt_completion(&saga, &next, &quote).unwrap();
        db.add_melt_quote(quote.clone()).await.unwrap();
        db.add_saga(saga.clone()).await.unwrap();
        db.add_transaction(result.pending.clone()).await.unwrap();
        assert!(db.complete_melt(&saga, next.clone(), &quote).await.unwrap());
        assert_eq!(
            db.get_transaction(result.receipt.id()).await.unwrap(),
            Some(result.receipt)
        );
    }
}
