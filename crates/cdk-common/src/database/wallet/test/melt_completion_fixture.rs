use super::*;
use crate::wallet::{MeltOperationData, MeltPayment, MeltSagaState, TransactionRole};

/// Canonical paid payment, exact owned quote and original Pending receipt.
pub fn melt_completion_fixture() -> (WalletSaga, WalletSaga, MeltQuote, Transaction) {
    let mut quote = test_melt_quote();
    quote.amount = 8.into();
    quote.fee_reserve = 3.into();
    quote.version = 4;
    quote.estimated_blocks = Some(6);
    quote.fee_index = Some(2);
    let id = uuid::Uuid::new_v4();
    quote.used_by_operation = Some(id.to_string());
    let inputs: Vec<_> = (0..2)
        .map(|index| {
            let mut input = test_proof_info(test_keyset_id(), 8, test_mint_url());
            input.state = State::Pending;
            input.used_by_operation = Some(id);
            input.created_by_operation = Some(uuid::Uuid::new_v4());
            input.derivation_index = Some(index);
            input.proof.p2pk_e = Some(SecretKey::generate().public_key());
            input.proof.dleq = Some(crate::ProofDleq::new(
                SecretKey::generate(),
                SecretKey::generate(),
                SecretKey::generate(),
            ));
            input.proof.witness = Some(crate::nuts::Witness::P2PKWitness(
                crate::nuts::nut11::P2PKWitness {
                    signatures: vec!["saved-witness".to_owned()],
                },
            ));
            input
        })
        .collect();
    let change = [4, 2]
        .into_iter()
        .enumerate()
        .map(|(index, amount)| {
            let mut output = test_proof_info(test_keyset_id(), amount, test_mint_url());
            output.created_by_operation = Some(id);
            output.derivation_index = Some(10 + index as u32);
            output
        })
        .collect();
    let ys: Vec<_> = inputs.iter().map(|input| input.y).collect();
    let metadata = HashMap::from([("purpose".to_owned(), "completion-test".to_owned())]);
    let mut saga = WalletSaga::new(
        id,
        WalletSagaState::Melt(MeltSagaState::PaidReady),
        quote.amount,
        test_mint_url(),
        quote.unit.clone(),
        OperationData::Melt(MeltOperationData {
            payment: None,
            swap: None,
            acquisition: None,
            quote_id: quote.id.clone(),
            amount: quote.amount,
            fee_reserve: quote.fee_reserve,
            counter_start: Some(10),
            counter_end: Some(12),
            change_amount: Some(8.into()),
            metadata: metadata.clone(),
            final_proof_ys: Some(ys.clone()),
            change_blinded_messages: None,
        }),
    );
    saga.version = 7;
    let receipt = Transaction {
        role: TransactionRole::Primary,
        mint_url: saga.mint_url.clone(),
        direction: TransactionDirection::Outgoing,
        amount: saga.amount,
        fee: 2.into(),
        unit: saga.unit.clone(),
        ys,
        timestamp: saga.created_at,
        memo: Some("unchanged memo".to_owned()),
        metadata: metadata.clone(),
        quote_id: Some(quote.id.clone()),
        payment_request: Some(quote.request.clone()),
        payment_proof: Some("verified-payment-proof".to_owned()),
        payment_method: Some(quote.payment_method.clone()),
        saga_id: Some(id),
        status: TransactionStatus::Completed,
    };
    let mut pending = receipt.clone();
    pending.status = TransactionStatus::Pending;
    pending.fee = quote.fee_reserve;
    pending.payment_proof = None;
    let OperationData::Melt(data) = &mut saga.data else {
        unreachable!()
    };
    data.payment = Some(Box::new(MeltPayment {
        inputs,
        receipt,
        change: Some(change),
    }));
    let mut next = saga.clone();
    next.update_state(WalletSagaState::Melt(MeltSagaState::Completed));
    (saga, next, quote, pending)
}
