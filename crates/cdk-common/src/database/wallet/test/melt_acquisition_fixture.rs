use super::*;
use crate::wallet::{MeltOperationData, MeltSagaState};

pub(super) fn intent(quote: &MeltQuote, before: ProofInfo) -> WalletSaga {
    let id = uuid::Uuid::new_v4();
    let mut after = before.clone();
    after.state = State::Reserved;
    after.used_by_operation = Some(id);
    WalletSaga::new(
        id,
        WalletSagaState::Melt(MeltSagaState::ProofsReserved),
        quote.amount,
        test_mint_url(),
        quote.unit.clone(),
        OperationData::Melt(MeltOperationData {
            payment: None,
            swap: None,
            metadata: Default::default(),
            acquisition: Some(vec![ProofChange {
                y: before.y,
                before: Some(before),
                after: Some(after),
            }]),
            final_proof_ys: None,
            quote_id: quote.id.clone(),
            amount: quote.amount,
            fee_reserve: quote.fee_reserve,
            counter_start: None,
            counter_end: None,
            change_amount: None,
            change_blinded_messages: None,
        }),
    )
}

/// A mixed local/external acquisition fixture, shared by transaction rollback tests.
pub fn melt_acquisition_fixture() -> (MeltQuote, WalletSaga, ProofInfo) {
    let mut quote = test_melt_quote();
    quote.estimated_blocks = Some(6);
    quote.fee_index = Some(2);
    quote.version = 3;
    let mut local = test_proof_info(test_keyset_id(), 2048, test_mint_url());
    local.created_by_operation = Some(uuid::Uuid::new_v4());
    local.derivation_index = Some(17);
    local.proof.p2pk_e = Some(SecretKey::generate().public_key());
    local.proof.dleq = Some(crate::ProofDleq::new(
        SecretKey::generate(),
        SecretKey::generate(),
        SecretKey::generate(),
    ));
    local.proof.witness = Some(crate::nuts::Witness::P2PKWitness(
        crate::nuts::nut11::P2PKWitness {
            signatures: vec!["retained-signature".to_owned()],
        },
    ));
    let mut saga = intent(&quote, local.clone());
    let mut external = test_proof_info(test_keyset_id(), 16, test_mint_url());
    external.state = State::Reserved;
    external.used_by_operation = Some(saga.id);
    let OperationData::Melt(data) = &mut saga.data else {
        unreachable!()
    };
    data.metadata
        .insert("purpose".to_owned(), "acquisition-test".to_owned());
    data.acquisition.as_mut().unwrap().push(ProofChange {
        y: external.y,
        before: None,
        after: Some(external),
    });
    (quote, saga, local)
}
