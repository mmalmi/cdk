//! Bound receive handoff using its exact acquired inputs and saved mint request.
use cdk_common::database::Error;
use cdk_common::wallet::{
    OperationData, ProofInfo, ReceiveCheckpoint, ReceiveOperationData, ReceiveSagaState,
    WalletSaga, WalletSagaState,
};
use cdk_common::State;

use super::sizing;
use crate::wallet::{proofs, sagas, transactions};

pub(super) fn inputs<'a>(
    before: &'a ReceiveOperationData,
    after: &ReceiveOperationData,
) -> Result<Vec<&'a ProofInfo>, Error> {
    let before = before
        .checkpoint
        .as_deref()
        .ok_or(Error::InvalidDbResponse)?;
    let after = after
        .checkpoint
        .as_deref()
        .ok_or(Error::InvalidDbResponse)?;
    if before.pre_swap.is_some()
        || before.outputs.is_some()
        || after.pre_swap.is_none()
        || after.outputs.is_some()
    {
        return Err(Error::InvalidDbResponse);
    }
    // Only the newly signed request and its resolved amount/fee may change.
    let mut expected = before.clone();
    expected.pre_swap = after.pre_swap.clone();
    expected.transaction.amount = after.transaction.amount;
    expected.transaction.fee = after.transaction.fee;
    if &expected != after {
        return Err(Error::InvalidDbResponse);
    }
    before
        .inputs
        .iter()
        .map(|input| input.after.as_ref().ok_or(Error::InvalidDbResponse))
        .collect()
}

pub(super) fn completion_growth(
    saga: &WalletSaga,
    saved: &ReceiveCheckpoint,
    size: &sizing::Sizing,
) -> Result<i64, Error> {
    let mut growth = 0i64;
    let mut add = |bytes| {
        growth = growth.checked_add(bytes).ok_or(Error::AmountOverflow)?;
        Ok::<_, Error>(())
    };
    for input in &saved.inputs {
        let acquired = input.after.as_ref().ok_or(Error::InvalidDbResponse)?;
        let mut spent = input.before.as_ref().unwrap_or(acquired).clone();
        spent.state = State::Spent;
        let after = sizing::payload(&proofs::COLUMNS, proofs::statement(spent, false)?)?;
        let before = sizing::payload(
            &proofs::COLUMNS,
            proofs::statement(acquired.clone(), false)?,
        )?;
        // Preserve each original custody record, including intermediate peaks
        // when one input grows before another shrinks during proof handoff.
        add(after.saturating_sub(before).max(0))?;
    }
    let ready = completion_record(saga, saved)?;
    let OperationData::Receive(data) = &ready.data else {
        return Err(Error::InvalidDbResponse);
    };
    for info in data
        .checkpoint
        .as_ref()
        .and_then(|saved| saved.outputs.as_ref())
        .ok_or(Error::InvalidDbResponse)?
    {
        add(size.charge(
            "proof",
            info.y.to_bytes().to_vec(),
            &proofs::COLUMNS,
            proofs::statement(info.clone(), false)?,
        )?)?;
    }
    let receipt = &saved.transaction;
    add(size.charge(
        "transactions",
        receipt.id().as_slice().to_vec(),
        &transactions::COLUMNS,
        transactions::statement(receipt.clone())?,
    )?)?;
    let peak = sizing::payload(&sagas::COLUMNS, sagas::statement(&ready)?)?;
    let initial = sizing::payload(&sagas::COLUMNS, sagas::statement(saga)?)?;
    add(peak.saturating_sub(initial).max(0))?;
    Ok(growth)
}

/// Share the same future record between storage admission and recovery limits.
pub(in crate::wallet) fn completion_record(
    saga: &WalletSaga,
    saved: &ReceiveCheckpoint,
) -> Result<WalletSaga, Error> {
    let swap = saved.pre_swap.as_ref().ok_or(Error::InvalidDbResponse)?;
    let outputs = swap
        .pre_mint_secrets
        .secrets
        .iter()
        .map(|secret| sizing::output(saga, secret, State::Unspent))
        .collect::<Result<_, _>>()?;
    let mut ready = saga.clone();
    let OperationData::Receive(data) = &mut ready.data else {
        return Err(Error::InvalidDbResponse);
    };
    data.checkpoint
        .as_mut()
        .ok_or(Error::InvalidDbResponse)?
        .outputs = Some(outputs);
    // OutputsReady is longer than both Redeeming and Completed. Its checkpoint
    // survives proof import and receipt insertion, so reserve that combined peak.
    ready.state = WalletSagaState::Receive(ReceiveSagaState::OutputsReady);
    Ok(ready)
}
