//! Admit a standalone swap before acquiring inputs or submitting its exact request.
use cdk_common::database::wallet::validate_proof_changes;
use cdk_common::database::Error;
use cdk_common::wallet::{
    OperationData, ProofInfo, SwapCheckpoint, SwapOutputs, SwapSagaState, WalletSaga,
    WalletSagaState,
};
use cdk_common::State;

use super::{preparation, schema, sizing, WalletStorageClaim};
use crate::database::DatabaseExecutor;
use crate::wallet::{proofs, sagas};

fn checkpoint(saga: &WalletSaga) -> Result<&SwapCheckpoint, Error> {
    let OperationData::Swap(data) = &saga.data else {
        return Err(Error::InvalidDbResponse);
    };
    let saved = data.checkpoint.as_deref().ok_or(Error::InvalidDbResponse)?;
    if saga.id.is_nil()
        || saga.version != 0
        || saga.state != WalletSagaState::Swap(SwapSagaState::ProofsReserved)
        || saved.ready.is_some()
        || saved.acquisition.is_empty()
    {
        return Err(Error::InvalidDbResponse);
    }
    Ok(saved)
}

/// The caller commits this only with its keys, counter and final allowance.
pub(in crate::wallet) async fn insert_initial_swap<T>(
    conn: &T,
    saga: &WalletSaga,
) -> Result<bool, Error>
where
    T: DatabaseExecutor,
{
    if !schema::enabled(conn).await? {
        return Ok(false);
    }
    let saved = checkpoint(saga)?;
    validate_proof_changes(&saved.acquisition)?;
    if !proofs::matches(
        conn,
        saved
            .acquisition
            .iter()
            .map(|input| (input.y, input.before.as_ref()))
            .collect(),
    )
    .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    let mut claims = Vec::with_capacity(saved.acquisition.len());
    for input in &saved.acquisition {
        let before = input.before.as_ref().ok_or(Error::InvalidDbResponse)?;
        let mut expected = before.clone();
        expected.state = State::Reserved;
        expected.used_by_operation = Some(saga.id);
        if before.state != State::Unspent
            || before.used_by_operation.is_some()
            || before.mint_url != saga.mint_url
            || before.unit != saga.unit
            || input.after.as_ref() != Some(&expected)
        {
            return Err(Error::InvalidDbResponse);
        }
        claims.push(WalletStorageClaim {
            table: "proof".to_owned(),
            primary_key: vec![input.y.to_bytes().to_vec()],
        });
    }
    preparation::insert_initial(conn, saga, &claims).await?;
    Ok(true)
}

pub(super) fn completion_growth(saga: &WalletSaga, size: &sizing::Sizing) -> Result<i64, Error> {
    let saved = checkpoint(saga)?;
    let mut growth = 0i64;
    let mut add = |bytes| {
        growth = growth.checked_add(bytes).ok_or(Error::AmountOverflow)?;
        Ok::<_, Error>(())
    };
    for input in &saved.acquisition {
        let before = input.before.as_ref().ok_or(Error::InvalidDbResponse)?;
        let after = input.after.as_ref().ok_or(Error::InvalidDbResponse)?;
        let original =
            sizing::payload(&proofs::COLUMNS, proofs::statement(before.clone(), false)?)?;
        let acquired = sizing::payload(&proofs::COLUMNS, proofs::statement(after.clone(), false)?)?;
        // Spent inputs keep the acquired ownership and have a shorter state.
        // Reserve each row's peak independently so either handoff order fits.
        add((acquired - original).max(0))?;
    }
    let (ready, outputs) = completion_record(saga, saved)?;
    for info in outputs {
        add(size.charge(
            "proof",
            info.y.to_bytes().to_vec(),
            &proofs::COLUMNS,
            proofs::statement(info, false)?,
        )?)?;
    }
    let initial = sizing::payload(&sagas::COLUMNS, sagas::statement(saga)?)?;
    let peak = sizing::payload(&sagas::COLUMNS, sagas::statement(&ready)?)?;
    // Both cancellation and request submission have shorter state encodings
    // than ProofsReserved and retain the same checkpoint until outputs arrive.
    add((peak - initial).max(0))?;
    Ok(growth)
}

/// Project both the recovery record and the independently stored output rows.
pub(in crate::wallet) fn completion_record(
    saga: &WalletSaga,
    saved: &SwapCheckpoint,
) -> Result<(WalletSaga, Vec<ProofInfo>), Error> {
    let outputs = saved
        .pre_swap
        .pre_mint_secrets
        .secrets
        .iter()
        .map(|secret| sizing::output(saga, secret, State::Reserved))
        .collect::<Result<Vec<_>, _>>()?;
    let mut ready = saga.clone();
    let OperationData::Swap(data) = &mut ready.data else {
        return Err(Error::InvalidDbResponse);
    };
    data.checkpoint
        .as_mut()
        .ok_or(Error::InvalidDbResponse)?
        .ready = Some(SwapOutputs {
        send: outputs.iter().map(|info| info.proof.clone()).collect(),
        change: Vec::new(),
    });
    ready.state = WalletSagaState::Swap(SwapSagaState::OutputsReady);
    Ok((ready, outputs))
}
