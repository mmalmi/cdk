//! Reserve optional reclaim before acquiring its original token records.
use cdk_common::database::Error;
use cdk_common::wallet::{
    OperationData, ProofInfo, SendOperationData, SendReclaim, SendSagaState, TransactionDirection,
    TransactionId, TransactionRole, TransactionStatus, WalletSaga, WalletSagaState,
};
use cdk_common::State;

use super::sizing;
use crate::database::DatabaseExecutor;
use crate::pool::DatabasePool;
use crate::wallet::{proofs, sagas, transactions, SQLWalletDatabase};

pub(super) fn inputs<'a>(
    before: &SendOperationData,
    after: &'a SendOperationData,
) -> Result<Vec<&'a ProofInfo>, Error> {
    if before.preparation.is_some()
        || before.confirmation.is_some()
        || before.reclaim.is_some()
        || before.token.is_none()
        || before.proofs.is_none()
    {
        return Err(Error::InvalidDbResponse);
    }
    let reclaim = after.reclaim.as_deref().ok_or(Error::InvalidDbResponse)?;
    if reclaim.outputs.is_some() || reclaim.inputs.is_empty() {
        return Err(Error::InvalidDbResponse);
    }
    // The original token, fees and recovery identity must remain unchanged.
    let mut expected = before.clone();
    expected.reclaim = after.reclaim.clone();
    if &expected != after {
        return Err(Error::InvalidDbResponse);
    }
    Ok(reclaim.inputs.iter().collect())
}

pub(super) async fn completion_growth<T>(
    conn: &T,
    saga: &WalletSaga,
    saved: &SendReclaim,
    size: &sizing::Sizing,
    bounded_transaction_reads: bool,
) -> Result<i64, Error>
where
    T: DatabaseExecutor,
{
    // Terminal reclaim needs this exact original outgoing receipt too. Reject
    // missing or conflicting evidence before any refund can spend at the mint.
    let mut primary = transactions::load(
        conn,
        TransactionId::from_saga_id(saga.id),
        bounded_transaction_reads,
    )
    .await?
    .ok_or(Error::InvalidDbResponse)?;
    if primary.role != TransactionRole::Primary
        || primary.direction != TransactionDirection::Outgoing
        || primary.saga_id != Some(saga.id)
        || primary.mint_url != saga.mint_url
        || primary.unit != saga.unit
        || primary.amount != saga.amount
        || primary.timestamp != saga.created_at
    {
        return Err(Error::InvalidDbResponse);
    }
    let before = sizing::payload(
        &transactions::COLUMNS,
        transactions::statement(primary.clone())?,
    )?;
    primary.status = TransactionStatus::Failed;
    let after = sizing::payload(&transactions::COLUMNS, transactions::statement(primary)?)?;
    after
        .saturating_sub(before)
        .max(0)
        .checked_add(record_growth(saga, saved, size)?)
        .ok_or(Error::AmountOverflow)
}

fn record_growth(
    saga: &WalletSaga,
    saved: &SendReclaim,
    size: &sizing::Sizing,
) -> Result<i64, Error> {
    let mut growth = 0i64;
    let mut add = |bytes| {
        growth = growth.checked_add(bytes).ok_or(Error::AmountOverflow)?;
        Ok::<_, Error>(())
    };
    for input in &saved.inputs {
        // Pending acquisition and Spent handoff only shorten the stored state.
        // The original PendingSpent row also covers exact cancellation.
        if input.state != State::PendingSpent {
            return Err(Error::InvalidDbResponse);
        }
    }
    let ready = completion_record(saga, saved)?;
    let OperationData::Send(data) = &ready.data else {
        return Err(Error::InvalidDbResponse);
    };
    for info in data
        .reclaim
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
    let initial = sizing::payload(&sagas::COLUMNS, sagas::statement(saga)?)?;
    let mut cancelled = saga.clone();
    cancelled.state = WalletSagaState::Send(SendSagaState::ReclaimCancelling);
    let cancelled = sizing::payload(&sagas::COLUMNS, sagas::statement(&cancelled)?)?;
    let ready = sizing::payload(&sagas::COLUMNS, sagas::statement(&ready)?)?;
    add(ready.max(cancelled).saturating_sub(initial).max(0))?;
    Ok(growth)
}

pub(in crate::wallet) fn completion_record(
    saga: &WalletSaga,
    saved: &SendReclaim,
) -> Result<WalletSaga, Error> {
    let outputs = saved
        .pre_swap
        .pre_mint_secrets
        .secrets
        .iter()
        .map(|secret| sizing::output(saga, secret, State::Unspent))
        .collect::<Result<_, _>>()?;
    let mut ready = saga.clone();
    let OperationData::Send(data) = &mut ready.data else {
        return Err(Error::InvalidDbResponse);
    };
    data.reclaim
        .as_mut()
        .ok_or(Error::InvalidDbResponse)?
        .outputs = Some(outputs);
    // Both completion states retain the original token and all refund outputs.
    ready.state = WalletSagaState::Send(SendSagaState::Reclaiming);
    Ok(ready)
}

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    /// Additional storage for a future reclaim of an already-admitted send.
    ///
    /// Measures input-row attribution, the saved request, maximal signed output
    /// rows, refund receipt and largest recovery checkpoint with the production
    /// encoders. Existing send data and its reservation must remain retained.
    /// Output-key metadata and any external journals need separate allowances.
    ///
    /// This read-only helper grants no financial authority and checks no mint
    /// state. The caller must supply inputs and an output plan that bound every
    /// future reclaim it admits, including variable secrets and witnesses.
    #[tracing::instrument(skip_all)]
    pub async fn send_reclaim_storage_bytes(
        &self,
        send: &WalletSaga,
        reclaim: &SendReclaim,
    ) -> Result<u64, Error> {
        let OperationData::Send(data) = &send.data else {
            return Err(Error::InvalidDbResponse);
        };
        if send.state != WalletSagaState::Send(SendSagaState::TokenCreated)
            || data.preparation.is_some()
            || data.confirmation.is_some()
            || data.reclaim.is_some()
            || reclaim.outputs.is_some()
        {
            return Err(Error::InvalidDbResponse);
        }
        let size = self.record_sizing(send.id).await?;
        let before = sizing::payload(&sagas::COLUMNS, sagas::statement(send)?)?;
        let mut future = send.clone();
        future.state = WalletSagaState::Send(SendSagaState::ReclaimPreparing);
        let OperationData::Send(data) = &mut future.data else {
            return Err(Error::InvalidDbResponse);
        };
        data.reclaim = Some(Box::new(reclaim.clone()));
        let after = sizing::payload(&sagas::COLUMNS, sagas::statement(&future)?)?;
        let mut bytes = after
            .saturating_sub(before)
            .max(0)
            .checked_add(record_growth(&future, reclaim, &size)?)
            .ok_or(Error::AmountOverflow)?;
        // Include whole input claims even if their existing storage allowance
        // remains charged to another owner. No coin ownership changes here.
        for input in &reclaim.inputs {
            bytes = bytes
                .checked_add(size.charge(
                    "proof",
                    input.y.to_bytes().to_vec(),
                    &proofs::COLUMNS,
                    proofs::statement(input.clone(), false)?,
                )?)
                .ok_or(Error::AmountOverflow)?;
        }
        // The only primary-receipt mutation is status=Failed. Charging its full
        // encoded value bounds growth without depending on the prior status.
        bytes = bytes
            .checked_add(TransactionStatus::Failed.to_string().len() as i64)
            .ok_or(Error::AmountOverflow)?;
        u64::try_from(bytes).map_err(|_| Error::AmountOverflow)
    }
}
