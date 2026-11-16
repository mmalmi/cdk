//! Keep admitted records readable, including their saved completion evidence.
use cdk_common::database::Error;
use cdk_common::wallet::{
    IssueSagaState, MeltSagaState, OperationData, ReceiveSagaState, SendSagaState, SwapSagaState,
    WalletSaga, WalletSagaState,
};

use super::{query_budget, statement, storage_capacity, COLUMNS};

pub(super) fn check(saga: &WalletSaga, limit: u32) -> Result<(), Error> {
    check_record(saga, limit)?;
    if let OperationData::Receive(data) = &saga.data {
        if let Some(saved) = &data.checkpoint {
            if saved.pre_swap.is_some() && saved.outputs.is_none() {
                check_record(
                    &storage_capacity::receive_completion_record(saga, saved)?,
                    limit,
                )?;
            }
        }
    }
    if let OperationData::Send(data) = &saga.data {
        if let Some(saved) = &data.reclaim {
            if saved.outputs.is_none() {
                check_record(
                    &storage_capacity::reclaim_completion_record(saga, saved)?,
                    limit,
                )?;
            }
        }
        if let Some(intent) = &data.confirmation {
            let mut ready = match intent.ready {
                None => {
                    let (ready, _) = storage_capacity::send_completion_record(saga, intent)?;
                    check_record(&ready, limit)?;
                    ready
                }
                Some(_) => saga.clone(),
            };
            storage_capacity::send_token_record(&mut ready, intent)?;
            check_record(&ready, limit)?;
        }
    }
    if let OperationData::Swap(data) = &saga.data {
        if let Some(saved) = &data.checkpoint {
            if saved.ready.is_none() {
                check_record(
                    &storage_capacity::swap_completion_record(saga, saved)?.0,
                    limit,
                )?;
            }
        }
    }
    if let OperationData::Melt(data) = &saga.data {
        let incomplete = match &data.payment {
            Some(payment) => payment.change.is_none(),
            None => data
                .swap
                .as_ref()
                .is_some_and(|swap| swap.outputs.is_none()),
        };
        if incomplete {
            check_record(&storage_capacity::melt_completion_record(saga)?.0, limit)?;
        }
    }
    Ok(())
}

fn check_record(saga: &WalletSaga, limit: u32) -> Result<(), Error> {
    // Mutable SQL scalars and state names must still fit when recovery advances.
    // Measure their longest encodings without cloning the private checkpoint.
    let state = match saga.state {
        WalletSagaState::Send(_) => WalletSagaState::Send(SendSagaState::PreparedCancelling),
        WalletSagaState::Receive(_) => WalletSagaState::Receive(ReceiveSagaState::InputsRecorded),
        WalletSagaState::Swap(_) => WalletSagaState::Swap(SwapSagaState::ProofsReserved),
        WalletSagaState::Melt(_) => WalletSagaState::Melt(MeltSagaState::PaymentPending),
        WalletSagaState::Issue(_) => WalletSagaState::Issue(IssueSagaState::SecretsPrepared),
    };
    let encoded = statement(saga)?
        .bind("state", serde_json::to_string(&state)?)
        .bind("updated_at", i64::MAX)
        .bind("version", i64::from(u32::MAX));
    let bytes = query_budget::encoded_payload_bytes(&COLUMNS, &encoded, 0)?;
    if bytes > u64::from(limit) {
        return Err(Error::SagaTooLarge { bytes, limit });
    }
    Ok(())
}
