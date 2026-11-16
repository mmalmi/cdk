//! Admit each withdrawal stage before its exact request can reach the mint.
use cdk_common::database::wallet::{validate_melt_inputs, MAX_MELT_PAYMENT_PROOF_BYTES};
use cdk_common::database::Error;
use cdk_common::secret::Secret;
use cdk_common::wallet::{
    MeltSagaState, OperationData, ProofInfo, SwapOutputs, TransactionStatus, WalletSaga,
    WalletSagaState,
};
use cdk_common::{Amount, PreMint, SecretKey, State};

use super::{preparation, require_reservation, reserve_growth, schema, sizing, WalletStorageClaim};
use crate::database::DatabaseExecutor;
use crate::stmt::{query, Column};
use crate::wallet::{proofs, sagas, transactions};

pub(in crate::wallet) struct MeltAcquisition {
    owner: String,
    growth: i64,
}

impl MeltAcquisition {
    /// The caller has locked the original quote and checked a fresh operation ID.
    pub(in crate::wallet) async fn begin<T>(
        conn: &T,
        saga: &WalletSaga,
    ) -> Result<Option<Self>, Error>
    where
        T: DatabaseExecutor,
    {
        if !schema::enabled(conn).await? {
            return Ok(None);
        }
        let inputs = validate_melt_inputs(saga)?;
        let mut claims: Vec<_> = inputs
            .iter()
            .map(|input| WalletStorageClaim {
                table: "proof".to_owned(),
                primary_key: vec![input.y.to_bytes().to_vec()],
            })
            .collect();
        claims.push(WalletStorageClaim {
            table: "melt_quote".to_owned(),
            primary_key: vec![saga
                .quote_id
                .as_ref()
                .ok_or(Error::InvalidDbResponse)?
                .as_bytes()
                .to_vec()],
        });
        preparation::insert_initial(conn, saga, &claims).await?;
        let mut cancelled = saga.clone();
        cancelled.state = WalletSagaState::Melt(MeltSagaState::SwapCancelling);
        let growth = (saga_bytes(&cancelled)? - saga_bytes(saga)?).max(0);
        let owner = saga.id.to_string();
        // The same transaction measures acquisition, then restores an allowance.
        // Releasing Reserved/Pending inputs and quote ownership only shrinks rows.
        query("UPDATE wallet_storage_accounts SET reserved=0 WHERE owner=:owner")?
            .bind("owner", owner.clone())
            .execute(conn)
            .await?;
        Ok(Some(Self { owner, growth }))
    }

    pub(in crate::wallet) async fn finish<T>(self, conn: &T) -> Result<(), Error>
    where
        T: DatabaseExecutor,
    {
        reserve_growth(conn, &self.owner, 0, self.growth).await
    }
}

/// The no-change payment path has no output-key preparation transaction.
pub(in crate::wallet) async fn admit_melt_payment<T>(
    conn: &T,
    current: &WalletSaga,
    next: &WalletSaga,
) -> Result<(), Error>
where
    T: DatabaseExecutor,
{
    if !matches!(
        next.state,
        WalletSagaState::Melt(MeltSagaState::MeltRequested)
    ) || !schema::enabled(conn).await?
    {
        return Ok(());
    }
    let owner = next.id.to_string();
    require_reservation(conn, &owner).await?;
    let Some(Column::Integer(reserved)) =
        query("SELECT reserved FROM wallet_storage_accounts WHERE owner=:owner")?
            .bind("owner", owner.clone())
            .pluck(conn)
            .await?
    else {
        return Err(Error::InvalidDbResponse);
    };
    let growth = proposal_growth(conn, current, next).await?;
    reserve_growth(conn, &owner, reserved, growth).await
}

pub(super) async fn proposal_growth<T>(
    conn: &T,
    current: &WalletSaga,
    next: &WalletSaga,
) -> Result<i64, Error>
where
    T: DatabaseExecutor,
{
    let (OperationData::Melt(before), OperationData::Melt(after)) = (&current.data, &next.data)
    else {
        return Err(Error::InvalidDbResponse);
    };
    if current.state != WalletSagaState::Melt(MeltSagaState::ProofsReserved)
        || before.acquisition != after.acquisition
        || before.payment.is_some()
        || before.metadata != after.metadata
    {
        return Err(Error::InvalidDbResponse);
    }
    match next.state {
        WalletSagaState::Melt(MeltSagaState::ProofsReserved)
            if before.swap.is_none() && after.payment.is_none() => {}
        WalletSagaState::Melt(MeltSagaState::MeltRequested)
            if before.swap == after.swap && after.payment.is_some() => {}
        _ => return Err(Error::InvalidDbResponse),
    }
    let size = sizing::Sizing::new(conn, next.id.to_string()).await?;
    let (peak, outputs) = completion_record(next)?;
    let mut growth = 0i64;
    for output in outputs {
        add(
            &mut growth,
            size.charge(
                "proof",
                output.y.to_bytes().to_vec(),
                &proofs::COLUMNS,
                proofs::statement(output, false)?,
            )?,
        )?;
    }
    let OperationData::Melt(data) = &peak.data else {
        return Err(Error::InvalidDbResponse);
    };
    if let Some(payment) = &data.payment {
        add(
            &mut growth,
            size.charge(
                "transactions",
                payment.receipt.id().as_slice().to_vec(),
                &transactions::COLUMNS,
                transactions::statement(payment.receipt.clone())?,
            )?,
        )?;
        // The quote keeps one raw copy; its terminal state and released owner shrink.
        add(&mut growth, MAX_MELT_PAYMENT_PROOF_BYTES as i64)?;
    }
    add(
        &mut growth,
        (saga_bytes(&peak)? - saga_bytes(current)?).max(0),
    )?;
    Ok(growth)
}

pub(in crate::wallet) fn completion_record(
    saga: &WalletSaga,
) -> Result<(WalletSaga, Vec<ProofInfo>), Error> {
    let mut peak = saga.clone();
    let OperationData::Melt(data) = &mut peak.data else {
        return Err(Error::InvalidDbResponse);
    };
    let mut outputs = Vec::new();
    match &mut data.payment {
        Some(payment) => {
            if payment.change.is_some()
                || payment.receipt.status != TransactionStatus::Pending
                || payment.receipt.payment_proof.is_some()
            {
                return Err(Error::InvalidDbResponse);
            }
            for message in data.change_blinded_messages.iter().flatten() {
                // NUT-13 change has 64 hex secret bytes and fixed-width crypto.
                // Full-width counters and the total change bound cover every slot.
                let secret = PreMint {
                    blinded_message: message.clone(),
                    secret: Secret::new("f".repeat(64)),
                    r: SecretKey::from_slice(&[1; 32])?,
                    amount: data.change_amount.ok_or(Error::InvalidDbResponse)?,
                    derivation_index: Some(u32::MAX),
                };
                outputs.push(sizing::output(saga, &secret, State::Unspent)?);
            }
            payment.change = Some(outputs.clone());
            payment.receipt.status = TransactionStatus::Completed;
            payment.receipt.fee = Amount::try_sum(payment.inputs.iter().map(|p| p.proof.amount))
                .map_err(|_| Error::AmountOverflow)?;
            // NUL has the maximum JSON expansion (six bytes per UTF-8 byte).
            // This placeholder is only measured, never persisted or transmitted.
            payment.receipt.payment_proof = Some("\0".repeat(MAX_MELT_PAYMENT_PROOF_BYTES));
            peak.state = WalletSagaState::Melt(MeltSagaState::PaymentPending);
        }
        None => {
            let swap = data.swap.as_mut().ok_or(Error::InvalidDbResponse)?;
            if swap.outputs.is_some() {
                return Err(Error::InvalidDbResponse);
            }
            for secret in &swap.pre_swap.pre_mint_secrets.secrets {
                let mut output = sizing::output(saga, secret, State::Reserved)?;
                output.used_by_operation = Some(saga.id);
                outputs.push(output);
            }
            // The partition changes neither the number nor encoding of proofs.
            swap.outputs = Some(SwapOutputs {
                send: outputs.iter().map(|info| info.proof.clone()).collect(),
                change: Vec::new(),
            });
            peak.state = WalletSagaState::Melt(MeltSagaState::SwapCancelling);
        }
    }
    Ok((peak, outputs))
}

fn saga_bytes(saga: &WalletSaga) -> Result<i64, Error> {
    sizing::payload(&sagas::COLUMNS, sagas::statement(saga)?)
}

fn add(total: &mut i64, bytes: i64) -> Result<(), Error> {
    *total = total.checked_add(bytes).ok_or(Error::AmountOverflow)?;
    Ok(())
}
