//! Admit native send, receive and reclaim completion before the saved request can reach a mint.
use std::collections::HashMap;

use cdk_common::database::Error;
use cdk_common::wallet::{
    OperationData, ProofInfo, ReceiveSagaState, SendConfirmation, SendSagaState, SwapOutputs,
    Transaction, TransactionDirection, TransactionId, TransactionRole, TransactionStatus,
    WalletSaga, WalletSagaState,
};
use cdk_common::{Amount, Proof, PublicKey, State, Token};

use super::{
    claim_rows, receive, reclaim, require_reservation, schema, sizing, WalletStorageClaim,
};
use crate::database::DatabaseExecutor;
use crate::stmt::{query, Column};
use crate::wallet::{proofs, sagas, transactions};

pub(in crate::wallet) struct CompletionAdmission {
    owner: String,
    reserved: i64,
    growth: i64,
}

impl CompletionAdmission {
    pub(in crate::wallet) async fn begin<T>(
        conn: &T,
        next: &WalletSaga,
        max_bytes: Option<u32>,
        bounded_transaction_reads: bool,
    ) -> Result<Option<Self>, Error>
    where
        T: DatabaseExecutor,
    {
        let previous = match next.state {
            WalletSagaState::Send(SendSagaState::Confirming) => {
                WalletSagaState::Send(SendSagaState::ProofsReserved)
            }
            WalletSagaState::Send(SendSagaState::ReclaimPreparing) => {
                WalletSagaState::Send(SendSagaState::TokenCreated)
            }
            WalletSagaState::Receive(ReceiveSagaState::Redeeming) => {
                WalletSagaState::Receive(ReceiveSagaState::InputsRecorded)
            }
            _ => return Ok(None),
        };
        if !schema::enabled(conn).await? {
            return Ok(None);
        }
        let Some(current) = sagas::load_locked(conn, &next.id, max_bytes, false).await? else {
            return Ok(None);
        };
        if current.version.checked_add(1) != Some(next.version) {
            return Ok(None);
        }
        if current.state != previous {
            return Ok(None);
        }
        if current.mint_url != next.mint_url
            || current.unit != next.unit
            || current.amount != next.amount
            || current.created_at != next.created_at
            || current.kind != next.kind
            || current.quote_id != next.quote_id
        {
            return Err(Error::InvalidDbResponse);
        }
        let expected = match (&current.data, &next.data) {
            (OperationData::Send(before), OperationData::Send(after))
                if next.state == WalletSagaState::Send(SendSagaState::ReclaimPreparing) =>
            {
                reclaim::inputs(before, after)?
            }
            (OperationData::Send(before), OperationData::Send(after)) => {
                if after.preparation.is_some()
                    || after.reclaim.is_some()
                    || after.token.is_some()
                    || after.proofs.is_some()
                    || after
                        .confirmation
                        .as_ref()
                        .is_none_or(|intent| intent.ready.is_some())
                {
                    return Err(Error::InvalidDbResponse);
                }
                before
                    .preparation
                    .as_ref()
                    .ok_or(Error::InvalidDbResponse)?
                    .inputs
                    .iter()
                    .collect::<Vec<_>>()
            }
            (OperationData::Receive(before), OperationData::Receive(after)) => {
                receive::inputs(before, after)?
            }
            _ => return Err(Error::InvalidDbResponse),
        };
        let mut inputs: HashMap<PublicKey, &ProofInfo> =
            expected.iter().map(|proof| (proof.y, *proof)).collect();
        if inputs.len() != expected.len()
            || expected
                .iter()
                .any(|proof| proof.used_by_operation != Some(next.id))
            || !proofs::matches(
                conn,
                expected
                    .iter()
                    .map(|proof| (proof.y, Some(*proof)))
                    .collect(),
            )
            .await?
        {
            return Err(Error::ConcurrentUpdate);
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
        let size = sizing::Sizing::new(conn, owner.clone()).await?;
        let growth = match &next.data {
            OperationData::Send(data)
                if next.state == WalletSagaState::Send(SendSagaState::ReclaimPreparing) =>
            {
                reclaim::completion_growth(
                    conn,
                    next,
                    data.reclaim.as_deref().ok_or(Error::InvalidDbResponse)?,
                    &size,
                    bounded_transaction_reads,
                )
                .await?
            }
            OperationData::Send(data) => completion_growth(
                next,
                data.confirmation.as_ref().ok_or(Error::InvalidDbResponse)?,
                &mut inputs,
                &size,
            )?,
            OperationData::Receive(data) => receive::completion_growth(
                next,
                data.checkpoint.as_deref().ok_or(Error::InvalidDbResponse)?,
                &size,
            )?,
            _ => return Err(Error::InvalidDbResponse),
        };
        query("UPDATE wallet_storage_accounts SET reserved=0 WHERE owner=:owner")?
            .bind("owner", owner.clone())
            .execute(conn)
            .await?;
        let mut claims = expected
            .iter()
            .map(|p| WalletStorageClaim {
                table: "proof".to_owned(),
                primary_key: vec![p.y.to_bytes().to_vec()],
            })
            .collect::<Vec<_>>();
        if next.state == WalletSagaState::Send(SendSagaState::ReclaimPreparing) {
            claims.push(WalletStorageClaim {
                table: "transactions".to_owned(),
                primary_key: vec![TransactionId::from_saga_id(next.id).as_slice().to_vec()],
            });
        }
        claim_rows(conn, &owner, &claims).await?;
        Ok(Some(Self {
            owner,
            reserved,
            growth,
        }))
    }

    pub(in crate::wallet) async fn finish<T>(self, conn: &T) -> Result<(), Error>
    where
        T: DatabaseExecutor,
    {
        super::reserve_growth(conn, &self.owner, self.reserved, self.growth).await?;
        Ok(())
    }
}

fn add(total: &mut i64, bytes: i64) -> Result<(), Error> {
    *total = total.checked_add(bytes).ok_or(Error::AmountOverflow)?;
    Ok(())
}

fn proof_payload(info: ProofInfo) -> Result<i64, Error> {
    sizing::payload(&proofs::COLUMNS, proofs::statement(info, false)?)
}

fn completion_growth(
    saga: &WalletSaga,
    intent: &SendConfirmation,
    inputs: &mut HashMap<PublicKey, &ProofInfo>,
    size: &sizing::Sizing,
) -> Result<i64, Error> {
    let mut growth = 0;
    for outgoing in &intent.retained_proofs {
        let original = inputs
            .remove(&outgoing.y()?)
            .ok_or(Error::InvalidDbResponse)?;
        let old = proof_payload(original.clone())?;
        let mut pending = original.clone();
        pending.state = State::PendingSpent;
        pending.used_by_operation = Some(saga.id);
        // Native import keeps local DLEQ/P2BK evidence and the larger verified
        // witness. Measure both legal alternatives using the production encoder.
        let retained = proof_payload(pending.clone())?;
        pending.proof.witness = outgoing.witness.clone();
        add(
            &mut growth,
            retained
                .max(proof_payload(pending)?)
                .saturating_sub(old)
                .max(0),
        )?;
    }
    let (mut ready, generated) = completion_record(saga, intent)?;
    for info in generated {
        add(
            &mut growth,
            size.charge(
                "proof",
                info.y.to_bytes().to_vec(),
                &proofs::COLUMNS,
                proofs::statement(info, false)?,
            )?,
        )?;
    }
    let OperationData::Send(data) = &ready.data else {
        return Err(Error::InvalidDbResponse);
    };
    let outputs = &data
        .confirmation
        .as_ref()
        .and_then(|intent| intent.ready.as_ref())
        .ok_or(Error::InvalidDbResponse)?
        .send;
    // Treat every generated output as sent. Actual change is a subset with
    // smaller proof rows, and cannot make the token or receipt larger.
    let receipt = Transaction {
        role: TransactionRole::Primary,
        mint_url: saga.mint_url.clone(),
        direction: TransactionDirection::Outgoing,
        amount: saga.amount,
        fee: intent
            .pre_swap
            .as_ref()
            .map_or(Amount::ZERO, |p| p.fee)
            .checked_add(intent.send_fee)
            .ok_or(Error::AmountOverflow)?,
        unit: saga.unit.clone(),
        ys: outputs.iter().map(Proof::y).collect::<Result<_, _>>()?,
        timestamp: saga.created_at,
        memo: intent.token_memo.clone(),
        metadata: intent.metadata.clone(),
        quote_id: None,
        payment_request: None,
        payment_proof: None,
        payment_method: None,
        saga_id: Some(saga.id),
        // Include the longer terminal status in the retained receipt allowance.
        status: TransactionStatus::Completed,
    };
    add(
        &mut growth,
        size.charge(
            "transactions",
            receipt.id().as_slice().to_vec(),
            &transactions::COLUMNS,
            transactions::statement(receipt)?,
        )?,
    )?;
    let ready_bytes = sizing::payload(&sagas::COLUMNS, sagas::statement(&ready)?)?;
    token_record(&mut ready, intent)?;
    let token_bytes = sizing::payload(&sagas::COLUMNS, sagas::statement(&ready)?)?;
    let initial = sizing::payload(&sagas::COLUMNS, sagas::statement(saga)?)?;
    add(
        &mut growth,
        ready_bytes.max(token_bytes).saturating_sub(initial).max(0),
    )?;
    Ok(growth)
}

pub(in crate::wallet) fn completion_record(
    saga: &WalletSaga,
    intent: &SendConfirmation,
) -> Result<(WalletSaga, Vec<ProofInfo>), Error> {
    let mut generated = Vec::new();
    if let Some(swap) = &intent.pre_swap {
        for secret in &swap.pre_mint_secrets.secrets {
            let mut info = sizing::output(saga, secret, State::PendingSpent)?;
            info.used_by_operation = Some(saga.id);
            generated.push(info);
        }
    }
    let mut outputs = intent.retained_proofs.clone();
    outputs.extend(generated.iter().map(|info| info.proof.clone()));
    let mut ready = saga.clone();
    let OperationData::Send(data) = &mut ready.data else {
        return Err(Error::InvalidDbResponse);
    };
    data.confirmation
        .as_mut()
        .ok_or(Error::InvalidDbResponse)?
        .ready = Some(SwapOutputs {
        send: outputs,
        change: Vec::new(),
    });
    Ok((ready, generated))
}

pub(in crate::wallet) fn token_record(
    ready: &mut WalletSaga,
    intent: &SendConfirmation,
) -> Result<(), Error> {
    let OperationData::Send(data) = &mut ready.data else {
        return Err(Error::InvalidDbResponse);
    };
    let outputs = data
        .confirmation
        .take()
        .and_then(|intent| intent.ready)
        .ok_or(Error::InvalidDbResponse)?
        .send;
    data.token = Some(
        Token::new(
            ready.mint_url.clone(),
            outputs.clone(),
            intent.token_memo.clone(),
            ready.unit.clone(),
        )
        .to_string(),
    );
    data.proofs = Some(outputs);
    ready.state = WalletSagaState::Send(SendSagaState::TokenCreated);
    Ok(())
}
