//! Admit output metadata only while native preparation remains cancellable.
use cdk_common::database::Error;
use cdk_common::mint_url::MintUrl;
use cdk_common::wallet::{
    IssueSagaState, OperationData, ReceiveSagaState, SendSagaState, SwapSagaState, WalletSaga,
    WalletSagaState,
};
use cdk_common::Id;
use uuid::Uuid;

use super::{claim_rows, require_reservation, schema, WalletStorageClaim};
use crate::database::DatabaseExecutor;
use crate::stmt::{query, Column};
use crate::wallet::{keysets, proofs, sagas};

pub(in crate::wallet) struct OutputKeys {
    owner: String,
    preparation: Option<(i64, i64)>,
}

impl OutputKeys {
    pub(in crate::wallet) async fn begin<T>(
        conn: &T,
        id: Uuid,
        mint_url: &MintUrl,
        keyset: Id,
        proposed: Option<&WalletSaga>,
        max_bytes: Option<u32>,
    ) -> Result<Option<Self>, Error>
    where
        T: DatabaseExecutor,
    {
        if !schema::enabled(conn).await? {
            return Ok(None);
        }
        if id.is_nil() {
            return Err(Error::InvalidDbResponse);
        }
        let owner = id.to_string();
        require_reservation(conn, &owner).await?;
        let saga = sagas::load_locked(conn, &id, max_bytes, false).await?;
        let growth = match saga {
            Some(saga) => {
                if saga.mint_url != *mint_url {
                    return Err(Error::InvalidDbResponse);
                }
                match proposed {
                    Some(next) if matches!(next.data, OperationData::Melt(_)) => {
                        Some(super::melt::proposal_growth(conn, &saga, next).await?)
                    }
                    _ => preparation_growth(conn, &saga).await?,
                }
            }
            None => None,
        };
        let preparation = if let Some(growth) = growth {
            let Some(Column::Integer(reserved)) =
                query("SELECT reserved FROM wallet_storage_accounts WHERE owner=:owner")?
                    .bind("owner", owner.clone())
                    .pluck(conn)
                    .await?
            else {
                return Err(Error::InvalidDbResponse);
            };
            query("UPDATE wallet_storage_accounts SET reserved=0 WHERE owner=:owner")?
                .bind("owner", owner.clone())
                .execute(conn)
                .await?;
            Some((reserved, growth))
        } else {
            None
        };
        let key = keyset.to_string().into_bytes();
        let claims = ["keyset", "key", "keyset_counter"].map(|table| WalletStorageClaim {
            table: table.to_owned(),
            primary_key: vec![key.clone()],
        });
        claim_rows(conn, &owner, &claims).await?;
        // Materialize the fixed-width counter row without allocating a secret.
        // Later increments then require no additional logical storage.
        keysets::counter_statement(keyset)?.execute(conn).await?;
        Ok(Some(Self { owner, preparation }))
    }

    pub(in crate::wallet) async fn finish<T>(self, conn: &T) -> Result<(), Error>
    where
        T: DatabaseExecutor,
    {
        if let Some((reserved, growth)) = self.preparation {
            super::reserve_growth(conn, &self.owner, reserved, growth).await?;
        }
        Ok(())
    }
}

async fn preparation_growth<T>(conn: &T, saga: &WalletSaga) -> Result<Option<i64>, Error>
where
    T: DatabaseExecutor,
{
    if matches!((&saga.state, &saga.data),
        (WalletSagaState::Issue(IssueSagaState::SecretsPrepared), OperationData::Mint(data))
        if saga.version == 0 && data.checkpoint.is_some())
    {
        let size = super::sizing::Sizing::new(conn, saga.id.to_string()).await?;
        return super::issue::completion_growth(saga, &size).map(Some);
    }
    if matches!((&saga.state, &saga.data),
        (WalletSagaState::Swap(SwapSagaState::ProofsReserved), OperationData::Swap(data))
        if saga.version == 0 && data.checkpoint.is_some())
    {
        let size = super::sizing::Sizing::new(conn, saga.id.to_string()).await?;
        return super::swap::completion_growth(saga, &size).map(Some);
    }
    let (cancelled, proofs) = match (&saga.state, &saga.data) {
        (WalletSagaState::Send(SendSagaState::ProofsReserved), OperationData::Send(data))
            if data.preparation.is_some() && data.confirmation.is_none() =>
        {
            (WalletSagaState::Send(SendSagaState::PreparedCancelling), 0)
        }
        (WalletSagaState::Send(SendSagaState::TokenCreated), OperationData::Send(data))
            if data.preparation.is_none()
                && data.confirmation.is_none()
                && data.reclaim.is_none()
                && data.token.is_some()
                && data.proofs.is_some() =>
        {
            // Preparing a new optional reclaim has not acquired or spent its
            // token. Keep the original allowance until the exact plan is saved.
            (WalletSagaState::Send(SendSagaState::TokenCreated), 0)
        }
        (
            WalletSagaState::Receive(ReceiveSagaState::InputsRecorded),
            OperationData::Receive(data),
        ) => {
            let checkpoint = data.checkpoint.as_ref().ok_or(Error::InvalidDbResponse)?;
            if checkpoint.pre_swap.is_some() || checkpoint.outputs.is_some() {
                return Err(Error::InvalidDbResponse);
            }
            let mut growth = 0i64;
            for input in &checkpoint.inputs {
                if let Some(before) = &input.before {
                    let after = input.after.as_ref().ok_or(Error::InvalidDbResponse)?;
                    let original = super::sizing::payload(
                        &proofs::COLUMNS,
                        proofs::statement(before.clone(), false)?,
                    )?;
                    let acquired = super::sizing::payload(
                        &proofs::COLUMNS,
                        proofs::statement(after.clone(), false)?,
                    )?;
                    growth = growth
                        .checked_add((original - acquired).max(0))
                        .ok_or(Error::AmountOverflow)?;
                }
            }
            (
                WalletSagaState::Receive(ReceiveSagaState::Cancelling),
                growth,
            )
        }
        _ => return Ok(None),
    };
    let state = serde_json::to_string(&cancelled)?
        .len()
        .saturating_sub(serde_json::to_string(&saga.state)?.len());
    let growth = i64::try_from(state)
        .map_err(|_| Error::AmountOverflow)?
        .checked_add(proofs)
        .ok_or(Error::AmountOverflow)?;
    Ok(Some(growth))
}
