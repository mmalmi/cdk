//! Save the exact acquisition intent before reserving any send input.
use cdk_common::database::wallet::ProofChange;
use cdk_common::wallet::{
    OperationData, SendOperationData, SendPreparation, SendSagaState, WalletSaga, WalletSagaState,
};

use super::{cancellation, state, SendOptions};
use crate::nuts::{Proof, State};
use crate::{Amount, Error, Wallet};

pub(super) async fn acquire<'a, I>(
    wallet: &Wallet,
    operation_id: uuid::Uuid,
    amount: Amount,
    options: &SendOptions,
    proofs: I,
) -> Result<WalletSaga, Error>
where
    I: Iterator<Item = &'a Proof>,
{
    let proofs: Vec<_> = proofs.collect();
    let ys = proofs.iter().map(|p| p.y()).collect::<Result<_, _>>()?;
    let mut inputs = wallet.localstore.get_proofs_by_ys(ys).await?;
    let mut changes = Vec::with_capacity(inputs.len());
    for input in &mut inputs {
        if input.state != State::Unspent || input.used_by_operation.is_some() {
            return Err(Error::ConcurrentUpdate);
        }
        let before = input.clone();
        input.state = State::Reserved;
        input.used_by_operation = Some(operation_id);
        changes.push(ProofChange {
            y: input.y,
            before: Some(before),
            after: Some(input.clone()),
        });
    }
    let mut saga = WalletSaga::new(
        operation_id,
        WalletSagaState::Send(SendSagaState::PreparedAcquiring),
        amount,
        wallet.mint_url.clone(),
        wallet.unit.clone(),
        OperationData::Send(SendOperationData {
            preparation: Some(Box::new(SendPreparation {
                inputs,
                metadata: options.metadata.clone(),
                retain_cancellation: options.retain_cancellation,
            })),
            confirmation: None,
            reclaim: None,
            amount,
            memo: options.memo.as_ref().map(|m| m.memo.clone()),
            counter_start: None,
            counter_end: None,
            token: None,
            proofs: None,
        }),
    );
    cancellation::validate(wallet, &saga, amount, proofs.into_iter())?;
    // Fresh intents start at version zero, leaving room for promotion and both
    // cancellation transitions. An uncertain write keeps recovery responsible;
    // never reinsert the same ID or release inputs without its saved fence.
    wallet.localstore.add_saga(saga.clone()).await?;
    if !wallet
        .localstore
        .compare_and_swap_proofs(changes, Some(&saga))
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    state::transition(wallet, &mut saga, SendSagaState::ProofsReserved).await?;
    Ok(saga)
}
