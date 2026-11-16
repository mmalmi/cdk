//! A retained completed receipt is the authority for returning an original result.
use cdk_common::wallet::{receive_identity, ReceiveSagaState, TransactionId, WalletSagaState};

use crate::nuts::{Proofs, ProofsMethods};
use crate::{Amount, Error, Wallet};

pub(super) async fn completed(wallet: &Wallet, proofs: &Proofs) -> Result<Option<Amount>, Error> {
    let fingerprint = receive_identity::fingerprint(&wallet.mint_url, &wallet.unit, proofs)?;
    let id = TransactionId::from_receive_fingerprint(&fingerprint);
    let Some(receipt) = wallet.localstore.get_transaction(id).await? else {
        return Ok(None);
    };
    let mut expected = proofs.ys()?;
    let mut received = receipt.ys.clone();
    expected.sort_unstable();
    received.sort_unstable();
    let operation = receipt.saga_id.ok_or(Error::InvalidOperationState)?;
    if receipt.id() != id
        || receipt.validate_role().is_err()
        || receipt.mint_url != wallet.mint_url
        || receipt.unit != wallet.unit
        || receipt.metadata.get(receive_identity::METADATA_KEY) != Some(&fingerprint)
        || received != expected
        || receipt.amount.checked_add(receipt.fee) != Some(proofs.total_amount()?)
    {
        return Err(Error::InvalidOperationState);
    }
    if let Some(saga) = wallet.localstore.get_saga(&operation).await? {
        if saga.state != WalletSagaState::Receive(ReceiveSagaState::Completed)
            || saga.mint_url != wallet.mint_url
            || saga.unit != wallet.unit
        {
            return Err(Error::InvalidOperationState);
        }
    }
    Ok(Some(receipt.amount))
}
