//! Original output, receipt and quote publication shares the terminal journal transaction.
use std::collections::HashMap;

use cdk_common::wallet::{OperationData, ProofInfo, WalletSaga};

use crate::wallet::util::output_change;
use crate::{Error, Wallet};

pub(super) async fn retire(
    wallet: &Wallet,
    expected: &WalletSaga,
    outputs: Option<Vec<ProofInfo>>,
) -> Result<(), Error> {
    let OperationData::Mint(data) = &expected.data else {
        return Err(Error::InvalidOperationState);
    };
    let mut quotes = Vec::new();
    for id in data.quote_ids() {
        quotes.push(
            wallet
                .localstore
                .get_mint_quote(&id)
                .await?
                .ok_or(Error::UnknownQuote)?,
        );
    }
    let changes = if let Some(outputs) = outputs {
        let current: HashMap<_, _> = wallet
            .localstore
            .get_proofs_by_ys(outputs.iter().map(|proof| proof.y).collect())
            .await?
            .into_iter()
            .map(|proof| (proof.y, proof))
            .collect();
        Some(
            outputs
                .into_iter()
                .map(|output| {
                    let actual = current.get(&output.y);
                    output_change(output, actual)
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
    } else {
        None
    };
    if !wallet
        .localstore
        .retire_issue(expected, &quotes, changes)
        .await?
    {
        return Err(Error::ConcurrentUpdate);
    }
    Ok(())
}
