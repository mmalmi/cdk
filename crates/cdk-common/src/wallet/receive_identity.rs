//! Stable identity for one mint's exact set of received coins.
use crate::mint_url::MintUrl;
use crate::wallet::TransactionId;
use crate::Error;
use bitcoin::hashes::{sha256, Hash, HashEngine};
use cashu::{CurrencyUnit, Proof};

/// Reserved receipt metadata containing the immutable received-coin fingerprint.
pub const METADATA_KEY: &str = "cdk_receive_fingerprint";

/// Identify a completed receipt without reusing its saga's per-attempt identity.
pub fn transaction_id(
    mint: &MintUrl,
    unit: &CurrencyUnit,
    proofs: &[Proof],
) -> Result<TransactionId, Error> {
    Ok(TransactionId::from_receive_fingerprint(&fingerprint(
        mint, unit, proofs,
    )?))
}

/// Fingerprint a receive independently of ordering and transport witnesses.
///
/// The mint, unit and immutable coin fields are included. Memo, DLEQ, P2BK hints
/// and spending witnesses do not change the payment. This identifies a receipt; it does not
/// establish that the wallet received any money. Retiring that receipt releases
/// the original result and does not permit redeeming spent coins again.
pub fn fingerprint(mint: &MintUrl, unit: &CurrencyUnit, proofs: &[Proof]) -> Result<String, Error> {
    let mut coins = proofs
        .iter()
        .map(|proof| Ok((proof.y()?, proof)))
        .collect::<Result<Vec<_>, Error>>()?;
    coins.sort_unstable_by_key(|(y, _)| *y);
    if coins.is_empty() || coins.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(Error::InvalidOperationState);
    }
    let fields = coins
        .iter()
        .map(|(_, proof)| (proof.amount, proof.keyset_id, &proof.secret, proof.c))
        .collect::<Vec<_>>();
    let mut engine = sha256::Hash::engine();
    engine.input(b"cdk:receive:v1\0");
    engine.input(&serde_json::to_vec(&(mint, unit, fields))?);
    Ok(sha256::Hash::from_engine(engine).to_string())
}
