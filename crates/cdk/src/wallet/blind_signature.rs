use crate::nuts::{nut12, BlindSignature, BlindedMessage};
use crate::wallet::Wallet;
use crate::{Amount, Error};

/// How strictly returned signature amounts must match requested output amounts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignatureAmountValidation {
    /// Returned signature amounts must exactly match the requested output amounts.
    Exact,
    /// A zero-amount requested output is a placeholder and may receive any amount.
    AllowZeroAmountPlaceholder,
}

/// Validate mint-returned blind signatures against the wallet's requested outputs.
///
/// The mint controls the `amount` and `keyset_id` fields in each returned
/// [`BlindSignature`], so callers must verify those fields against the
/// corresponding premint blinded message before constructing wallet proofs.
///
/// Use [`SignatureAmountValidation::Exact`] for mint/swap responses where the
/// wallet requested a specific denomination. Use
/// [`SignatureAmountValidation::AllowZeroAmountPlaceholder`] for NUT-08/NUT-09
/// style outputs where the wallet sends amount `0` and the mint fills in the
/// actual change or restored amount. DLEQ proofs are optional for compatibility,
/// but when present they are verified after the signature metadata has been
/// cross-checked.
pub(crate) async fn validate_mint_response_signatures<'a>(
    wallet: &Wallet,
    signatures: &[BlindSignature],
    blinded_messages: impl IntoIterator<Item = &'a BlindedMessage>,
    amount_validation: SignatureAmountValidation,
) -> Result<(), Error> {
    let blinded_messages = blinded_messages.into_iter().collect::<Vec<_>>();

    if signatures.len() != blinded_messages.len() {
        return Err(Error::InvalidMintResponse(format!(
            "mint signatures ({}) does not match secrets sent ({})",
            signatures.len(),
            blinded_messages.len()
        )));
    }

    for (sig, blinded_message) in signatures.iter().zip(blinded_messages) {
        let amount_matches = match amount_validation {
            SignatureAmountValidation::Exact => sig.amount == blinded_message.amount,
            SignatureAmountValidation::AllowZeroAmountPlaceholder => {
                blinded_message.amount == Amount::ZERO || sig.amount == blinded_message.amount
            }
        };

        if !amount_matches {
            return Err(Error::InvalidMintResponse(format!(
                "mint signature amount ({}) does not match requested amount ({})",
                sig.amount, blinded_message.amount
            )));
        }

        if sig.keyset_id != blinded_message.keyset_id {
            return Err(Error::InvalidMintResponse(format!(
                "mint signature keyset ({}) does not match requested keyset ({})",
                sig.keyset_id, blinded_message.keyset_id
            )));
        }

        let keys = wallet.keyset(sig.keyset_id).await?.keys;
        let key = keys.amount_key(sig.amount).ok_or(Error::AmountKey)?;
        match sig.verify_dleq(key, blinded_message.blinded_secret) {
            Ok(_) | Err(nut12::Error::MissingDleqProof) => (),
            Err(_) => return Err(Error::CouldNotVerifyDleq),
        }
    }

    Ok(())
}

/// Check stored secrets against every original blinded output before using them.
pub(crate) fn validate_saved_outputs(
    secrets: &crate::nuts::PreMintSecrets,
    outputs: &[BlindedMessage],
) -> Result<(), Error> {
    if secrets.secrets.is_empty() || outputs.len() != secrets.secrets.len() {
        return Err(Error::InvalidOperationState);
    }
    let mut seen = std::collections::HashSet::new();
    for (secret, output) in secrets.secrets.iter().zip(outputs) {
        let (blinded, _) =
            crate::dhke::blind_message(secret.secret.as_bytes(), Some(secret.r.clone()))?;
        if blinded != output.blinded_secret
            || *output != secret.blinded_message
            || secret.amount != output.amount
            || output.keyset_id != secrets.keyset_id
            || !seen.insert(blinded)
        {
            return Err(Error::InvalidOperationState);
        }
    }
    Ok(())
}

/// Require complete restore coverage and order signatures by the original outputs.
pub(crate) fn ordered_restore_signatures(
    expected: &[BlindedMessage],
    restored: crate::nuts::RestoreResponse,
) -> Result<Vec<BlindSignature>, Error> {
    let invalid =
        || Error::InvalidMintResponse("restore does not match every original output".to_owned());
    if restored.outputs.len() != expected.len() || restored.signatures.len() != expected.len() {
        return Err(invalid());
    }
    let mut signatures = std::collections::HashMap::new();
    for (output, signature) in restored.outputs.into_iter().zip(restored.signatures) {
        if signatures
            .insert(output.blinded_secret, (output, signature))
            .is_some()
        {
            return Err(invalid());
        }
    }
    expected
        .iter()
        .map(|output| {
            let (actual, signature) = signatures
                .remove(&output.blinded_secret)
                .ok_or_else(invalid)?;
            if actual != *output {
                return Err(invalid());
            }
            Ok(signature)
        })
        .collect()
}
