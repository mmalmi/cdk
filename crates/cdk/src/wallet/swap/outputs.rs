//! Shared signature verification, unblinding and recipient/change selection.
use cdk_common::wallet::SwapOutputs;

use crate::amount::SplitTarget;
use crate::dhke::construct_proofs;
use crate::nuts::{nut10, PreSwap, Proofs, RestoreRequest, SpendingConditions, SwapResponse};
use crate::wallet::blind_signature::{
    validate_mint_response_signatures, SignatureAmountValidation,
};
use crate::{Amount, Error, Wallet};

pub(crate) async fn resolve(
    wallet: &Wallet,
    pre_swap: &PreSwap,
    amount: Option<Amount>,
    split_target: &SplitTarget,
    spending_conditions: Option<&SpendingConditions>,
    response: SwapResponse,
) -> Result<SwapOutputs, Error> {
    let keyset_id = pre_swap.pre_mint_secrets.keyset_id;
    let active_keys = wallet.keyset(keyset_id).await?.keys;
    validate_mint_response_signatures(
        wallet,
        &response.signatures,
        pre_swap.swap_request.outputs().iter(),
        SignatureAmountValidation::Exact,
    )
    .await?;
    let post_swap_proofs = construct_proofs(
        response.signatures,
        pre_swap.pre_mint_secrets.rs(),
        pre_swap.pre_mint_secrets.secrets(),
        &active_keys,
    )?;
    let Some(amount) = amount else {
        return Ok(SwapOutputs {
            send: Vec::new(),
            change: post_swap_proofs,
        });
    };
    let fee_and_amounts = wallet.get_keyset_fees_and_amounts_by_id(keyset_id).await?;
    let (with_condition, without_condition): (Proofs, Proofs) =
        post_swap_proofs.into_iter().partition(|p| {
            let secret: Result<nut10::Secret, _> = p.secret.clone().try_into();
            secret.is_ok()
        });
    let (mut send, change) = match spending_conditions {
        Some(_) => (with_condition, without_condition),
        None => {
            let mut all = without_condition;
            all.reverse();
            let mut send = Proofs::new();
            let mut change = Proofs::new();
            let mut split = amount.split_targeted(split_target, &fee_and_amounts)?;
            for proof in all {
                if let Some(index) = split.iter().position(|a| *a == proof.amount) {
                    send.push(proof);
                    split.remove(index);
                } else {
                    change.push(proof);
                }
            }
            (send, change)
        }
    };
    if let Some(keys) = &pre_swap.p2bk_secret_keys {
        for (index, proof) in send.iter_mut().enumerate() {
            let key = keys
                .get(if keys.len() == 1 { 0 } else { index })
                .ok_or_else(|| Error::Custom("Missing P2BK output key".to_string()))?;
            proof.p2pk_e = Some(key.public_key());
        }
    }
    Ok(SwapOutputs { send, change })
}

pub(crate) async fn restore(wallet: &Wallet, swap: &PreSwap) -> Result<SwapResponse, Error> {
    let expected = swap.swap_request.outputs();
    let restored = wallet
        .client
        .post_restore(RestoreRequest {
            outputs: expected.clone(),
        })
        .await?;
    if restored.outputs.is_empty() && restored.signatures.is_empty() {
        // Retrying this exact request cannot choose new inputs or a new fee.
        return wallet.client.post_swap(swap.swap_request.clone()).await;
    }
    let signatures =
        crate::wallet::blind_signature::ordered_restore_signatures(expected, restored)?;
    Ok(SwapResponse { signatures })
}

pub(crate) fn validate_request(swap: &PreSwap) -> Result<(), Error> {
    crate::wallet::blind_signature::validate_saved_outputs(
        &swap.pre_mint_secrets,
        swap.swap_request.outputs(),
    )
}
