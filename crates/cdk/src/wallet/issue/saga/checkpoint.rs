//! Submit and recover only the original signed request and bearer secrets.
use cdk_common::wallet::{
    KeysetLoadPolicy, MintCheckpoint, MintOperationData, MintRequestData, ProofInfo,
};

use crate::nuts::{MintResponse, Proofs, RestoreRequest, State};
use crate::wallet::blind_signature::{
    ordered_restore_signatures, validate_mint_response_signatures, validate_saved_outputs,
    SignatureAmountValidation,
};
use crate::{Error, Wallet};

pub(super) fn get(data: &MintOperationData) -> Result<&MintCheckpoint, Error> {
    let checkpoint = data.original_request()?;
    validate_saved_outputs(&checkpoint.premint_secrets, checkpoint.request.outputs())?;
    Ok(checkpoint)
}

pub(super) async fn post(
    wallet: &Wallet,
    checkpoint: &MintCheckpoint,
) -> Result<MintResponse, Error> {
    let method = &checkpoint.payment_method;
    match &checkpoint.request {
        MintRequestData::Single {
            request,
            legacy_signature,
            allow_unsigned,
        } => {
            let error = match wallet.client.post_mint(method, request.clone()).await {
                Ok(response) => return Ok(response),
                Err(error) => error,
            };
            if !super::should_retry_with_legacy_quote_signature(&error) {
                return Err(error);
            }
            let Some(signature) = legacy_signature else {
                return Err(error);
            };
            let mut retry = request.clone();
            retry.signature = Some(signature.clone());
            if let Ok(response) = wallet.client.post_mint(method, retry).await {
                return Ok(response);
            }
            if *allow_unsigned {
                let mut unsigned = request.clone();
                unsigned.signature = None;
                if let Ok(response) = wallet.client.post_mint(method, unsigned).await {
                    return Ok(response);
                }
            }
            Err(error)
        }
        MintRequestData::Batch {
            request,
            legacy_signatures,
        } => {
            let error = match wallet.client.post_batch_mint(method, request.clone()).await {
                Ok(response) => return Ok(response),
                Err(error) => error,
            };
            if !super::should_retry_with_legacy_quote_signature(&error) {
                return Err(error);
            }
            let Some(signatures) = legacy_signatures else {
                return Err(error);
            };
            let mut retry = request.clone();
            retry.signatures = Some(signatures.clone());
            wallet
                .client
                .post_batch_mint(method, retry)
                .await
                .or(Err(error))
        }
    }
}

pub(super) async fn proofs(
    wallet: &Wallet,
    checkpoint: &MintCheckpoint,
    policy: KeysetLoadPolicy,
    response: MintResponse,
) -> Result<Proofs, Error> {
    let secrets = &checkpoint.premint_secrets;
    validate_mint_response_signatures(
        wallet,
        &response.signatures,
        checkpoint.request.outputs(),
        SignatureAmountValidation::Exact,
    )
    .await?;
    let keys = wallet
        .keyset_with_policy(secrets.keyset_id, policy)
        .await?
        .keys;
    Ok(crate::dhke::construct_proofs(
        response.signatures,
        secrets.rs(),
        secrets.secrets(),
        &keys,
    )?)
}

pub(super) async fn recover(
    wallet: &Wallet,
    checkpoint: &MintCheckpoint,
    operation_id: uuid::Uuid,
) -> Result<Vec<ProofInfo>, Error> {
    let response = match post(wallet, checkpoint).await {
        Ok(response) => response,
        Err(error @ (Error::MaxInputsExceeded { .. } | Error::MaxOutputsExceeded { .. })) => {
            return Err(error)
        }
        Err(_) => {
            let outputs = checkpoint.request.outputs();
            let restored = wallet
                .client
                .post_restore(RestoreRequest {
                    outputs: outputs.to_vec(),
                })
                .await?;
            MintResponse {
                signatures: ordered_restore_signatures(outputs, restored)?,
            }
        }
    };
    let recovered = proofs(wallet, checkpoint, KeysetLoadPolicy::default(), response).await?;
    proof_infos(wallet, checkpoint, recovered, operation_id)
}

pub(super) fn proof_infos(
    wallet: &Wallet,
    checkpoint: &MintCheckpoint,
    proofs: Proofs,
    operation_id: uuid::Uuid,
) -> Result<Vec<ProofInfo>, Error> {
    proofs
        .into_iter()
        .zip(&checkpoint.premint_secrets.secrets)
        .map(|(proof, secret)| {
            let mut info = ProofInfo::new(
                proof,
                wallet.mint_url.clone(),
                State::Unspent,
                wallet.unit.clone(),
            )?;
            info.created_by_operation = Some(operation_id);
            Ok(match secret.derivation_index {
                Some(index) => info.with_derivation_index(index),
                None => info,
            })
        })
        .collect()
}
