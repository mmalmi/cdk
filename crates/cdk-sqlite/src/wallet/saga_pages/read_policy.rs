//! Exercise production record encoding; sizing proofs are not spendable currency.
mod transactional;

use cdk_common::database::wallet::{ProofChange, MAX_MELT_PAYMENT_PROOF_BYTES};
use cdk_common::database::{Error, WalletDatabase};
use cdk_common::secret::Secret;
use cdk_common::wallet::{
    MeltOperationData, MeltPayment, MeltSagaState, MeltSwap, OperationData, ProofInfo,
    ReceiveCheckpoint, ReceiveOperationData, ReceiveSagaState, SendConfirmation, SendOperationData,
    SendReclaim, SendSagaState, SwapCheckpoint, SwapOperationData, SwapOutputs, SwapSagaState,
    Transaction, TransactionDirection, TransactionRole, TransactionStatus, WalletSaga,
    WalletSagaState,
};
use cdk_common::{
    BlindedMessage, CurrencyUnit, PreMint, PreMintSecrets, PreSwap, Proof, ProofDleq, SecretKey,
    State, SwapRequest, Token,
};

use super::super::test_db::{open, Fixture};

#[tokio::test]
async fn proof_read_admission_covers_future_withdrawal_replacements() {
    let f = Fixture::new().await;
    let (prepared, _) = completion_states("melt swap");
    let OperationData::Melt(data) = &prepared.data else {
        unreachable!()
    };
    let input = data.acquisition.as_ref().unwrap()[0].after.clone().unwrap();
    let db = open(&f.path)
        .await
        .with_saga_read_limit(4 * 1024 * 1024)
        .with_proof_read_limit(1600);
    db.update_proofs(vec![input.clone()], vec![]).await.unwrap();
    assert_eq!(
        db.get_proofs_by_ys(vec![input.y]).await.unwrap(),
        std::slice::from_ref(&input)
    );
    assert!(
        matches!(
            db.add_saga(prepared.clone()).await,
            Err(Error::ProofInventoryTooLarge { .. })
        ),
        "admission must fit future withdrawal proof batches before spending"
    );
    assert!(f.db.get_saga(&prepared.id).await.unwrap().is_none());
    assert_eq!(f.db.get_proofs_by_ys(vec![input.y]).await.unwrap(), [input]);
    // Other workflows share the selected-proof reader and must admit their
    // future output rows before submitting the corresponding mint request.
    let (_, receive, _, _) = receive_states(64);
    let (_, reclaim, _, _) = reclaim_states(receive_states(64));
    for proposal in [
        receive,
        reclaim,
        completion_states("send ready").0,
        completion_states("send retained").0,
        completion_states("swap").0,
        completion_states("melt payment").0,
    ] {
        let f = Fixture::new().await;
        let limited = open(&f.path)
            .await
            .with_saga_read_limit(4 * 1024 * 1024)
            .with_proof_read_limit(1600);
        assert!(matches!(
            limited.add_saga(proposal.clone()).await,
            Err(Error::ProofInventoryTooLarge { .. })
        ));
        assert!(f.db.get_saga(&proposal.id).await.unwrap().is_none());
        let accepted = open(&f.path)
            .await
            .with_saga_read_limit(4 * 1024 * 1024)
            .with_proof_read_limit(1024 * 1024);
        accepted.add_saga(proposal.clone()).await.unwrap();
        assert_eq!(
            accepted.get_saga(&proposal.id).await.unwrap(),
            Some(proposal)
        );
    }
}

#[tokio::test]
async fn admission_reserves_scalar_and_state_growth_before_writing() {
    let f = Fixture::new().await;
    let mut saga = super::original();
    let OperationData::Send(data) = &mut saga.data else {
        unreachable!()
    };
    data.memo = None;
    saga.version = 9;
    f.db.add_saga(saga.clone()).await.unwrap();
    let Err(Error::SagaTooLarge { bytes, .. }) = f.db.get_saga_bounded(&saga.id, 0).await else {
        panic!("expected the stored record size");
    };
    f.db.delete_saga(&saga.id).await.unwrap();
    let db = open(&f.path)
        .await
        .with_saga_read_limit(bytes.try_into().unwrap());
    assert!(matches!(db.add_saga(saga.clone()).await,
        Err(Error::SagaTooLarge { bytes: required, .. }) if required > bytes));
    assert!(f.db.get_saga(&saga.id).await.unwrap().is_none());
}

fn receive_states(count: u32) -> (WalletSaga, WalletSaga, WalletSaga, ProofInfo) {
    let owner = uuid::Uuid::new_v4();
    let mint: cdk_common::mint_url::MintUrl = "http://localhost:3338".parse().unwrap();
    let keyset = "00916bbf7ef91a36".parse().unwrap();
    let key = SecretKey::generate();
    let mut input = ProofInfo::new(
        Proof::new(
            u64::from(count).into(),
            keyset,
            Secret::generate(),
            key.public_key(),
        ),
        mint.clone(),
        State::Pending,
        CurrencyUnit::Sat,
    )
    .unwrap();
    input.used_by_operation = Some(owner);
    let transaction = Transaction {
        role: TransactionRole::Primary,
        status: TransactionStatus::Completed,
        mint_url: mint.clone(),
        direction: TransactionDirection::Incoming,
        amount: u64::from(count).into(),
        fee: 0.into(),
        unit: CurrencyUnit::Sat,
        ys: vec![input.y],
        timestamp: 1,
        memo: None,
        metadata: Default::default(),
        quote_id: None,
        payment_request: None,
        payment_proof: None,
        payment_method: None,
        saga_id: Some(owner),
    };
    let original = WalletSaga::new(
        owner,
        WalletSagaState::Receive(ReceiveSagaState::InputsRecorded),
        u64::from(count).into(),
        mint.clone(),
        CurrencyUnit::Sat,
        OperationData::Receive(ReceiveOperationData {
            checkpoint: Some(Box::new(ReceiveCheckpoint {
                inputs: vec![ProofChange {
                    y: input.y,
                    before: None,
                    after: Some(input.clone()),
                }],
                pre_swap: None,
                transaction,
                outputs: None,
            })),
        }),
    );
    let mut secrets = Vec::new();
    let mut outputs = Vec::new();
    for index in 0..count {
        let secret = Secret::new(format!("{index:064x}"));
        secrets.push(PreMint {
            blinded_message: BlindedMessage::new(1.into(), keyset, key.public_key()),
            secret: secret.clone(),
            r: key.clone(),
            amount: 1.into(),
            derivation_index: Some(index),
        });
        let mut proof = Proof::new(1.into(), keyset, secret, key.public_key());
        proof.dleq = Some(ProofDleq::new(key.clone(), key.clone(), key.clone()));
        let mut output =
            ProofInfo::new(proof, mint.clone(), State::Unspent, CurrencyUnit::Sat).unwrap();
        output.created_by_operation = Some(owner);
        output.derivation_index = Some(index);
        outputs.push(output);
    }
    let mut prepared = original.clone();
    let OperationData::Receive(data) = &mut prepared.data else {
        unreachable!()
    };
    data.checkpoint.as_mut().unwrap().pre_swap = Some(PreSwap {
        swap_request: SwapRequest::new(
            vec![input.proof.clone()],
            secrets.iter().map(|s| s.blinded_message.clone()).collect(),
        ),
        pre_mint_secrets: PreMintSecrets {
            secrets,
            keyset_id: keyset,
        },
        derived_secret_count: count,
        fee: 0.into(),
        p2bk_secret_keys: None,
    });
    prepared.update_state(WalletSagaState::Receive(ReceiveSagaState::Redeeming));
    let mut completed = prepared.clone();
    let OperationData::Receive(data) = &mut completed.data else {
        unreachable!()
    };
    data.checkpoint.as_mut().unwrap().outputs = Some(outputs);
    completed.update_state(WalletSagaState::Receive(ReceiveSagaState::OutputsReady));
    (original, prepared, completed, input)
}

fn reclaim_states(
    (mut original, prepared, completed, mut input): (WalletSaga, WalletSaga, WalletSaga, ProofInfo),
) -> (WalletSaga, WalletSaga, WalletSaga, ProofInfo) {
    let OperationData::Receive(prepared_data) = prepared.data else {
        unreachable!()
    };
    let OperationData::Receive(completed_data) = completed.data else {
        unreachable!()
    };
    let saved = prepared_data.checkpoint.unwrap();
    input.state = State::PendingSpent;
    original.state = WalletSagaState::Send(SendSagaState::TokenCreated);
    original.kind = original.state.kind();
    original.data = OperationData::Send(SendOperationData {
        preparation: None,
        reclaim: None,
        confirmation: None,
        amount: original.amount,
        memo: None,
        counter_start: None,
        counter_end: None,
        token: Some("sizing-only".to_owned()),
        proofs: Some(vec![input.proof.clone()]),
    });
    let mut prepared = original.clone();
    let OperationData::Send(data) = &mut prepared.data else {
        unreachable!()
    };
    data.reclaim = Some(Box::new(SendReclaim {
        inputs: vec![input.clone()],
        pre_swap: saved.pre_swap.unwrap(),
        transaction: Transaction {
            role: TransactionRole::SendRefund,
            ..saved.transaction
        },
        outputs: None,
    }));
    prepared.update_state(WalletSagaState::Send(SendSagaState::ReclaimPreparing));
    let mut completed = prepared.clone();
    let OperationData::Send(data) = &mut completed.data else {
        unreachable!()
    };
    data.reclaim.as_mut().unwrap().outputs = completed_data.checkpoint.unwrap().outputs;
    completed.update_state(WalletSagaState::Send(SendSagaState::Reclaiming));
    (original, prepared, completed, input)
}

#[tokio::test]
async fn admission_keeps_completion_within_the_recovery_read_limit() {
    let base = receive_states(4096);
    for kind in ["receive", "reclaim"] {
        let f = Fixture::new().await;
        let limit = 4 * 1024 * 1024;
        let db = open(&f.path).await.with_saga_read_limit(limit);
        let (original, prepared, completed, input) = match kind {
            "receive" => base.clone(),
            "reclaim" => reclaim_states(base.clone()),
            _ => unreachable!(),
        };
        let prepared_bytes = serde_json::to_vec(&prepared.data).unwrap().len();
        let completed_bytes = serde_json::to_vec(&completed.data).unwrap().len();
        assert!(prepared_bytes < limit as usize);
        assert!(completed_bytes > limit as usize);
        eprintln!("{kind} record bytes: prepared={prepared_bytes}, completed={completed_bytes}, limit={limit}");
        db.add_saga(original.clone()).await.unwrap();
        db.update_proofs(vec![input.clone()], vec![]).await.unwrap();
        assert!(
            matches!(
                db.update_saga(prepared).await,
                Err(Error::SagaTooLarge { .. })
            ),
            "pre-submission admission must fit the configured recovery read limit: {kind}"
        );
        assert_eq!(db.get_saga(&original.id).await.unwrap(), Some(original));
        assert_eq!(
            db.get_proofs_by_ys(vec![input.y]).await.unwrap(),
            vec![input]
        );
    }
}

fn completion_states(kind: &str) -> (WalletSaga, WalletSaga) {
    let (_, mut prepared, completed, input) = receive_states(64);
    let OperationData::Receive(data) = &prepared.data else {
        unreachable!()
    };
    let saved = data.checkpoint.as_ref().unwrap().clone();
    let OperationData::Receive(data) = completed.data else {
        unreachable!()
    };
    let infos = data.checkpoint.unwrap().outputs.unwrap();
    let outputs = SwapOutputs {
        send: infos.iter().map(|info| info.proof.clone()).collect(),
        change: Vec::new(),
    };
    let state = match kind {
        "send ready" | "send token" | "send retained" => {
            let retained = kind == "send retained";
            prepared.data = OperationData::Send(SendOperationData {
                preparation: None,
                reclaim: None,
                confirmation: Some(Box::new(SendConfirmation {
                    pre_swap: if retained {
                        None
                    } else {
                        saved.pre_swap.clone()
                    },
                    swap_amount: prepared.amount,
                    spending_conditions: None,
                    retained_proofs: if retained {
                        outputs.send.clone()
                    } else {
                        Vec::new()
                    },
                    send_fee: 0.into(),
                    include_fee: false,
                    max_total_amount: None,
                    required_keyset_id: None,
                    token_memo: Some("sizing-only".to_owned()),
                    metadata: Default::default(),
                    ready: None,
                })),
                amount: prepared.amount,
                memo: None,
                counter_start: None,
                counter_end: None,
                token: None,
                proofs: None,
            });
            WalletSagaState::Send(SendSagaState::Confirming)
        }
        "swap" => {
            prepared.data = OperationData::Swap(SwapOperationData {
                checkpoint: Some(Box::new(SwapCheckpoint {
                    acquisition: saved.inputs.clone(),
                    pre_swap: saved.pre_swap.clone().unwrap(),
                    amount: None,
                    amount_split_target: Default::default(),
                    spending_conditions: None,
                    ready: None,
                })),
                input_amount: prepared.amount,
                output_amount: prepared.amount,
            });
            WalletSagaState::Swap(SwapSagaState::ProofsReserved)
        }
        "melt swap" | "melt payment" => {
            let payment = kind == "melt payment";
            prepared.data = OperationData::Melt(MeltOperationData {
                payment: payment.then(|| {
                    Box::new(MeltPayment {
                        inputs: vec![input.clone()],
                        receipt: Transaction {
                            status: TransactionStatus::Pending,
                            direction: TransactionDirection::Outgoing,
                            ..saved.transaction.clone()
                        },
                        change: None,
                    })
                }),
                swap: (!payment).then(|| {
                    Box::new(MeltSwap {
                        inputs: vec![input.clone()],
                        pre_swap: saved.pre_swap.clone().unwrap(),
                        amount: prepared.amount,
                        outputs: None,
                    })
                }),
                acquisition: Some(saved.inputs.clone()),
                quote_id: "sizing-only".to_owned(),
                amount: prepared.amount,
                fee_reserve: 0.into(),
                counter_start: Some(0),
                counter_end: Some(64),
                change_amount: Some(prepared.amount),
                metadata: Default::default(),
                final_proof_ys: Some(vec![input.y]),
                change_blinded_messages: payment.then(|| {
                    saved
                        .pre_swap
                        .as_ref()
                        .unwrap()
                        .pre_mint_secrets
                        .secrets
                        .iter()
                        .map(|s| s.blinded_message.clone())
                        .collect()
                }),
            });
            WalletSagaState::Melt(if payment {
                MeltSagaState::MeltRequested
            } else {
                MeltSagaState::ProofsReserved
            })
        }
        _ => unreachable!(),
    };
    prepared.state = state;
    prepared.kind = state.kind();
    let mut completed = prepared.clone();
    match &mut completed.data {
        OperationData::Send(data) => {
            if kind == "send ready" {
                data.confirmation.as_mut().unwrap().ready = Some(outputs);
            } else {
                let memo = data.confirmation.take().unwrap().token_memo;
                data.token = Some(
                    Token::new(
                        prepared.mint_url.clone(),
                        outputs.send.clone(),
                        memo,
                        prepared.unit.clone(),
                    )
                    .to_string(),
                );
                data.proofs = Some(outputs.send);
                completed.update_state(WalletSagaState::Send(SendSagaState::TokenCreated));
            }
        }
        OperationData::Swap(data) => {
            data.checkpoint.as_mut().unwrap().ready = Some(outputs);
            completed.update_state(WalletSagaState::Swap(SwapSagaState::OutputsReady));
        }
        OperationData::Melt(data) => {
            if let Some(payment) = &mut data.payment {
                payment.change = Some(infos);
                payment.receipt.status = TransactionStatus::Completed;
                payment.receipt.payment_proof = Some("\0".repeat(MAX_MELT_PAYMENT_PROOF_BYTES));
                completed.update_state(WalletSagaState::Melt(MeltSagaState::PaymentPending));
            } else {
                data.swap.as_mut().unwrap().outputs = Some(outputs);
                completed.update_state(WalletSagaState::Melt(MeltSagaState::SwapCancelling));
            }
        }
        _ => unreachable!(),
    }
    (prepared, completed)
}

#[tokio::test]
async fn completion_limits_cover_send_swap_and_both_withdrawal_stages() {
    for kind in [
        "send ready",
        "send token",
        "send retained",
        "swap",
        "melt swap",
        "melt payment",
    ] {
        let f = Fixture::new().await;
        let (prepared, completed) = completion_states(kind);
        let initial = serde_json::to_vec(&prepared.data).unwrap().len();
        let final_bytes = serde_json::to_vec(&completed.data).unwrap().len();
        assert!(final_bytes > initial + 1024, "completion growth: {kind}");
        let limit = u32::try_from((initial + final_bytes) / 2).unwrap();
        let db = open(&f.path).await.with_saga_read_limit(limit);
        assert!(
            matches!(
                db.add_saga(prepared.clone()).await,
                Err(Error::SagaTooLarge { .. })
            ),
            "must reject before submission: {kind}"
        );
        assert!(f.db.get_saga(&prepared.id).await.unwrap().is_none());
        // The proposal itself fits; only its completion makes admission unsafe.
        f.db.add_saga(prepared.clone()).await.unwrap();
        assert_eq!(
            db.get_saga(&prepared.id).await.unwrap(),
            Some(prepared.clone())
        );
        f.db.delete_saga(&prepared.id).await.unwrap();
        let accepted = open(&f.path)
            .await
            .with_saga_read_limit(u32::try_from(initial + final_bytes + 1024 * 1024).unwrap());
        accepted.add_saga(prepared).await.unwrap();
        let mut completed = completed;
        // Some fixtures retain their state but still persist a new checkpoint.
        completed.version = 2;
        assert!(accepted.update_saga(completed.clone()).await.unwrap());
        assert_eq!(
            accepted.get_saga(&completed.id).await.unwrap(),
            Some(completed)
        );
    }
}
