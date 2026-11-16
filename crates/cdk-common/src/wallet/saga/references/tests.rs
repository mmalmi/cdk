use super::*;
use crate::wallet::*;
use crate::{Amount, CurrencyUnit, Id, SecretKey, State};

#[derive(Default)]
struct Fixture {
    expected: HashSet<PublicKey>,
}

impl Fixture {
    fn proof(&mut self) -> Proof {
        let proof = Proof::new(
            1.into(),
            Id::from_bytes(&[0; 8]).unwrap(),
            cashu::secret::Secret::new(format!("reference-{}", self.expected.len())),
            SecretKey::generate().public_key(),
        );
        self.expected.insert(proof.y().unwrap());
        proof
    }

    fn coin(&mut self) -> ProofInfo {
        let mut coin = ProofInfo::new(
            self.proof(),
            "https://references.test".parse().unwrap(),
            State::Spent,
            CurrencyUnit::Sat,
        )
        .unwrap();
        // Even a contradictory saved Y must not discard either identity.
        coin.y = self.proof().y().unwrap();
        coin
    }

    fn changes(&mut self) -> Vec<ProofChange> {
        vec![ProofChange {
            y: self.proof().y().unwrap(),
            before: Some(self.coin()),
            after: Some(self.coin()),
        }]
    }

    fn swap(&mut self) -> PreSwap {
        let input = self.proof();
        let output = self.proof();
        let secrets = PreMintSecrets::from_secrets(
            output.keyset_id,
            vec![Amount::from(1)],
            vec![output.secret],
        )
        .unwrap();
        PreSwap {
            swap_request: crate::SwapRequest::new(vec![input], secrets.blinded_messages()),
            pre_mint_secrets: secrets,
            derived_secret_count: 0,
            fee: 0.into(),
            p2bk_secret_keys: None,
        }
    }

    fn outputs(&mut self) -> SwapOutputs {
        SwapOutputs {
            send: vec![self.proof()],
            change: vec![self.proof()],
        }
    }

    fn receipt(&mut self) -> Transaction {
        Transaction {
            role: TransactionRole::Primary,
            mint_url: "https://references.test".parse().unwrap(),
            direction: TransactionDirection::Incoming,
            amount: 1.into(),
            fee: 0.into(),
            unit: CurrencyUnit::Sat,
            ys: vec![self.proof().y().unwrap()],
            timestamp: 1,
            memo: None,
            metadata: Default::default(),
            quote_id: None,
            payment_request: None,
            payment_proof: None,
            payment_method: None,
            saga_id: None,
            status: TransactionStatus::Pending,
        }
    }

    fn data(&mut self, kind: usize) -> (WalletSagaState, OperationData) {
        match kind {
            0 | 1 => {
                let token = Token::new(
                    "https://references.test".parse().unwrap(),
                    vec![self.proof()],
                    None,
                    CurrencyUnit::Sat,
                );
                (
                    WalletSagaState::Send(SendSagaState::TokenCreated),
                    OperationData::Send(SendOperationData {
                        preparation: Some(Box::new(SendPreparation {
                            inputs: vec![self.coin()],
                            metadata: Default::default(),
                            retain_cancellation: true,
                        })),
                        confirmation: Some(Box::new(SendConfirmation {
                            pre_swap: Some(self.swap()),
                            swap_amount: 1.into(),
                            spending_conditions: None,
                            retained_proofs: vec![self.proof()],
                            send_fee: 0.into(),
                            include_fee: false,
                            max_total_amount: None,
                            required_keyset_id: None,
                            token_memo: None,
                            metadata: Default::default(),
                            ready: Some(self.outputs()),
                        })),
                        reclaim: Some(Box::new(SendReclaim {
                            inputs: vec![self.coin()],
                            pre_swap: self.swap(),
                            transaction: self.receipt(),
                            outputs: Some(vec![self.coin()]),
                        })),
                        amount: 1.into(),
                        memo: None,
                        counter_start: None,
                        counter_end: None,
                        token: Some(if kind == 0 {
                            token.to_string()
                        } else {
                            token.to_v3_string()
                        }),
                        proofs: Some(vec![self.proof()]),
                    }),
                )
            }
            2 => (
                WalletSagaState::Receive(ReceiveSagaState::OutputsReady),
                OperationData::Receive(ReceiveOperationData {
                    checkpoint: Some(Box::new(ReceiveCheckpoint {
                        inputs: self.changes(),
                        pre_swap: Some(self.swap()),
                        transaction: self.receipt(),
                        outputs: Some(vec![self.coin()]),
                    })),
                }),
            ),
            3 => (
                WalletSagaState::Swap(SwapSagaState::OutputsReady),
                OperationData::Swap(SwapOperationData {
                    checkpoint: Some(Box::new(SwapCheckpoint {
                        acquisition: self.changes(),
                        pre_swap: self.swap(),
                        amount: None,
                        amount_split_target: Default::default(),
                        spending_conditions: None,
                        ready: Some(self.outputs()),
                    })),
                    input_amount: 1.into(),
                    output_amount: 1.into(),
                }),
            ),
            4 => {
                let output = self.proof();
                let secrets = PreMintSecrets::from_secrets(
                    output.keyset_id,
                    vec![1.into()],
                    vec![output.secret],
                )
                .unwrap();
                let mut data = MintOperationData::new_single("quote".to_owned(), 1.into());
                data.checkpoint = Some(Box::new(MintCheckpoint {
                    quotes: vec![],
                    payment_method: "bolt11".parse().unwrap(),
                    request: MintRequestData::Single {
                        request: crate::MintRequest {
                            quote: "quote".to_owned(),
                            outputs: secrets.blinded_messages(),
                            signature: None,
                        },
                        legacy_signature: None,
                        allow_unsigned: false,
                    },
                    premint_secrets: secrets,
                }));
                (
                    WalletSagaState::Issue(IssueSagaState::MintRequested),
                    OperationData::Mint(data),
                )
            }
            5 => (
                WalletSagaState::Melt(MeltSagaState::PaidReady),
                OperationData::Melt(MeltOperationData {
                    acquisition: Some(self.changes()),
                    swap: Some(Box::new(MeltSwap {
                        inputs: vec![self.coin()],
                        pre_swap: self.swap(),
                        amount: 1.into(),
                        outputs: Some(self.outputs()),
                    })),
                    payment: Some(Box::new(MeltPayment {
                        inputs: vec![self.coin()],
                        receipt: self.receipt(),
                        change: Some(vec![self.coin()]),
                    })),
                    quote_id: "quote".to_owned(),
                    amount: 1.into(),
                    fee_reserve: 0.into(),
                    counter_start: None,
                    counter_end: None,
                    change_amount: None,
                    metadata: Default::default(),
                    final_proof_ys: Some(vec![self.proof().y().unwrap()]),
                    change_blinded_messages: None,
                }),
            ),
            _ => unreachable!(),
        }
    }
}

#[test]
fn every_recovery_coin_source_is_retained_without_using_row_ownership() {
    for kind in 0..6 {
        let mut fixture = Fixture::default();
        let (state, data) = fixture.data(kind);
        let saga = WalletSaga::new(
            uuid::Uuid::new_v4(),
            state,
            1.into(),
            "https://references.test".parse().unwrap(),
            CurrencyUnit::Sat,
            data,
        );
        assert_eq!(
            saga.referenced_proof_ys().unwrap(),
            fixture.expected,
            "kind {kind}"
        );
        let mut invalid = saga.clone();
        invalid.kind = if kind == 0 {
            OperationKind::Receive
        } else {
            OperationKind::Send
        };
        if kind == 1 {
            invalid.kind = OperationKind::Receive;
        }
        assert!(invalid.referenced_proof_ys().is_err());
        match &mut invalid.data {
            OperationData::Send(data) => data.proofs = None,
            OperationData::Receive(data) => data.checkpoint = None,
            OperationData::Swap(data) => data.checkpoint = None,
            OperationData::Mint(data) => data.checkpoint = None,
            OperationData::Melt(data) => data.acquisition = None,
        }
        invalid.kind = saga.kind;
        assert!(invalid.referenced_proof_ys().is_err());
    }
}
