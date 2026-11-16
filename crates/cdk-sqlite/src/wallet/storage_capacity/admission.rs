use cdk_common::amount::SplitTarget;
use cdk_common::database::wallet::ProofChange;
use cdk_common::database::Error;
use cdk_common::secret::Secret;
use cdk_common::wallet::SwapCheckpoint;
use cdk_common::{BlindedMessage, PreMint, PreMintSecrets, PreSwap, SecretKey, State, SwapRequest};

use super::*;

#[tokio::test]
async fn swap_admission_bounds_changed_proofs_and_keeps_large_originals() {
    for large in [false, true] {
        let f = Fixture::new().await;
        f.db.configure_storage_capacity(128 * 1024 * 1024)
            .await
            .unwrap();
        let mut intent = saga(80);
        f.db.add_mint(intent.mint_url.clone(), None).await.unwrap();
        let keyset = super::output_keys::keys();
        let mut input = crate::wallet::proof_changes::proof();
        input.proof.amount = 1.into();
        if large {
            input.proof.witness = Some(cdk_common::nuts::Witness::P2PKWitness(
                cdk_common::nuts::P2PKWitness {
                    signatures: vec!["a".repeat(5 * 1024 * 1024)],
                },
            ));
        }
        let mut acquired = input.clone();
        acquired.state = State::Reserved;
        acquired.used_by_operation = Some(intent.id);
        let key = SecretKey::generate();
        let secret = PreMint {
            blinded_message: BlindedMessage::new(1.into(), keyset.id, key.public_key()),
            secret: Secret::generate(),
            r: key,
            amount: 1.into(),
            derivation_index: Some(0),
        };
        let OperationData::Swap(data) = &mut intent.data else {
            unreachable!()
        };
        data.checkpoint = Some(Box::new(SwapCheckpoint {
            acquisition: vec![ProofChange {
                y: input.y,
                before: Some(input.clone()),
                after: Some(acquired),
            }],
            pre_swap: PreSwap {
                pre_mint_secrets: PreMintSecrets {
                    secrets: vec![secret.clone()],
                    keyset_id: keyset.id,
                },
                swap_request: SwapRequest::new(
                    vec![input.proof.clone()],
                    vec![secret.blinded_message],
                ),
                derived_secret_count: 1,
                fee: 0.into(),
                p2bk_secret_keys: None,
            },
            amount: None,
            amount_split_target: SplitTarget::default(),
            spending_conditions: None,
            ready: None,
        }));
        f.db.update_proofs(vec![input.clone()], vec![])
            .await
            .unwrap();
        if !large {
            let oversized = "x".repeat(4 * 1024 * 1024);
            f.raw
                .execute(
                    "UPDATE proof SET witness=?1 WHERE y=?2",
                    rusqlite::params![oversized, input.y.to_bytes().to_vec()],
                )
                .unwrap();
            let retained = f.db.storage_capacity().await.unwrap();
            let error =
                f.db.prepare_saga_outputs(None, intent.clone(), keyset.clone(), 0, 1, None)
                    .await
                    .unwrap_err();
            assert!(
                matches!(error, Error::ProofPageTooLarge { bytes, limit }
                if bytes > limit && limit == 4 * 1024 * 1024),
                "swap admission must bound unexpected evidence before decoding: {error}"
            );
            assert_eq!(f.db.storage_capacity().await.unwrap(), retained);
            assert!(f.db.get_saga(&intent.id).await.unwrap().is_none());
            assert!(f.db.get_keys(&keyset.id).await.unwrap().is_none());
            let counters: u64 = f
                .raw
                .query_row("SELECT count(*) FROM keyset_counter", [], |row| row.get(0))
                .unwrap();
            assert_eq!(counters, 0);
            let saved: String = f
                .raw
                .query_row(
                    "SELECT witness FROM proof WHERE y=?1",
                    [input.y.to_bytes().to_vec()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(saved, oversized);
            f.db.update_proofs(vec![input.clone()], vec![])
                .await
                .unwrap();
        }
        assert!(f
            .db
            .prepare_saga_outputs(None, intent.clone(), keyset.clone(), 0, 1, None)
            .await
            .unwrap());
        assert_eq!(f.db.get_saga(&intent.id).await.unwrap(), Some(intent));
        assert_eq!(
            f.db.get_proofs_by_ys(vec![input.y]).await.unwrap(),
            vec![input]
        );
        assert_eq!(f.db.get_keys(&keyset.id).await.unwrap(), Some(keyset.keys));
        let counter: u64 = f
            .raw
            .query_row(
                "SELECT counter FROM keyset_counter WHERE keyset_id=?1",
                [keyset.id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(counter, 1);
    }
}
