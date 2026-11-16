use super::*;

fn insert(proof: ProofInfo) -> ProofChange {
    ProofChange {
        y: proof.y,
        before: None,
        after: Some(proof),
    }
}

/// A proof write must be fenced by the complete saved operation, even when empty.
pub async fn compare_and_swap_proofs_saga_guard<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let proof = test_proof_info(test_keyset_id(), 8, test_mint_url());
    let saga = test_wallet_saga(test_mint_url());
    let changes = vec![insert(proof.clone())];
    assert!(!db
        .compare_and_swap_proofs(changes.clone(), Some(&saga))
        .await
        .unwrap());
    db.add_saga(saga.clone()).await.unwrap();
    let mut wrong = saga.clone();
    wrong.amount = 999.into();
    assert!(!db
        .compare_and_swap_proofs(changes.clone(), Some(&wrong))
        .await
        .unwrap());
    assert!(db.get_proofs_by_ys(vec![proof.y]).await.unwrap().is_empty());
    assert!(db
        .compare_and_swap_proofs(vec![], Some(&saga))
        .await
        .unwrap());
    let mut advanced = saga.clone();
    advanced.update_state(advanced.state);
    assert!(db.update_saga(advanced.clone()).await.unwrap());
    assert!(!db
        .compare_and_swap_proofs(changes.clone(), Some(&saga))
        .await
        .unwrap());
    assert!(db
        .compare_and_swap_proofs(changes, Some(&advanced))
        .await
        .unwrap());
    db.delete_saga(&saga.id).await.unwrap();
    assert!(!db
        .compare_and_swap_proofs(vec![], Some(&advanced))
        .await
        .unwrap());
    assert!(!db
        .compare_and_swap_proofs(
            vec![ProofChange {
                y: proof.y,
                before: Some(proof.clone()),
                after: None,
            }],
            Some(&advanced)
        )
        .await
        .unwrap());
    assert_eq!(db.get_proofs_by_ys(vec![proof.y]).await.unwrap(), [proof]);
}

/// Exact records and all-or-nothing comparisons protect other operations.
pub async fn compare_and_swap_proofs<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let first = test_proof_info(test_keyset_id(), 8, test_mint_url());
    let second = test_proof_info(test_keyset_id(), 4, test_mint_url());
    assert!(db.compare_and_swap_proofs(vec![], None).await.unwrap());
    assert!(db
        .compare_and_swap_proofs(vec![insert(first.clone()), insert(second.clone())], None)
        .await
        .unwrap());
    let mut spent = first.clone();
    spent.state = State::Spent;
    spent.used_by_operation = Some(uuid::Uuid::new_v4());
    let replace = ProofChange {
        y: first.y,
        before: Some(first.clone()),
        after: Some(spent.clone()),
    };
    let remove = ProofChange {
        y: second.y,
        before: Some(second.clone()),
        after: None,
    };
    // A failed later comparison must undo earlier changes in the same batch.
    let mut wrong = second.clone();
    wrong.created_by_operation = Some(uuid::Uuid::new_v4());
    let wrong_remove = ProofChange {
        before: Some(wrong),
        ..remove.clone()
    };
    assert!(!db
        .compare_and_swap_proofs(vec![replace.clone(), wrong_remove], None)
        .await
        .unwrap());
    assert_eq!(
        db.get_proofs_by_ys(vec![first.y]).await.unwrap(),
        std::slice::from_ref(&first)
    );
    assert_eq!(
        db.get_proofs_by_ys(vec![second.y]).await.unwrap(),
        std::slice::from_ref(&second)
    );
    assert!(db
        .compare_and_swap_proofs(vec![replace.clone(), remove], None)
        .await
        .unwrap());
    assert_eq!(
        db.get_proofs_by_ys(vec![first.y]).await.unwrap(),
        std::slice::from_ref(&spent)
    );
    assert!(db
        .get_proofs_by_ys(vec![second.y])
        .await
        .unwrap()
        .is_empty());
    // Stale recovery cannot resurrect a spent output, even if the other change is valid.
    assert!(!db
        .compare_and_swap_proofs(vec![insert(second.clone()), replace], None)
        .await
        .unwrap());
    assert!(db
        .get_proofs_by_ys(vec![second.y])
        .await
        .unwrap()
        .is_empty());
    let mut wrong_owner = spent.clone();
    wrong_owner.used_by_operation = None;
    assert!(!db
        .compare_and_swap_proofs(
            vec![ProofChange {
                y: first.y,
                before: Some(wrong_owner),
                after: None,
            }],
            None
        )
        .await
        .unwrap());
    assert_eq!(db.get_proofs_by_ys(vec![first.y]).await.unwrap(), [spent]);
    assert!(db
        .compare_and_swap_proofs(vec![insert(second.clone()), insert(second.clone())], None)
        .await
        .is_err());
    let mut invalid = insert(second.clone());
    invalid.y = first.y;
    assert!(db
        .compare_and_swap_proofs(vec![invalid], None)
        .await
        .is_err());
    assert!(db
        .get_proofs_by_ys(vec![second.y])
        .await
        .unwrap()
        .is_empty());
}

/// Racing writers cannot both claim an absent or existing coin record.
pub async fn compare_and_swap_proofs_concurrent<D>(db: D)
where
    D: Database<Error> + Send + Sync + 'static,
{
    let db = std::sync::Arc::new(db);
    let proof = test_proof_info(test_keyset_id(), 8, test_mint_url());
    let mut other = proof.clone();
    other.used_by_operation = Some(uuid::Uuid::new_v4());
    let (a, b) = race(db.clone(), insert(proof.clone()), insert(other)).await;
    assert_eq!(
        usize::from(a.unwrap_or(false)) + usize::from(b.unwrap_or(false)),
        1
    );
    let original = db.get_proofs_by_ys(vec![proof.y]).await.unwrap().remove(0);
    let mut spent = original.clone();
    spent.state = State::Spent;
    let update = ProofChange {
        y: proof.y,
        before: Some(original.clone()),
        after: Some(spent),
    };
    let remove = ProofChange {
        y: proof.y,
        before: Some(original),
        after: None,
    };
    let (a, b) = race(db.clone(), update, remove).await;
    assert_eq!(
        usize::from(a.unwrap_or(false)) + usize::from(b.unwrap_or(false)),
        1
    );
    assert!(db
        .get_proofs_by_ys(vec![proof.y])
        .await
        .unwrap()
        .iter()
        .all(|p| p.state == State::Spent));
}

async fn race<D>(
    db: std::sync::Arc<D>,
    a: ProofChange,
    b: ProofChange,
) -> (Result<bool, Error>, Result<bool, Error>)
where
    D: Database<Error> + Send + Sync + 'static,
{
    // Use two actual threads: Redb completes synchronously during a single
    // future poll, so join! on one executor thread would only test serialization.
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let start = |change| {
        let db = db.clone();
        let barrier = barrier.clone();
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            barrier.wait();
            runtime.block_on(db.compare_and_swap_proofs(vec![change], None))
        })
    };
    let a = start(a);
    let b = start(b);
    (a.await.unwrap(), b.await.unwrap())
}

/// Every stored field participates in the comparison and exact replacement.
pub async fn compare_and_swap_proofs_fields<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let mut original = test_proof_info(test_keyset_id(), 8, test_mint_url());
    original.derivation_index = Some(i32::MAX as u32);
    original.used_by_operation = Some(uuid::Uuid::new_v4());
    original.created_by_operation = Some(uuid::Uuid::new_v4());
    original.proof.p2pk_e = Some(SecretKey::generate().public_key());
    original.proof.dleq = Some(crate::ProofDleq::new(
        SecretKey::generate(),
        SecretKey::generate(),
        SecretKey::generate(),
    ));
    original.proof.witness = Some(crate::nuts::Witness::P2PKWitness(
        crate::nuts::nut11::P2PKWitness {
            signatures: vec!["test-signature".to_owned()],
        },
    ));
    original.spending_condition = Some(SpendingConditions::P2PKConditions {
        data: SecretKey::generate().public_key(),
        conditions: None,
    });
    assert!(db
        .compare_and_swap_proofs(vec![insert(original.clone())], None)
        .await
        .unwrap());
    assert_eq!(
        db.get_proofs_by_ys(vec![original.y]).await.unwrap(),
        [original.clone()]
    );
    // The ordinary update API retains an existing derivation index when omitted.
    // Exact replacement below must instead be able to clear that same field.
    let mut ordinary_update = original.clone();
    ordinary_update.derivation_index = None;
    db.update_proofs(vec![ordinary_update], vec![])
        .await
        .unwrap();
    assert_eq!(
        db.get_proofs_by_ys(vec![original.y]).await.unwrap(),
        [original.clone()]
    );
    for field in [
        "amount",
        "keyset_id",
        "signature",
        "witness",
        "dleq",
        "p2pk_e",
        "mint_url",
        "state",
        "unit",
        "spending_condition",
        "used_by_operation",
        "created_by_operation",
        "derivation_index",
    ] {
        let mut wrong = original.clone();
        match field {
            "amount" => wrong.proof.amount = 16.into(),
            "keyset_id" => wrong.proof.keyset_id = test_keyset_id_2(),
            "signature" => wrong.proof.c = SecretKey::generate().public_key(),
            "witness" => wrong.proof.witness = None,
            "dleq" => wrong.proof.dleq = None,
            "p2pk_e" => wrong.proof.p2pk_e = None,
            "mint_url" => wrong.mint_url = test_mint_url_2(),
            "state" => wrong.state = State::Spent,
            "unit" => wrong.unit = CurrencyUnit::Msat,
            "spending_condition" => wrong.spending_condition = None,
            "used_by_operation" => wrong.used_by_operation = None,
            "created_by_operation" => wrong.created_by_operation = None,
            "derivation_index" => wrong.derivation_index = None,
            _ => unreachable!(),
        }
        assert!(
            !db.compare_and_swap_proofs(
                vec![ProofChange {
                    y: original.y,
                    before: Some(wrong),
                    after: None
                }],
                None
            )
            .await
            .unwrap(),
            "{field} must match"
        );
    }
    let mut replacement = original.clone();
    replacement.derivation_index = None;
    replacement.used_by_operation = None;
    replacement.created_by_operation = None;
    replacement.proof.witness = None;
    replacement.proof.dleq = None;
    replacement.proof.p2pk_e = None;
    replacement.spending_condition = None;
    assert!(db
        .compare_and_swap_proofs(
            vec![ProofChange {
                y: original.y,
                before: Some(original),
                after: Some(replacement.clone())
            }],
            None
        )
        .await
        .unwrap());
    assert_eq!(
        db.get_proofs_by_ys(vec![replacement.y]).await.unwrap(),
        [replacement]
    );
}
