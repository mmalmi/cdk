use super::*;

fn owned(operation: uuid::Uuid) -> ProofInfo {
    let mut proof = test_proof_info(test_keyset_id(), 8, test_mint_url());
    proof.created_by_operation = Some(operation);
    proof
}

/// Operation pages preserve exact records from every mint, unit and state.
pub async fn proofs_created_by_operation_pages<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let operation = uuid::Uuid::new_v4();
    let other = uuid::Uuid::new_v4();
    let mut expected: Vec<_> = (0..7).map(|_| owned(operation)).collect();
    expected[0].mint_url = MintUrl::from_str("https://other-mint.example").unwrap();
    expected[1].state = State::Spent;
    expected[2].state = State::Pending;
    expected[3].state = State::Reserved;
    expected[4].unit = CurrencyUnit::Msat;
    expected[5].used_by_operation = Some(other);
    expected.sort_by_key(|proof| proof.y.to_bytes());
    let mut inserted = expected.clone();
    inserted.reverse();
    let mut unrelated = owned(other);
    unrelated.used_by_operation = Some(operation);
    let mut imported = owned(operation);
    imported.created_by_operation = None;
    imported.used_by_operation = Some(operation);
    inserted.extend([unrelated, imported]);
    db.update_proofs(inserted, vec![]).await.unwrap();

    let mut after = None;
    let mut observed = Vec::new();
    for offset in (0..9).step_by(3) {
        let page = db
            .get_proofs_created_by_operation_page(operation, after, 3)
            .await
            .unwrap();
        let end = (offset + 3).min(expected.len());
        assert!(
            page == expected[offset..end],
            "page differs from exact rows"
        );
        after = page.last().map(|proof| proof.y);
        observed.extend(page);
    }
    assert!(observed == expected, "operation traversal changed evidence");
    assert!(db
        .get_proofs_created_by_operation_page(operation, after, 3)
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .get_proofs_created_by_operation_page(uuid::Uuid::new_v4(), None, 3)
        .await
        .unwrap()
        .is_empty());
}

/// Invalid sizes fail without writes; the maximum page does not truncate traversal.
pub async fn proofs_created_by_operation_page_limits<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let operation = uuid::Uuid::new_v4();
    let mut expected: Vec<_> = (0..=MAX_PROOF_PAGE_SIZE)
        .map(|_| owned(operation))
        .collect();
    expected.sort_by_key(|proof| proof.y.to_bytes());
    db.update_proofs(expected.clone(), vec![]).await.unwrap();
    for limit in [0, MAX_PROOF_PAGE_SIZE + 1, u16::MAX] {
        assert!(db
            .get_proofs_created_by_operation_page(operation, None, limit)
            .await
            .is_err());
    }
    let first = db
        .get_proofs_created_by_operation_page(operation, None, MAX_PROOF_PAGE_SIZE)
        .await
        .unwrap();
    let split = usize::from(MAX_PROOF_PAGE_SIZE);
    assert!(
        first == expected[..split],
        "maximum page differs from exact rows"
    );
    let last = db
        .get_proofs_created_by_operation_page(operation, Some(first[split - 1].y), 1)
        .await
        .unwrap();
    assert!(
        last == expected[split..],
        "last page differs from exact row"
    );
    assert!(db
        .get_proofs_created_by_operation_page(operation, Some(last[0].y), 1)
        .await
        .unwrap()
        .is_empty());
}

/// A seek cursor remains valid when earlier records, including the cursor, disappear.
pub async fn proofs_created_by_operation_cursor_survives_deletion<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let operation = uuid::Uuid::new_v4();
    let mut expected: Vec<_> = (0..5).map(|_| owned(operation)).collect();
    expected.sort_by_key(|proof| proof.y.to_bytes());
    db.update_proofs(expected.clone(), vec![]).await.unwrap();
    let first = db
        .get_proofs_created_by_operation_page(operation, None, 2)
        .await
        .unwrap();
    assert!(first == expected[..2], "first page differs from exact rows");
    let cursor = first[1].y;
    db.update_proofs(vec![], first.iter().map(|proof| proof.y).collect())
        .await
        .unwrap();
    let next = db
        .get_proofs_created_by_operation_page(operation, Some(cursor), 3)
        .await
        .unwrap();
    assert!(
        next == expected[2..],
        "deletion moved the exclusive seek boundary"
    );
}
