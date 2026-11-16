use super::*;

#[test]
fn count_matches_greedy_splits_and_representation_errors() {
    for denominations in [
        vec![],
        vec![0, 1],
        vec![1],
        vec![1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024],
        vec![2, 6],
        vec![1, 3, 10],
        vec![1, 1, 3],
    ] {
        let keyset = FeeAndAmounts::from((0, denominations));
        for value in 0..=1024 {
            let amount = Amount::from(value);
            match (
                amount.split_count(&keyset),
                amount.split(&keyset),
                amount.split_counts(&keyset),
            ) {
                (Ok(count), Ok(parts), Ok(counts)) => {
                    assert_eq!(count, parts.len() as u64);
                    assert_eq!(Amount::try_sum(parts.iter().copied()).unwrap(), amount);
                    assert_eq!(counts.iter().map(|(_, count)| count).sum::<u64>(), count);
                    let expanded: Vec<_> = counts
                        .into_iter()
                        .flat_map(|(part, count)| (0..count).map(move |_| part))
                        .collect();
                    assert_eq!(expanded, parts);
                }
                (Err(count), Err(split), Err(counts)) => {
                    assert_eq!(count.to_string(), split.to_string());
                    assert_eq!(counts.to_string(), split.to_string());
                }
                other => panic!("different count/split result for {value}: {other:?}"),
            }
        }
    }
}

#[test]
fn counts_full_width_amounts_without_expanding_outputs() {
    let ones = FeeAndAmounts::from((0, vec![1]));
    assert_eq!(Amount::from(u64::MAX).split_count(&ones).unwrap(), u64::MAX);
    assert_eq!(
        Amount::from(u64::MAX).split_counts(&ones).unwrap(),
        vec![(Amount::ONE, u64::MAX)]
    );
    let binary = FeeAndAmounts::from((0, (0..64).map(|bit| 1u64 << bit).collect()));
    assert_eq!(Amount::from(u64::MAX).split_count(&binary).unwrap(), 64);
    let even = FeeAndAmounts::from((0, vec![2]));
    assert!(matches!(
        Amount::from(u64::MAX).split_count(&even),
        Err(Error::CannotSplitAmount(u64::MAX, represented)) if represented == u64::MAX - 1
    ));
}

#[test]
fn targeted_counts_include_repeated_values_and_full_width_remainders() {
    use crate::amount::SplitTarget;

    let ones = FeeAndAmounts::from((0, vec![1]));
    let full_width = FeeAndAmounts::from((0, vec![1, u64::MAX - 1]));
    assert_eq!(
        Amount::from(u64::MAX)
            .split_targeted(&SplitTarget::Value((u64::MAX - 1).into()), &full_width)
            .unwrap(),
        vec![Amount::ONE, Amount::from(u64::MAX - 1)]
    );
    assert_eq!(
        Amount::from(u64::MAX)
            .split_targeted_count(&SplitTarget::Value(4.into()), &ones)
            .unwrap(),
        u64::MAX
    );
    assert_eq!(
        Amount::from(23)
            .split_targeted_count(&SplitTarget::Value(5.into()), &(0, vec![1, 2, 8]).into())
            .unwrap(),
        14
    );
    assert_eq!(
        Amount::from(8)
            .split_targeted_count(
                &SplitTarget::Values(vec![0.into(), 8.into()]),
                &(0, vec![]).into()
            )
            .unwrap(),
        2
    );
}
