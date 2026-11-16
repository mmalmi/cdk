use super::*;

#[test]
fn split_with_fee_matches_exhaustive_greedy_oracle() {
    for denominations in [
        vec![1],
        vec![2],
        vec![1, 2, 4, 8],
        vec![1, 3, 5],
        vec![3, 5],
    ] {
        for fee in [0, 100, 500, 999, 1000, 1500, 3000, 5000] {
            let keyset = FeeAndAmounts::from((fee, denominations.clone()));
            for requested in 0u64..=20 {
                let expected = (requested..=128).find(|&value| {
                    let Ok(parts) = Amount::from(value).split(&keyset) else {
                        return false;
                    };
                    value
                        .checked_sub((parts.len() as u64 * fee).div_ceil(1000))
                        .is_some_and(|net| net >= requested)
                });
                let result = Amount::from(requested).split_total_with_fee(&keyset);
                match expected {
                    Some(value) => {
                        assert_eq!(result.unwrap(), Amount::from(value));
                        let parts = Amount::from(requested).split_with_fee(&keyset).unwrap();
                        assert_eq!(
                            Amount::try_sum(parts.iter().copied()).unwrap(),
                            Amount::from(value)
                        );
                        assert!(value - (parts.len() as u64 * fee).div_ceil(1000) >= requested);
                    }
                    None => assert!(result.is_err() || result.unwrap() > Amount::from(128)),
                }
            }
        }
    }
}

#[test]
fn split_with_fee_handles_full_width_counts_and_infeasible_fees() {
    let ones = FeeAndAmounts::from((500, vec![1]));
    assert_eq!(
        Amount::from(u64::MAX / 2)
            .split_total_with_fee(&ones)
            .unwrap(),
        Amount::from(u64::MAX - 1)
    );
    assert!(matches!(
        Amount::from(u64::MAX / 2 + 1).split_total_with_fee(&ones),
        Err(Error::AmountOverflow)
    ));
    for fee in [1000, u64::MAX] {
        assert!(matches!(
            Amount::ONE.split_total_with_fee(&FeeAndAmounts::from((fee, vec![1]))),
            Err(Error::AmountOverflow)
        ));
    }
    assert!(matches!(
        Amount::ONE.split_total_with_fee(&FeeAndAmounts::from((0, vec![0, 1]))),
        Err(Error::InvalidAmount(_))
    ));
}
