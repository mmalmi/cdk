//! Bounded recovery crosses the foreign storage bridge in both directions.
use cdk_common::database::wallet::MAX_MINT_QUOTE_PAGE_BYTES;
use cdk_common::database::Error;
use cdk_common::mint_url::MintUrl;
use cdk_common::wallet::MintQuote;
use cdk_common::{CurrencyUnit, PaymentMethod};

use super::{CdkWalletDatabase, FfiWalletDatabaseWrapper, WalletDatabaseBridge};

#[tokio::test]
async fn quote_page_preserves_scope_cursor_and_size_retry() {
    let mint: MintUrl = "https://selected.example".parse().unwrap();
    let other: MintUrl = "https://other.example".parse().unwrap();
    let db = cdk_sqlite::wallet::memory::empty().await.unwrap();
    let large_request = "x".repeat((MAX_MINT_QUOTE_PAGE_BYTES / 2) as usize);
    for (id, url, unit) in [
        ("a", mint.clone(), CurrencyUnit::Sat),
        ("b", other, CurrencyUnit::Sat),
        ("c", mint.clone(), CurrencyUnit::Msat),
        ("d", mint.clone(), CurrencyUnit::Sat),
    ] {
        db.add_mint_quote(MintQuote::new(
            id.to_owned(),
            url,
            PaymentMethod::BOLT11,
            Some(8.into()),
            unit,
            large_request.clone(),
            0,
            None,
        ))
        .await
        .unwrap();
    }
    let bridge = WalletDatabaseBridge::new(FfiWalletDatabaseWrapper::new(db));
    let result = bridge
        .get_unissued_mint_quotes_page(mint.clone(), CurrencyUnit::Sat, None, 2)
        .await;
    assert!(
        matches!(result, Err(Error::MintQuotePageTooLarge {bytes, limit}) if bytes > limit && limit == MAX_MINT_QUOTE_PAGE_BYTES)
    );
    for (after, id) in [(None, "a"), (Some("a".to_owned()), "d")] {
        let page = bridge
            .get_unissued_mint_quotes_page(mint.clone(), CurrencyUnit::Sat, after, 1)
            .await
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].id, id);
    }
    assert!(bridge
        .get_unissued_mint_quotes_page(mint, CurrencyUnit::Sat, Some("d".to_owned()), 1)
        .await
        .unwrap()
        .is_empty());
}
