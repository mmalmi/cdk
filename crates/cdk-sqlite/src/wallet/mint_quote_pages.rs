//! Pending quote pages preserve recovery evidence without decoding other scopes.
use cdk_common::database::wallet::{MAX_MINT_QUOTE_PAGE_BYTES, MAX_MINT_QUOTE_PAGE_SIZE};
use cdk_common::database::{Error, WalletDatabase};
use cdk_common::mint_url::MintUrl;
use cdk_common::wallet::MintQuote;
use cdk_common::{CurrencyUnit, PaymentMethod};

use super::test_db::{open, Fixture};
use super::WalletSqliteDatabase;

fn mint() -> MintUrl {
    "https://selected.example".parse().unwrap()
}

fn quote(index: u16) -> MintQuote {
    MintQuote::new(
        format!("quote-{index:04}"),
        mint(),
        PaymentMethod::BOLT11,
        Some(8.into()),
        CurrencyUnit::Sat,
        "request".to_owned(),
        0,
        None,
    )
}

async fn page(
    db: &WalletSqliteDatabase,
    after: Option<String>,
    limit: u16,
) -> Result<Vec<MintQuote>, Error> {
    db.get_unissued_mint_quotes_page(mint(), CurrencyUnit::Sat, after, limit)
        .await
}

#[tokio::test]
async fn mint_quote_pages_preserve_scope_order_and_deleted_cursor() {
    let f = Fixture::new().await;
    let mut expected = Vec::new();
    for index in 0..259 {
        let mut q = quote(index);
        if index % 3 == 0 {
            q.amount_issued = 8.into();
        }
        if index % 5 == 0 {
            q.payment_method = PaymentMethod::BOLT12;
        }
        if q.amount_issued.to_u64() == 0 || q.payment_method == PaymentMethod::BOLT12 {
            expected.push(q.clone());
        }
        f.db.add_mint_quote(q).await.unwrap();
    }
    for (id, other_mint) in [("foreign-mint", true), ("foreign-unit", false)] {
        let mut foreign = quote(0);
        foreign.id = id.to_owned();
        if other_mint {
            foreign.mint_url = "https://other.example".parse().unwrap();
        } else {
            foreign.unit = CurrencyUnit::Msat;
        }
        f.db.add_mint_quote(foreign).await.unwrap();
        f.raw
            .execute("UPDATE mint_quote SET state='malformed' WHERE id=?1", [id])
            .unwrap();
    }
    let first = page(&f.db, None, MAX_MINT_QUOTE_PAGE_SIZE).await.unwrap();
    assert_eq!(first, expected[..128]);
    let cursor = first.last().unwrap().id.clone();
    f.raw
        .execute("DELETE FROM mint_quote WHERE id=?1", [&cursor])
        .unwrap();
    let reopened = open(&f.path).await;
    let rest = page(&reopened, Some(cursor), MAX_MINT_QUOTE_PAGE_SIZE)
        .await
        .unwrap();
    assert_eq!(rest, expected[128..]);
    assert!(page(&reopened, Some(rest.last().unwrap().id.clone()), 128)
        .await
        .unwrap()
        .is_empty());
    for limit in [0, MAX_MINT_QUOTE_PAGE_SIZE + 1] {
        assert!(page(&f.db, None, limit).await.is_err());
    }
}

#[tokio::test]
async fn mint_quote_pages_reject_bytes_before_decoding_without_writes() {
    let f = Fixture::new().await;
    for index in 0..2 {
        f.db.add_mint_quote(quote(index)).await.unwrap();
    }
    f.raw
        .execute(
            "UPDATE mint_quote SET request=?1",
            ["x".repeat(3 * 1024 * 1024)],
        )
        .unwrap();
    assert!(matches!(
        page(&f.db, None, 2).await,
        Err(Error::MintQuotePageTooLarge { .. })
    ));
    assert_eq!(page(&f.db, None, 1).await.unwrap().len(), 1);
    f.raw
        .execute(
            "UPDATE mint_quote SET state='malformed', request=?1 WHERE id='quote-0001'",
            ["x".repeat(MAX_MINT_QUOTE_PAGE_BYTES as usize)],
        )
        .unwrap();
    let after = Some("quote-0000".to_owned());
    assert!(matches!(
        page(&f.db, after.clone(), 1).await,
        Err(Error::MintQuotePageTooLarge { .. })
    ));
    f.raw
        .execute(
            "UPDATE mint_quote SET request='small' WHERE id='quote-0001'",
            [],
        )
        .unwrap();
    let error = page(&f.db, after, 1).await.unwrap_err();
    assert!(!matches!(error, Error::MintQuotePageTooLarge { .. }));
    assert_eq!(
        f.raw
            .query_row(
                "SELECT state FROM mint_quote WHERE id='quote-0001'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "malformed"
    );
    assert_eq!(
        f.raw
            .query_row("SELECT count(*) FROM mint_quote", [], |r| r
                .get::<_, u64>(0))
            .unwrap(),
        2
    );
}
