//! Verify read projections without a deployed Supabase service.
use cdk_common::database::wallet::SagaCursor;

use super::*;

#[tokio::test]
async fn saga_pages_project_headers_and_reject_ambiguous_replies() {
    let mut server = mockito::Server::new_async().await;
    let db = SupabaseWalletDatabase::new(Url::parse(&server.url()).unwrap(), "test-key".to_owned())
        .await
        .unwrap();
    let after = SagaCursor {
        created_at: 42,
        id: uuid::Uuid::from_u128(10),
    };
    let expected = vec![
        SagaCursor {
            id: uuid::Uuid::from_u128(11),
            ..after
        },
        SagaCursor {
            created_at: 43,
            id: after.id,
        },
    ];
    let good = serde_json::to_string(&expected).unwrap();
    let uppercase = good.replace("00000000000b", "00000000000B");
    for (body, valid) in [
        (good, true),
        (serde_json::to_string(&vec![after]).unwrap(), false),
        (serde_json::to_string(&vec![expected[0]; 3]).unwrap(), false),
        (uppercase, false),
        (
            format!(r#"[{{"id":"{}","created_at":-1}}]"#, after.id),
            false,
        ),
        (String::new(), false),
        ("[]".to_owned(), true),
    ] {
        let mock = server
            .mock("GET", "/rest/v1/saga")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("completed".to_owned(), "eq.false".to_owned()),
                mockito::Matcher::UrlEncoded("select".to_owned(), "id,created_at".to_owned()),
                mockito::Matcher::UrlEncoded(
                    "order".to_owned(),
                    "created_at.asc,id.asc".to_owned(),
                ),
                mockito::Matcher::UrlEncoded("limit".to_owned(), "2".to_owned()),
                mockito::Matcher::UrlEncoded(
                    "or".to_owned(),
                    format!(
                        "(created_at.gt.42,and(created_at.eq.42,id.gt.{}))",
                        after.id
                    ),
                ),
            ]))
            .with_status(200)
            .with_body(body.clone())
            .create_async()
            .await;
        let result = db.get_incomplete_saga_ids(Some(after), 2).await;
        assert_eq!(result.is_ok(), valid, "{result:?}");
        if valid {
            assert_eq!(
                result.unwrap(),
                if body == "[]" {
                    vec![]
                } else {
                    expected.clone()
                }
            );
        }
        mock.assert_async().await;
    }
}

#[tokio::test]
async fn zero_counter_increment_reads_without_an_rpc_write() {
    let mut server = mockito::Server::new_async().await;
    let db = SupabaseWalletDatabase::new(Url::parse(&server.url()).unwrap(), "test-key".to_owned())
        .await
        .unwrap();
    let id: Id = "0000000000000000".parse().unwrap();
    for (body, expected) in [
        ("[]", Some(0)),
        (r#"[{"counter":42}]"#, Some(42)),
        (r#"[{"counter":-1}]"#, None),
    ] {
        let mock = server
            .mock("GET", "/rest/v1/keyset_counter")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("keyset_id".to_owned(), format!("eq.{id}")),
                mockito::Matcher::UrlEncoded("select".to_owned(), "counter".to_owned()),
                mockito::Matcher::UrlEncoded("limit".to_owned(), "1".to_owned()),
            ]))
            .with_status(200)
            .with_body(body)
            .create_async()
            .await;
        assert_eq!(db.increment_keyset_counter(&id, 0).await.ok(), expected);
        mock.assert_async().await;
    }
}
