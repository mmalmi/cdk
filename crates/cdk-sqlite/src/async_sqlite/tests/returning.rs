use cdk_sql_common::stmt::query;
use cdk_sql_common::value::Value;

use super::Fixture;

#[tokio::test]
async fn first_returned_value_requires_successful_statement_completion() {
    for whole_row in [false, true] {
        let fixture = Fixture::new().await;
        let conn = fixture.pool.get().await.unwrap();
        query("PRAGMA foreign_keys=ON;
            CREATE TABLE allowed(value INTEGER PRIMARY KEY);
            INSERT INTO allowed VALUES(1);
            CREATE TABLE counter(value INTEGER REFERENCES allowed(value) DEFERRABLE INITIALLY DEFERRED);")
            .unwrap().batch(&*conn).await.unwrap();
        let statement = query("INSERT INTO counter VALUES(1),(2) RETURNING value").unwrap();
        let rejected = if whole_row {
            statement.fetch_one(&*conn).await.is_err()
        } else {
            statement.pluck(&*conn).await.is_err()
        };
        assert!(rejected, "a returned value cannot hide a failed commit");
        assert_eq!(
            query("SELECT count(*) FROM counter")
                .unwrap()
                .pluck(&*conn)
                .await
                .unwrap(),
            Some(Value::Integer(0))
        );

        let statement = query("INSERT INTO counter VALUES(1),(1) RETURNING value").unwrap();
        if whole_row {
            assert_eq!(
                statement.fetch_one(&*conn).await.unwrap(),
                Some(vec![Value::Integer(1)])
            );
        } else {
            assert_eq!(
                statement.pluck(&*conn).await.unwrap(),
                Some(Value::Integer(1))
            );
        }
        assert_eq!(
            query("SELECT count(*) FROM counter")
                .unwrap()
                .pluck(&*conn)
                .await
                .unwrap(),
            Some(Value::Integer(2))
        );

        // Ordinary bounded reads must not evaluate later rows. Only a mutating
        // statement has to reach completion before returning its first value.
        let statement = query("SELECT 1 UNION ALL SELECT json_extract('invalid', '$')").unwrap();
        if whole_row {
            assert_eq!(
                statement.fetch_one(&*conn).await.unwrap(),
                Some(vec![Value::Integer(1)])
            );
        } else {
            assert_eq!(
                statement.pluck(&*conn).await.unwrap(),
                Some(Value::Integer(1))
            );
        }
    }
}
