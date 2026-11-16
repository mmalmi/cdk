use super::Table;

const TABLES: [(&str, &str); 4] = [
    (
        "wallet_storage_total",
        "CREATE TABLE wallet_storage_total(
        id INTEGER PRIMARY KEY CHECK(id=1),
        version INTEGER NOT NULL CHECK(version=1),
        maximum INTEGER NOT NULL CHECK(typeof(maximum)='integer' AND maximum>=0),
        charged INTEGER NOT NULL CONSTRAINT wallet_storage_capacity
            CHECK(typeof(charged)='integer' AND charged>=0 AND charged<=maximum))",
    ),
    (
        "wallet_storage_accounts",
        "CREATE TABLE wallet_storage_accounts(
        owner TEXT PRIMARY KEY NOT NULL,
        used INTEGER NOT NULL CHECK(typeof(used)='integer' AND used>=0),
        reserved INTEGER NOT NULL CHECK(typeof(reserved)='integer' AND reserved>=0),
        charged INTEGER GENERATED ALWAYS AS(
            64+length(CAST(owner AS BLOB))+max(used,reserved)) STORED,
        CONSTRAINT wallet_storage_reservation CHECK(reserved=0 OR used<=reserved))",
    ),
    (
        "wallet_storage_objects",
        "CREATE TABLE wallet_storage_objects(
        object_key TEXT PRIMARY KEY NOT NULL,
        owner TEXT NOT NULL,
        payload_bytes INTEGER NOT NULL CHECK(typeof(payload_bytes)='integer' AND payload_bytes>=0),
        charged INTEGER GENERATED ALWAYS AS(
            48+length(CAST(object_key AS BLOB))+length(CAST(owner AS BLOB))+payload_bytes) STORED)",
    ),
    (
        "wallet_storage_future_by_owner",
        "CREATE INDEX wallet_storage_future_by_owner
        ON wallet_storage_objects(owner) WHERE payload_bytes=0",
    ),
];

/// Byte constants include logical row/accounting metadata, not SQLite pages.
pub(super) fn definitions(tables: &[Table]) -> Vec<(String, String)> {
    let mut sql = TABLES
        .iter()
        .map(|(name, statement)| (name.to_string(), statement.to_string()))
        .collect::<Vec<_>>();
    for (event, change) in [
        ("INSERT", "+NEW.charged"),
        ("UPDATE", "-OLD.charged+NEW.charged"),
        ("DELETE", "-OLD.charged"),
    ] {
        let name = format!("wallet_storage_account_{event}");
        sql.push((
            name.clone(),
            format!(
                "CREATE TRIGGER {name} AFTER {event} ON wallet_storage_accounts BEGIN
            UPDATE wallet_storage_total SET charged=charged{change}; END"
            ),
        ));
    }
    let add =
        "INSERT INTO wallet_storage_accounts(owner,used,reserved) VALUES(NEW.owner,NEW.charged,0)
        ON CONFLICT(owner) DO UPDATE SET used=used+excluded.used;";
    let subtract =
        "UPDATE wallet_storage_accounts SET used=used-OLD.charged WHERE owner=OLD.owner;";
    let remove_empty =
        "DELETE FROM wallet_storage_accounts WHERE owner=OLD.owner AND used=0 AND reserved=0;";
    for (event, body) in [
        ("INSERT", add.to_owned()),
        ("UPDATE", format!("{subtract}{remove_empty}{add}")),
        ("DELETE", format!("{subtract}{remove_empty}")),
    ] {
        let name = format!("wallet_storage_object_{event}");
        sql.push((
            name.clone(),
            format!(
                "CREATE TRIGGER {name} AFTER {event} ON wallet_storage_objects BEGIN
            {body} END"
            ),
        ));
    }
    for table in tables {
        let key = table.key("NEW.");
        let old = table.key("OLD.");
        let size = table.size("NEW.");
        for (event, body) in [
            ("INSERT", format!("INSERT INTO wallet_storage_objects(object_key,owner,payload_bytes) VALUES({key},{},{size})
                ON CONFLICT(object_key) DO UPDATE SET payload_bytes=excluded.payload_bytes;", table.owner("NEW."))),
            ("UPDATE", format!("SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM wallet_storage_objects WHERE object_key={old})
                THEN RAISE(ABORT,'missing storage ownership') END;
                UPDATE wallet_storage_objects SET object_key={key},payload_bytes={size} WHERE object_key={old};")),
            ("DELETE", format!("SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM wallet_storage_objects WHERE object_key={old})
                THEN RAISE(ABORT,'missing storage ownership') END;
                DELETE FROM wallet_storage_objects WHERE object_key={old};")),
        ] {
            let name = format!("wallet_storage_{}_{event}", table.name);
            sql.push((name.clone(), format!("CREATE TRIGGER {name} AFTER {event} ON \"{}\" BEGIN {body} END", table.name)));
        }
    }
    sql
}
