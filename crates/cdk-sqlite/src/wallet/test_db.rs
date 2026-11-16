use std::path::PathBuf;

use super::WalletSqliteDatabase;

pub(super) struct Fixture {
    pub path: PathBuf,
    pub db: WalletSqliteDatabase,
    pub raw: rusqlite::Connection,
}

impl Fixture {
    pub async fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("cdk-wallet-page-{}.sqlite", uuid::Uuid::new_v4()));
        let db = open(&path).await;
        let raw = rusqlite::Connection::open(&path).unwrap();
        #[cfg(feature = "sqlcipher")]
        raw.pragma_update(None, "key", "page-test").unwrap();
        Self { path, db, raw }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub(super) async fn open(path: &std::path::Path) -> WalletSqliteDatabase {
    #[cfg(not(feature = "sqlcipher"))]
    let config = path.to_path_buf();
    #[cfg(feature = "sqlcipher")]
    let config = (path.to_path_buf(), "page-test".to_owned());
    WalletSqliteDatabase::new(config).await.unwrap()
}
