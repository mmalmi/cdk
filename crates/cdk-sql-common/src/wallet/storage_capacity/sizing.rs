//! Size the same encoded scalar values the production row writers bind.
use cdk_common::database::Error;
use cdk_common::wallet::{ProofInfo, WalletSaga};
use cdk_common::{PreMint, Proof, ProofDleq, State};
use uuid::Uuid;

use super::{schema, WalletStorageClaim};
use crate::database::DatabaseExecutor;
use crate::keyvalue;
use crate::pool::DatabasePool;
use crate::stmt::{Column, Statement};
use crate::wallet::{keysets, SQLWalletDatabase};

pub(super) struct Sizing {
    tables: Vec<schema::Table>,
    owner: String,
}

impl Sizing {
    pub(super) async fn new<T>(conn: &T, owner: String) -> Result<Self, Error>
    where
        T: DatabaseExecutor,
    {
        let tables = schema::catalog(conn).await?;
        for (name, columns) in [
            ("proof", crate::wallet::proofs::COLUMNS.as_slice()),
            (
                "transactions",
                crate::wallet::transactions::COLUMNS.as_slice(),
            ),
            ("wallet_sagas", crate::wallet::sagas::COLUMNS.as_slice()),
            ("kv_store", keyvalue::COLUMNS.as_slice()),
            ("keyset", keysets::INFO_COLUMNS.as_slice()),
            ("key", keysets::KEYS_COLUMNS.as_slice()),
            ("keyset_counter", keysets::COUNTER_COLUMNS.as_slice()),
        ] {
            if !tables
                .iter()
                .any(|table| table.matches_columns(name, columns))
            {
                return Err(schema::invalid());
            }
        }
        Ok(Self { tables, owner })
    }

    pub(super) fn charge(
        &self,
        table: &str,
        key: Vec<u8>,
        columns: &[&str],
        statement: Statement,
    ) -> Result<i64, Error> {
        let claim = WalletStorageClaim {
            table: table.to_owned(),
            primary_key: vec![key],
        };
        self.charge_claim(&claim, columns, statement)
    }

    fn charge_claim(
        &self,
        claim: &WalletStorageClaim,
        columns: &[&str],
        statement: Statement,
    ) -> Result<i64, Error> {
        let key = schema::claim_key(&self.tables, claim)?;
        // Match schema::Table::size and the object ledger's generated charge.
        let metadata =
            i64::try_from(48 + key.len() + self.owner.len()).map_err(|_| Error::AmountOverflow)?;
        payload(columns, statement)?
            .checked_add(metadata)
            .ok_or(Error::AmountOverflow)
    }
}

pub(super) fn payload(columns: &[&str], statement: Statement) -> Result<i64, Error> {
    columns.iter().try_fold(32i64, |total, name| {
        let bytes = match statement.bound_value(name)? {
            Column::Null => 0,
            Column::Integer(_) | Column::Real(_) => 8,
            Column::Text(text) => text.len(),
            Column::Blob(bytes) => bytes.len(),
        };
        total
            .checked_add(i64::try_from(bytes).map_err(|_| Error::AmountOverflow)?)
            .ok_or(Error::AmountOverflow)
    })
}

/// A sizing-only output with exact secrets and maximal fixed-width signatures.
pub(in crate::wallet) fn output(
    saga: &WalletSaga,
    secret: &PreMint,
    state: State,
) -> Result<ProofInfo, Error> {
    // Placeholder points/scalars never enter storage or a mint request. DLEQ
    // and P2BK evidence have fixed widths; absent evidence is always smaller.
    let point = secret.r.public_key();
    let mut proof = Proof::new(
        secret.amount,
        secret.blinded_message.keyset_id,
        secret.secret.clone(),
        point,
    );
    proof.dleq = Some(ProofDleq::new(
        secret.r.clone(),
        secret.r.clone(),
        secret.r.clone(),
    ));
    proof.p2pk_e = Some(point);
    let mut info = ProofInfo::new(proof, saga.mint_url.clone(), state, saga.unit.clone())
        .map_err(|e| Error::Internal(e.to_string()))?;
    info.derivation_index = secret.derivation_index;
    info.created_by_operation = Some(saga.id);
    Ok(info)
}

impl<RM> SQLWalletDatabase<RM>
where
    RM: DatabasePool + 'static,
{
    pub(super) async fn record_sizing(&self, owner: Uuid) -> Result<Sizing, Error> {
        Self::require_sqlite_capacity()?;
        if owner.is_nil() {
            return Err(Error::InvalidDbResponse);
        }
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Database(Box::new(e)))?;
        if !schema::enabled(&*conn).await? {
            return Err(schema::invalid());
        }
        Sizing::new(&*conn, owner.to_string()).await
    }

    /// Charge for one exact proof row and its storage-ownership metadata.
    ///
    /// Uses the production SQL encoder. The caller must account for every
    /// possible future proof shape and quantity before reserving an obligation.
    /// This read-only sizing helper does not reserve space or include the owner's
    /// account row, which capacity accounting charges separately.
    #[tracing::instrument(skip_all)]
    pub async fn proof_storage_bytes(&self, owner: Uuid, proof: ProofInfo) -> Result<u64, Error> {
        let size = self.record_sizing(owner).await?;
        let bytes = size.charge(
            "proof",
            proof.y.to_bytes().to_vec(),
            &crate::wallet::proofs::COLUMNS,
            crate::wallet::proofs::statement(proof, false)?,
        )?;
        u64::try_from(bytes).map_err(|_| Error::AmountOverflow)
    }

    /// Charge for a key-value row with a bounded future value length.
    ///
    /// Measures the production row encoding and storage-ownership metadata
    /// without allocating the future value. This neither reserves capacity nor
    /// changes the row; callers must enforce `value_bytes` on every later write.
    #[tracing::instrument(skip_all)]
    pub async fn kv_storage_bytes(
        &self,
        owner: Uuid,
        primary_namespace: &str,
        secondary_namespace: &str,
        key: &str,
        value_bytes: u64,
    ) -> Result<u64, Error> {
        let size = self.record_sizing(owner).await?;
        let claim = WalletStorageClaim {
            table: "kv_store".to_owned(),
            primary_key: [primary_namespace, secondary_namespace, key]
                .map(|part| part.as_bytes().to_vec())
                .to_vec(),
        };
        let bytes = size
            .charge_claim(
                &claim,
                &keyvalue::COLUMNS,
                keyvalue::write_statement(primary_namespace, secondary_namespace, key, &[])?,
            )?
            .checked_add(i64::try_from(value_bytes).map_err(|_| Error::AmountOverflow)?)
            .ok_or(Error::AmountOverflow)?;
        u64::try_from(bytes).map_err(|_| Error::AmountOverflow)
    }
}
