//! Stored-byte accounting shared by bounded wallet reads.
use cdk_common::database::Error;

use crate::database::DatabaseExecutor;
use crate::stmt::{Column, Statement};

/// Measure the production scalar bindings with the same byte lengths as SQL reads.
pub(super) fn encoded_payload_bytes(
    columns: &[&str],
    statement: &Statement,
    overhead: u64,
) -> Result<u64, Error> {
    columns.iter().try_fold(overhead, |total, name| {
        let bytes = match statement.bound_value(name)? {
            Column::Null => 0,
            Column::Integer(value) => value.to_string().len(),
            Column::Text(text) => text.len(),
            Column::Blob(bytes) => bytes.len(),
            Column::Real(_) => return Err(Error::InvalidDbResponse),
        };
        total
            .checked_add(u64::try_from(bytes).map_err(|_| Error::AmountOverflow)?)
            .ok_or(Error::AmountOverflow)
    })
}

pub(super) fn payload_lengths<T>(columns: &[&str], numeric: &[&str]) -> Result<String, Error>
where
    T: DatabaseExecutor,
{
    columns
        .iter()
        .map(|column| {
            let length = match T::name() {
                "sqlite" => format!("length(CAST({column} AS BLOB))"),
                "postgres" if numeric.contains(column) => {
                    format!("octet_length(CAST({column} AS TEXT))")
                }
                "postgres" => format!("octet_length({column})"),
                _ => {
                    return Err(Error::Internal(
                        "Unsupported wallet read dialect".to_owned(),
                    ))
                }
            };
            Ok(format!("CAST(COALESCE({length}, 0) AS BIGINT)"))
        })
        .collect::<Result<Vec<_>, Error>>()
        .map(|lengths| lengths.join(" + "))
}
