//! Admit original issuance outputs and both terminal quote/receipt paths.
use cdk_common::database::wallet::{issue_receipts, validate_issue_acquisition};
use cdk_common::database::Error;
use cdk_common::wallet::{MintQuote, OperationData, TransactionStatus, WalletSaga};
use cdk_common::{MintQuoteState, State};

use super::{preparation, schema, sizing, WalletStorageClaim};
use crate::database::DatabaseExecutor;
use crate::wallet::{mint_quotes, proofs, transactions};

/// Quotes and receipts have already been checked under the acquisition transaction.
pub(in crate::wallet) async fn insert_initial_issue<T>(
    conn: &T,
    saga: &WalletSaga,
    quotes: &[MintQuote],
) -> Result<bool, Error>
where
    T: DatabaseExecutor,
{
    if !schema::enabled(conn).await? {
        return Ok(false);
    }
    let claims: Vec<_> = quotes
        .iter()
        .map(|quote| WalletStorageClaim {
            table: "mint_quote".to_owned(),
            primary_key: vec![quote.id.as_bytes().to_vec()],
        })
        .collect();
    preparation::insert_initial(conn, saga, &claims).await?;
    Ok(true)
}

pub(super) fn completion_growth(saga: &WalletSaga, size: &sizing::Sizing) -> Result<i64, Error> {
    let OperationData::Mint(data) = &saga.data else {
        return Err(Error::InvalidDbResponse);
    };
    let saved = data
        .original_request()
        .map_err(|_| Error::InvalidDbResponse)?;
    let reserved = validate_issue_acquisition(saga, &saved.quotes)?;
    let mut growth = 0i64;
    let mut add = |bytes| {
        growth = growth.checked_add(bytes).ok_or(Error::AmountOverflow)?;
        Ok::<_, Error>(())
    };
    for (original, mut acquired) in saved.quotes.iter().zip(reserved) {
        let initial = sizing::payload(
            &mint_quotes::ISSUE_COLUMNS,
            mint_quotes::issue_statement(original)?,
        )?;
        // Only these fields change. Keep ownership and the larger terminal state
        // together so quote updates fit independently of receipt/proof ordering.
        if acquired.state.to_string().len() < MintQuoteState::Issued.to_string().len() {
            acquired.state = MintQuoteState::Issued;
        }
        let peak = sizing::payload(
            &mint_quotes::ISSUE_COLUMNS,
            mint_quotes::issue_statement(&acquired)?,
        )?;
        add((peak - initial).max(0))?;
    }
    for secret in &saved.premint_secrets.secrets {
        let info = sizing::output(saga, secret, State::Unspent)?;
        add(size.charge(
            "proof",
            info.y.to_bytes().to_vec(),
            &proofs::COLUMNS,
            proofs::statement(info, false)?,
        )?)?;
    }
    for mut receipt in issue_receipts(saga)? {
        // Completed is longer than Pending or Failed; all other fields are exact.
        receipt.status = TransactionStatus::Completed;
        add(size.charge(
            "transactions",
            receipt.id().as_slice().to_vec(),
            &transactions::COLUMNS,
            transactions::statement(receipt)?,
        )?)?;
    }
    // SecretsPrepared is the largest issue state; execution retains this exact
    // checkpoint until atomic publication. SQL versions/timestamps are fixed width.
    Ok(growth)
}
