//! Retain the legacy recovery fixture keyset and share wallet proof construction.
use std::str::FromStr;

use cdk_common::mint_url::MintUrl;
use cdk_common::nuts::{Id, State};
use cdk_common::wallet::ProofInfo;

pub use crate::wallet::test_utils::{create_test_db, test_mint_url};

pub fn test_keyset_id() -> Id {
    Id::from_str("00916bbf7ef91a36").unwrap()
}

pub fn test_proof_info(keyset_id: Id, amount: u64, mint_url: MintUrl, state: State) -> ProofInfo {
    let mut proof = crate::wallet::test_utils::test_proof_info(keyset_id, amount, mint_url);
    proof.state = state;
    proof
}
