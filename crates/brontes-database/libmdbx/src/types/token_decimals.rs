use alloy_primitives::Address;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use sorella_db_databases::{clickhouse, Row};

use super::LibmbdxData;
use crate::{tables::TokenDecimals, types::utils::address_string};

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, Row)]
pub(crate) struct TokenDecimalsData {
    #[serde(with = "address_string")]
    address:  Address,
    decimals: u8,
}

impl LibmbdxData<TokenDecimals> for TokenDecimalsData {
    fn into_key_val(
        &self,
    ) -> (
        <TokenDecimals as reth_db::table::Table>::Key,
        <TokenDecimals as reth_db::table::Table>::Value,
    ) {
        (self.address, self.decimals)
    }
}
