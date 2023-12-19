use std::{
    collections::{HashMap, HashSet},
    ops::MulAssign,
    str::FromStr,
};

use alloy_rlp::{Decodable, Encodable};
pub use brontes_types::extra_processing::Pair;
use bytes::BufMut;
use malachite::Rational;
use reth_db::{
    table::{Compress, Decompress},
    DatabaseError,
};
use reth_primitives::{Address, TxHash, U256};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use sorella_db_databases::{clickhouse, Row};

use super::{
    utils::{address, option_address, u256},
    LibmdbxData,
};
use crate::tables::Metadata;

#[serde_as]
#[derive(Debug, Clone, Row, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataData {
    pub block_number: u64,
    //#[serde(flatten)]
    pub inner:        MetadataInner,
}

impl LibmdbxData<Metadata> for MetadataData {
    fn into_key_val(
        &self,
    ) -> (<Metadata as reth_db::table::Table>::Key, <Metadata as reth_db::table::Table>::Value)
    {
        (self.block_number, self.inner.clone())
    }
}

#[serde_as]
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataInner {
    #[serde(with = "u256")]
    pub block_hash:             U256,
    pub relay_timestamp:        u64,
    pub p2p_timestamp:          u64,
    #[serde(with = "option_address")]
    pub proposer_fee_recipient: Option<Address>,
    pub proposer_mev_reward:    u128,
    #[serde_as(as = "Vec<DisplayFromStr>")]
    pub mempool_flow:           Vec<TxHash>,
}

impl Encodable for MetadataInner {
    fn encode(&self, out: &mut dyn BufMut) {
        self.block_hash.encode(out);
        self.relay_timestamp.encode(out);
        self.p2p_timestamp.encode(out);
        self.proposer_fee_recipient.unwrap_or_default().encode(out);
        self.proposer_mev_reward.encode(out);
        self.mempool_flow.encode(out);
    }
}

impl Decodable for MetadataInner {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let block_hash = U256::decode(buf)?;
        let relay_timestamp = u64::decode(buf)?;
        let p2p_timestamp = u64::decode(buf)?;
        let mut proposer_fee_recipient = Some(Address::decode(buf)?);
        if proposer_fee_recipient.as_ref().unwrap().is_zero() {
            proposer_fee_recipient = None
        }
        let proposer_mev_reward = u128::decode(buf)?;
        let mempool_flow = Vec::<TxHash>::decode(buf)?;

        Ok(Self {
            block_hash,
            relay_timestamp,
            p2p_timestamp,
            proposer_fee_recipient,
            proposer_mev_reward,
            mempool_flow,
        })
    }
}

impl Compress for MetadataInner {
    type Compressed = Vec<u8>;

    fn compress_to_buf<B: reth_primitives::bytes::BufMut + AsMut<[u8]>>(self, buf: &mut B) {
        let mut encoded = Vec::new();
        self.encode(&mut encoded);
        buf.put_slice(&encoded);
    }
}

impl Decompress for MetadataInner {
    fn decompress<B: AsRef<[u8]>>(value: B) -> Result<Self, reth_db::DatabaseError> {
        let binding = value.as_ref().to_vec();
        let buf = &mut binding.as_slice();
        Ok(MetadataInner::decode(buf).map_err(|_| DatabaseError::Decode)?)
    }
}