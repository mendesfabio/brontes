CREATE TABLE mev.mev_blocks ON CLUSTER eth_cluster0
(
    `block_hash` String,
    `block_number` UInt64,
    `mev_count` Nested (
        `mev_count` UInt64,
        `sandwich_count` UInt64,
        `cex_dex_count` UInt64,
        `jit_count` UInt64,
        `jit_sandwich_count` UInt64,
        `atomic_backrun_count` UInt64,
        `liquidation_count` UInt64
    ),
    `eth_price` Float64,
    `cumulative_gas_used` UInt128,
    `cumulative_priority_fee` UInt128,
    `total_bribe` UInt128,
    `cumulative_mev_priority_fee_paid` UInt128,
    `builder_address` String,
    `builder_eth_profit` Float64,
    `builder_profit_usd` Float64,
    `builder_mev_profit_usd` Float64,
    `proposer_fee_recipient` Nullable(String),
    `proposer_mev_reward` Nullable(UInt128),
    `proposer_profit_usd` Nullable(Float64),
    `cumulative_mev_profit_usd` Float64,
    `possible_mev` Nested (
        `tx_hash` String,
        `tx_idx` UInt64,
        `gas_details.coinbase_transfer` Nullable(UInt128), 
        `gas_details.priority_fee` UInt128,
        `gas_details.gas_used` UInt128,
        `gas_details.effective_gas_price` UInt128,
        `triggers.is_private` Bool,
        `triggers.coinbase_transfer` Bool,
        `triggers.high_priority_fee` Bool
    ),
    `last_updated` UInt64 DEFAULT now()
) 
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/eth_cluster0/tables/all/mev/mev_blocks', '{replica}', `last_updated`)
PRIMARY KEY (`block_hash`, `block_number`)
ORDER BY (`block_hash`, `block_number`)