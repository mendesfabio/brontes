use brontes_macros::action_impl;
use brontes_pricing::Protocol;
use brontes_types::{
    normalized_actions::{
        NormalizedBurn, NormalizedMint, NormalizedNewPool,
        NormalizedSwap,
    },
    structured_trace::CallInfo,
    ToScaledRational,
};
use reth_primitives::U256;

action_impl!(
    Protocol::BalancerV3,
    crate::BalancerV3Vault::swapCall,
    Swap,
    [..],
    call_data: true,
    return_data: true,
    |info: CallInfo, call_data: swapCall, return_data: swapReturn, db: &DB| {
        let pool = call_data.vaultSwapParams.pool;
        let token_in = db.try_fetch_token_info(call_data.vaultSwapParams.tokenIn)?;
        let token_out = db.try_fetch_token_info(call_data.vaultSwapParams.tokenOut)?;
        let amount_in = return_data.amountIn.to_scaled_rational(token_in.decimals);
        let amount_out = return_data.amountOut.to_scaled_rational(token_out.decimals);

        Ok(NormalizedSwap {
            protocol: Protocol::BalancerV3,
            trace_index: info.trace_idx,
            from: info.from_address,
            recipient: info.from_address,
            pool,
            token_in,
            amount_in,
            token_out,
            amount_out,
            msg_value: U256::ZERO,
        })
    }
);

action_impl!(
    Protocol::BalancerV3,
    crate::BalancerV3Vault::addLiquidityCall,
    Mint,
    [..LiquidityAdded],
    call_data: true,
    logs: true,
    |info: CallInfo, call_data: addLiquidityCall, log_data: BalancerV3AddLiquidityCallLogs, db: &DB| {
        let logs = log_data.liquidity_added_field?;

        let mut tokens = Vec::new();
        let mut amounts = Vec::new();

        Ok(NormalizedMint {
            protocol: Protocol::BalancerV3,
            trace_index: info.trace_idx,
            from: info.from_address,
            recipient: info.from_address,
            pool: logs.pool,
            token: tokens,
            amount: amounts
        })
    }
);

action_impl!(
    Protocol::BalancerV3,
    crate::BalancerV3Vault::removeLiquidityCall,
    Burn,
    [..LiquidityRemoved],
    call_data: true,
    logs: true,
    |info: CallInfo, call_data: removeLiquidityCall, log_data: BalancerV3RemoveLiquidityCallLogs, db: &DB| {
        let logs = log_data.liquidity_removed_field?;

        let mut tokens = Vec::new();
        let mut amounts = Vec::new();

        Ok(NormalizedBurn {
            protocol: Protocol::BalancerV3,
            trace_index: info.trace_idx,
            from: info.from_address,
            recipient: info.from_address,
            pool: logs.pool,
            token: tokens,
            amount: amounts
        })
    }
);

action_impl!(
    Protocol::BalancerV3,
    crate::BalancerV3VaultExtension::registerPoolCall,
    NewPool,
    [..PoolRegistered],
    logs: true,
    |info: CallInfo, log_data: BalancerV3RegisterPoolCallLogs, _| {
        let logs = log_data.pool_registered_field?;
        let tokens = logs.tokenConfig.into_iter().map(|token| token.token).collect::<Vec<_>>();

        Ok(NormalizedNewPool {
            trace_index: info.trace_idx,
            protocol: Protocol::BalancerV3,
            pool_address: logs.pool,
            tokens: tokens,
        })
    }
);

// TODO - create v3 tests
#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use alloy_primitives::{hex, B256};
    use brontes_classifier::test_utils::ClassifierTestUtils;
    use brontes_types::{
        db::token_info::TokenInfo, normalized_actions::Action,
        Protocol::BalancerV3, TreeSearchBuilder,
    };

    use super::*;

    #[brontes_macros::test]
    async fn test_balancer_v2_swap() {
        let classifier_utils = ClassifierTestUtils::new().await;
        let swap =
            B256::from(hex!("da10a5e3cb8c34c77634cb9a1cfe02ec2b23029f1f288d79b6252b2f8cae20d3"));

        classifier_utils.ensure_token(TokenInfoWithAddress {
            address: Address::new(hex!("6C22910c6F75F828B305e57c6a54855D8adeAbf8")),
            inner:   TokenInfo { decimals: 9, symbol: "SATS".to_string() },
        });

        classifier_utils.ensure_protocol(
            Protocol::BalancerV3,
            hex!("358e056c50eea4ca707e891404e81d9b898d0b41").into(),
            Some(hex!("6C22910c6F75F828B305e57c6a54855D8adeAbf8").into()),
            Some(hex!("6C22910c6F75F828B305e57c6a54855D8adeAbf8").into()),
            None,
            None,
            None,
            None,
        );

        let eq_action = Action::Swap(NormalizedSwap {
            protocol:    BalancerV3,
            trace_index: 1,
            from:        Address::new(hex!("5d2146eAB0C6360B864124A99BD58808a3014b5d")),
            recipient:   Address::new(hex!("5d2146eAB0C6360B864124A99BD58808a3014b5d")),
            pool:        Address::new(hex!("358e056c50eea4ca707e891404e81d9b898d0b41")),
            token_in:    TokenInfoWithAddress {
                address: Address::new(hex!("6C22910c6F75F828B305e57c6a54855D8adeAbf8")),
                inner:   TokenInfo { decimals: 9, symbol: "SATS".to_string() },
            },
            amount_in:   U256::from_str("10000000000000000")
                .unwrap()
                .to_scaled_rational(18),
            token_out:   TokenInfoWithAddress {
                address: Address::new(hex!("6C22910c6F75F828B305e57c6a54855D8adeAbf8")),
                inner:   TokenInfo { decimals: 9, symbol: "SATS".to_string() },
            },
            amount_out:  U256::from_str("7727102831493")
                .unwrap()
                .to_scaled_rational(9),

            msg_value: U256::ZERO,
        });

        classifier_utils
            .contains_action(
                swap,
                0,
                eq_action,
                TreeSearchBuilder::default().with_action(Action::is_swap),
            )
            .await
            .unwrap();
    }

    #[brontes_macros::test]
    async fn test_balancer_v2_join_pool() {
        let classifier_utils = ClassifierTestUtils::new().await;
        let mint =
            B256::from(hex!("ffed34d6f2d9e239b5cd3985840a37f1fa0c558edcd1a2f3d2b8bd7f314ef6a3"));

        classifier_utils.ensure_token(TokenInfoWithAddress {
            address: Address::new(hex!("cd5fe23c85820f7b72d0926fc9b05b43e359b7ee")),
            inner:   TokenInfo { decimals: 18, symbol: "weETH".to_string() },
        });

        let eq_action = Action::Mint(NormalizedMint {
            protocol:    Protocol::BalancerV2,
            trace_index: 0,
            from:        Address::new(hex!("750c31d2290c456fcca1c659b6add80e7a88f881")),
            recipient:   Address::new(hex!("750c31d2290c456fcca1c659b6add80e7a88f881")),
            pool:        Address::new(hex!("848a5564158d84b8A8fb68ab5D004Fae11619A54")),
            token:       vec![TokenInfoWithAddress {
                address: Address::new(hex!("cd5fe23c85820f7b72d0926fc9b05b43e359b7ee")),
                inner:   TokenInfo { decimals: 18, symbol: "weETH".to_string() },
            }],
            amount:      vec![U256::from_str("1935117712922949743")
                .unwrap()
                .to_scaled_rational(18)],
        });

        classifier_utils
            .contains_action(
                mint,
                0,
                eq_action,
                TreeSearchBuilder::default().with_action(Action::is_mint),
            )
            .await
            .unwrap();
    }

    #[brontes_macros::test]
    async fn test_balancer_v2_exit_pool() {
        let classifier_utils = ClassifierTestUtils::new().await;
        let burn =
            B256::from(hex!("ad13973ee8e507b36adc5d28dc53b77d58d00d5ac6a09aa677936be8aaf6c8a1"));

        classifier_utils.ensure_token(TokenInfoWithAddress {
            address: Address::new(hex!("bf5495efe5db9ce00f80364c8b423567e58d2110")),
            inner:   TokenInfo { decimals: 18, symbol: "ezETH".to_string() },
        });

        classifier_utils.ensure_token(TokenInfoWithAddress {
            address: Address::new(hex!("cd5fe23c85820f7b72d0926fc9b05b43e359b7ee")),
            inner:   TokenInfo { decimals: 18, symbol: "weETH".to_string() },
        });

        classifier_utils.ensure_token(TokenInfoWithAddress {
            address: Address::new(hex!("fae103dc9cf190ed75350761e95403b7b8afa6c0")),
            inner:   TokenInfo { decimals: 18, symbol: "rswETH".to_string() },
        });

        let eq_action = Action::Burn(NormalizedBurn {
            protocol:    Protocol::BalancerV2,
            trace_index: 0,
            from:        Address::new(hex!("f4283d13ba1e17b33bb3310c3149136a2ef79ef7")),
            recipient:   Address::new(hex!("f4283d13ba1e17b33bb3310c3149136a2ef79ef7")),
            pool:        Address::new(hex!("848a5564158d84b8A8fb68ab5D004Fae11619A54")),
            token:       vec![
                TokenInfoWithAddress {
                    address: Address::new(hex!("bf5495efe5db9ce00f80364c8b423567e58d2110")),
                    inner:   TokenInfo { decimals: 18, symbol: "ezETH".to_string() },
                },
                TokenInfoWithAddress {
                    address: Address::new(hex!("cd5fe23c85820f7b72d0926fc9b05b43e359b7ee")),
                    inner:   TokenInfo { decimals: 18, symbol: "weETH".to_string() },
                },
                TokenInfoWithAddress {
                    address: Address::new(hex!("fae103dc9cf190ed75350761e95403b7b8afa6c0")),
                    inner:   TokenInfo { decimals: 18, symbol: "rswETH".to_string() },
                },
            ],
            amount:      vec![
                U256::from_str("471937215318872937")
                    .unwrap()
                    .to_scaled_rational(18),
                U256::from_str("757823171697267931")
                    .unwrap()
                    .to_scaled_rational(18),
                U256::from_str("699970729674926490")
                    .unwrap()
                    .to_scaled_rational(18),
            ],
        });

        classifier_utils
            .contains_action(
                burn,
                0,
                eq_action,
                TreeSearchBuilder::default().with_action(Action::is_burn),
            )
            .await
            .unwrap();
    }
}
