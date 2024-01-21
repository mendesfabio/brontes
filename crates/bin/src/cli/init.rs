use std::{env, sync::Arc};

use brontes_database::{
    clickhouse::Clickhouse,
    libmdbx::{tables::Tables, Libmdbx},
};
use clap::Parser;

#[derive(Debug, Parser)]
pub struct Init {
    /// Initialize the local Libmdbx DB
    #[arg(long, short, default_value = "true")]
    pub init_libmdbx:         bool,
    /// Libmdbx tables to init:
    ///     TokenDecimals
    ///     AddressToTokens
    ///     AddressToProtocol
    ///     CexPrice
    ///     Metadata
    ///     PoolState
    ///     DexPrice
    #[arg(long, short, requires = "init_libmdbx", value_delimiter = ',')]
    pub tables_to_init:       Option<Vec<Tables>>,
    /// Start Block to download metadata from Sorella's MEV DB
    #[arg(long, short, default_value = "0")]
    pub start_block:          Option<u64>,
    /// End Block to download metadata from Sorella's MEV DB
    #[arg(long, short, default_value = "0")]
    pub end_block:            Option<u64>,
    /// Download Dex Prices from Sorella's MEV DB for the given block range. If
    /// false it will run the dex pricing locally using raw on-chain data
    #[arg(long, short, default_value = "false")]
    pub download_dex_pricing: bool,
}

impl Init {
    pub async fn execute(self) -> eyre::Result<()> {
        let brontes_db_endpoint = env::var("BRONTES_DB_PATH").expect("No BRONTES_DB_PATH in .env");

        let clickhouse = Arc::new(Clickhouse::default());

        let libmdbx = Arc::new(Libmdbx::init_db(brontes_db_endpoint, None)?);
        if self.init_libmdbx {
            // currently inits all tables
            let range = if let (Some(start), Some(end)) = (self.start_block, self.end_block) {
                Some((start, end))
            } else {
                None
            };
            libmdbx
                .init_tables(
                    clickhouse.clone(),
                    self.tables_to_init
                        .unwrap_or({
                            if self.download_dex_pricing {
                                let tables = Tables::ALL.to_vec();
                                //tables.retain(|table| table != &Tables::CexPrice);
                                //println!("TABLES: {:?}", tables);
                                tables
                            } else {
                                Tables::ALL_NO_DEX.to_vec()
                            }
                        })
                        .as_slice(),
                    range,
                )
                .await?;
        }

        //TODO: Joe, have it download the full range of metadata from the MEV DB so
        // they can run everything in parallel
        Ok(())
    }
}