use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

use alloy_etherscan::Client;
use brontes_types::structured_trace::TxTrace;
use ethers_core::types::Chain;
use futures::Future;
use reth_interfaces::RethResult;
use reth_primitives::{BlockId, BlockNumber, BlockNumberOrTag, Header, H256};
use reth_provider::{BlockIdReader, BlockNumReader};
use reth_tracing::TracingClient;
use tokio::{sync::mpsc::UnboundedSender, task::JoinError};

use self::parser::TraceParser;
use crate::{
    executor::{Executor, TaskKind},
    init_trace,
};

mod parser;
mod utils;
use brontes_metrics::{trace::types::TraceMetricEvent, PoirotMetricEvents};
#[allow(dead_code)]
pub(crate) const UNKNOWN: &str = "unknown";
#[allow(dead_code)]
pub(crate) const RECEIVE: &str = "receive";
#[allow(dead_code)]
pub(crate) const FALLBACK: &str = "fallback";

const CACHE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10_000);
const CACHE_DIRECTORY: &str = "./abi_cache";

pub type ParserFuture = Pin<
    Box<dyn Future<Output = Result<Option<(Vec<TxTrace>, Header)>, JoinError>> + Send + 'static>,
>;

pub struct Parser {
    executor: Executor,
    parser:   Arc<TraceParser>,
}

impl Parser {
    pub fn new(
        metrics_tx: UnboundedSender<PoirotMetricEvents>,
        etherscan_key: &str,
        db_path: &str,
    ) -> Self {
        let executor = Executor::new();
        let tracer =
            Arc::new(TracingClient::new(Path::new(db_path), executor.runtime.handle().clone()));

        let etherscan_client = Client::new_cached(
            Chain::Mainnet,
            etherscan_key,
            Some(PathBuf::from(CACHE_DIRECTORY)),
            CACHE_TIMEOUT,
        )
        .unwrap();
        let parser = TraceParser::new(etherscan_client, Arc::clone(&tracer), Arc::new(metrics_tx));

        Self { executor, parser: Arc::new(parser) }
    }

    pub fn get_block_hash_for_number(
        &self,
        block_num: u64,
    ) -> reth_interfaces::RethResult<Option<H256>> {
        self.parser
            .tracer
            .trace
            .provider()
            .block_hash_for_id(block_num.into())
    }

    pub fn get_latest_block_number(&self) -> RethResult<BlockNumber> {
        self.parser.tracer.trace.provider().best_block_number()
    }

    /// executes the tracing of a given block
    pub fn execute(&self, block_num: u64) -> ParserFuture {
        let parser = self.parser.clone();
        Box::pin(self.executor.spawn_result_task_as(
            async move { parser.execute_block(block_num).await },
            TaskKind::Default,
        )) as ParserFuture
    }
}