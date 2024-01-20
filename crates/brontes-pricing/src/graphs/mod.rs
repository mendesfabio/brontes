mod all_pair_graph;
mod dijkstras;
mod registry;
mod subgraph;
mod yens;
use std::{
    cmp::{max, Ordering},
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        BinaryHeap, HashMap, HashSet,
    },
    hash::Hash,
    ops::{Deref, DerefMut},
    time::SystemTime,
};

pub use all_pair_graph::AllPairGraph;
use alloy_primitives::Address;
use brontes_types::{
    exchanges::StaticBindingsDb,
    extra_processing::Pair,
    price_graph::{PoolPairInfoDirection, PoolPairInformation, SubGraphEdge},
    tree::Node,
};
use ethers::core::k256::sha2::digest::HashMarker;
use itertools::Itertools;
use malachite::Rational;
use petgraph::{
    data::DataMap,
    graph::{self, UnGraph},
    prelude::*,
    visit::{Bfs, GraphBase, IntoEdges, IntoNeighbors, VisitMap, Visitable},
    Graph,
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use self::registry::SubGraphRegistry;
use super::PoolUpdate;
use crate::types::PoolState;

pub struct GraphManager {
    all_pair_graph:     AllPairGraph,
    sub_graph_registry: SubGraphRegistry,
    /// this is degen but don't want to reorganize all types so that
    /// this struct can hold the db so these closures allow for the wanted
    /// interactions.
    db_load:            Box<dyn Fn(u64, Pair) -> Option<(Pair, Vec<SubGraphEdge>)> + Send + Sync>,
    db_save:            Box<dyn Fn(u64, Pair, Vec<SubGraphEdge>) + Send + Sync>,
}

impl GraphManager {
    pub fn init_from_db_state(
        all_pool_data: HashMap<(Address, StaticBindingsDb), Pair>,
        sub_graph_registry: HashMap<Pair, Vec<SubGraphEdge>>,
        db_load: Box<dyn Fn(u64, Pair) -> Option<(Pair, Vec<SubGraphEdge>)> + Send + Sync>,
        db_save: Box<dyn Fn(u64, Pair, Vec<SubGraphEdge>) + Send + Sync>,
    ) -> Self {
        let graph = AllPairGraph::init_from_hashmap(all_pool_data);
        let registry = SubGraphRegistry::new(sub_graph_registry);

        Self { all_pair_graph: graph, sub_graph_registry: registry, db_load, db_save }
    }

    pub fn add_pool(&mut self, pair: Pair, pool_addr: Address, dex: StaticBindingsDb) {
        self.all_pair_graph.add_node(pair.ordered(), pool_addr, dex);
    }

    pub fn crate_subpool_multithread(
        &self,
        block: u64,
        pair: Pair,
    ) -> (Vec<PoolPairInfoDirection>, Vec<SubGraphEdge>) {
        let pair = pair.ordered();
        if self.sub_graph_registry.has_subpool(&pair) {
            /// fetch all state to be loaded
            return (self.sub_graph_registry.fetch_unloaded_state(&pair), vec![])
        } else if let Some((pair, edges)) = (&self.db_load)(block, pair) {
            info!("db load");
            return (self.sub_graph_registry.all_unloaded_state(&edges), edges)
        }

        let paths = self
            .all_pair_graph
            .get_paths(pair)
            .into_iter()
            .flatten()
            .flatten()
            .collect_vec();

        (self.sub_graph_registry.all_unloaded_state(&paths), paths)
    }

    pub fn add_subgraph(&mut self, pair: Pair, edges: Vec<SubGraphEdge>) {
        if !self.sub_graph_registry.has_subpool(&pair.ordered()) {
            self.sub_graph_registry
                .create_new_subgraph(pair.ordered(), edges);
        }
    }

    /// creates a subpool for the pair returning all pools that need to be
    /// loaded
    pub fn create_subpool(&mut self, block: u64, pair: Pair) -> Vec<PoolPairInfoDirection> {
        let pair = pair.ordered();
        if self.sub_graph_registry.has_subpool(&pair) {
            /// fetch all state to be loaded
            return self.sub_graph_registry.fetch_unloaded_state(&pair)
        } else if let Some((pair, edges)) = (&mut self.db_load)(block, pair) {
            return self.sub_graph_registry.create_new_subgraph(pair, edges)
        }

        let paths = self
            .all_pair_graph
            .get_paths(pair)
            .into_iter()
            .flatten()
            .flatten()
            .collect_vec();

        // search failed
        if paths.is_empty() {
            info!(?pair, "empty search path");
            return vec![]
        }

        self.sub_graph_registry
            .create_new_subgraph(pair, paths.clone())
    }

    pub fn bad_pool_state(
        &mut self,
        subgraph_pair: Pair,
        pool_pair: Pair,
        pool_address: Address,
    ) -> (bool, Option<(Address, StaticBindingsDb, Pair)>) {
        let requery_subgraph = self.sub_graph_registry.bad_pool_state(
            subgraph_pair.ordered(),
            pool_pair.ordered(),
            pool_address,
        );

        (
            requery_subgraph,
            self.all_pair_graph
                .remove_empty_address(pool_pair, pool_address),
        )
    }

    pub fn get_price(&self, pair: Pair) -> Option<Rational> {
        self.sub_graph_registry.get_price(pair)
    }

    pub fn new_state(&mut self, block: u64, address: Address, state: PoolState) {
        self.sub_graph_registry
            .new_pool_state(address, state)
            .into_iter()
            .for_each(|(pair, edges)| {
                (&mut self.db_save)(block, pair, edges);
            });
    }

    pub fn update_state(&mut self, address: Address, update: PoolUpdate) {
        self.sub_graph_registry.update_pool_state(address, update);
    }

    pub fn has_state(&self, addr: &Address) -> bool {
        self.sub_graph_registry.has_state(addr)
    }

    pub fn has_subgraph(&self, pair: Pair) -> bool {
        self.sub_graph_registry.has_subpool(&pair.ordered())
    }
}