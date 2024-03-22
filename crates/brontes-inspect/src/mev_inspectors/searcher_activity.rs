use std::sync::Arc;

use brontes_database::libmdbx::LibmdbxReader;
use brontes_types::{
    db::dex::PriceAt,
    mev::{Bundle, BundleData, MevType, SearcherTx},
    normalized_actions::{accounting::ActionAccounting, Actions},
    tree::BlockTree,
    ActionIter, FastHashSet, ToFloatNearest, TreeSearchBuilder,
};
use itertools::Itertools;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use reth_primitives::Address;

use crate::{shared_utils::SharedInspectorUtils, Inspector, Metadata};

pub struct SearcherActivity<'db, DB: LibmdbxReader> {
    utils: SharedInspectorUtils<'db, DB>,
}

impl<'db, DB: LibmdbxReader> SearcherActivity<'db, DB> {
    pub fn new(quote: Address, db: &'db DB) -> Self {
        Self { utils: SharedInspectorUtils::new(quote, db) }
    }
}

impl<DB: LibmdbxReader> Inspector for SearcherActivity<'_, DB> {
    type Result = Vec<Bundle>;

    fn get_id(&self) -> &str {
        "SearcherActivity"
    }

    fn process_tree(&self, tree: Arc<BlockTree<Actions>>, metadata: Arc<Metadata>) -> Self::Result {
        let search_args = TreeSearchBuilder::default()
            .with_actions([Actions::is_transfer, Actions::is_eth_transfer]);

        let searcher_txs = tree.clone().collect_all(search_args).collect_vec();

        searcher_txs
            .into_par_iter()
            .filter_map(|(tx_hash, transfers)| {
                if transfers.is_empty() {
                    return None
                }

                let info = tree.get_tx_info(tx_hash, self.utils.db)?;

                (info.searcher_eoa_info.is_some() || info.searcher_contract_info.is_some()).then(
                    || {
                        let deltas = transfers.clone().into_iter().account_for_actions();

                        let mut searcher_address: FastHashSet<Address> = FastHashSet::default();
                        searcher_address.insert(info.eoa);
                        if let Some(mev_contract) = info.mev_contract {
                            searcher_address.insert(mev_contract);
                        }

                        let rev_usd = self.utils.get_deltas_usd(
                            info.tx_index,
                            PriceAt::After,
                            searcher_address,
                            &deltas,
                            metadata.clone(),
                        )?;
                        let gas_paid = metadata
                            .get_gas_price_usd(info.gas_details.gas_paid(), self.utils.quote);
                        let profit = rev_usd - gas_paid;

                        let header = self.utils.build_bundle_header(
                            vec![deltas],
                            vec![tx_hash],
                            &info,
                            profit.to_float(),
                            PriceAt::After,
                            &[info.gas_details],
                            metadata.clone(),
                            MevType::SearcherTx,
                        );

                        Some(Bundle {
                            header,
                            data: BundleData::Unknown(SearcherTx {
                                tx_hash,
                                gas_details: info.gas_details,
                                transfers: transfers
                                    .into_iter()
                                    .collect_action_vec(Actions::try_transfer),
                            }),
                        })
                    },
                )?
            })
            .collect::<Vec<_>>()
    }
}
