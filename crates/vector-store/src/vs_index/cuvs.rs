/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::Config;
use crate::VsIndexFactory;
use crate::perf;
use crate::table::Table;
use crate::vs_index;
use crate::vs_index::Message;
use crate::vs_index::VsIndexModify;
use crate::vs_index::VsIndexSearch;
use crate::vs_index::factory::VsIndexConfiguration;
use anyhow::anyhow;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::warn;

pub struct CuvsIndexFactory;

impl VsIndexFactory for CuvsIndexFactory {
    fn create_index(
        &self,
        index: VsIndexConfiguration,
        _table: Arc<RwLock<Table>>,
    ) -> anyhow::Result<(mpsc::Sender<VsIndexModify>, mpsc::Sender<VsIndexSearch>)> {
        new(index.key)
    }

    fn index_engine_version(&self) -> String {
        match cuvs::version::version() {
            Ok((major, minor, patch)) => format!("cuvs-{major}.{minor}.{patch}"),
            Err(err) => format!("cuvs-unknown ({err})"),
        }
    }
}

pub fn new_cuvs(_config_rx: watch::Receiver<Arc<Config>>) -> anyhow::Result<CuvsIndexFactory> {
    cuvs::Resources::new()
        .map_err(|err| anyhow!("failed to initialize cuVS/CUDA resources: {err}"))?;
    Ok(CuvsIndexFactory)
}

fn new(
    index_key: crate::IndexKey,
) -> anyhow::Result<(mpsc::Sender<VsIndexModify>, mpsc::Sender<VsIndexSearch>)> {
    let (tx_modify, mut rx_modify) = mpsc::channel(perf::channel_size().into());
    let (tx_search, mut rx_search) = mpsc::channel(perf::channel_size().into());

    let span_key = index_key.clone();

    tokio::spawn(perf::hotpath_async(
        {
            async move {
                debug!("starting");

                while let Some(msg) = vs_index::recv(&mut rx_search, &mut rx_modify).await {
                    match msg {
                        Message::Modify(
                            VsIndexModify::AddVector { .. }
                            | VsIndexModify::RemoveVector { .. }
                            | VsIndexModify::RemovePartition { .. },
                        ) => {
                            warn!("not implemented yet");
                        }
                        Message::Search(
                            VsIndexSearch::Ann { tx, .. } | VsIndexSearch::FilteredAnn { tx, .. },
                        ) => {
                            _ = tx.send(Err(anyhow!("GPU index is not implemented yet")));
                        }
                        Message::Search(VsIndexSearch::Count { tx, .. }) => {
                            _ = tx.send(Err(anyhow!("GPU index is not implemented yet")));
                        }
                    }
                }

                debug!("finished");
            }
        }
        .instrument(debug_span!("cuvs", "{span_key}")),
    ));

    Ok((tx_modify, tx_search))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Connectivity;
    use crate::Dimensions;
    use crate::ExpansionAdd;
    use crate::ExpansionSearch;
    use crate::IndexKey;
    use crate::NonemptyArc;
    use crate::Quantization;
    use crate::SpaceType;
    use crate::vs_index::VsIndexSearchExt;
    use scylla::cluster::metadata::NativeType;
    use std::collections::HashMap;
    use std::num::NonZeroUsize;

    #[test]
    fn index_engine_version_reports_cuvs_library_version() {
        let factory = CuvsIndexFactory;
        let (major, minor, patch) = cuvs::version::version().unwrap();
        assert_eq!(
            factory.index_engine_version(),
            format!("cuvs-{major}.{minor}.{patch}")
        );
    }

    #[tokio::test]
    async fn create_index_returns_actor_that_reports_not_yet_implemented() {
        let factory = CuvsIndexFactory;

        let index_key = IndexKey::new(&"vector".into(), &"store".into());
        let table = Arc::new(RwLock::new(
            Table::new(
                index_key.clone(),
                NonemptyArc::new(["pk"]).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                None,
                NonZeroUsize::new(1).unwrap(),
                Arc::new([]),
                Arc::new(HashMap::from([("pk".into(), NativeType::Int)])),
            )
            .unwrap(),
        ));

        let (_modify, search) = factory
            .create_index(
                VsIndexConfiguration {
                    key: index_key.clone(),
                    dimensions: Dimensions::from(NonZeroUsize::new(3).unwrap()),
                    connectivity: Connectivity::default(),
                    expansion_add: ExpansionAdd::default(),
                    expansion_search: ExpansionSearch::default(),
                    space_type: SpaceType::default(),
                    quantization: Quantization::default(),
                },
                table,
            )
            .expect("index creation itself should succeed");

        let err = search.count(index_key).await.unwrap_err().to_string();
        assert!(err.contains("not implemented yet"));
    }
}
