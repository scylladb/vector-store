/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Connectivity;
use crate::Dimensions;
use crate::Distance;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexFactory;
use crate::IndexKey;
use crate::Limit;
use crate::PartitionId;
use crate::SpaceType;
use crate::Vector;
use crate::index::actor::Index;
use crate::index::factory::IndexConfiguration;
use crate::index::validator;
use crate::memory::Memory;
use crate::perf;
use crate::table::IndexIdGenerator;
use crate::table::PrimaryId;
use crate::table::Table;
use crate::table::TableSearch;
use anyhow::anyhow;
use opensearch::DeleteParts;
use opensearch::IndexParts;
use opensearch::OpenSearch;
use opensearch::http::Url;
use opensearch::http::transport::SingleNodeConnectionPool;
use opensearch::http::transport::TransportBuilder;
use opensearch::indices::IndicesCreateParts;
use serde_json::Value;
use serde_json::json;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use super::actor::AnnR;
use super::actor::CountR;

pub struct OpenSearchIndexFactory {
    client: Arc<OpenSearch>,
    shutdown_notify: Arc<Notify>,
}

impl Drop for OpenSearchIndexFactory {
    fn drop(&mut self) {
        self.shutdown_notify.notify_one();
    }
}

impl OpenSearchIndexFactory {
    fn create_opensearch_client(addr: &str) -> anyhow::Result<OpenSearch> {
        let address = Url::parse(addr)?;
        let conn_pool = SingleNodeConnectionPool::new(address);
        let transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
        let client = OpenSearch::new(transport);
        Ok(client)
    }
}

impl Display for SpaceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Euclidean => write!(f, "l2"),
            Self::Cosine => write!(f, "cosinesimil"),
            Self::DotProduct => write!(f, "innerproduct"),
            Self::Hamming => unimplemented!("Hamming distance is not supported"),
        }
    }
}

impl IndexFactory for OpenSearchIndexFactory {
    fn create_index(
        &self,
        index: IndexConfiguration,
        table: Arc<RwLock<Table>>,
        _: mpsc::Sender<Memory>,
    ) -> anyhow::Result<mpsc::Sender<Index>> {
        new(
            index.key,
            index.dimensions,
            index.connectivity,
            index.expansion_add,
            index.expansion_search,
            index.space_type,
            table,
            self.client.clone(),
        )
    }

    fn index_engine_version(&self) -> String {
        "opensearch".into()
    }
}

pub fn new_opensearch(
    addr: &str,
    config_rx: watch::Receiver<Arc<crate::Config>>,
) -> Result<OpenSearchIndexFactory, anyhow::Error> {
    let initial_addr = addr.to_string();
    let shutdown_notify = Arc::new(Notify::new());
    let factory = OpenSearchIndexFactory {
        client: Arc::new(OpenSearchIndexFactory::create_opensearch_client(addr)?),
        shutdown_notify: shutdown_notify.clone(),
    };

    // Spawn monitoring task
    tokio::spawn(async move {
        let mut rx = config_rx;
        loop {
            tokio::select! {
                result = rx.changed() => {
                    if result.is_err() {
                        break;
                    }
                    let new_config = rx.borrow();
                    let new_addr = new_config.opensearch_addr.as_deref();

                    if Some(initial_addr.as_str()) != new_addr {
                        let new_display = new_addr.unwrap_or("None (using Usearch)");
                        warn!(
                            "OpenSearch address changed: {initial_addr} -> {new_display}. Restart required."
                        );
                    }
                }
                _ = shutdown_notify.notified() => {
                    break;
                }
            }
        }
    });

    Ok(factory)
}

async fn create_index(
    key: &IndexKey,
    dimensions: Dimensions,
    connectivity: Connectivity,
    expansion_add: ExpansionAdd,
    expansion_search: ExpansionSearch,
    space_type: SpaceType,
    client: Arc<OpenSearch>,
) -> Result<opensearch::http::response::Response, ()> {
    let response: Result<opensearch::http::response::Response, ()> = client
        .indices()
        .create(IndicesCreateParts::Index(key.as_ref()))
        .body(json!({
            "settings": {
                "index.knn": true
            },
            "mappings": {
                "properties": {
                    "vector": {
                        "type": "knn_vector",
                        "dimension": dimensions.0.get(),
                        "method": {
                            "name": "hnsw",
                            "space_type": space_type.to_string(),
                            "parameters": {
                                "ef_search": if expansion_search.0 > 0 {
                                    expansion_search.0
                                } else {
                                    100
                                },
                                "ef_construction": if expansion_add.0 > 0 {
                                    expansion_add.0
                                } else {
                                    100
                                },
                                "m": if connectivity.0 > 0 {
                                    connectivity.0
                                } else {
                                    16
                                },
                            }
                        }
                    },
                }
            }
        }))
        .send()
        .await
        .map_or_else(
            Err,
            opensearch::http::response::Response::error_for_status_code,
        )
        .map_err(|err| {
            error!("engine::new: unable to create index with key {key}: {err}");
        });

    response
}

// TODO: remove allow
#[allow(clippy::too_many_arguments)]
pub fn new(
    key: IndexKey,
    dimensions: Dimensions,
    connectivity: Connectivity,
    expansion_add: ExpansionAdd,
    expansion_search: ExpansionSearch,
    space_type: SpaceType,
    table: Arc<RwLock<impl TableSearch + Send + Sync + 'static>>,
    client: Arc<OpenSearch>,
) -> anyhow::Result<mpsc::Sender<Index>> {
    info!("Creating new index with key: {key}");
    let (tx, mut rx) = mpsc::channel(perf::channel_size().into());

    tokio::spawn({
        let cloned_key = key.clone();
        async move {
            let response = create_index(
                &key,
                dimensions,
                connectivity,
                expansion_add,
                expansion_search,
                space_type,
                client.clone(),
            )
            .await;

            if response.is_err() {
                error!("engine::new: unable to create index with key {key}");
                return;
            }

            debug!("starting");

            // This semaphore decides how many tasks are queued for an opensearch process.
            // We are currently using SingleNodeConnectionPool, so we can only have one
            // connection to the server. This means that we can only have one task at a time,
            // so we set the semaphore to 2, so we always have something in queue.
            let semaphore = Arc::new(Semaphore::new(2));

            let key = Arc::new(key);

            while let Some(msg) = rx.recv().await {
                let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
                tokio::spawn({
                    let key = Arc::clone(&key);
                    let table = Arc::clone(&table);
                    let client = Arc::clone(&client);
                    async move {
                        process(msg, dimensions, space_type, key, table, client).await;
                        drop(permit);
                    }
                });
            }

            debug!("finished");
        }
        .instrument(debug_span!("opensearch", "{cloned_key}"))
    });

    Ok(tx)
}

async fn process(
    msg: Index,
    dimensions: Dimensions,
    space_type: SpaceType,
    index_key: Arc<IndexKey>,
    table: Arc<RwLock<impl TableSearch>>,
    client: Arc<OpenSearch>,
) {
    match msg {
        Index::AddVector {
            primary_id,
            embedding,
            in_progress: _in_progress,
            ..
        } => add(index_key, primary_id, &embedding, client).await,
        Index::RemoveVector {
            primary_id,
            in_progress: _in_progress,
            ..
        } => remove(index_key, primary_id, client).await,
        Index::Ann {
            embedding,
            limit,
            tx,
            ..
        } => {
            ann(
                index_key, tx, embedding, dimensions, limit, space_type, table, client,
            )
            .await
        }
        Index::FilteredAnn { tx, .. } => filtered_ann(tx).await,
        Index::Count { tx, .. } => count(index_key, tx, client).await,

        _ => todo!(),
    }
}

async fn add(
    index_key: Arc<IndexKey>,
    primary_id: PrimaryId,
    embeddings: &Vector,
    client: Arc<OpenSearch>,
) {
    _ = client
        .index(IndexParts::IndexId(
            index_key.as_ref().as_ref(),
            &primary_id.as_ref().to_string(),
        ))
        .body(json!({
            "vector": embeddings.as_slice(),
        }))
        .send()
        .await
        .map_or_else(
            Err,
            opensearch::http::response::Response::error_for_status_code,
        )
        .map_err(|err| {
            error!("add: unable to add embedding for primary_id {primary_id:?}: {err}");
        });
}

async fn remove(index_key: Arc<IndexKey>, primary_id: PrimaryId, client: Arc<OpenSearch>) {
    _ = client
        .delete(DeleteParts::IndexId(
            index_key.as_ref().as_ref(),
            &primary_id.as_ref().to_string(),
        ))
        .send()
        .await
        .map_or_else(
            Err,
            opensearch::http::response::Response::error_for_status_code,
        )
        .map_err(|err| {
            error!("remove: unable to remove embedding for primary_id {primary_id:?}: {err}");
        });
}

#[allow(clippy::too_many_arguments)]
async fn ann(
    key: Arc<IndexKey>,
    tx_ann: oneshot::Sender<AnnR>,
    embedding: Vector,
    dimensions: Dimensions,
    limit: Limit,
    space_type: SpaceType,
    table: Arc<RwLock<impl TableSearch>>,
    client: Arc<OpenSearch>,
) {
    if let Err(err) = validator::embedding_dimensions(&embedding, dimensions) {
        return tx_ann
            .send(Err(err))
            .unwrap_or_else(|_| trace!("ann: unable to send response"));
    }

    let response = client
        .search(opensearch::SearchParts::Index(&[key.as_ref().as_ref()]))
        .body(json!({
            "query": {
                "knn": {
                    "vector": {
                        "vector": embedding.as_slice(),
                        "k": limit.0,
                    }
                }
            }
        }))
        .send()
        .await
        .map_or_else(
            Err,
            opensearch::http::response::Response::error_for_status_code,
        )
        .map_err(|err| {
            error!("ann: unable to search for embedding: {err}");
        });

    if response.is_err() {
        _ = tx_ann.send(Err(anyhow!("ann: unable to search for embedding")));
        return;
    }

    let response_body = response.unwrap().json::<Value>().await;

    if response_body.is_err() {
        _ = tx_ann.send(Err(anyhow!("ann: unable to search for embedding")));
        return;
    }
    let response_body = response_body.unwrap();

    let hits = response_body
        .get("hits")
        .and_then(|hits| hits.get("hits"))
        .and_then(|hits| hits.as_array());

    if hits.is_none() {
        _ = tx_ann.send(Err(anyhow!("ann: unable to search for embedding")));
        return;
    }
    // We need to setup global index for opensearch, so we can run tests
    let index_id = IndexIdGenerator::new().next(true).unwrap();
    let partition_id = PartitionId::global(index_id);

    let hits = {
        let table = table.read().unwrap();
        hits.unwrap()
            .iter()
            .map(|hit| {
                let id = hit["_id"].as_str().unwrap();
                let score = hit["_score"].as_f64().unwrap();
                let primary_id = PrimaryId::from(id.parse::<u64>().unwrap());
                let primary_key = table.primary_key(partition_id, primary_id).unwrap();
                (primary_key, score)
            })
            .collect::<Vec<_>>()
    };

    let (keys, scores): (Vec<_>, Vec<_>) = hits.iter().cloned().unzip();
    let distances: anyhow::Result<Vec<_>> = scores
        .iter()
        .map(|score| Distance::try_from((*score as f32, space_type, Some(dimensions))))
        .collect();
    let distances = match distances {
        Ok(distances) => distances,
        Err(err) => {
            _ = tx_ann.send(Err(err));
            return;
        }
    };

    tx_ann
        .send(Ok((keys, distances)))
        .unwrap_or_else(|_| trace!("ann: unable to send response"));
}

async fn filtered_ann(tx_ann: oneshot::Sender<AnnR>) {
    _ = tx_ann.send(Err(anyhow!("Filtering not supported")));
}

async fn count(key: Arc<IndexKey>, tx: oneshot::Sender<CountR>, client: Arc<OpenSearch>) {
    let response = client
        .count(opensearch::CountParts::Index(&[key.as_ref().as_ref()]))
        .send()
        .await
        .map_or_else(
            Err,
            opensearch::http::response::Response::error_for_status_code,
        )
        .map_err(|err| {
            error!("count: unable to count embeddings: {err}");
        });

    if response.is_err() {
        _ = tx.send(Ok(0));
        return;
    }

    let response_body = response.unwrap().json::<Value>().await;

    if response_body.is_err() {
        _ = tx.send(Ok(0));
        return;
    }
    let response_body = response_body.unwrap();

    let count = response_body.get("count").and_then(|count| count.as_u64());

    if count.is_none() {
        _ = tx.send(Ok(0));
        return;
    }
    let count = count.unwrap();

    _ = tx.send(Ok(count as usize));
}
