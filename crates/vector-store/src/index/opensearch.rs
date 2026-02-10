/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::Connectivity;
use crate::Dimensions;
use crate::Distance;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexFactory;
use crate::IndexId;
use crate::Limit;
use crate::PrimaryKey;
use crate::SpaceType;
use crate::Vector;
use crate::index::actor::Index;
use crate::index::factory::IndexConfiguration;
use crate::index::validator;
use crate::memory::Memory;
use anyhow::anyhow;
use bimap::BiMap;
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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
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
        _: Arc<Vec<ColumnName>>,
        _: mpsc::Sender<Memory>,
    ) -> anyhow::Result<mpsc::Sender<Index>> {
        new(
            index.id,
            index.dimensions,
            index.connectivity,
            index.expansion_add,
            index.expansion_search,
            index.space_type,
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

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::AsRef,
    derive_more::Display,
)]
/// Key for index embeddings
struct Key(u64);

async fn create_index(
    id: &IndexId,
    dimensions: Dimensions,
    connectivity: Connectivity,
    expansion_add: ExpansionAdd,
    expansion_search: ExpansionSearch,
    space_type: SpaceType,
    client: Arc<OpenSearch>,
) -> Result<opensearch::http::response::Response, ()> {
    let response: Result<opensearch::http::response::Response, ()> = client
        .indices()
        .create(IndicesCreateParts::Index(&id.0))
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
            error!("engine::new: unable to create index with id {id}: {err}");
        });

    response
}

pub fn new(
    id: IndexId,
    dimensions: Dimensions,
    connectivity: Connectivity,
    expansion_add: ExpansionAdd,
    expansion_search: ExpansionSearch,
    space_type: SpaceType,
    client: Arc<OpenSearch>,
) -> anyhow::Result<mpsc::Sender<Index>> {
    info!("Creating new index with id: {id}");
    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn({
        let cloned_id = id.clone();
        async move {
            let response = create_index(
                &id,
                dimensions,
                connectivity,
                expansion_add,
                expansion_search,
                space_type,
                client.clone(),
            )
            .await;

            if response.is_err() {
                error!("engine::new: unable to create index with id {id}");
                return;
            }

            debug!("starting");

            // bimap between PrimaryKey and Key for an opensearch index
            let keys = Arc::new(RwLock::new(BiMap::new()));

            // Incremental key for a opensearch index
            let opensearch_key = Arc::new(AtomicU64::new(0));

            // This semaphore decides how many tasks are queued for an opensearch process.
            // We are currently using SingleNodeConnectionPool, so we can only have one
            // connection to the server. This means that we can only have one task at a time,
            // so we set the semaphore to 2, so we always have something in queue.
            let semaphore = Arc::new(Semaphore::new(2));

            let id = Arc::new(id);

            while let Some(msg) = rx.recv().await {
                let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
                tokio::spawn({
                    let id = Arc::clone(&id);
                    let keys = Arc::clone(&keys);
                    let opensearch_key = Arc::clone(&opensearch_key);
                    let client = Arc::clone(&client);
                    async move {
                        process(msg, dimensions, id, keys, opensearch_key, client).await;
                        drop(permit);
                    }
                });
            }

            debug!("finished");
        }
        .instrument(debug_span!("opensearch", "{cloned_id}"))
    });

    Ok(tx)
}

async fn process(
    msg: Index,
    dimensions: Dimensions,
    id: Arc<IndexId>,
    keys: Arc<RwLock<BiMap<PrimaryKey, Key>>>,
    opensearch_key: Arc<AtomicU64>,
    client: Arc<OpenSearch>,
) {
    match msg {
        Index::Add {
            primary_key,
            embedding,
            in_progress: _in_progress,
        } => add_or_replace(id, keys, opensearch_key, primary_key, embedding, client).await,
        Index::Remove {
            primary_key,
            in_progress: _in_progress,
        } => remove(id, keys, primary_key, client).await,
        Index::Ann {
            embedding,
            limit,
            tx,
        } => ann(id, tx, keys, embedding, dimensions, limit, client).await,
        Index::FilteredAnn { tx, .. } => filtered_ann(tx).await,
        Index::Count { tx } => count(id, tx, client).await,
    }
}

async fn add_or_replace(
    id: Arc<IndexId>,
    keys: Arc<RwLock<BiMap<PrimaryKey, Key>>>,
    opensearch_key: Arc<AtomicU64>,
    primary_key: PrimaryKey,
    embeddings: Vector,
    client: Arc<OpenSearch>,
) {
    let key = opensearch_key.fetch_add(1, Ordering::Relaxed).into();

    let (key, remove) = if keys
        .write()
        .unwrap()
        .insert_no_overwrite(primary_key.clone(), key)
        .is_ok()
    {
        (key, false)
    } else {
        opensearch_key.fetch_sub(1, Ordering::Relaxed);
        (
            *keys.read().unwrap().get_by_left(&primary_key).unwrap(),
            true,
        )
    };

    if remove {
        let response = client
            .delete(DeleteParts::IndexId(&id.0, &key.0.to_string()))
            .send()
            .await
            .map_or_else(
                Err,
                opensearch::http::response::Response::error_for_status_code,
            )
            .map_err(|err| {
                error!("add_or_replace: unable to remove embedding for key {key}: {err}");
            });

        if response.is_err() {
            return;
        }
    }

    let response = client
        .index(IndexParts::IndexId(&id.0, &key.0.to_string()))
        .body(json!({
            "vector": embeddings.0,
        }))
        .send()
        .await
        .map_or_else(
            Err,
            opensearch::http::response::Response::error_for_status_code,
        )
        .map_err(|err| {
            error!("add_or_replace: unable to add embedding for key {key}: {err}");
        });

    if response.is_err() {
        keys.write().unwrap().remove_by_right(&key);
    }
}

async fn remove(
    id: Arc<IndexId>,
    keys: Arc<RwLock<BiMap<PrimaryKey, Key>>>,
    primary_key: PrimaryKey,
    client: Arc<OpenSearch>,
) {
    let (primary_key, key) = keys.write().unwrap().remove_by_left(&primary_key).unwrap();

    let response = client
        .delete(DeleteParts::IndexId(&id.0, &key.0.to_string()))
        .send()
        .await
        .map_or_else(
            Err,
            opensearch::http::response::Response::error_for_status_code,
        )
        .map_err(|err| {
            error!("remove: unable to remove embedding for key {key}: {err}");
        });

    if response.is_err() {
        keys.write().unwrap().insert(primary_key, key);
    }
}

async fn ann(
    id: Arc<IndexId>,
    tx_ann: oneshot::Sender<AnnR>,
    keys: Arc<RwLock<BiMap<PrimaryKey, Key>>>,
    embedding: Vector,
    dimensions: Dimensions,
    limit: Limit,
    client: Arc<OpenSearch>,
) {
    if let Err(err) = validator::embedding_dimensions(&embedding, dimensions) {
        return tx_ann
            .send(Err(err))
            .unwrap_or_else(|_| trace!("ann: unable to send response"));
    }

    let response = client
        .search(opensearch::SearchParts::Index(&[&id.0]))
        .body(json!({
            "query": {
                "knn": {
                    "vector": {
                        "vector": embedding.0,
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
    let hits = hits
        .unwrap()
        .iter()
        .map(|hit| {
            let id = hit["_id"].as_str().unwrap();
            let score = hit["_score"].as_f64().unwrap();
            let keys = keys.read().unwrap();
            let key = keys.get_by_right(&Key(id.parse::<u64>().unwrap())).unwrap();
            (key.clone(), score)
        })
        .collect::<Vec<_>>();

    let (keys, scores): (Vec<_>, Vec<_>) = hits.iter().cloned().unzip();
    let distances = scores
        .iter()
        .map(|score| Distance(*score as f32))
        .collect::<Vec<_>>();

    tx_ann
        .send(Ok((keys, distances)))
        .unwrap_or_else(|_| trace!("ann: unable to send response"));
}

async fn filtered_ann(tx_ann: oneshot::Sender<AnnR>) {
    _ = tx_ann.send(Err(anyhow!("Filtering not supported")));
}

async fn count(id: Arc<IndexId>, tx: oneshot::Sender<CountR>, client: Arc<OpenSearch>) {
    let response = client
        .count(opensearch::CountParts::Index(&[&id.0]))
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
