/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

// TODO: Please remove if necessary implementation is provided.
#![allow(dead_code)]

use crate::Connectivity;
use crate::Dimensions;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexFactory;
use crate::IndexId;
use crate::PrimaryKey;
use crate::index::actor::Index;
use bimap::BiMap;
use opensearch::OpenSearch;
use opensearch::http::Url;
use opensearch::http::transport::SingleNodeConnectionPool;
use opensearch::http::transport::TransportBuilder;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc;
use tracing::info;

pub struct OpenSearchIndexFactory {
    client: Arc<OpenSearch>,
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

impl IndexFactory for OpenSearchIndexFactory {
    fn create_index(
        &self,
        id: IndexId,
        dimensions: Dimensions,
        connectivity: Connectivity,
        expansion_add: ExpansionAdd,
        expansion_search: ExpansionSearch,
    ) -> anyhow::Result<mpsc::Sender<Index>> {
        new(
            id,
            dimensions,
            connectivity,
            expansion_add,
            expansion_search,
            self.client.clone(),
        )
    }
}

pub fn new_opensearch(addr: &str) -> Result<OpenSearchIndexFactory, anyhow::Error> {
    Ok(OpenSearchIndexFactory {
        client: Arc::new(OpenSearchIndexFactory::create_opensearch_client(addr)?),
    })
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

pub fn new(
    id: IndexId,
    _dimensions: Dimensions,
    _connectivity: Connectivity,
    _expansion_add: ExpansionAdd,
    _expansion_search: ExpansionSearch,
    _client: Arc<OpenSearch>,
) -> anyhow::Result<mpsc::Sender<Index>> {
    info!("Creating new index with id: {id}");
    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 100000;
    let (tx, mut _rx) = mpsc::channel(CHANNEL_SIZE);

    Ok(tx)
}

async fn _process(
    msg: Index,
    _dimensions: Dimensions,
    _id: Arc<IndexId>,
    _keys: Arc<RwLock<BiMap<PrimaryKey, Key>>>,
    _opensearch_key: Arc<AtomicU64>,
    _client: Arc<OpenSearch>,
) {
    // TODO: Implement the logic for processing the messages
    match msg {
        Index::AddOrReplace {
            primary_key,
            embedding,
        } => {
            let _ = primary_key;
            let _ = embedding;
        }
        Index::Remove { primary_key } => {
            let _ = primary_key;
        }
        Index::Ann {
            embedding,
            limit,
            tx,
        } => {
            let _ = embedding;
            let _ = limit;
            let _ = tx;
        }
        Index::Count { tx } => {
            let _ = tx;
        }
    }
}
