/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::db_basic;
use crate::db_basic::Index;
use crate::db_basic::Table;
use crate::httpclient::HttpClient;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Duration;
use tokio::task;
use tokio::time;
use uuid::Uuid;
use vector_store::IndexMetadata;

#[tokio::test]
async fn simple_create_search_delete_index() {
    crate::enable_tracing();

    let (db_actor, db) = db_basic::new();

    let index = IndexMetadata {
        keyspace_name: "vector".to_string().into(),
        table_name: "items".to_string().into(),
        index_name: "ann".to_string().into(),
        key_name: "id".to_string().into(),
        target_column: "embeddings".to_string().into(),
        dimensions: NonZeroUsize::new(3).unwrap().into(),
        connectivity: Default::default(),
        expansion_add: Default::default(),
        expansion_search: Default::default(),
        version: Uuid::new_v4().into(),
    };

    let (_server_actor, addr) = vector_store::run(
        SocketAddr::from(([127, 0, 0, 1], 0)).into(),
        Some(1),
        db_actor,
    )
    .await
    .unwrap();
    let client = HttpClient::new(addr);

    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        index.key_name.clone(),
        Table {
            dimensions: [(index.target_column.clone(), index.dimensions)]
                .into_iter()
                .collect(),
        },
    )
    .unwrap();
    db.add_index(
        &index.keyspace_name,
        index.index_name.clone(),
        Index {
            table_name: index.table_name.clone(),
            target_column: index.target_column.clone(),
            connectivity: index.connectivity,
            expansion_add: index.expansion_add,
            expansion_search: index.expansion_search,
        },
    )
    .unwrap();
    db.insert_values(
        &index.keyspace_name,
        &index.table_name,
        &index.target_column,
        vec![
            (vec![1.into()].into(), vec![1., 1., 1.].into()),
            (vec![2.into()].into(), vec![2., -2., 2.].into()),
            (vec![3.into()].into(), vec![3., 3., 3.].into()),
        ],
    )
    .unwrap();

    time::timeout(Duration::from_secs(10), async {
        while db
            .get_indexed_elements_count(&index.keyspace_name, &index.index_name)
            .unwrap()
            .as_ref()
            != &3
        {
            task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes.first().unwrap().as_ref(), "vector.ann");

    let (primary_keys, distances) = client
        .ann(
            &index,
            vec![2.1, -2., 2.].into(),
            NonZeroUsize::new(1).unwrap().into(),
        )
        .await;
    assert_eq!(distances.len(), 1);
    let primary_keys_id = primary_keys.get(&"id".to_string().into()).unwrap();
    assert_eq!(distances.len(), primary_keys_id.len());
    assert_eq!(primary_keys_id.first().unwrap().as_ref(), &2);

    db.del_index(&index.keyspace_name, &index.index_name)
        .unwrap();

    time::timeout(Duration::from_secs(10), async {
        while !client.indexes().await.is_empty() {
            task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
