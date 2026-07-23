/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common::*;
use bytes::BytesMut;
use e2etest_scylla_proxy_cluster::ScyllaProxyClusterExt;
use itertools::Itertools;
use scylla_proxy::Condition;
use scylla_proxy::Reaction;
use scylla_proxy::ResponseOpcode;
use scylla_proxy::ResponseReaction;
use scylla_proxy::ResponseRule;
use std::array;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tracing::info;

const FINE_GRAINED_CDC_MAX_LATENCY: Duration = Duration::from_secs(2);
const CDC_MAX_LATENCY: Duration = Duration::from_secs(60);
const CDC_ACTOR_STOP_TIMEOUT: Duration = Duration::from_secs(10);
const TTL_EXPIRATION_TIMEOUT: Duration = Duration::from_secs(10);

e2etest::group!(
    name = cdc_direct,
    fixtures = (FixtureDirect),
    parent = crate::validator
);

struct FixtureDirect {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for FixtureDirect {
    async fn setup(setup: &mut impl e2etest::Setup) -> Self {
        setup.setup::<TestActors>().await;
        let actors = setup.get::<TestActors>().await.unwrap();
        init(&actors).await;
        Self { actors }
    }

    async fn teardown(self) {
        cleanup(&self.actors).await;
    }
}

e2etest::group!(
    name = cdc_proxy,
    fixtures = (FixtureProxy),
    parent = crate::validator
);

struct FixtureProxy {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for FixtureProxy {
    async fn setup(setup: &mut impl e2etest::Setup) -> Self {
        setup.setup::<TestActors>().await;
        let actors = setup.get::<TestActors>().await.unwrap();
        init_with_proxy_single_vs(&actors).await;
        Self { actors }
    }

    async fn teardown(self) {
        cleanup(&self.actors).await;
    }
}

#[e2etest::test(group = cdc_direct)]
async fn cdc_insert_visible_immediately(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs(&actors).await;
    let client = clients.first().unwrap();
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;
    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    let status = wait_for_index(client, &index).await;
    assert_eq!(status.count, 0, "Index should start empty");

    // Insert after index creation - this should be picked up by the fine-grained CDC reader
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, v) VALUES (1, [1.0, 2.0, 3.0])"),
            (),
        )
        .await
        .expect("failed to insert data");

    // Verify ANN query returns the inserted row
    // Should timeout only when using wide-framed CDC reader
    let pk = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [1.0, 2.0, 3.0] LIMIT 1"),
                &session,
            )
            .await?;
            let mut rows = result.rows::<(i32,)>().ok()?;
            let pk = rows.next()?.ok()?.0;
            (rows.next().is_none() && pk == 1).then_some(pk)
        },
        "Waiting for ANN query after CDC insert",
        FINE_GRAINED_CDC_MAX_LATENCY,
    )
    .await;
    assert_eq!(pk, 1, "Expected pk=1 returned by ANN query");

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}

#[e2etest::test(group = cdc_direct)]
async fn cdc_update_visible_immediately(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs(&actors).await;
    let client = clients.first().unwrap();
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, v) VALUES (1, [0.0, 0.0, 0.0])"),
            (),
        )
        .await
        .expect("failed to insert data");
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, v) VALUES (2, [5.0, 5.0, 5.0])"),
            (),
        )
        .await
        .expect("failed to insert data");

    let index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v")
            .options([("similarity_function", "euclidean")]),
    )
    .await;

    let status = wait_for_index(client, &index).await;
    assert_eq!(
        status.count, 2,
        "Index should have 2 vectors after full scan"
    );

    // Verify ANN query for [10,10,10] returns pk=2 (closer than pk=1)
    let pk = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [10.0, 10.0, 10.0] LIMIT 1"),
                &session,
            )
            .await?;
            let mut rows = result.rows::<(i32,)>().ok()?;
            let pk = rows.next()?.ok()?.0;
            (rows.next().is_none() && pk == 2).then_some(pk)
        },
        "Waiting for initial ANN query after index build",
        CDC_MAX_LATENCY,
    )
    .await;
    assert_eq!(pk, 2, "Before update: pk=2 should be closest to [10,10,10]");

    // Update pk=1 vector to [10,10,10] (closer than pk=2) - fine-grained CDC reader should pick this up
    session
        .query_unpaged(
            format!("UPDATE {table} SET v = [10.0, 10.0, 10.0] WHERE pk = 1"),
            (),
        )
        .await
        .expect("failed to update data");

    // Should timeout only when using wide-framed CDC reader
    let pk = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [10.0, 10.0, 10.0] LIMIT 1"),
                &session,
            )
            .await?;
            let mut rows = result.rows::<(i32,)>().ok()?;
            let pk = rows.next()?.ok()?.0;
            (rows.next().is_none() && pk == 1).then_some(pk)
        },
        "Waiting for ANN query after CDC update",
        FINE_GRAINED_CDC_MAX_LATENCY,
    )
    .await;
    assert_eq!(pk, 1, "After update: pk=1 should be closest to [10,10,10]");

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}
fn now_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_secs() as i64
}

#[e2etest::test(group = cdc_direct)]
async fn cdc_delete_visible_immediately(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs(&actors).await;
    let client = clients.first().unwrap();
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    for pk in 1..=3 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES ({pk}, [1.0, 1.0, 1.0])"),
                (),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    let status = wait_for_index(client, &index).await;
    assert_eq!(status.count, 3, "Index should have 3 vectors");

    // Delete one row - fine-grained CDC reader should pick this up
    session
        .query_unpaged(format!("DELETE FROM {table} WHERE pk = 2"), ())
        .await
        .expect("failed to delete data");

    // Check that count decreases to 2 within fine-grained CDC latency
    wait_for(
        || async {
            let status = client.index_status(&index.keyspace, &index.index).await;
            matches!(status, Ok(s) if s.count == 2)
        },
        "Waiting for CDC delete",
        FINE_GRAINED_CDC_MAX_LATENCY,
    )
    .await;

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}

#[e2etest::test(group = cdc_direct)]
async fn cdc_lwt_insert_visible(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs(&actors).await;
    let client = clients.first().unwrap();
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;
    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    let status = wait_for_index(client, &index).await;
    assert_eq!(status.count, 0, "Index should start empty");

    // LWT insert
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, v) VALUES (1, [1.0, 2.0, 3.0]) IF NOT EXISTS"),
            (),
        )
        .await
        .expect("failed to insert data");

    // Verify ANN query returns the inserted row
    let pk = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [1.0, 2.0, 3.0] LIMIT 1"),
                &session,
            )
            .await?;
            let mut rows = result.rows::<(i32,)>().ok()?;
            let pk = rows.next()?.ok()?.0;
            (rows.next().is_none() && pk == 1).then_some(pk)
        },
        "Waiting for ANN query after CDC LWT insert",
        CDC_MAX_LATENCY,
    )
    .await;
    assert_eq!(pk, 1, "Unexpected primary key returned by ANN query");

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}

#[e2etest::test(group = cdc_direct)]
async fn cdc_lwt_update_visible(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs(&actors).await;
    let client = clients.first().unwrap();
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, v) VALUES (1, [0.0, 0.0, 0.0])"),
            (),
        )
        .await
        .expect("failed to insert data");
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, v) VALUES (2, [5.0, 5.0, 5.0])"),
            (),
        )
        .await
        .expect("failed to insert data");

    let index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v")
            .options([("similarity_function", "euclidean")]),
    )
    .await;

    let status = wait_for_index(client, &index).await;
    assert_eq!(
        status.count, 2,
        "Index should have 2 vectors after full scan"
    );

    // Verify ANN query for [10,10,10] returns pk=2 (closer than pk=1)
    let pk = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [10.0, 10.0, 10.0] LIMIT 1"),
                &session,
            )
            .await?;
            let mut rows = result.rows::<(i32,)>().ok()?;
            let pk = rows.next()?.ok()?.0;
            (rows.next().is_none() && pk == 2).then_some(pk)
        },
        "Waiting for initial ANN query after index build",
        CDC_MAX_LATENCY,
    )
    .await;
    assert_eq!(pk, 2, "Before update: pk=2 should be closest to [10,10,10]");

    // LWT update pk=1 vector to [10,10,10] (closer than pk=2)
    session
        .query_unpaged(
            format!("UPDATE {table} SET v = [10.0, 10.0, 10.0] WHERE pk = 1 IF EXISTS"),
            (),
        )
        .await
        .expect("failed to update data");

    // Wait for the LWT write to become visible in the index
    let pk = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [10.0, 10.0, 10.0] LIMIT 1"),
                &session,
            )
            .await?;
            let mut rows = result.rows::<(i32,)>().ok()?;
            let pk = rows.next()?.ok()?.0;
            (rows.next().is_none() && pk == 1).then_some(pk)
        },
        "Waiting for ANN query after CDC LWT update",
        CDC_MAX_LATENCY,
    )
    .await;
    assert_eq!(
        pk, 1,
        "After LWT update: pk=1 should be closest to [10,10,10]"
    );

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}

#[e2etest::test(group = cdc_direct)]
async fn cdc_lwt_delete_visible(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs(&actors).await;
    let client = clients.first().unwrap();
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    for pk in 1..=3 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES ({pk}, [1.0, 1.0, 1.0])"),
                (),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    let status = wait_for_index(client, &index).await;
    assert_eq!(status.count, 3, "Index should have 3 vectors");

    // LWT delete one row
    session
        .query_unpaged(format!("DELETE FROM {table} WHERE pk = 2 IF EXISTS"), ())
        .await
        .expect("failed to delete data");

    // Check that count decreases to 2 within CDC latency
    wait_for(
        || async {
            let status = client.index_status(&index.keyspace, &index.index).await;
            matches!(status, Ok(s) if s.count == 2)
        },
        "Waiting for CDC LWT delete",
        CDC_MAX_LATENCY,
    )
    .await;

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}

/// Regression test for VECTOR-653.
///
/// When the engine replaces a `db_index` for a given index key (e.g. after
/// DROP + CREATE of the same index), old CDC actors must terminate. If
/// they are orphaned, the same index ends up with multiple CDC readers
/// running concurrently, which is the symptom observed in the field.
#[e2etest::test(group = cdc_direct)]
async fn recreating_index_terminates_old_cdc_actors(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs(&actors).await;
    let client = &clients[0];

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    let index_name = "idx_vector_653_regression";
    let index_ident = format!("{keyspace}.{index_name}");

    let wide_started = format!("{index_ident}-wide-cdc-actor-started");
    let wide_stopped = format!("{index_ident}-wide-cdc-actor-stopped");
    let fine_started = format!("{index_ident}-fine-cdc-actor-started");
    let fine_stopped = format!("{index_ident}-fine-cdc-actor-stopped");

    client.internals_clear_counters().await.unwrap();
    for name in [&wide_started, &wide_stopped, &fine_started, &fine_stopped] {
        client.internals_start_counter(name.clone()).await.unwrap();
    }

    info!("creating generation 1 of index {index_ident}");
    let index = create_index(
        CreateIndexQuery::new(&session, &clients, table.as_ref(), "v").index_name(index_name),
    )
    .await;
    wait_for_index(client, &index).await;

    wait_for(
        || async {
            let counters = match client.internals_counters().await {
                Ok(c) => c,
                Err(_) => return false,
            };
            counters.get(&wide_started).copied().unwrap_or(0) >= 1
                && counters.get(&fine_started).copied().unwrap_or(0) >= 1
        },
        "waiting for generation 1 CDC actors to start",
        Duration::from_secs(30),
    )
    .await;

    info!("dropping index {index_ident}");
    session
        .query_unpaged(format!("DROP INDEX {index_name}"), ())
        .await
        .expect("failed to drop an index");

    info!("re-creating index {index_ident} as generation 2");
    let index = create_index(
        CreateIndexQuery::new(&session, &clients, table.as_ref(), "v").index_name(index_name),
    )
    .await;
    wait_for_index(client, &index).await;

    wait_for(
        || async {
            let counters = match client.internals_counters().await {
                Ok(c) => c,
                Err(_) => return false,
            };
            counters.get(&wide_started).copied().unwrap_or(0) >= 2
                && counters.get(&fine_started).copied().unwrap_or(0) >= 2
        },
        "waiting for generation 2 CDC actors to start",
        Duration::from_secs(30),
    )
    .await;

    wait_for(
        || async {
            let counters = match client.internals_counters().await {
                Ok(c) => c,
                Err(_) => return false,
            };
            counters.get(&wide_stopped).copied().unwrap_or(0) >= 1
                && counters.get(&fine_stopped).copied().unwrap_or(0) >= 1
        },
        "VECTOR-653: old CDC actors must terminate after db_index replacement",
        CDC_ACTOR_STOP_TIMEOUT,
    )
    .await;

    let counters = client.internals_counters().await.unwrap();
    info!(
        "final counters: wide started={} stopped={}, fine started={} stopped={}",
        counters.get(&wide_started).copied().unwrap_or(0),
        counters.get(&wide_stopped).copied().unwrap_or(0),
        counters.get(&fine_started).copied().unwrap_or(0),
        counters.get(&fine_stopped).copied().unwrap_or(0),
    );
}

/// Test that rows inserted with CQL per-row TTL are not returned by ANN
/// queries after they expire, and that the index count decrements accordingly.
///
/// 1. Create a table with a TTL column and insert rows — some already expired,
///    some that will never expire.
/// 2. Create an index and verify all rows are queryable via ANN.
/// 3. Wait for the expiration service to delete expired rows (via CDC).
/// 4. Verify the index count drops and ANN queries return only non-TTL rows.
#[e2etest::test(group = cdc_direct)]
async fn cql_per_row_ttl_expires_from_index(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>, expiration BIGINT TTL",
        None,
    )
    .await;

    // Expire 5 seconds from now — enough time to build the index and
    // observe all 5 rows before the expiration service deletes them.
    let expire_at = now_epoch_secs() + 5;

    info!("Insert 3 rows with near-future expiration and 2 rows without expiration");
    for pk in 0..3 {
        session
            .query_unpaged(
                format!(
                    "INSERT INTO {table} (pk, v, expiration) VALUES ({pk}, [{v}, 0.0, 0.0], {expire_at})",
                    v = pk as f32,
                ),
                (),
            )
            .await
            .expect("failed to insert data with TTL");
    }
    for pk in 10..12 {
        session
            .query_unpaged(
                format!(
                    "INSERT INTO {table} (pk, v) VALUES ({pk}, [{v}, 1.0, 1.0])",
                    v = pk as f32,
                ),
                (),
            )
            .await
            .expect("failed to insert data without TTL");
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(
            index_status.count, 5,
            "Expected 5 vectors to be indexed before TTL expiry"
        );
    }

    info!("Verify all 5 rows are returned before expiration");
    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 5)
        },
        "Waiting for ANN query to return all 5 rows",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;
    assert_eq!(result.rows_num(), 5, "Expected 5 rows before expiration");

    info!("Wait for index count to drop after expiration service runs");
    for client in &clients {
        wait_for(
            || async {
                let status = client.index_status(&index.keyspace, &index.index).await;
                matches!(status, Ok(s) if s.count == 2)
            },
            "Waiting for expired rows to be removed from index",
            TTL_EXPIRATION_TIMEOUT,
        )
        .await;
    }

    info!("Verify ANN query returns only the non-TTL rows after expiration");
    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [10.0, 1.0, 1.0] LIMIT 10"),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 2)
        },
        "Waiting for ANN query to return only non-TTL rows",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;
    let rows: Vec<i32> = result
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row").0)
        .collect();
    assert_eq!(rows.len(), 2, "Expected 2 rows after expiration");
    for pk in &rows {
        assert!(
            *pk >= 10 && *pk < 12,
            "Expected only non-TTL rows (pk=10,11), got pk={pk}"
        );
    }

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test the "existing user" scenario: a table already serving a working vector
/// index, on which per-row TTL is introduced only afterwards.
///
/// This differs from [`cql_per_row_ttl_expires_from_index`], where rows carry a
/// TTL from the moment they are first inserted. Here the index is fully built
/// and queryable over permanent rows before any expiration is set, then a TTL
/// is added to a subset of the already-indexed rows.
///
/// 1. Create a table with a TTL column and insert 5 rows *without* expiration.
/// 2. Create an index and verify all 5 rows are queryable via ANN.
/// 3. Add a near-future per-row TTL to 3 of the already-indexed rows.
/// 4. Wait for the expiration service to delete the expired rows (via CDC).
/// 5. Verify the index count drops to 2 and ANN returns only the rows that
///    were never given a TTL.
#[e2etest::test(group = cdc_direct)]
async fn cql_per_row_ttl_added_to_indexed_rows_expires(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>, expiration BIGINT TTL",
        None,
    )
    .await;

    info!("Insert 5 rows without expiration - all permanent for now");
    for pk in 0..5 {
        session
            .query_unpaged(
                format!(
                    "INSERT INTO {table} (pk, v) VALUES ({pk}, [{v}, 0.0, 0.0])",
                    v = pk as f32,
                ),
                (),
            )
            .await
            .expect("failed to insert data without TTL");
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(
            index_status.count, 5,
            "Expected 5 vectors to be indexed before adding TTL"
        );
    }

    info!("Verify all 5 rows are returned before adding TTL");
    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 5)
        },
        "Waiting for ANN query to return all 5 rows",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;
    assert_eq!(result.rows_num(), 5, "Expected 5 rows before adding TTL");

    // Add a per-row TTL to 3 of the already-indexed rows. Compute the deadline
    // only now, once the index is confirmed serving, so the rows do not expire
    // before the update lands. The other 2 rows (pk=3,4) keep no TTL.
    let expire_at = now_epoch_secs() + 5;
    info!("Add per-row TTL to 3 already-indexed rows (pk=0,1,2)");
    for pk in 0..3 {
        session
            .query_unpaged(
                format!("UPDATE {table} SET expiration = {expire_at} WHERE pk = {pk}"),
                (),
            )
            .await
            .expect("failed to add TTL to existing row");
    }

    info!("Wait for index count to drop after the added TTL expires");
    for client in &clients {
        wait_for(
            || async {
                let status = client.index_status(&index.keyspace, &index.index).await;
                matches!(status, Ok(s) if s.count == 2)
            },
            "Waiting for newly-expired rows to be removed from index",
            TTL_EXPIRATION_TIMEOUT,
        )
        .await;
    }

    info!("Verify ANN query returns only the rows that were never given a TTL");
    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [4.0, 0.0, 0.0] LIMIT 10"),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 2)
        },
        "Waiting for ANN query to return only non-expired rows",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;
    let rows: Vec<i32> = result
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row").0)
        .collect();
    assert_eq!(rows.len(), 2, "Expected 2 rows after expiration");
    for pk in &rows {
        assert!(
            *pk >= 3 && *pk < 5,
            "Expected only rows that kept no TTL (pk=3,4), got pk={pk}"
        );
    }

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Check if null clustering keys are skipped by the index.
///
/// Use static columns as updating a static column gives a CDC's row with null clustering key.
///
/// Steps:
/// 1. Create a table with a clustering key and a static column.
/// 2. Create an index on the vector column.
/// 3. Insert rows with a clustering key and a static column value.
/// 4. Verify that the index count is correct and the ANN query returns the inserted rows.
/// 5. Drop the keyspace and verify that the index is dropped.
#[e2etest::test(group = cdc_direct)]
async fn skip_null_ck(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs(&actors).await;
    let client = clients.first().unwrap();
    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, v VECTOR<FLOAT, 3>, s INT STATIC, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;
    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    let status = wait_for_index(client, &index).await;
    assert_eq!(status.count, 0, "Index should start empty");

    info!("Insert data to the table for CDC");
    let pks: BTreeSet<_> = array::from_fn::<i32, 10, _>(|i| i as i32)
        .into_iter()
        .collect();
    for pk in &pks {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v, s) VALUES ({pk}, 2, [1.0, 2.0, 3.0], 3)"),
                (),
            )
            .await
            .expect("failed to insert data");
    }

    wait_for(
        || async {
            let status = client.index_status(&index.keyspace, &index.index).await;
            matches!(status, Ok(s) if s.count == pks.len())
        },
        "Waiting for all rows to be indexed",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    // Verify ANN query returns the inserted row
    let rows = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [1.0, 2.0, 3.0] LIMIT 100"),
                &session,
            )
            .await?;
            let rows = result
                .rows::<(i32,)>()
                .ok()?
                .map_ok(|row| row.0)
                .collect::<Result<BTreeSet<_>, _>>()
                .ok()?;
            (rows.len() == pks.len()).then_some(rows)
        },
        "Waiting for ANN query after CDC insert",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;
    assert_eq!(rows, pks);

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}

/// Check if null target columns are skipped by the index.
///
/// Steps:
/// 1. Create a table with a non-vector column.
/// 2. Create an index on the vector column.
/// 3. Insert rows.
/// 4. Update non-vector column to trigger CDC with null vector value.
/// 5. Insert new rows to wait for the CDC reader to process the update with null vector value.
/// 6. Verify that the index count after index and the ANN query returns the updated and inserted
///    rows.
/// 7. Drop the keyspace and verify that the index is dropped.
#[e2etest::test(group = cdc_direct)]
async fn skip_null_target(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs(&actors).await;
    let client = clients.first().unwrap();
    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>, i INT",
        None,
    )
    .await;
    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    let status = wait_for_index(client, &index).await;
    assert_eq!(status.count, 0, "Index should start empty");

    info!("Insert data to the table for CDC");
    let pks: BTreeSet<_> = array::from_fn::<i32, 10, _>(|i| i as i32 + 1)
        .into_iter()
        .collect();
    for pk in &pks {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v, i) VALUES ({pk}, [1.0, 2.0, 3.0], {pk})"),
                (),
            )
            .await
            .expect("failed to insert data");
    }

    wait_for(
        || async {
            let status = client.index_status(&index.keyspace, &index.index).await;
            matches!(status, Ok(s) if s.count == pks.len())
        },
        "Waiting for all rows to be indexed",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    for pk in &pks {
        session
            .query_unpaged(
                format!("UPDATE {table} SET i = {i} WHERE pk = {pk}", i = pk + 10),
                (),
            )
            .await
            .expect("failed to insert data");
    }

    for pk in &pks {
        session
            .query_unpaged(
                format!(
                    "INSERT INTO {table} (pk, v, i) VALUES ({pk}, [1.0, 2.0, 3.0], {i})",
                    pk = pk + 100,
                    i = pk,
                ),
                (),
            )
            .await
            .expect("failed to insert data");
    }

    wait_for(
        || async {
            let status = client.index_status(&index.keyspace, &index.index).await;
            matches!(status, Ok(s) if s.count == pks.len() * 2)
        },
        "Waiting for all rows to be indexed after updates and inserts",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    // Verify ANN query returns the inserted and updated rows
    let expected: BTreeSet<_> = pks
        .into_iter()
        .flat_map(|pk| [pk, pk + 10].into_iter())
        .collect();
    let rows = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk, i FROM {table} ORDER BY v ANN OF [1.0, 2.0, 3.0] LIMIT 100"),
                &session,
            )
            .await?;
            let rows = result
                .rows::<(i32, i32)>()
                .ok()?
                .map_ok(|row| row.1)
                .collect::<Result<BTreeSet<_>, _>>()
                .ok()?;
            (rows.len() == expected.len()).then_some(rows)
        },
        "Waiting for ANN query after CDC insert",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;
    assert_eq!(rows, expected);

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}

/// Check that the CDC reader retries after encountering an error in the response from the
/// database.
///
/// Steps:
/// 1. Create a table and an index using a scylla proxy.
/// 2. Create an index and wait for it to be built.
/// 3. Setup the proxy to inject an artificial error into the CDC reader response.
/// 4. Insert a row with a marker in pk into the table.
/// 5. Wait for the CDC reader to stop and start again, indicating that it retried after the error.
/// 6. Remove the error injection and wait for the index to be built successfully.
/// 7. Drop the keyspace.
#[e2etest::test(group = cdc_proxy)]
async fn reader_retries_after_error(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs_no_tls(&actors).await;
    let client = clients.first().unwrap();

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk ASCII PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    let index_name = unique_index_name();
    let index_ident = format!("{keyspace}.{index_name}");

    let index = create_index(
        CreateIndexQuery::new(&session, &clients, table.as_ref(), "v").index_name(index_name),
    )
    .await;
    wait_for_index(client, &index).await;

    const MARKER: &str = "test-cdc-retry";

    info!("Inject artificial errors into the CDC reader to test retry logic");
    actors
        .db_proxy
        .change_response_rules(Some(vec![ResponseRule(
            Condition::And(
                Box::new(Condition::ResponseOpcode(ResponseOpcode::Result)),
                Box::new(Condition::BodyContainsCaseSensitive(
                    MARKER.as_bytes().iter().copied().collect(),
                )),
            ),
            ResponseReaction::transform_frame(Arc::new(|mut input| {
                let marker = MARKER.as_bytes();
                let Some(offset) =
                    (0..input.body.len()).find(|i| input.body[*i..].starts_with(marker))
                else {
                    return input;
                };
                // Corrupt the marker in the frame body to trigger an error in the CDC reader
                let mut body = BytesMut::from(input.body);
                body[offset..offset + MARKER.len()].fill(0xff);
                input.body = body.freeze();
                input
            })),
        )]))
        .await;

    let wide_started = format!("{index_ident}-wide-cdc-handler-started");
    let wide_stopped = format!("{index_ident}-wide-cdc-handler-stopped");
    let fine_started = format!("{index_ident}-fine-cdc-handler-started");
    let fine_stopped = format!("{index_ident}-fine-cdc-handler-stopped");

    client.internals_clear_counters().await.unwrap();
    for name in [&wide_started, &wide_stopped, &fine_started, &fine_stopped] {
        client.internals_start_counter(name.clone()).await.unwrap();
    }

    let log_counters = async || {
        let counters = client.internals_counters().await.unwrap();
        info!(
            "counters: wide started={} stopped={}, fine started={} stopped={}",
            counters.get(&wide_started).copied().unwrap_or(0),
            counters.get(&wide_stopped).copied().unwrap_or(0),
            counters.get(&fine_started).copied().unwrap_or(0),
            counters.get(&fine_stopped).copied().unwrap_or(0),
        );
    };

    info!("Insert rows with marker");
    let pk = format!("{MARKER}-pk");
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, v) VALUES ('{pk}', [1.0, 2.0, 3.0])"),
            (),
        )
        .await
        .expect("failed to insert data");

    for count in 1..=3 {
        log_counters().await;
        wait_for(
            || async {
                let counters = match client.internals_counters().await {
                    Ok(c) => c,
                    Err(_) => return false,
                };
                counters.get(&wide_stopped).copied().unwrap_or(0) >= count
                    && counters.get(&fine_stopped).copied().unwrap_or(0) >= count
            },
            format!("waiting for index readers to stop (iteration {count})"),
            DEFAULT_OPERATION_TIMEOUT,
        )
        .await;

        log_counters().await;
        wait_for(
            || async {
                let counters = match client.internals_counters().await {
                    Ok(c) => c,
                    Err(_) => return false,
                };
                counters.get(&wide_started).copied().unwrap_or(0) >= count
                    && counters.get(&fine_started).copied().unwrap_or(0) >= count
            },
            format!("waiting for index readers to start (iteration {count})"),
            DEFAULT_OPERATION_TIMEOUT,
        )
        .await;
    }

    log_counters().await;

    info!("Remove the error injection");
    actors.db_proxy.turn_off_rules().await;

    wait_for(
        || async {
            let status = client.index_status(&index.keyspace, &index.index).await;
            matches!(status, Ok(s) if s.count == 1)
        },
        "Waiting for all rows to be indexed after error injection is removed",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}

#[e2etest::test(group = cdc_direct)]
async fn cdc_indexing_lag_metric_exported(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs(&actors).await;
    let client = clients.first().unwrap();
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;
    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    let status = wait_for_index(client, &index).await;
    assert_eq!(status.count, 0, "Index should start empty");

    // Insert after index creation - picked up by the CDC reader, which should
    // record an indexing_lag_seconds observation.
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, v) VALUES (1, [1.0, 2.0, 3.0])"),
            (),
        )
        .await
        .expect("failed to insert data");

    // Wait for the vector to be indexed (proves the CDC pipeline ran).
    wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [1.0, 2.0, 3.0] LIMIT 1"),
                &session,
            )
            .await?;
            let mut rows = result.rows::<(i32,)>().ok()?;
            let pk = rows.next()?.ok()?.0;
            (pk == 1).then_some(pk)
        },
        "Waiting for ANN query after CDC insert",
        FINE_GRAINED_CDC_MAX_LATENCY,
    )
    .await;

    // Scrape the /metrics endpoint and verify indexing_lag_seconds is present
    // with the expected labels and at least one observation.
    let metrics_output = wait_for_value(
        || async {
            let output = client.get_metrics_text().await;
            output
                .contains("# TYPE indexing_lag_seconds histogram")
                .then_some(output)
        },
        "Waiting for indexing_lag_seconds histogram in /metrics output",
        FINE_GRAINED_CDC_MAX_LATENCY,
    )
    .await;
    assert!(
        metrics_output.contains(&format!(r#"keyspace="{}""#, keyspace.as_ref())),
        "expected keyspace label in indexing_lag_seconds metric:\n{metrics_output}"
    );
    assert!(
        metrics_output.contains(&format!(r#"index_name="{}""#, index.index.as_ref())),
        "expected index_name label in indexing_lag_seconds metric:\n{metrics_output}"
    );
    // The _count suffix confirms at least one observation was recorded.
    let expected_count_line = format!(
        r#"indexing_lag_seconds_count{{index_name="{}",keyspace="{}"}}"#,
        index.index.as_ref(),
        keyspace.as_ref()
    );
    assert!(
        metrics_output.contains(&expected_count_line),
        "expected indexing_lag_seconds_count with labels in /metrics output:\n{metrics_output}"
    );

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}
