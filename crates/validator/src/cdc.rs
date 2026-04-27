/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common::*;
use async_backtrace::framed;
use e2etest::TestCase;
use std::time::Duration;
use tracing::info;

const FINE_GRAINED_CDC_MAX_LATENCY: Duration = Duration::from_secs(2);
const CDC_MAX_LATENCY: Duration = Duration::from_secs(60);
const CDC_ACTOR_STOP_TIMEOUT: Duration = Duration::from_secs(10);

#[framed]
pub(crate) async fn new() -> TestCase<TestActors> {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "cdc_insert_visible_immediately",
            timeout,
            cdc_insert_visible_immediately,
        )
        .with_test(
            "cdc_update_visible_immediately",
            timeout,
            cdc_update_visible_immediately,
        )
        .with_test(
            "cdc_delete_visible_immediately",
            timeout,
            cdc_delete_visible_immediately,
        )
        .with_test("cdc_lwt_insert_visible", timeout, cdc_lwt_insert_visible)
        .with_test("cdc_lwt_update_visible", timeout, cdc_lwt_update_visible)
        .with_test("cdc_lwt_delete_visible", timeout, cdc_lwt_delete_visible)
        .with_test(
            "recreating_index_terminates_old_cdc_actors",
            timeout,
            recreating_index_terminates_old_cdc_actors,
        )
}

#[framed]
async fn cdc_insert_visible_immediately(actors: TestActors) {
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

#[framed]
async fn cdc_update_visible_immediately(actors: TestActors) {
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

#[framed]
async fn cdc_delete_visible_immediately(actors: TestActors) {
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

#[framed]
async fn cdc_lwt_insert_visible(actors: TestActors) {
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

#[framed]
async fn cdc_lwt_update_visible(actors: TestActors) {
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

#[framed]
async fn cdc_lwt_delete_visible(actors: TestActors) {
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
#[framed]
async fn recreating_index_terminates_old_cdc_actors(actors: TestActors) {
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

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
