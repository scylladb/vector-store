/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! FTS scenarios of the pipeline benchmark: real Tantivy indexes fed through the range scan
//! path, measured until the documents are searchable.

use super::db_basic;
use super::db_basic::DbBasic;
use super::default_config;
use super::default_runtime;
use super::run_vector_store;
use super::wait_until_all_tasks_finished;
use std::sync::Arc;
use testclient::TestClient;
use tokio::sync::watch;
use vector_store::Config;

/// A running Vector Store over `DbBasic`, kept alive for one benchmark.
struct Fixture {
    runtime: tokio::runtime::Runtime,
    db: DbBasic,
    client: Arc<TestClient>,
    _server: Box<dyn Send>,
    _config_tx: watch::Sender<Arc<Config>>,
}

impl Fixture {
    fn new() -> Self {
        let runtime = default_runtime();
        let (db, client, server, config_tx) = runtime.block_on(async {
            let db = db_basic::new();
            let (config_tx, config_rx) = watch::channel(default_config().await);
            let (server, client) = run_vector_store(config_rx, db.clone()).await;
            (db, client, Box::new(server) as Box<dyn Send>, config_tx)
        });
        Self {
            runtime,
            db,
            client,
            _server: server,
            _config_tx: config_tx,
        }
    }

    fn teardown(self) {
        let Self {
            runtime,
            client,
            _server,
            _config_tx,
            ..
        } = self;
        drop(client);
        drop(_server);
        drop(_config_tx);
        wait_until_all_tasks_finished(&runtime);
    }
}
