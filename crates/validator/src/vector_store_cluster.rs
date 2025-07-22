/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use std::path::PathBuf;
use tokio::sync::mpsc;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

pub(crate) enum VectorStoreCluster {}

pub(crate) trait VectorStoreClusterExt {}

impl VectorStoreClusterExt for mpsc::Sender<VectorStoreCluster> {}

pub(crate) async fn new(path: PathBuf, verbose: bool) -> mpsc::Sender<VectorStoreCluster> {
    let (tx, mut rx) = mpsc::channel(10);

    assert!(
        crate::executable_exists(&path).await,
        "vector-store executable '{path:?}' does not exist"
    );

    let mut state = State::new(path, verbose).await;

    tokio::spawn(
        async move {
            debug!("starting");

            while let Some(msg) = rx.recv().await {
                process(msg, &mut state).await;
            }

            debug!("finished");
        }
        .instrument(debug_span!("vs")),
    );

    tx
}

struct State {
    path: PathBuf,
    verbose: bool,
}

impl State {
    async fn new(path: PathBuf, verbose: bool) -> Self {
        Self { path, verbose }
    }
}

async fn process(msg: VectorStoreCluster, state: &mut State) {
    match msg {}
}
