/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::Config;
use crate::Dimensions;
use crate::IndexKey;
use crate::PositiveFiniteF32;
use crate::PrimaryId;
use crate::SpaceType;
use crate::Vector;
use crate::VsIndexFactory;
use crate::memory::Memory;
use crate::perf;
use crate::table::Table;
use crate::vs_index::actor::VsIndex;
use crate::vs_index::factory::VsIndexConfiguration;
use anyhow::Context;
use diskann::graph::Config as DiskannConfig;
use diskann::graph::config::Builder;
use diskann::graph::config::MaxDegree;
use diskann::graph::config::defaults::ALPHA as DISKANN_DEFAULT_ALPHA;
use diskann::utils::ONE;
use diskann_disk::DiskIndexBuildParameters;
use diskann_disk::QuantizationType;
use diskann_disk::build::builder::build::DiskIndexBuilder;
use diskann_disk::data_model::AdHoc;
use diskann_disk::disk_index_build_parameter::DISK_SECTOR_LEN;
use diskann_disk::disk_index_build_parameter::MemoryBudget;
use diskann_disk::disk_index_build_parameter::NumPQChunks;
use diskann_disk::storage::DiskIndexWriter;
use diskann_providers::model::configuration::IndexConfiguration;
use diskann_providers::storage::FileStorageProvider;
use diskann_vector::distance::Metric;
use std::fs::File;
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::warn;

const NUM_THREADS: usize = 1;
const BUILD_PQ_CHUNKS: usize = 1;
const BUILD_DATASET_POINTS: usize = 1_000;
const BUILD_MEMORY_LIMIT_GB: f64 = 2.0;
const MAX_POINTS: NonZeroUsize = NonZeroUsize::new(1_000_000).unwrap();

#[derive(Debug)]
pub struct DiskannIndexFactory {
    diskann_index_path: PathBuf,
    #[allow(dead_code)]
    // alpha is wired into the DiskANN config, but events are not implemented yet
    alpha: PositiveFiniteF32,
}

impl VsIndexFactory for DiskannIndexFactory {
    fn create_index(
        &self,
        index: VsIndexConfiguration,
        _table: Arc<RwLock<Table>>,
        _memory: mpsc::Sender<Memory>,
    ) -> anyhow::Result<mpsc::Sender<VsIndex>> {
        let params = DiskannParams::try_from((&index, self.alpha, MAX_POINTS))?;

        new(params, index.key, &self.diskann_index_path)
    }

    fn index_engine_version(&self) -> String {
        format!("diskann-{}", diskann::version())
    }
}

pub fn new_diskann(
    mut config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<DiskannIndexFactory> {
    let config = config_rx.borrow_and_update();

    let diskann_index_path = config
        .diskann_index_path
        .clone()
        .ok_or(anyhow::anyhow!("DiskANN index path should be set"))?;

    Ok(DiskannIndexFactory {
        diskann_index_path,
        alpha: config
            .diskann_alpha
            .unwrap_or(PositiveFiniteF32::new(DISKANN_DEFAULT_ALPHA).unwrap()),
    })
}

fn new(
    params: DiskannParams,
    index_key: IndexKey,
    diskann_index_path: &Path,
) -> anyhow::Result<mpsc::Sender<VsIndex>> {
    let index_dir = diskann_index_path.join(index_key.as_ref());

    let (tx, mut rx) = mpsc::channel(perf::channel_size().into());

    tokio::spawn(perf::hotpath_async(
        {
            async move {
                debug!("starting");
                async {
                    if tokio::fs::try_exists(&index_dir).await.unwrap_or(false) {
                        let mut dir = tokio::fs::read_dir(&index_dir).await?;
                        if dir.next_entry().await?.is_some() {
                            anyhow::bail!("DiskANN index directory is non-empty: {index_dir:?}");
                        }
                    }

                    tokio::fs::create_dir_all(&index_dir)
                        .await
                        .context("failed to create DiskANN index directory")
                }
                .await
                .unwrap_or_else(|e| warn!("Failed to initialize DiskANN index directory: {:?}", e));

                let mut state = DiskannState::Collecting(Collector::new(params, index_dir));

                while let Some(msg) = rx.recv().await {
                    process_message(&mut state, msg).await;
                }

                debug!("finished");
            }
        }
        .instrument(debug_span!("diskann", "{index_key}")),
    ));

    Ok(tx)
}

struct Collector {
    params: DiskannParams,
    index_dir: PathBuf,
    vectors: Vec<(PrimaryId, Vector)>,
}

impl Collector {
    fn new(params: DiskannParams, index_dir: PathBuf) -> Self {
        Self {
            params,
            index_dir,
            vectors: Vec::with_capacity(BUILD_DATASET_POINTS),
        }
    }
}

/// State machine for the DiskANN index lifecycle.
/// It is needed because creating a DiskANN index requires a dataset of vectors which are taken from the initial full scan.
///
/// - `Collecting`: Accumulates vectors until the dataset threshold is reached, then triggers a build.
/// - `Serving`: The index has been built successfully and is ready to handle queries.
/// - `Fail`: An unrecoverable error occurred; all subsequent requests will receive this error.
enum DiskannState {
    Collecting(Collector),
    Serving,
    Fail(String),
}

async fn process_message(state: &mut DiskannState, msg: VsIndex) {
    let new_state = match std::mem::replace(state, DiskannState::Serving) {
        DiskannState::Collecting(collector) => process_collecting(msg, collector).await,
        DiskannState::Serving => {
            process_serving(msg);
            DiskannState::Serving
        }
        DiskannState::Fail(err) => {
            process_fail(&err, msg);
            DiskannState::Fail(err)
        }
    };
    *state = new_state;
}

async fn process_collecting(msg: VsIndex, collector: Collector) -> DiskannState {
    let mut collector = collector;
    match msg {
        VsIndex::AddVector {
            primary_id,
            embedding,
            in_progress: _in_progress,
            ..
        } => {
            if embedding.dim() != Some(collector.params.dim) {
                let err = format!(
                    "DiskANN collector vector dimensions mismatch: expected {}, got {}",
                    usize::from(collector.params.dim.0),
                    embedding.len()
                );
                error!("{err}");
                return DiskannState::Fail(err);
            }

            collector.vectors.push((primary_id, embedding));
            if collector.vectors.len() == BUILD_DATASET_POINTS {
                return spawn_build(collector).await;
            }
        }
        VsIndex::RemoveVector { .. } | VsIndex::RemovePartition { .. } => {
            warn!("not implemented yet");
        }
        VsIndex::Ann { tx, .. } | VsIndex::FilteredAnn { tx, .. } => {
            _ = tx.send(Err(anyhow::anyhow!(
                "DiskANN index still collecting vectors ({}/{})",
                collector.vectors.len(),
                BUILD_DATASET_POINTS
            )));
        }
        VsIndex::Count { tx, .. } => {
            _ = tx.send(Err(anyhow::anyhow!(
                "DiskANN index still collecting vectors ({}/{})",
                collector.vectors.len(),
                BUILD_DATASET_POINTS
            )));
        }
    }

    DiskannState::Collecting(collector)
}

async fn spawn_build(collector: Collector) -> DiskannState {
    let Collector {
        params,
        index_dir,
        vectors,
    } = collector;

    let result = tokio::task::spawn_blocking({
        move || {
            let build_result = build_disk_index(params, vectors, &index_dir);
            match build_result {
                Ok(()) => {
                    debug!("DiskANN index built from collected vectors");
                    DiskannState::Serving
                }
                Err(err) => {
                    let err = err.to_string();
                    error!("DiskANN index build failed: {err}");
                    if let Err(e) = std::fs::remove_dir_all(&index_dir) {
                        error!(
                            "failed to clean up DiskANN index directory after build failure: {e}"
                        );
                    }
                    DiskannState::Fail(err)
                }
            }
        }
    })
    .await;

    match result {
        Ok(next_state) => next_state,
        Err(err) => DiskannState::Fail(format!("DiskANN index build task panicked: {err}")),
    }
}

fn process_serving(msg: VsIndex) {
    match msg {
        VsIndex::AddVector { .. }
        | VsIndex::RemoveVector { .. }
        | VsIndex::RemovePartition { .. } => {
            warn!("not implemented yet");
        }
        VsIndex::Ann { tx, .. } | VsIndex::FilteredAnn { tx, .. } => {
            _ = tx.send(Err(anyhow::anyhow!("DiskANN index is not implemented yet")));
        }
        VsIndex::Count { tx, .. } => {
            _ = tx.send(Err(anyhow::anyhow!("DiskANN index is not implemented yet")));
        }
    }
}

fn process_fail(err: &str, msg: VsIndex) {
    match msg {
        VsIndex::Ann { tx, .. } | VsIndex::FilteredAnn { tx, .. } => {
            _ = tx.send(Err(anyhow::anyhow!("{err}")));
        }
        VsIndex::Count { tx, .. } => {
            _ = tx.send(Err(anyhow::anyhow!("{err}")));
        }
        VsIndex::AddVector { .. }
        | VsIndex::RemoveVector { .. }
        | VsIndex::RemovePartition { .. } => {
            warn!("DiskANN index failed: {err}");
        }
    }
}

fn build_disk_index(
    params: DiskannParams,
    vectors: Vec<(PrimaryId, Vector)>,
    index_dir: &Path,
) -> anyhow::Result<()> {
    let storage_provider = FileStorageProvider;

    // TODO: make these DiskANN build constants configurable./
    let disk_index_build_parameters = DiskIndexBuildParameters::new(
        MemoryBudget::try_from_gb(BUILD_MEMORY_LIMIT_GB)
            .context("failed to create DiskANN build memory budget")?,
        QuantizationType::default(),
        NumPQChunks::new_with(BUILD_PQ_CHUNKS, usize::from(params.dim.0))
            .context("failed to create DiskANN PQ chunk configuration")?,
    );

    let dataset_path = index_dir.join("dataset.bin");
    let prefix_path = index_dir.join("index");

    let dataset_file_str = dataset_path
        .to_str()
        .ok_or(anyhow::anyhow!(
            "DiskANN dataset path is not valid UTF-8: {dataset_path:?}"
        ))?
        .to_string();
    let index_path_prefix_str = prefix_path
        .to_str()
        .ok_or(anyhow::anyhow!(
            "DiskANN index prefix path is not valid UTF-8: {prefix_path:?}"
        ))?
        .to_string();

    write_dataset(&dataset_path, &params, &vectors)?;

    let index_configuration = IndexConfiguration::from(params);

    let index_writer = DiskIndexWriter::new(
        dataset_file_str,
        index_path_prefix_str,
        None, // No associated data file
        DISK_SECTOR_LEN,
    )
    .context("failed to create a DiskIndexWriter")?;

    let mut builder = DiskIndexBuilder::<'_, AdHoc<f32, u32>, _>::new(
        &storage_provider,
        disk_index_build_parameters,
        index_configuration,
        index_writer,
    )
    .map_err(|e| anyhow::anyhow!("failed to create DiskANN index builder: {}", e))?;

    builder.build().context("failed to build DiskANN index")?;

    Ok(())
}

fn write_dataset(
    dataset_path: &Path,
    params: &DiskannParams,
    vectors: &[(PrimaryId, Vector)],
) -> anyhow::Result<()> {
    let dataset = File::create(dataset_path).map_err(|e| {
        anyhow::anyhow!(
            "failed to create DiskANN dataset at {:?}: {}",
            dataset_path,
            e
        )
    })?;
    let mut dataset = std::io::BufWriter::new(dataset);
    let dimensions_u32 = u32::try_from(usize::from(params.dim.0))
        .context("DiskANN dataset dimensions do not fit in u32")?;
    let vector_count = u32::try_from(vectors.len()).context("DiskANN dataset is too large")?;

    dataset.write_all(&vector_count.to_le_bytes())?;
    dataset.write_all(&dimensions_u32.to_le_bytes())?;

    for (_, vector) in vectors {
        for value in vector.as_slice() {
            dataset.write_all(&value.to_le_bytes())?;
        }
    }

    Ok(())
}

#[derive(Clone)]
struct DiskannParams {
    config: DiskannConfig,
    metric: Metric,
    dim: Dimensions,
    max_points: NonZeroUsize,
    #[allow(dead_code)]
    // AFAIK l_search is used per query, but we don't have queries yet, so this is unused for now
    l_search_default: NonZeroUsize,
}

impl TryFrom<(&VsIndexConfiguration, PositiveFiniteF32, NonZeroUsize)> for DiskannParams {
    type Error = anyhow::Error;

    fn try_from(
        (cfg, alpha, max_points): (&VsIndexConfiguration, PositiveFiniteF32, NonZeroUsize),
    ) -> anyhow::Result<Self> {
        let metric: Metric = cfg.space_type.try_into()?;

        let mut builder = Builder::new(
            cfg.connectivity.0,
            MaxDegree::default_slack(),
            cfg.expansion_add.0,
            metric.into(),
        );

        builder.alpha(alpha.get());

        let config = builder
            .build()
            .context("failed to build DiskANN configuration")?;

        Ok(Self {
            config,
            metric,
            dim: cfg.dimensions,
            max_points,
            l_search_default: NonZeroUsize::new(cfg.expansion_search.0)
                .ok_or(anyhow::anyhow!("expansion_search must be > 0"))?,
        })
    }
}

impl From<Dimensions> for usize {
    fn from(dim: Dimensions) -> Self {
        usize::from(dim.0)
    }
}

impl From<DiskannParams> for IndexConfiguration {
    fn from(params: DiskannParams) -> Self {
        IndexConfiguration::new(
            params.metric,
            usize::from(params.dim),
            usize::from(params.max_points),
            ONE,
            NUM_THREADS,
            params.config,
        )
    }
}

impl TryFrom<SpaceType> for Metric {
    type Error = anyhow::Error;

    fn try_from(space_type: SpaceType) -> anyhow::Result<Self> {
        match space_type {
            SpaceType::Euclidean => Ok(Self::L2),
            SpaceType::Cosine => Ok(Self::Cosine),
            SpaceType::DotProduct => Ok(Self::InnerProduct),
            SpaceType::Hamming => {
                anyhow::bail!("DiskANN does not support Hamming space type")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Connectivity;
    use crate::ExpansionAdd;
    use crate::ExpansionSearch;
    use crate::IndexKey;
    use crate::IndexName;
    use crate::KeyspaceName;
    use crate::Quantization;
    use crate::vs_index::VsIndexExt;
    use diskann_providers::storage::get_compressed_pq_file;
    use diskann_providers::storage::get_disk_index_file;
    use diskann_providers::storage::get_pq_pivot_file;
    use std::num::NonZeroUsize;
    use std::path::Path;
    use tempfile::tempdir;
    use tokio::task;
    use tokio::time;
    use tokio::time::Duration;

    const MAX_POINTS: NonZeroUsize = NonZeroUsize::new(1_000_000).unwrap();

    fn test_index() -> VsIndexConfiguration {
        VsIndexConfiguration {
            key: IndexKey::new(
                &KeyspaceName::from("ks".to_string()),
                &IndexName::from("tbl".to_string()),
            ),
            dimensions: NonZeroUsize::new(3).unwrap().into(),
            connectivity: Connectivity(16),
            expansion_add: ExpansionAdd(64),
            expansion_search: ExpansionSearch(32),
            space_type: SpaceType::Euclidean,
            quantization: Quantization::F32,
        }
    }

    fn setup_collecting_actor() -> (mpsc::Sender<VsIndex>, IndexKey, tempfile::TempDir) {
        let tmp_dir = tempdir().unwrap();
        let cfg = test_index();
        let index_key = cfg.key.clone();
        let params = DiskannParams::try_from((
            &cfg,
            PositiveFiniteF32::new(DISKANN_DEFAULT_ALPHA).unwrap(),
            MAX_POINTS,
        ))
        .unwrap();

        let actor = new(params, index_key.clone(), tmp_dir.path()).unwrap();

        (actor, index_key, tmp_dir)
    }

    fn collecting_error_message(collected: usize) -> String {
        format!(
            "DiskANN index still collecting vectors ({collected}/{})",
            BUILD_DATASET_POINTS
        )
    }

    #[test]
    fn diskann_metric_try_from_space_type() {
        assert_eq!(Metric::try_from(SpaceType::Euclidean).unwrap(), Metric::L2);
        assert_eq!(Metric::try_from(SpaceType::Cosine).unwrap(), Metric::Cosine);
        assert_eq!(
            Metric::try_from(SpaceType::DotProduct).unwrap(),
            Metric::InnerProduct
        );
        assert!(Metric::try_from(SpaceType::Hamming).is_err());
    }

    #[test]
    fn diskann_params_try_from_index_configuration() {
        let params = DiskannParams::try_from((
            &test_index(),
            PositiveFiniteF32::new(DISKANN_DEFAULT_ALPHA).unwrap(),
            MAX_POINTS,
        ))
        .unwrap();

        assert_eq!(
            params.config.pruned_degree(),
            NonZeroUsize::new(16).unwrap()
        );
        assert_eq!(usize::from(params.dim), 3);
        assert_eq!(params.l_search_default, NonZeroUsize::new(32).unwrap());
        assert_eq!(params.config.l_build(), NonZeroUsize::new(64).unwrap());
        assert_eq!(params.metric, Metric::L2);
    }

    #[tokio::test]
    async fn new_materializes_disk_provider_files() {
        let (actor, index_key, tmp_dir) = setup_collecting_actor();

        let index_dir = tmp_dir.path().join(index_key.as_ref());
        let index_prefix = index_dir.join("index");
        let index_prefix = index_prefix.to_str().unwrap();

        time::timeout(Duration::from_secs(5), async {
            while !index_dir.exists() {
                task::yield_now().await;
            }
        })
        .await
        .unwrap();

        for id in 0..BUILD_DATASET_POINTS {
            actor
                .add_vector(
                    crate::table::PartitionId::global(crate::table::IndexId::from(1)),
                    PrimaryId::from(id as u64),
                    vec![id as f32, id as f32 + 1.0, id as f32 + 2.0].into(),
                    crate::AsyncInProgress::None,
                )
                .await;
        }

        time::timeout(Duration::from_secs(5), async {
            loop {
                if Path::new(&get_disk_index_file(index_prefix)).exists()
                    && Path::new(&get_pq_pivot_file(index_prefix)).exists()
                    && Path::new(&get_compressed_pq_file(index_prefix)).exists()
                {
                    break;
                }
                task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn count_fails_while_collecting() {
        let (actor, index_key, _tmp_dir) = setup_collecting_actor();

        let err = actor.count(index_key).await.unwrap_err();
        assert_eq!(err.to_string(), collecting_error_message(0));
    }

    #[tokio::test]
    async fn dimension_mismatch_transitions_to_fail() {
        let (actor, index_key, _tmp_dir) = setup_collecting_actor();

        actor
            .add_vector(
                crate::table::PartitionId::global(crate::table::IndexId::from(1)),
                PrimaryId::from(1),
                vec![1.0, 2.0].into(),
                crate::AsyncInProgress::None,
            )
            .await;

        let err = actor.count(index_key).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "DiskANN collector vector dimensions mismatch: expected 3, got 2"
        );
    }

    #[tokio::test]
    async fn search_fails_while_collecting() {
        let (actor, index_key, _tmp_dir) = setup_collecting_actor();

        let err = actor
            .ann(
                index_key,
                vec![1.0, 2.0, 3.0].into(),
                NonZeroUsize::new(10).unwrap().into(),
            )
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), collecting_error_message(0));
    }

    #[tokio::test]
    async fn search_filtered_fails_while_collecting() {
        let (actor, index_key, _tmp_dir) = setup_collecting_actor();

        let err = actor
            .filtered_ann(
                index_key,
                vec![1.0, 2.0, 3.0].into(),
                crate::Filter {
                    restrictions: vec![],
                    allow_filtering: true,
                },
                NonZeroUsize::new(10).unwrap().into(),
            )
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), collecting_error_message(0));
    }
}
