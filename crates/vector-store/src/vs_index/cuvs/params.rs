/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Translation from the service's index options to CAGRA build parameters.
//!
//! `cuvs::neighbors::cagra::IndexParams` wraps a raw pointer and so cannot cross
//! a thread boundary. The backend confines all cuVS state to a dedicated thread,
//! so this module splits the translation in two: [`CagraParams`] is a plain
//! `Send` description validated when the index is created -- which is where a
//! bad option should be reported -- and [`CagraParams::to_index_params`] turns it
//! into the real thing on the GPU thread.

use crate::Connectivity;
use crate::ExpansionAdd;
use crate::Quantization;
use crate::SpaceType;
use crate::vs_index::VsIndexConfiguration;
use anyhow::anyhow;
use anyhow::bail;
use cuvs::distance::DistanceType;
use cuvs::neighbors::cagra::IndexParams;

/// CAGRA build parameters, in a form that can be sent to the GPU thread.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct CagraParams {
    pub(super) metric: DistanceType,
    pub(super) graph_degree: usize,
    pub(super) intermediate_graph_degree: usize,
}

impl TryFrom<&VsIndexConfiguration> for CagraParams {
    type Error = anyhow::Error;

    fn try_from(config: &VsIndexConfiguration) -> anyhow::Result<Self> {
        // Quantization is out of scope for the GPU backend: it indexes plain
        // `f32` vectors only. Reject anything else here rather than silently
        // ignoring it, as DiskANN does.
        if config.quantization != Quantization::F32 {
            bail!(
                "cuVS index does not support quantization {:?}: the GPU backend indexes F32 \
                 vectors only",
                config.quantization
            );
        }

        let metric = distance_type(config.space_type)?;
        let graph_degree = graph_degree(config.connectivity);
        let intermediate_graph_degree = intermediate_graph_degree(config.expansion_add);

        // CAGRA builds an intermediate graph and prunes it down, so the
        // intermediate degree cannot be the smaller of the two. `IndexParams`
        // enforces this too, but only on the GPU thread, long after the index
        // was accepted -- checking here turns it into a creation-time error.
        if intermediate_graph_degree < graph_degree {
            bail!(
                "cuVS index requires expansion_add ({intermediate_graph_degree}) to be >= \
                 connectivity ({graph_degree}), because CAGRA prunes an intermediate graph of \
                 that degree down to the final one"
            );
        }

        Ok(Self {
            metric,
            graph_degree,
            intermediate_graph_degree,
        })
    }
}

impl CagraParams {
    /// Materializes the cuVS parameters. Must run on the thread owning the cuVS
    /// state, since [`IndexParams`] is not `Send`.
    pub(super) fn to_index_params(self) -> anyhow::Result<IndexParams> {
        IndexParams::builder()
            .metric(self.metric)
            .graph_degree(self.graph_degree)
            .intermediate_graph_degree(self.intermediate_graph_degree)
            .build()
            .map_err(|err| anyhow!("failed to build cuVS CAGRA index params: {err}"))
    }
}

/// Maps the service's distance metric onto CAGRA's.
///
/// `CosineExpanded` is a native CAGRA metric, so cosine needs no vector
/// normalization. Hamming has no CAGRA equivalent -- the bitwise metric applies
/// to binary-quantized data, which this backend does not index.
fn distance_type(space_type: SpaceType) -> anyhow::Result<DistanceType> {
    Ok(match space_type {
        SpaceType::Euclidean => DistanceType::L2Expanded,
        SpaceType::Cosine => DistanceType::CosineExpanded,
        SpaceType::DotProduct => DistanceType::InnerProduct,
        SpaceType::Hamming => bail!(
            "cuVS index does not support the Hamming space type: CAGRA has no equivalent metric \
             for F32 vectors"
        ),
    })
}

/// Degree of the final CAGRA graph, i.e. neighbours kept per node.
fn graph_degree(connectivity: Connectivity) -> usize {
    *connectivity.as_ref()
}

/// Degree of the intermediate graph CAGRA prunes down to `graph_degree`.
fn intermediate_graph_degree(expansion_add: ExpansionAdd) -> usize {
    *expansion_add.as_ref()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Dimensions;
    use crate::ExpansionSearch;
    use crate::IndexKey;
    use std::num::NonZeroUsize;

    fn configuration() -> VsIndexConfiguration {
        VsIndexConfiguration {
            key: IndexKey::new(&"vector".into(), &"store".into()),
            dimensions: Dimensions::from(NonZeroUsize::new(3).unwrap()),
            connectivity: Connectivity::default(),
            expansion_add: ExpansionAdd::default(),
            expansion_search: ExpansionSearch::default(),
            space_type: SpaceType::default(),
            quantization: Quantization::default(),
        }
    }

    #[test]
    fn defaults_map_to_valid_cagra_params() {
        let params = CagraParams::try_from(&configuration()).unwrap();

        // The service defaults to cosine.
        assert_eq!(params.metric, DistanceType::CosineExpanded);
        assert_eq!(params.graph_degree, *Connectivity::default().as_ref());
        assert_eq!(
            params.intermediate_graph_degree,
            *ExpansionAdd::default().as_ref()
        );
        assert!(params.intermediate_graph_degree >= params.graph_degree);
    }

    #[test]
    fn space_types_map_to_cagra_metrics() {
        for (space_type, expected) in [
            (SpaceType::Euclidean, DistanceType::L2Expanded),
            (SpaceType::Cosine, DistanceType::CosineExpanded),
            (SpaceType::DotProduct, DistanceType::InnerProduct),
        ] {
            let config = VsIndexConfiguration {
                space_type,
                ..configuration()
            };
            assert_eq!(CagraParams::try_from(&config).unwrap().metric, expected);
        }
    }

    #[test]
    fn hamming_space_type_is_rejected() {
        let config = VsIndexConfiguration {
            space_type: SpaceType::Hamming,
            ..configuration()
        };
        let err = CagraParams::try_from(&config).unwrap_err().to_string();
        assert!(err.contains("Hamming"), "got: {err}");
    }

    #[test]
    fn non_f32_quantization_is_rejected() {
        for quantization in [
            Quantization::F16,
            Quantization::BF16,
            Quantization::I8,
            Quantization::B1,
        ] {
            let config = VsIndexConfiguration {
                quantization,
                ..configuration()
            };
            let err = CagraParams::try_from(&config).unwrap_err().to_string();
            assert!(err.contains("quantization"), "got: {err}");
        }
    }

    #[test]
    fn connectivity_above_expansion_add_is_rejected() {
        let config = VsIndexConfiguration {
            connectivity: Connectivity::from(256),
            expansion_add: ExpansionAdd::from(64),
            ..configuration()
        };
        let err = CagraParams::try_from(&config).unwrap_err().to_string();
        assert!(err.contains("expansion_add"), "got: {err}");
        assert!(err.contains("connectivity"), "got: {err}");
    }

    #[test]
    fn params_materialize_into_cuvs_index_params() {
        // Exercises the GPU-thread half: the builder revalidates what we checked.
        CagraParams::try_from(&configuration())
            .unwrap()
            .to_index_params()
            .unwrap();
    }
}
