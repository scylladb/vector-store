"""
Comprehensive unit tests for the ScyllaDB VSS sizing algorithm.

Tests cover:
- Input validation (bounds, type enforcement)
- HNSW parameter estimation (M formula)
- Compression ratios and quantization
- Throughput sizing and bucket selection
- Instance selection
- ScyllaDB-node sizing
- End-to-end scenarios
- Edge cases and regression tests
"""

from __future__ import annotations

import math
import unittest

import vss_sizing as vs


class TestQuantizationEnum(unittest.TestCase):
    """Tests for the vs.Quantization enum."""

    def test_values(self) -> None:
        self.assertEqual(vs.Quantization.NONE.value, "none")
        self.assertEqual(vs.Quantization.SCALAR.value, "scalar")
        self.assertEqual(vs.Quantization.BINARY.value, "binary")


class TestCloudProviderEnum(unittest.TestCase):
    """Tests for the vs.CloudProvider enum."""

    def test_values(self) -> None:
        self.assertEqual(vs.CloudProvider.AWS.value, "aws")
        self.assertEqual(vs.CloudProvider.GCP.value, "gcp")


class TestGetInstances(unittest.TestCase):
    """Tests for vs.get_instances helper."""

    def test_aws_returns_aws_instances(self) -> None:
        instances = vs.get_instances(vs.CloudProvider.AWS)
        self.assertIs(instances, vs.AWS_INSTANCES)
        self.assertTrue(any("r7g" in i.name for i in instances))

    def test_gcp_returns_gcp_instances(self) -> None:
        instances = vs.get_instances(vs.CloudProvider.GCP)
        self.assertIs(instances, vs.GCP_INSTANCES)
        self.assertTrue(any("n4-highmem" in i.name for i in instances))

    def test_default_is_aws(self) -> None:
        instances = vs.get_instances()
        self.assertIs(instances, vs.AWS_INSTANCES)


class TestRoundUpM(unittest.TestCase):
    """Tests for vs._round_up_m helper."""

    def test_exact_powers(self) -> None:
        self.assertEqual(vs._round_up_m(4), 4)
        self.assertEqual(vs._round_up_m(8), 8)
        self.assertEqual(vs._round_up_m(16), 16)

    def test_non_powers(self) -> None:
        self.assertEqual(vs._round_up_m(5), 8)
        self.assertEqual(vs._round_up_m(9), 16)
        self.assertEqual(vs._round_up_m(17), 32)

    def test_minimum_m(self) -> None:
        self.assertEqual(vs._round_up_m(1), vs.MIN_M)
        self.assertEqual(vs._round_up_m(2), vs.MIN_M)
        self.assertEqual(vs._round_up_m(3), vs.MIN_M)


class TestEstimateM(unittest.TestCase):
    """Tests for the vs._estimate_m function."""

    def test_small_dataset_low_recall(self) -> None:
        # raw_m = 1.5 * log10(10_000) * (-ln(1 - 0.70)) ~= 7.22 -> 8
        m = vs._estimate_m(10_000, 70)
        self.assertEqual(m, 8)

    def test_medium_dataset_95_recall(self) -> None:
        # raw_m = 1.5 * log10(10_000_000) * (-ln(1 - 0.95)) ~= 31.46 -> 32
        m = vs._estimate_m(10_000_000, 95)
        self.assertEqual(m, 32)

    def test_large_dataset_high_recall(self) -> None:
        # raw_m = 1.5 * log10(1_000_000_000) * (-ln(1 - 0.99)) ~= 62.17 -> 64
        m = vs._estimate_m(1_000_000_000, 99)
        self.assertEqual(m, 64)

    def test_m_increases_with_recall(self) -> None:
        n = 50_000_000
        m70 = vs._estimate_m(n, 70)
        m90 = vs._estimate_m(n, 90)
        m99 = vs._estimate_m(n, 99)
        self.assertLessEqual(m70, m90)
        self.assertLessEqual(m90, m99)

    def test_m_increases_with_dataset_size(self) -> None:
        recall = 95
        m_small = vs._estimate_m(10_000, recall)
        m_large = vs._estimate_m(1_000_000_000, recall)
        self.assertLessEqual(m_small, m_large)


class TestCompressionRatio(unittest.TestCase):
    """Tests for vs._get_compression_ratio - simplified to fixed ratios."""

    def test_none_always_1(self) -> None:
        self.assertEqual(vs._get_compression_ratio(vs.Quantization.NONE), 1.0)

    def test_scalar_always_3(self) -> None:
        self.assertEqual(
            vs._get_compression_ratio(vs.Quantization.SCALAR), vs.SCALAR_COMPRESSION_RATIO
        )

    def test_binary_always_10(self) -> None:
        self.assertEqual(
            vs._get_compression_ratio(vs.Quantization.BINARY),
            vs.BINARY_COMPRESSION_RATIO,
        )


class TestComputeIndexRam(unittest.TestCase):
    """Tests for vs._compute_index_ram function."""

    def test_basic_computation(self) -> None:
        # N=10_000, D=128, M=16, compression=1.0
        # graph = 16 * ln(10000) / ln(8) ~= 70.98
        # raw = 10000 * 4 * (128 + 70.98) * 1.1 = 8750183.5...
        # ceil -> 8_750_184
        ram = vs._compute_index_ram(
            num_vectors=10_000,
            dimensions=128,
            m=16,
            compression_ratio=1.0,
        )
        self.assertEqual(ram, 8_750_184)

    def test_ram_increases_with_vectors(self) -> None:
        r1 = vs._compute_index_ram(10_000, 128, 16, 1.0)
        r2 = vs._compute_index_ram(100_000, 128, 16, 1.0)
        self.assertGreater(r2, r1)

    def test_ram_increases_with_dimensions(self) -> None:
        r1 = vs._compute_index_ram(10_000, 128, 16, 1.0)
        r2 = vs._compute_index_ram(10_000, 1024, 16, 1.0)
        self.assertGreater(r2, r1)

    def test_compression_reduces_ram(self) -> None:
        none = vs._compute_index_ram(10_000, 768, 16, 1.0)
        scalar = vs._compute_index_ram(10_000, 768, 16, vs.SCALAR_COMPRESSION_RATIO)
        binary = vs._compute_index_ram(10_000, 768, 16, vs.BINARY_COMPRESSION_RATIO)
        self.assertGreater(none, scalar)
        self.assertGreater(scalar, binary)

    def test_compression_ratio_roughly_correct(self) -> None:
        n, d, m = 100_000, 768, 16
        none = vs._compute_index_ram(n, d, m, 1.0)
        scalar = vs._compute_index_ram(n, d, m, vs.SCALAR_COMPRESSION_RATIO)
        # Should be roughly 3x reduction
        ratio = none / scalar
        self.assertAlmostEqual(ratio, vs.SCALAR_COMPRESSION_RATIO, delta=0.5)

    def test_compression_ratio_roughly_correct_binary(self) -> None:
        n, d, m = 100_000, 768, 16
        none = vs._compute_index_ram(n, d, m, 1.0)
        binary = vs._compute_index_ram(n, d, m, vs.BINARY_COMPRESSION_RATIO)
        # Should be roughly 10x reduction
        ratio = none / binary
        self.assertAlmostEqual(ratio, vs.BINARY_COMPRESSION_RATIO, delta=0.5)


class TestRecallQpsMultiplier(unittest.TestCase):
    """Tests for vs._recall_qps_multiplier interpolation."""

    def test_recall_90_is_baseline(self) -> None:
        self.assertAlmostEqual(vs._recall_qps_multiplier(90), 1.0)

    def test_recall_95(self) -> None:
        self.assertAlmostEqual(vs._recall_qps_multiplier(95), 0.40)

    def test_recall_99(self) -> None:
        self.assertAlmostEqual(vs._recall_qps_multiplier(99), 0.02)

    def test_recall_70_is_2x(self) -> None:
        self.assertAlmostEqual(vs._recall_qps_multiplier(70), 2.0)

    def test_interpolation_between_70_and_90(self) -> None:
        # Linear: 70->2.0, 90->1.0. At 80: 2.0 + (1.0 - 2.0) * (10/20) = 1.5
        self.assertAlmostEqual(vs._recall_qps_multiplier(80), 1.5)
        # At 85: 2.0 + (1.0 - 2.0) * (15/20) = 1.25
        self.assertAlmostEqual(vs._recall_qps_multiplier(85), 1.25)

    def test_interpolation_between_90_and_95(self) -> None:
        m = vs._recall_qps_multiplier(92)
        self.assertGreater(m, 0.40)
        self.assertLess(m, 1.0)
        # Linear: 90→1.0, 95→0.40. At 92: 1.0 + (0.40 - 1.0) * (2/5) = 0.76
        self.assertAlmostEqual(m, 0.76)

    def test_interpolation_between_95_and_99(self) -> None:
        m = vs._recall_qps_multiplier(97)
        self.assertGreater(m, 0.02)
        self.assertLess(m, 0.40)
        # Linear: 95→0.40, 99→0.02. At 97: 0.40 + (0.02 - 0.40) * (2/4) = 0.21
        self.assertAlmostEqual(m, 0.21)

    def test_monotonically_decreasing(self) -> None:
        values = [vs._recall_qps_multiplier(r) for r in range(70, 100)]
        for i in range(len(values) - 1):
            self.assertGreaterEqual(values[i], values[i + 1])


class TestBaseQpsPerVcpu(unittest.TestCase):
    """Tests for vs._get_base_qps_per_vcpu bucket selection."""

    def test_small_dataset(self) -> None:
        label, qps = vs._get_base_qps_per_vcpu(100_000, 128)
        self.assertEqual(label, "low-medium")
        self.assertEqual(qps, 350.0)

    def test_medium_dataset(self) -> None:
        # 10M is in (5M, 100M] -> interpolated between 350 and 250.
        label, qps = vs._get_base_qps_per_vcpu(10_000_000, 512)
        self.assertEqual(label, "medium-large")
        expected = 350 + (250 - 350) * (10e6 - 5e6) / (100e6 - 5e6)
        self.assertAlmostEqual(qps, expected, places=2)

    def test_large_dataset(self) -> None:
        # 500M is in (100M, 1B] -> interpolated between 250 and 170.
        label, qps = vs._get_base_qps_per_vcpu(500_000_000, 768)
        self.assertEqual(label, "large-xlarge")
        expected = 250 + (170 - 250) * (500e6 - 100e6) / (1e9 - 100e6)
        self.assertAlmostEqual(qps, expected, places=2)

    def test_xlarge_dataset(self) -> None:
        # 2B is in (1B, 4B] -> interpolated between 170 and 80.
        label, qps = vs._get_base_qps_per_vcpu(2_000_000_000, 768)
        self.assertEqual(label, "xlarge+")
        expected = 170 + (80 - 170) * (2e9 - 1e9) / (4e9 - 1e9)
        self.assertAlmostEqual(qps, expected, places=2)


class TestEffectiveQpsPerVcpu(unittest.TestCase):
    """Tests for vs._effective_qps_per_vcpu."""

    def test_baseline_k10_recall_90(self) -> None:
        eff = vs._effective_qps_per_vcpu(350.0, 90, 10)
        self.assertAlmostEqual(eff, 350.0)

    def test_k100_halves_throughput(self) -> None:
        eff = vs._effective_qps_per_vcpu(350.0, 90, 100)
        self.assertAlmostEqual(eff, 175.0)

    def test_k50_intermediate(self) -> None:
        eff = vs._effective_qps_per_vcpu(350.0, 90, 50)
        # K factor: 1.0 - 0.5 * (50-10)/90 = 1.0 - 0.222 = 0.778
        expected = 350.0 * 0.778
        self.assertAlmostEqual(eff, expected, delta=1.0)

    def test_high_recall_reduces_throughput(self) -> None:
        eff_90 = vs._effective_qps_per_vcpu(350.0, 90, 10)
        eff_95 = vs._effective_qps_per_vcpu(350.0, 95, 10)
        eff_99 = vs._effective_qps_per_vcpu(350.0, 99, 10)
        self.assertGreater(eff_90, eff_95)
        self.assertGreater(eff_95, eff_99)

    def test_k_below_10(self) -> None:
        eff = vs._effective_qps_per_vcpu(350.0, 90, 1)
        self.assertAlmostEqual(eff, 350.0)


class TestSelectInstance(unittest.TestCase):
    """Tests for vs._select_instance function."""

    def test_small_deployment(self) -> None:
        sel = vs._select_instance(index_ram_gb=2.0, required_vcpus=2)
        self.assertGreaterEqual(sel.num_instances, vs.MIN_VECTOR_STORE_REPLICAS)
        self.assertGreaterEqual(sel.instance_type.ram_gb, 2.0)

    def test_large_deployment(self) -> None:
        sel = vs._select_instance(index_ram_gb=100.0, required_vcpus=64)
        self.assertGreaterEqual(sel.instance_type.ram_gb, 100.0)
        self.assertGreaterEqual(sel.total_vcpus, 64)

    def test_no_instance_large_enough_raises(self) -> None:
        largest = max(i.ram_gb for i in vs.AWS_INSTANCES)
        with self.assertRaises(ValueError):
            vs._select_instance(index_ram_gb=largest + 100, required_vcpus=1)

    def test_min_replicas_enforced(self) -> None:
        sel = vs._select_instance(index_ram_gb=1.0, required_vcpus=1)
        self.assertGreaterEqual(sel.num_instances, vs.MIN_VECTOR_STORE_REPLICAS)

    def test_cheapest_option_selected(self) -> None:
        # Small footprint — should pick a cheap instance.
        sel = vs._select_instance(index_ram_gb=2.0, required_vcpus=2)
        # With 2 replicas of a cheap instance, total cost should be reasonable.
        self.assertLess(sel.total_cost_per_hour, 10.0)

    def test_gcp_small_deployment(self) -> None:
        sel = vs._select_instance(
            index_ram_gb=2.0, required_vcpus=2,
            cloud_provider=vs.CloudProvider.GCP,
        )
        self.assertGreaterEqual(sel.num_instances, vs.MIN_VECTOR_STORE_REPLICAS)
        self.assertGreaterEqual(sel.instance_type.ram_gb, 2.0)
        # Should pick a GCP instance name.
        self.assertTrue(
            sel.instance_type.name.startswith("e2-") or
            sel.instance_type.name.startswith("n4-")
        )

    def test_gcp_large_deployment(self) -> None:
        sel = vs._select_instance(
            index_ram_gb=100.0, required_vcpus=64,
            cloud_provider=vs.CloudProvider.GCP,
        )
        self.assertGreaterEqual(sel.instance_type.ram_gb, 100.0)
        self.assertGreaterEqual(sel.total_vcpus, 64)

    def test_gcp_no_instance_large_enough_raises(self) -> None:
        largest = max(i.ram_gb for i in vs.GCP_INSTANCES)
        with self.assertRaises(ValueError):
            vs._select_instance(
                index_ram_gb=largest + 100, required_vcpus=1,
                cloud_provider=vs.CloudProvider.GCP,
            )


class TestSizingInputValidation(unittest.TestCase):
    """Tests for vs.SizingInput validation rules."""

    def test_defaults_are_valid(self) -> None:
        inp = vs.SizingInput()
        self.assertEqual(inp.num_vectors, vs.DEFAULT_NUM_VECTORS)
        self.assertEqual(inp.dimensions, vs.DEFAULT_DIMENSIONS)
        self.assertEqual(inp.target_qps, vs.DEFAULT_TARGET_QPS)
        self.assertEqual(inp.recall, vs.DEFAULT_RECALL)
        self.assertEqual(inp.k, vs.DEFAULT_K)
        self.assertEqual(inp.quantization, vs.Quantization.NONE)
        self.assertEqual(inp.metadata_bytes_per_vector, vs.DEFAULT_METADATA_BYTES)
        self.assertEqual(inp.filtering_columns, vs.MIN_FILTERING_COLUMNS)

    def test_num_vectors_below_min(self) -> None:
        with self.assertRaises(ValueError):
            vs.SizingInput(num_vectors=9_999)

    def test_num_vectors_above_max(self) -> None:
        with self.assertRaises(ValueError):
            vs.SizingInput(num_vectors=vs.MAX_VECTORS + 1)

    def test_num_vectors_at_min(self) -> None:
        inp = vs.SizingInput(num_vectors=vs.MIN_VECTORS)
        self.assertEqual(inp.num_vectors, vs.MIN_VECTORS)

    def test_num_vectors_at_max(self) -> None:
        inp = vs.SizingInput(num_vectors=vs.MAX_VECTORS)
        self.assertEqual(inp.num_vectors, vs.MAX_VECTORS)

    def test_dimensions_below_min(self) -> None:
        with self.assertRaises(ValueError):
            vs.SizingInput(dimensions=0)

    def test_dimensions_above_max(self) -> None:
        with self.assertRaises(ValueError):
            vs.SizingInput(dimensions=vs.MAX_DIMENSIONS + 1)

    def test_target_qps_zero_rejected(self) -> None:
        with self.assertRaises(ValueError):
            vs.SizingInput(target_qps=0)

    def test_target_qps_below_min(self) -> None:
        with self.assertRaises(ValueError):
            vs.SizingInput(target_qps=5)

    def test_target_qps_above_max(self) -> None:
        with self.assertRaises(ValueError):
            vs.SizingInput(target_qps=vs.MAX_QPS + 1)

    def test_target_qps_at_min(self) -> None:
        inp = vs.SizingInput(target_qps=vs.MIN_QPS)
        self.assertEqual(inp.target_qps, vs.MIN_QPS)

    def test_recall_below_min(self) -> None:
        with self.assertRaises(ValueError):
            vs.SizingInput(recall=69)

    def test_recall_above_max(self) -> None:
        with self.assertRaises(ValueError):
            vs.SizingInput(recall=100)

    def test_recall_at_bounds(self) -> None:
        vs.SizingInput(recall=vs.MIN_RECALL)
        vs.SizingInput(recall=vs.MAX_RECALL)

    def test_k_below_min(self) -> None:
        with self.assertRaises(ValueError):
            vs.SizingInput(k=0)

    def test_k_above_max(self) -> None:
        with self.assertRaises(ValueError):
            vs.SizingInput(k=vs.MAX_K + 1)

    def test_metadata_below_min(self) -> None:
        with self.assertRaises(ValueError):
            vs.SizingInput(metadata_bytes_per_vector=3)

    def test_metadata_above_max(self) -> None:
        with self.assertRaises(ValueError):
            vs.SizingInput(metadata_bytes_per_vector=vs.MAX_METADATA_BYTES + 1)

    def test_metadata_at_bounds(self) -> None:
        vs.SizingInput(metadata_bytes_per_vector=vs.MIN_METADATA_BYTES)
        vs.SizingInput(metadata_bytes_per_vector=vs.MAX_METADATA_BYTES)

    def test_filtering_columns_below_min(self) -> None:
        with self.assertRaises(ValueError):
            vs.SizingInput(filtering_columns=-1)

    def test_filtering_columns_above_max(self) -> None:
        with self.assertRaises(ValueError):
            vs.SizingInput(filtering_columns=vs.MAX_FILTERING_COLUMNS + 1)

    def test_filtering_columns_at_bounds(self) -> None:
        vs.SizingInput(filtering_columns=0)
        vs.SizingInput(filtering_columns=vs.MAX_FILTERING_COLUMNS)


class TestComputeSizingBasic(unittest.TestCase):
    """Basic end-to-end tests for vs.compute_sizing."""

    def test_default_input(self) -> None:
        result = vs.compute_sizing(vs.SizingInput())
        self.assertIsInstance(result, vs.SizingResult)
        self.assertAlmostEqual(result.vector_store_node.total_ram_gb, 381.79, delta=0.01)
        self.assertEqual(result.vector_store_node.required_vcpus, 13)
        self.assertAlmostEqual(result.scylladb_node.total_storage_gb, 290.76, delta=0.01)

    def test_summary_populated(self) -> None:
        result = vs.compute_sizing(vs.SizingInput())
        self.assertIn("ScyllaDB VSS Sizing Summary", result.summary)
        self.assertIn("50,000,000", result.summary)

    def test_returns_all_components(self) -> None:
        result = vs.compute_sizing(vs.SizingInput())
        self.assertIsInstance(result.hnsw_params, vs.HNSWParams)
        self.assertIsInstance(result.vector_store_node, vs.VectorStoreNodeSizing)
        self.assertIsInstance(result.scylladb_node, vs.ScyllaDBNodeSizing)
        self.assertIsInstance(result.instance_selection, vs.InstanceSelection)


class TestComputeSizingQuantization(unittest.TestCase):
    """Test quantization effects on sizing."""

    def setUp(self) -> None:
        self.base = vs.SizingInput(num_vectors=10_000_000, dimensions=768)
        self.base_result = vs.compute_sizing(self.base)

    def test_scalar_reduces_ram(self) -> None:
        inp = vs.SizingInput(
            num_vectors=10_000_000,
            dimensions=768,
            quantization=vs.Quantization.SCALAR,
        )
        result = vs.compute_sizing(inp)
        self.assertLess(
            result.vector_store_node.index_ram_bytes,
            self.base_result.vector_store_node.index_ram_bytes,
        )

    def test_binary_reduces_ram_most(self) -> None:
        scalar_inp = vs.SizingInput(
            num_vectors=10_000_000,
            dimensions=768,
            quantization=vs.Quantization.SCALAR,
        )
        binary_inp = vs.SizingInput(
            num_vectors=10_000_000,
            dimensions=768,
            quantization=vs.Quantization.BINARY,
        )
        scalar_result = vs.compute_sizing(scalar_inp)
        binary_result = vs.compute_sizing(binary_inp)
        self.assertLess(
            binary_result.vector_store_node.index_ram_bytes,
            scalar_result.vector_store_node.index_ram_bytes,
        )

    def test_scalar_compression_ratio(self) -> None:
        inp = vs.SizingInput(
            num_vectors=10_000_000,
            dimensions=768,
            quantization=vs.Quantization.SCALAR,
        )
        result = vs.compute_sizing(inp)
        self.assertEqual(result.compression_ratio, vs.SCALAR_COMPRESSION_RATIO)

    def test_binary_compression_ratio(self) -> None:
        inp = vs.SizingInput(
            num_vectors=10_000_000,
            dimensions=768,
            quantization=vs.Quantization.BINARY,
        )
        result = vs.compute_sizing(inp)
        self.assertEqual(result.compression_ratio, vs.BINARY_COMPRESSION_RATIO)


class TestScalarQuantizationDoubleK(unittest.TestCase):
    """SCALAR quantization doubles the effective K for throughput sizing."""

    def test_scalar_needs_more_vcpus_than_none_same_k(self) -> None:
        # With SCALAR, effective_k = k * 2, so more vCPUs are needed for throughput.
        # It is the result of oversampling procedure.
        # But RAM is reduced, so compare throughput aspect only (same dataset).
        none_inp = vs.SizingInput(
            num_vectors=10_000_000, dimensions=768, k=50,
            quantization=vs.Quantization.NONE,
        )
        scalar_inp = vs.SizingInput(
            num_vectors=10_000_000, dimensions=768, k=50,
            quantization=vs.Quantization.SCALAR,
        )
        none_r = vs.compute_sizing(none_inp)
        scalar_r = vs.compute_sizing(scalar_inp)
        # SCALAR should have lower effective_qps_per_vcpu because K is doubled.
        self.assertLess(
            scalar_r.vector_store_node.effective_qps_per_vcpu,
            none_r.vector_store_node.effective_qps_per_vcpu,
        )


class TestBinaryQuantizationQuadrupleQps(unittest.TestCase):
    """BINARY quantization quadruples the effective QPS for throughput sizing."""

    def test_binary_uses_quadrupled_qps(self) -> None:
        inp = vs.SizingInput(
            num_vectors=10_000_000,
            dimensions=768,
            target_qps=1_000,
            quantization=vs.Quantization.BINARY,
        )
        result = vs.compute_sizing(inp)
        # With BINARY, the effective QPS is 4×1000 = 4000, so more vCPUs
        # are needed compared to 1000 QPS with no quantization (other factors
        # being equal). This is the result of rescoring.
        none_inp = vs.SizingInput(
            num_vectors=10_000_000,
            dimensions=768,
            target_qps=1_000,
            quantization=vs.Quantization.NONE,
        )
        none_result = vs.compute_sizing(none_inp)
        # BINARY should need significantly more search vCPUs due to 4x QPS.
        self.assertGreater(
            result.vector_store_node.required_vcpus,
            none_result.vector_store_node.required_vcpus,
        )


class TestFilteringColumns(unittest.TestCase):
    """Tests for filtering columns RAM overhead."""

    def test_zero_filtering_columns(self) -> None:
        result = vs.compute_sizing(vs.SizingInput(filtering_columns=0))
        self.assertEqual(result.vector_store_node.filtering_ram_bytes, 0)

    def test_filtering_columns_adds_ram(self) -> None:
        no_filter = vs.compute_sizing(vs.SizingInput(filtering_columns=0))
        with_filter = vs.compute_sizing(vs.SizingInput(filtering_columns=5))
        self.assertGreater(
            with_filter.vector_store_node.total_ram_bytes,
            no_filter.vector_store_node.total_ram_bytes,
        )

    def test_filtering_ram_formula(self) -> None:
        n = 50_000_000
        cols = 3
        result = vs.compute_sizing(vs.SizingInput(
            num_vectors=n, filtering_columns=cols,
        ))
        expected = cols * vs.FILTERING_COLUMN_BYTES_PER_VECTOR * n
        self.assertEqual(result.vector_store_node.filtering_ram_bytes, expected)

    def test_filtering_ram_does_not_affect_index_ram(self) -> None:
        r0 = vs.compute_sizing(vs.SizingInput(filtering_columns=0))
        r5 = vs.compute_sizing(vs.SizingInput(filtering_columns=5))
        self.assertEqual(
            r0.vector_store_node.index_ram_bytes,
            r5.vector_store_node.index_ram_bytes,
        )


class TestRecallLevels(unittest.TestCase):
    """Verify recall affects HNSW M and throughput monotonically."""

    def test_m_increases_with_recall(self) -> None:
        results = {}
        for r in [70, 80, 90, 95, 99]:
            inp = vs.SizingInput(recall=r)
            results[r] = vs.compute_sizing(inp)
        ms = [results[r].hnsw_params.m for r in [70, 80, 90, 95, 99]]
        # M should be non-decreasing with recall.
        for i in range(len(ms) - 1):
            self.assertLessEqual(ms[i], ms[i + 1])

    def test_throughput_decreases_with_recall(self) -> None:
        results = {}
        for r in [70, 90, 95, 99]:
            inp = vs.SizingInput(recall=r)
            results[r] = vs.compute_sizing(inp)
        qps = [results[r].vector_store_node.effective_qps_per_vcpu for r in [70, 90, 95, 99]]
        for i in range(len(qps) - 1):
            self.assertGreaterEqual(qps[i], qps[i + 1])


class TestKValues(unittest.TestCase):
    """Verify K affects throughput."""

    def test_k1_higher_throughput_than_k100(self) -> None:
        r1 = vs.compute_sizing(vs.SizingInput(k=1))
        r100 = vs.compute_sizing(vs.SizingInput(k=100))
        self.assertGreater(
            r1.vector_store_node.effective_qps_per_vcpu,
            r100.vector_store_node.effective_qps_per_vcpu,
        )

    def test_k10_equals_baseline_throughput(self) -> None:
        result = vs.compute_sizing(vs.SizingInput(k=10, recall=90))
        self.assertAlmostEqual(
            result.vector_store_node.effective_qps_per_vcpu,
            result.vector_store_node.base_qps_per_vcpu,
            places=2,
        )


class TestScyllaDBNodeSizingComputation(unittest.TestCase):
    """Tests for ScyllaDB-node sizing."""

    def test_scylladb_nodes_equal_replication_factor(self) -> None:
        result = vs.compute_sizing(vs.SizingInput())
        self.assertEqual(result.scylladb_node.num_nodes, vs.REPLICATION_FACTOR)

    def test_storage_breakdown(self) -> None:
        n, d = 10_000_000, 768
        result = vs.compute_sizing(vs.SizingInput(
            num_vectors=n, dimensions=d, metadata_bytes_per_vector=100,
        ))
        expected_embedding = n * d * vs.FLOAT32_BYTES
        expected_metadata = n * 100
        self.assertEqual(
            result.scylladb_node.embedding_storage_bytes, expected_embedding
        )
        self.assertEqual(
            result.scylladb_node.metadata_storage_bytes, expected_metadata
        )
        self.assertEqual(
            result.scylladb_node.total_storage_bytes,
            expected_embedding + expected_metadata,
        )

    def test_scylladb_vcpus_derived_from_search_vcpus(self) -> None:
        result = vs.compute_sizing(vs.SizingInput())
        vs_vcpus = result.vector_store_node.required_vcpus
        expected_total = max(
            1, math.ceil(vs_vcpus / vs.VS_TO_SCYLLADB_VCPU_RATIO),
        )
        expected_per_node = max(
            1, math.ceil(expected_total / vs.REPLICATION_FACTOR),
        )
        self.assertEqual(result.scylladb_node.vcpus_per_node, expected_per_node)




class TestInstanceSelection(unittest.TestCase):
    """Additional instance selection tests."""

    def test_vcpu_requirement_met(self) -> None:
        # Request 100 vCPUs, should provision enough instances.
        sel = vs._select_instance(index_ram_gb=2.0, required_vcpus=100)
        self.assertGreaterEqual(sel.total_vcpus, 100)

    def test_ram_requirement_met(self) -> None:
        sel = vs._select_instance(index_ram_gb=200.0, required_vcpus=10)
        self.assertGreaterEqual(sel.instance_type.ram_gb, 200.0)

    def test_more_than_two_replicas_divisible_by_three(self) -> None:
        """When more than 2 replicas are needed, count must be divisible by 3."""
        # 100 vCPUs on a 2-vCPU instance -> ceil(100/2) = 50 -> rounded to 51.
        sel = vs._select_instance(index_ram_gb=2.0, required_vcpus=100)
        if sel.num_instances > vs.MIN_VECTOR_STORE_REPLICAS:
            self.assertEqual(sel.num_instances % 3, 0)

    def test_exactly_two_replicas_not_rounded(self) -> None:
        """Two replicas (the HA minimum) should stay at 2, not be rounded to 3."""
        sel = vs._select_instance(index_ram_gb=2.0, required_vcpus=1)
        self.assertEqual(sel.num_instances, vs.MIN_VECTOR_STORE_REPLICAS)

    def test_gcp_vcpu_requirement_met(self) -> None:
        sel = vs._select_instance(
            index_ram_gb=2.0, required_vcpus=100,
            cloud_provider=vs.CloudProvider.GCP,
        )
        self.assertGreaterEqual(sel.total_vcpus, 100)

    def test_gcp_more_than_two_replicas_divisible_by_three(self) -> None:
        sel = vs._select_instance(
            index_ram_gb=2.0, required_vcpus=100,
            cloud_provider=vs.CloudProvider.GCP,
        )
        if sel.num_instances > vs.MIN_VECTOR_STORE_REPLICAS:
            self.assertEqual(sel.num_instances % 3, 0)


class TestInstanceUpscaling(unittest.TestCase):
    """Tests for the upscale optimisation phase of instance selection."""

    def test_no_upscale_when_three_or_fewer_instances(self) -> None:
        """When <= 3 instances suffice, the smallest eligible is returned."""
        sel = vs._select_instance(index_ram_gb=2.0, required_vcpus=4)
        # t4g.medium (2 vCPUs, 4 GB): ceil(4/2)=2 instances, no upscale.
        self.assertEqual(sel.instance_type.name, "t4g.medium")
        self.assertEqual(sel.num_instances, 2)

    def test_upscale_reduces_instance_count(self) -> None:
        """Upscaling should pick a larger instance to reduce instance count."""
        # Smallest eligible for 22 GB RAM: r7g.xlarge (32 GB, 4 vCPUs).
        # With 40 vCPUs: ceil(40/4) = 10 -> 12 instances (>3, triggers upscale).
        # r7g.4xlarge (128 GB, 16 vCPUs): ceil(40/16) = 3 instances, cost
        # within 10% threshold.
        sel = vs._select_instance(index_ram_gb=22.0, required_vcpus=40)
        self.assertLessEqual(sel.num_instances, vs.INSTANCE_UPSCALE_TRIGGER)
        self.assertEqual(sel.instance_type.name, "r7g.4xlarge")
        self.assertEqual(sel.num_instances, 3)

    def test_upscale_respects_cost_threshold(self) -> None:
        """Upscaling must not exceed INSTANCE_UPSCALE_COST_FACTOR × initial cost."""
        # Smallest eligible for 22 GB RAM: r7g.xlarge (32 GB, 4 vCPUs).
        # 40 vCPUs → 12 instances → initial cost = 12 × $0.4285 = $5.142.
        # Max cost = $5.142 × 1.1 = $5.6562.
        # r7g.8xlarge (32 vCPUs): 2 instances → $6.854 (exceeds threshold).
        # So r7g.4xlarge (3 instances, ~$5.14) should be picked, not r7g.8xlarge.
        sel = vs._select_instance(index_ram_gb=22.0, required_vcpus=40)
        cost = sel.num_instances * sel.instance_type.cost_per_hour
        initial_cost = 12 * 0.4285  # r7g.xlarge × 12
        self.assertLessEqual(cost, initial_cost * vs.INSTANCE_UPSCALE_COST_FACTOR)

    def test_upscale_not_applied_when_all_larger_exceed_budget(self) -> None:
        """If all larger instances exceed the cost threshold, keep the initial."""
        # t4g.medium (2 vCPUs, $0.067): 100 vCPUs → 51 instances → $3.417.
        # Max cost $3.759.  All larger instances need ≥50 replicas at higher
        # per-unit cost, so none fit within 10%.
        sel = vs._select_instance(index_ram_gb=2.0, required_vcpus=100)
        self.assertEqual(sel.instance_type.name, "t4g.medium")
        self.assertEqual(sel.num_instances, 51)

    def test_upscale_picks_smallest_that_fits_budget(self) -> None:
        """Among multiple candidates within budget, pick the one with fewest instances."""
        # r7g.xlarge (4 vCPUs): 40 vCPUs → 12 instances.
        # r7g.2xlarge (8 vCPUs): 6 instances, within budget.
        # r7g.4xlarge (16 vCPUs): 3 instances, within budget (fewest).
        # r7g.8xlarge (32 vCPUs): 2 instances, exceeds budget.
        sel = vs._select_instance(index_ram_gb=22.0, required_vcpus=40)
        self.assertEqual(sel.num_instances, 3)
        self.assertEqual(sel.instance_type.name, "r7g.4xlarge")

    def test_upscale_gcp(self) -> None:
        """Upscaling works with GCP instances too."""
        # Smallest GCP for 10 GB RAM: n4-highmem-2 (16 GB, 2 vCPUs).
        # With 20 vCPUs: ceil(20/2) = 10 → 12 instances.
        # n4-highmem-4 (4 vCPUs): 6 instances.
        # n4-highmem-8 (8 vCPUs): 3 instances.
        # Check that upscaling reduces count.
        sel = vs._select_instance(
            index_ram_gb=10.0, required_vcpus=20,
            cloud_provider=vs.CloudProvider.GCP,
        )
        self.assertLessEqual(sel.num_instances, vs.INSTANCE_UPSCALE_TRIGGER)


class TestEndToEndRAG(unittest.TestCase):
    """End-to-end scenario: RAG application."""

    def test_rag_scenario(self) -> None:
        # Typical RAG: 100M documents with OpenAI embeddings (1536d).
        inp = vs.SizingInput(
            num_vectors=100_000_000,
            dimensions=1536,
            target_qps=5_000,
            recall=95,
            k=10,
            quantization=vs.Quantization.NONE,
            metadata_bytes_per_vector=256,
        )
        result = vs.compute_sizing(inp)

        # Basic sanity checks for a RAG-scale deployment.
        self.assertGreater(result.vector_store_node.total_ram_gb, 100)
        self.assertGreater(result.vector_store_node.required_vcpus, 10)
        self.assertGreater(result.scylladb_node.total_storage_gb, 100)
        self.assertGreaterEqual(result.vector_store_replicas, vs.MIN_VECTOR_STORE_REPLICAS)
        self.assertEqual(result.instance_selection.instance_type.name, "r7i.48xlarge")
        self.assertEqual(result.instance_selection.num_instances, 2)


class TestEndToEndImageSearch(unittest.TestCase):
    """End-to-end scenario: Image similarity search."""

    def test_image_search_scenario(self) -> None:
        inp = vs.SizingInput(
            num_vectors=50_000_000,
            dimensions=128,
            target_qps=10_000,
            recall=90,
            k=20,
            quantization=vs.Quantization.SCALAR,
            metadata_bytes_per_vector=64,
        )
        result = vs.compute_sizing(inp)

        # RAM should be relatively modest (128d, SCALAR compressed).
        self.assertLess(result.vector_store_node.total_ram_gb, 50)
        self.assertEqual(result.compression_ratio, vs.SCALAR_COMPRESSION_RATIO)
        self.assertGreater(result.vector_store_node.required_vcpus, 0)
        # Smallest eligible is r7g.xlarge (32 GB, 4 vCPUs) → 12 instances,
        # but the upscale optimisation reduces to r7g.4xlarge (128 GB,
        # 16 vCPUs) with only 3 instances at comparable cost.
        self.assertEqual(result.instance_selection.instance_type.name, "r7g.4xlarge")
        self.assertEqual(result.instance_selection.num_instances, 3)


class TestEndToEndLargeScale(unittest.TestCase):
    """End-to-end scenario: Large-scale deployment."""

    def test_large_scale_binary(self) -> None:
        inp = vs.SizingInput(
            num_vectors=1_000_000_000,
            dimensions=768,
            target_qps=50_000,
            recall=90,
            k=10,
            quantization=vs.Quantization.BINARY,
            metadata_bytes_per_vector=100,
            filtering_columns=2,
        )
        result = vs.compute_sizing(inp)

        # Should use BINARY compression.
        self.assertEqual(result.compression_ratio, vs.BINARY_COMPRESSION_RATIO)
        # Filtering RAM should be significant.
        expected_filter_ram = (
            2 * vs.FILTERING_COLUMN_BYTES_PER_VECTOR * 1_000_000_000
        )
        self.assertEqual(
            result.vector_store_node.filtering_ram_bytes, expected_filter_ram
        )
        self.assertEqual(result.instance_selection.instance_type.name, "r7g.16xlarge")
        self.assertEqual(result.instance_selection.num_instances, 21)


class TestSummaryFormat(unittest.TestCase):
    """Tests for the human-readable summary string."""

    def test_summary_contains_key_info(self) -> None:
        result = vs.compute_sizing(vs.SizingInput(
            num_vectors=10_000_000, dimensions=768, filtering_columns=2,
        ))
        self.assertIn("10,000,000", result.summary)
        self.assertIn("768", result.summary)
        self.assertIn("Filtering columns: 2", result.summary)

    def test_summary_quantization_label(self) -> None:
        for q in vs.Quantization:
            result = vs.compute_sizing(vs.SizingInput(quantization=q))
            self.assertIn(q.value, result.summary)


class TestGCPEndToEnd(unittest.TestCase):
    """End-to-end tests using GCP cloud provider."""

    def test_default_input_gcp(self) -> None:
        result = vs.compute_sizing(vs.SizingInput(
            cloud_provider=vs.CloudProvider.GCP,
        ))
        self.assertIsInstance(result, vs.SizingResult)
        self.assertEqual(result.instance_selection.instance_type.name, "n4-highmem-48")
        self.assertEqual(result.instance_selection.num_instances, 2)

    def test_gcp_small_deployment(self) -> None:
        inp = vs.SizingInput(
            num_vectors=100_000,
            dimensions=128,
            target_qps=100,
            recall=90,
            k=10,
            cloud_provider=vs.CloudProvider.GCP,
        )
        result = vs.compute_sizing(inp)
        self.assertEqual(result.instance_selection.instance_type.name, "e2-medium")
        self.assertEqual(result.instance_selection.num_instances, 2)

    def test_gcp_large_deployment(self) -> None:
        inp = vs.SizingInput(
            num_vectors=50_000_000,
            dimensions=1536,
            target_qps=5_000,
            recall=95,
            k=10,
            cloud_provider=vs.CloudProvider.GCP,
        )
        result = vs.compute_sizing(inp)
        self.assertGreater(result.vector_store_node.total_ram_gb, 100)
        self.assertEqual(result.instance_selection.instance_type.name, "n4-highmem-48")
        self.assertEqual(result.instance_selection.num_instances, 2)

    def test_gcp_no_instance_for_huge_index(self) -> None:
        """GCP max RAM is 640 GB; index requiring more should raise."""
        with self.assertRaises(ValueError):
            vs.compute_sizing(vs.SizingInput(
                num_vectors=4_000_000_000,
                dimensions=4096,
                cloud_provider=vs.CloudProvider.GCP,
            ))

    def test_cloud_provider_default_is_aws(self) -> None:
        inp = vs.SizingInput()
        self.assertEqual(inp.cloud_provider, vs.CloudProvider.AWS)


if __name__ == "__main__":
    unittest.main()
