/*
 * Cosine distance kernels for Vector Store's HNSW indexes: 1 - ab / sqrt(a2 * b2).
 *
 * Derived from NumKong 7.8.6 (https://github.com/ashvardanian/NumKong) and the three kernel
 * patches of its scylladb/NumKong fork (fea6c757, aa53affa, 2e43be7d), which Vector Store built
 * against until it moved these kernels here (VECTOR-1086).
 *
 * Copyright Ash Vardanian and the NumKong contributors.
 * Changes Copyright 2026-present ScyllaDB.
 * SPDX-License-Identifier: Apache-2.0 (see LICENSE-NumKong in this directory)
 *
 * NumKong accumulates f32 in f64 and converts f16 lane by lane, for a result within a few ULP of
 * a double-double reference. An HNSW search only orders distances and cannot use that precision,
 * so these kernels accumulate in f32, in two independent chains per sum so that a core is not
 * bound by the multiply-add latency, and the f16 ones use the widening multiply-adds (FMLAL on
 * FEAT_FHM, FMLALB/FMLALT on SVE2) instead of converting. Each kernel is the fork's code; only
 * the helpers it shared with the rest of NumKong are inlined here.
 *
 * Every kernel returns what NumKong's angular kernel returns, as f32: 0 when both vectors are
 * zero, 1 when the dot product is zero, otherwise 1 - ab / sqrt(a2 * b2) clamped at 0, and NaN
 * when an input is NaN. The square roots are reciprocal estimates refined by Newton-Raphson, as
 * in NumKong.
 *
 * The translation unit is compiled for the target's baseline ISA. Each kernel enables the
 * extensions it needs with a target pragma, and the caller (src/lib.rs) calls it only on a CPU
 * that has them.
 */

#include <stddef.h>
#include <stdint.h>

#if defined(__aarch64__)

#include <arm_neon.h>
#include <arm_sve.h>

#if defined(__clang__)
#pragma clang attribute push(__attribute__((target("arch=armv8-a+simd"))), apply_to = function)
#elif defined(__GNUC__)
#pragma GCC push_options
#pragma GCC target("arch=armv8-a+simd")
#endif

/* NumKong's `nk_angular_normalize_f64_neon_`. */
static inline double vs_cosine_normalize_f64_neon(double ab, double a2, double b2) {
    if (a2 == 0 && b2 == 0) return 0;
    if (ab == 0) return 1;
    double squares[2] = {a2, b2};
    float64x2_t squares_f64x2 = vld1q_f64(squares);
    // Three Newton-Raphson rounds of the reciprocal square root estimate, for f64 precision.
    float64x2_t rsqrts_f64x2 = vrsqrteq_f64(squares_f64x2);
    rsqrts_f64x2 = vmulq_f64(rsqrts_f64x2, vrsqrtsq_f64(vmulq_f64(squares_f64x2, rsqrts_f64x2), rsqrts_f64x2));
    rsqrts_f64x2 = vmulq_f64(rsqrts_f64x2, vrsqrtsq_f64(vmulq_f64(squares_f64x2, rsqrts_f64x2), rsqrts_f64x2));
    rsqrts_f64x2 = vmulq_f64(rsqrts_f64x2, vrsqrtsq_f64(vmulq_f64(squares_f64x2, rsqrts_f64x2), rsqrts_f64x2));
    vst1q_f64(squares, rsqrts_f64x2);
    double result = 1 - ab * squares[0] * squares[1];
    return result < 0 ? 0 : result;
}

/* NumKong's `nk_angular_normalize_f32_neon_`. */
static inline float vs_cosine_normalize_f32_neon(float ab, float a2, float b2) {
    if (a2 == 0 && b2 == 0) return 0;
    if (ab == 0) return 1;
    float squares[2] = {a2, b2};
    float32x2_t squares_f32x2 = vld1_f32(squares);
    // Two Newton-Raphson rounds of the reciprocal square root estimate, for f32 precision.
    float32x2_t rsqrts_f32x2 = vrsqrte_f32(squares_f32x2);
    rsqrts_f32x2 = vmul_f32(rsqrts_f32x2, vrsqrts_f32(vmul_f32(squares_f32x2, rsqrts_f32x2), rsqrts_f32x2));
    rsqrts_f32x2 = vmul_f32(rsqrts_f32x2, vrsqrts_f32(vmul_f32(squares_f32x2, rsqrts_f32x2), rsqrts_f32x2));
    vst1_f32(squares, rsqrts_f32x2);
    float result = 1 - ab * squares[0] * squares[1];
    return result < 0 ? 0 : result;
}

/* The fork's `nk_angular_f32_neon` with NK_ACCUMULATE_F32_IN_F64=0 (fea6c757). */
float vs_cosine_f32_neon(float const *a, float const *b, size_t n) {
    float32x4_t ab0 = vdupq_n_f32(0), a20 = vdupq_n_f32(0), b20 = vdupq_n_f32(0);
    float32x4_t ab1 = vdupq_n_f32(0), a21 = vdupq_n_f32(0), b21 = vdupq_n_f32(0);
    size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        float32x4_t av0 = vld1q_f32(a + i), bv0 = vld1q_f32(b + i);
        float32x4_t av1 = vld1q_f32(a + i + 4), bv1 = vld1q_f32(b + i + 4);
        ab0 = vfmaq_f32(ab0, av0, bv0), a20 = vfmaq_f32(a20, av0, av0), b20 = vfmaq_f32(b20, bv0, bv0);
        ab1 = vfmaq_f32(ab1, av1, bv1), a21 = vfmaq_f32(a21, av1, av1), b21 = vfmaq_f32(b21, bv1, bv1);
    }
    for (; i + 4 <= n; i += 4) {
        float32x4_t av = vld1q_f32(a + i), bv = vld1q_f32(b + i);
        ab0 = vfmaq_f32(ab0, av, bv), a20 = vfmaq_f32(a20, av, av), b20 = vfmaq_f32(b20, bv, bv);
    }
    float ab = vaddvq_f32(vaddq_f32(ab0, ab1));
    float a2 = vaddvq_f32(vaddq_f32(a20, a21));
    float b2 = vaddvq_f32(vaddq_f32(b20, b21));
    for (; i < n; ++i) {
        float ai = a[i], bi = b[i];
        ab += ai * bi, a2 += ai * ai, b2 += bi * bi;
    }
    return (float)vs_cosine_normalize_f64_neon((double)ab, (double)a2, (double)b2);
}

#if defined(__clang__)
#pragma clang attribute pop
#elif defined(__GNUC__)
#pragma GCC pop_options
#endif

#if defined(__clang__)
#pragma clang attribute push(__attribute__((target("arch=armv8.2-a+sve"))), apply_to = function)
#elif defined(__GNUC__)
#pragma GCC push_options
#pragma GCC target("arch=armv8.2-a+sve")
#endif

/* The fork's `nk_angular_f32_sve` with NK_ACCUMULATE_F32_IN_F64=0 (fea6c757). */
float vs_cosine_f32_sve(float const *a, float const *b, size_t n) {
    svfloat32_t ab0 = svdup_n_f32(0.f), a20 = svdup_n_f32(0.f), b20 = svdup_n_f32(0.f);
    svfloat32_t ab1 = svdup_n_f32(0.f), a21 = svdup_n_f32(0.f), b21 = svdup_n_f32(0.f);
    size_t const lanes = svcntw();
    size_t i = 0;
    for (; i + 2 * lanes <= n; i += 2 * lanes) {
        svbool_t pg = svptrue_b32();
        svfloat32_t av0 = svld1_f32(pg, a + i), bv0 = svld1_f32(pg, b + i);
        svfloat32_t av1 = svld1_f32(pg, a + i + lanes), bv1 = svld1_f32(pg, b + i + lanes);
        ab0 = svmla_f32_x(pg, ab0, av0, bv0), a20 = svmla_f32_x(pg, a20, av0, av0);
        b20 = svmla_f32_x(pg, b20, bv0, bv0);
        ab1 = svmla_f32_x(pg, ab1, av1, bv1), a21 = svmla_f32_x(pg, a21, av1, av1);
        b21 = svmla_f32_x(pg, b21, bv1, bv1);
    }
    for (; i < n; i += lanes) {
        svbool_t pg = svwhilelt_b32_u64(i, n);
        svfloat32_t av = svld1_f32(pg, a + i), bv = svld1_f32(pg, b + i);
        ab0 = svmla_f32_m(pg, ab0, av, bv), a20 = svmla_f32_m(pg, a20, av, av), b20 = svmla_f32_m(pg, b20, bv, bv);
    }
    svbool_t all = svptrue_b32();
    float ab = svaddv_f32(all, svadd_f32_x(all, ab0, ab1));
    float a2 = svaddv_f32(all, svadd_f32_x(all, a20, a21));
    float b2 = svaddv_f32(all, svadd_f32_x(all, b20, b21));
    return (float)vs_cosine_normalize_f64_neon((double)ab, (double)a2, (double)b2);
}

#if defined(__clang__)
#pragma clang attribute pop
#elif defined(__GNUC__)
#pragma GCC pop_options
#endif

#if defined(__clang__)
#pragma clang attribute push(__attribute__((target("arch=armv8.2-a+simd+fp16+fp16fml"))), apply_to = function)
#elif defined(__GNUC__)
#pragma GCC push_options
#pragma GCC target("arch=armv8.2-a+simd+fp16+fp16fml")
#endif

/* NumKong's `nk_partial_load_b16x8_serial_`: the first n < 8 halves, the rest zero. */
static inline uint16x8_t vs_cosine_partial_load_b16x8(uint16_t const *src, size_t n) {
    union {
        uint16x8_t u16x8;
        uint16_t u16s[8];
    } dst = {.u16s = {0, 0, 0, 0, 0, 0, 0, 0}};
    for (size_t i = 0; i < n && i < 8; ++i) dst.u16s[i] = src[i];
    return dst.u16x8;
}

/* The fork's `nk_angular_f16_neonfhm` (aa53affa). */
float vs_cosine_f16_neonfhm(uint16_t const *a, uint16_t const *b, size_t n) {
    float16x8_t a_f16x8, b_f16x8;
    // The low and high halves of every vector feed separate accumulators, so each of the three sums
    // runs as two independent chains and a core with a 4-cycle FMLAL latency is not bound by it.
    float32x4_t ab_low_f32x4 = vdupq_n_f32(0), ab_high_f32x4 = vdupq_n_f32(0);
    float32x4_t a2_low_f32x4 = vdupq_n_f32(0), a2_high_f32x4 = vdupq_n_f32(0);
    float32x4_t b2_low_f32x4 = vdupq_n_f32(0), b2_high_f32x4 = vdupq_n_f32(0);
vs_cosine_f16_neonfhm_cycle:
    if (n < 8) {
        a_f16x8 = vreinterpretq_f16_u16(vs_cosine_partial_load_b16x8(a, n));
        b_f16x8 = vreinterpretq_f16_u16(vs_cosine_partial_load_b16x8(b, n));
        n = 0;
    }
    else {
        a_f16x8 = vreinterpretq_f16_u16(vld1q_u16(a));
        b_f16x8 = vreinterpretq_f16_u16(vld1q_u16(b));
        a += 8, b += 8, n -= 8;
    }
    // FMLAL/FMLAL2: widening multiply-accumulate f16 -> f32 of the elements 0-3 and 4-7.
    ab_low_f32x4 = vfmlalq_low_f16(ab_low_f32x4, a_f16x8, b_f16x8);
    ab_high_f32x4 = vfmlalq_high_f16(ab_high_f32x4, a_f16x8, b_f16x8);
    a2_low_f32x4 = vfmlalq_low_f16(a2_low_f32x4, a_f16x8, a_f16x8);
    a2_high_f32x4 = vfmlalq_high_f16(a2_high_f32x4, a_f16x8, a_f16x8);
    b2_low_f32x4 = vfmlalq_low_f16(b2_low_f32x4, b_f16x8, b_f16x8);
    b2_high_f32x4 = vfmlalq_high_f16(b2_high_f32x4, b_f16x8, b_f16x8);
    if (n) goto vs_cosine_f16_neonfhm_cycle;
    float ab = vaddvq_f32(vaddq_f32(ab_low_f32x4, ab_high_f32x4));
    float a2 = vaddvq_f32(vaddq_f32(a2_low_f32x4, a2_high_f32x4));
    float b2 = vaddvq_f32(vaddq_f32(b2_low_f32x4, b2_high_f32x4));
    return vs_cosine_normalize_f32_neon(ab, a2, b2);
}

#if defined(__clang__)
#pragma clang attribute pop
#elif defined(__GNUC__)
#pragma GCC pop_options
#endif

#if defined(__clang__)
#pragma clang attribute push(__attribute__((target("arch=armv8.2-a+sve+sve2"))), apply_to = function)
#elif defined(__GNUC__)
#pragma GCC push_options
#pragma GCC target("arch=armv8.2-a+sve+sve2")
#endif

/* The fork's `nk_angular_f16_sve2` (aa53affa). */
float vs_cosine_f16_sve2(uint16_t const *a_bits, uint16_t const *b_bits, size_t n) {
    size_t i = 0;
    // The bottom and top f16 lanes feed separate accumulators, so each of the three sums runs as
    // two independent chains and a core with a 4-cycle FMLALB/FMLALT latency is not bound by it.
    svfloat32_t ab_bot_f32x = svdup_n_f32(0.0f), ab_top_f32x = svdup_n_f32(0.0f);
    svfloat32_t a2_bot_f32x = svdup_n_f32(0.0f), a2_top_f32x = svdup_n_f32(0.0f);
    svfloat32_t b2_bot_f32x = svdup_n_f32(0.0f), b2_top_f32x = svdup_n_f32(0.0f);
    float16_t const *a = (float16_t const *)a_bits;
    float16_t const *b = (float16_t const *)b_bits;
    do {
        // The zeroing loads leave the lanes past `n` at zero, which add nothing to the sums,
        // so the multiply-adds need no predicate of their own.
        svbool_t predicate_b16x = svwhilelt_b16_u64(i, n);
        svfloat16_t a_f16x = svld1_f16(predicate_b16x, a + i);
        svfloat16_t b_f16x = svld1_f16(predicate_b16x, b + i);
        ab_bot_f32x = svmlalb_f32(ab_bot_f32x, a_f16x, b_f16x);
        ab_top_f32x = svmlalt_f32(ab_top_f32x, a_f16x, b_f16x);
        a2_bot_f32x = svmlalb_f32(a2_bot_f32x, a_f16x, a_f16x);
        a2_top_f32x = svmlalt_f32(a2_top_f32x, a_f16x, a_f16x);
        b2_bot_f32x = svmlalb_f32(b2_bot_f32x, b_f16x, b_f16x);
        b2_top_f32x = svmlalt_f32(b2_top_f32x, b_f16x, b_f16x);
        i += svcnth();
    } while (i < n);

    svbool_t all_b32x = svptrue_b32();
    float ab = svaddv_f32(all_b32x, svadd_f32_x(all_b32x, ab_bot_f32x, ab_top_f32x));
    float a2 = svaddv_f32(all_b32x, svadd_f32_x(all_b32x, a2_bot_f32x, a2_top_f32x));
    float b2 = svaddv_f32(all_b32x, svadd_f32_x(all_b32x, b2_bot_f32x, b2_top_f32x));
    return vs_cosine_normalize_f32_neon(ab, a2, b2);
}

#if defined(__clang__)
#pragma clang attribute pop
#elif defined(__GNUC__)
#pragma GCC pop_options
#endif

#endif /* __aarch64__ */

#if defined(__x86_64__)

#include <immintrin.h>

#if defined(__clang__)
#pragma clang attribute push(__attribute__((target("avx2,f16c,fma,bmi,bmi2"))), apply_to = function)
#elif defined(__GNUC__)
#pragma GCC push_options
#pragma GCC target("avx2", "f16c", "fma", "bmi", "bmi2")
#endif

/* NumKong's `nk_reduce_add_f32x8_haswell_`. */
static inline float vs_cosine_reduce_add_f32x8_haswell(__m256 sum_f32x8) {
    __m128 low_f32x4 = _mm256_castps256_ps128(sum_f32x8);
    __m128 high_f32x4 = _mm256_extractf128_ps(sum_f32x8, 1);
    __m128 sum_f32x4 = _mm_add_ps(low_f32x4, high_f32x4);
    sum_f32x4 = _mm_hadd_ps(sum_f32x4, sum_f32x4);
    sum_f32x4 = _mm_hadd_ps(sum_f32x4, sum_f32x4);
    return _mm_cvtss_f32(sum_f32x4);
}

/* NumKong's `nk_angular_normalize_f32_haswell_`. */
static inline float vs_cosine_normalize_f32_haswell(float ab, float a2, float b2) {
    if (a2 == 0.0f && b2 == 0.0f) return 0.0f;
    else if (ab == 0.0f) return 1.0f;
    __m128 squares_f32x4 = _mm_set_ps(a2, b2, a2, b2);
    __m128 rsqrts_f32x4 = _mm_rsqrt_ps(squares_f32x4);
    // One Newton-Raphson round: y' = y * (1.5 - 0.5 * x * y * y).
    __m128 half_f32x4 = _mm_set1_ps(0.5f);
    __m128 three_halves_f32x4 = _mm_set1_ps(1.5f);
    rsqrts_f32x4 = _mm_mul_ps(
        rsqrts_f32x4,
        _mm_sub_ps(three_halves_f32x4,
                   _mm_mul_ps(half_f32x4, _mm_mul_ps(squares_f32x4, _mm_mul_ps(rsqrts_f32x4, rsqrts_f32x4)))));
    float a2_reciprocal = _mm_cvtss_f32(_mm_shuffle_ps(rsqrts_f32x4, rsqrts_f32x4, _MM_SHUFFLE(0, 0, 0, 1)));
    float b2_reciprocal = _mm_cvtss_f32(rsqrts_f32x4);
    float result = 1.0f - ab * a2_reciprocal * b2_reciprocal;
    return result < 0 ? 0 : result;
}

/* The fork's `nk_angular_f32_haswell` with NK_ACCUMULATE_F32_IN_F64=0 (2e43be7d). */
float vs_cosine_f32_haswell(float const *a, float const *b, size_t n) {
    __m256 dot0_f32x8 = _mm256_setzero_ps(), a_norm_sq0_f32x8 = _mm256_setzero_ps(),
           b_norm_sq0_f32x8 = _mm256_setzero_ps();
    __m256 dot1_f32x8 = _mm256_setzero_ps(), a_norm_sq1_f32x8 = _mm256_setzero_ps(),
           b_norm_sq1_f32x8 = _mm256_setzero_ps();
    size_t i = 0;
    for (; i + 16 <= n; i += 16) {
        __m256 a0_f32x8 = _mm256_loadu_ps(a + i), b0_f32x8 = _mm256_loadu_ps(b + i);
        __m256 a1_f32x8 = _mm256_loadu_ps(a + i + 8), b1_f32x8 = _mm256_loadu_ps(b + i + 8);
        dot0_f32x8 = _mm256_fmadd_ps(a0_f32x8, b0_f32x8, dot0_f32x8);
        a_norm_sq0_f32x8 = _mm256_fmadd_ps(a0_f32x8, a0_f32x8, a_norm_sq0_f32x8);
        b_norm_sq0_f32x8 = _mm256_fmadd_ps(b0_f32x8, b0_f32x8, b_norm_sq0_f32x8);
        dot1_f32x8 = _mm256_fmadd_ps(a1_f32x8, b1_f32x8, dot1_f32x8);
        a_norm_sq1_f32x8 = _mm256_fmadd_ps(a1_f32x8, a1_f32x8, a_norm_sq1_f32x8);
        b_norm_sq1_f32x8 = _mm256_fmadd_ps(b1_f32x8, b1_f32x8, b_norm_sq1_f32x8);
    }
    if (i + 8 <= n) {
        __m256 a_f32x8 = _mm256_loadu_ps(a + i), b_f32x8 = _mm256_loadu_ps(b + i);
        dot0_f32x8 = _mm256_fmadd_ps(a_f32x8, b_f32x8, dot0_f32x8);
        a_norm_sq0_f32x8 = _mm256_fmadd_ps(a_f32x8, a_f32x8, a_norm_sq0_f32x8);
        b_norm_sq0_f32x8 = _mm256_fmadd_ps(b_f32x8, b_f32x8, b_norm_sq0_f32x8);
        i += 8;
    }
    float dot_f32 = vs_cosine_reduce_add_f32x8_haswell(_mm256_add_ps(dot0_f32x8, dot1_f32x8));
    float a_norm_sq_f32 = vs_cosine_reduce_add_f32x8_haswell(_mm256_add_ps(a_norm_sq0_f32x8, a_norm_sq1_f32x8));
    float b_norm_sq_f32 = vs_cosine_reduce_add_f32x8_haswell(_mm256_add_ps(b_norm_sq0_f32x8, b_norm_sq1_f32x8));
    for (; i < n; ++i) {
        float a_f32 = a[i], b_f32 = b[i];
        dot_f32 += a_f32 * b_f32;
        a_norm_sq_f32 += a_f32 * a_f32;
        b_norm_sq_f32 += b_f32 * b_f32;
    }
    return vs_cosine_normalize_f32_haswell(dot_f32, a_norm_sq_f32, b_norm_sq_f32);
}

#if defined(__clang__)
#pragma clang attribute pop
#elif defined(__GNUC__)
#pragma GCC pop_options
#endif

#if defined(__clang__)
#pragma clang attribute push(                                                                         \
    __attribute__((target("avx2,avx512f,avx512vl,avx512bw,avx512dq,f16c,fma,bmi,bmi2"))), \
    apply_to = function)
#elif defined(__GNUC__)
#pragma GCC push_options
#pragma GCC target("avx2", "avx512f", "avx512vl", "avx512bw", "avx512dq", "f16c", "fma", "bmi", "bmi2")
#endif

/* NumKong's `nk_reduce_add_f32x16_skylake_`. */
static inline float vs_cosine_reduce_add_f32x16_skylake(__m512 sum_f32x16) {
    __m256 low_f32x8 = _mm512_castps512_ps256(sum_f32x16);
    __m256 high_f32x8 = _mm512_extractf32x8_ps(sum_f32x16, 1);
    __m256 sum_f32x8 = _mm256_add_ps(low_f32x8, high_f32x8);
    __m128 low_f32x4 = _mm256_castps256_ps128(sum_f32x8);
    __m128 high_f32x4 = _mm256_extractf128_ps(sum_f32x8, 1);
    __m128 sum_f32x4 = _mm_add_ps(low_f32x4, high_f32x4);
    sum_f32x4 = _mm_hadd_ps(sum_f32x4, sum_f32x4);
    sum_f32x4 = _mm_hadd_ps(sum_f32x4, sum_f32x4);
    return _mm_cvtss_f32(sum_f32x4);
}

/* The fork's `nk_angular_f32_skylake` with NK_ACCUMULATE_F32_IN_F64=0 (2e43be7d). */
float vs_cosine_f32_skylake(float const *a, float const *b, size_t n) {
    __m512 dot0_f32x16 = _mm512_setzero_ps(), a_norm_sq0_f32x16 = _mm512_setzero_ps(),
           b_norm_sq0_f32x16 = _mm512_setzero_ps();
    __m512 dot1_f32x16 = _mm512_setzero_ps(), a_norm_sq1_f32x16 = _mm512_setzero_ps(),
           b_norm_sq1_f32x16 = _mm512_setzero_ps();
    size_t i = 0;
    for (; i + 32 <= n; i += 32) {
        __m512 a0_f32x16 = _mm512_loadu_ps(a + i), b0_f32x16 = _mm512_loadu_ps(b + i);
        __m512 a1_f32x16 = _mm512_loadu_ps(a + i + 16), b1_f32x16 = _mm512_loadu_ps(b + i + 16);
        dot0_f32x16 = _mm512_fmadd_ps(a0_f32x16, b0_f32x16, dot0_f32x16);
        a_norm_sq0_f32x16 = _mm512_fmadd_ps(a0_f32x16, a0_f32x16, a_norm_sq0_f32x16);
        b_norm_sq0_f32x16 = _mm512_fmadd_ps(b0_f32x16, b0_f32x16, b_norm_sq0_f32x16);
        dot1_f32x16 = _mm512_fmadd_ps(a1_f32x16, b1_f32x16, dot1_f32x16);
        a_norm_sq1_f32x16 = _mm512_fmadd_ps(a1_f32x16, a1_f32x16, a_norm_sq1_f32x16);
        b_norm_sq1_f32x16 = _mm512_fmadd_ps(b1_f32x16, b1_f32x16, b_norm_sq1_f32x16);
    }
    if (i + 16 <= n) {
        __m512 a_f32x16 = _mm512_loadu_ps(a + i), b_f32x16 = _mm512_loadu_ps(b + i);
        dot0_f32x16 = _mm512_fmadd_ps(a_f32x16, b_f32x16, dot0_f32x16);
        a_norm_sq0_f32x16 = _mm512_fmadd_ps(a_f32x16, a_f32x16, a_norm_sq0_f32x16);
        b_norm_sq0_f32x16 = _mm512_fmadd_ps(b_f32x16, b_f32x16, b_norm_sq0_f32x16);
        i += 16;
    }
    if (i < n) {
        __mmask16 mask = (__mmask16)_bzhi_u32(0xFFFF, (unsigned)(n - i));
        __m512 a_f32x16 = _mm512_maskz_loadu_ps(mask, a + i), b_f32x16 = _mm512_maskz_loadu_ps(mask, b + i);
        dot1_f32x16 = _mm512_fmadd_ps(a_f32x16, b_f32x16, dot1_f32x16);
        a_norm_sq1_f32x16 = _mm512_fmadd_ps(a_f32x16, a_f32x16, a_norm_sq1_f32x16);
        b_norm_sq1_f32x16 = _mm512_fmadd_ps(b_f32x16, b_f32x16, b_norm_sq1_f32x16);
    }
    float dot_f32 = vs_cosine_reduce_add_f32x16_skylake(_mm512_add_ps(dot0_f32x16, dot1_f32x16));
    float a_norm_sq_f32 = vs_cosine_reduce_add_f32x16_skylake(_mm512_add_ps(a_norm_sq0_f32x16, a_norm_sq1_f32x16));
    float b_norm_sq_f32 = vs_cosine_reduce_add_f32x16_skylake(_mm512_add_ps(b_norm_sq0_f32x16, b_norm_sq1_f32x16));
    return vs_cosine_normalize_f32_haswell(dot_f32, a_norm_sq_f32, b_norm_sq_f32);
}

#if defined(__clang__)
#pragma clang attribute pop
#elif defined(__GNUC__)
#pragma GCC pop_options
#endif

#endif /* __x86_64__ */
