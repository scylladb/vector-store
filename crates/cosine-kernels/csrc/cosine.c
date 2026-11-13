/*
 * Cosine distance kernels for Vector Store's HNSW indexes: 1 - ab / sqrt(a2 * b2).
 *
 * Excerpts of NumKong 7.8.6 (https://github.com/ashvardanian/NumKong) as patched by its
 * scylladb/NumKong fork (fea6c757, aa53affa, 2e43be7d), which Vector Store built against until it
 * moved these kernels here (VECTOR-1086). Every NumKong function keeps its name and its text, under
 * a comment naming the header it comes from, so that the file can be compared with upstream:
 * `diff-upstream.sh` next to it prints the differences from a NumKong checkout.
 *
 * Copyright Ash Vardanian and the NumKong contributors.
 * Changes Copyright 2026-present ScyllaDB.
 * SPDX-License-Identifier: Apache-2.0 (see LICENSE-NumKong in this directory)
 *
 * ScyllaDB's changes:
 * - NK_ACCUMULATE_F32_IN_F64 (fea6c757, 2e43be7d; 0 here unless defined otherwise): the f32
 *   kernels accumulate in f32, in two independent chains per sum, where NumKong accumulates in f64.
 *   In each f32 kernel the `#if NK_ACCUMULATE_F32_IN_F64` branch is NumKong's code and the `#else`
 *   branch ScyllaDB's. An HNSW search only orders distances and cannot use NumKong's precision,
 *   which costs about a third of the search throughput on Graviton4 (VECTOR-720).
 * - nk_angular_f16_neonfhm and nk_angular_f16_sve2 (aa53affa): f16 kernels with the widening
 *   multiply-adds (FMLAL on FEAT_FHM, FMLALB/FMLALT on SVE2) instead of converting every lane.
 *   NumKong has no such kernels: they are ScyllaDB's code, written in NumKong's style.
 * - The vs_cosine_* functions: Vector Store's entry points (src/kernel/ffi.rs), each returning
 *   the distance its kernel writes.
 * - The definitions before the kernels: what the excerpts use of NumKong's headers.
 *
 * Every kernel returns what NumKong's angular kernel returns: 0 when both vectors are zero, 1 when
 * the dot product is zero, otherwise 1 - ab / sqrt(a2 * b2) clamped at 0, and NaN when an input is
 * NaN.
 *
 * The translation unit is compiled for the target's baseline ISA (build.rs). Each group of
 * functions enables the extensions it needs with the target pragma of its NumKong header, and
 * src/kernel/ffi.rs calls a kernel only on a CPU that has them.
 */

#include <stddef.h>
#include <stdint.h>

/* ScyllaDB: what the excerpts use of include/numkong/types.h, for GCC and Clang on a 64-bit target
 * without native f16 arithmetic, as usearch builds NumKong. NK_INTERNAL is also `unused` here: the
 * helpers of NumKong's f64 accumulation are when NK_ACCUMULATE_F32_IN_F64 is 0. */
#define NK_PUBLIC   __attribute__((unused)) inline static
#define NK_INTERNAL __attribute__((always_inline, unused)) inline static
typedef float nk_f32_t;
typedef double nk_f64_t;
typedef uint16_t nk_u16_t;
typedef uint64_t nk_u64_t;
typedef nk_u64_t nk_size_t;
typedef unsigned short nk_f16_t;

/* ScyllaDB: the f32 kernels accumulate in f32 (see above); 1 builds NumKong's f64 accumulation. */
#if !defined(NK_ACCUMULATE_F32_IN_F64)
#define NK_ACCUMULATE_F32_IN_F64 0
#endif

#if defined(__aarch64__)

#include <arm_neon.h>
#include <arm_sve.h>

/* ScyllaDB: what the excerpts use of include/numkong/types.h and reduce/sve.h on aarch64. NumKong's
 * nk_b128_vec_t has a member for every type, and its nk_svaddv_* also unpoison the sum for
 * MemorySanitizer. */
#define nk_f16_for_arm_simd_t float16_t
typedef union nk_b128_vec_t {
    uint16x8_t u16x8;
    nk_u16_t u16s[8];
    nk_u64_t u64s[2];
} nk_b128_vec_t;
#define nk_svaddv_f64_(predicate, vector) svaddv_f64((predicate), (vector))
#define nk_svaddv_f32_(predicate, vector) svaddv_f32((predicate), (vector))

/* include/numkong/cast/serial.h */
NK_INTERNAL void nk_partial_load_b16x8_serial_(void const *src, nk_b128_vec_t *dst, nk_size_t n) {
    dst->u64s[0] = 0, dst->u64s[1] = 0;
    nk_u16_t const *s = (nk_u16_t const *)src;
    switch (n) {
    default:
    case 8: dst->u16s[7] = s[7]; // fallthrough
    case 7: dst->u16s[6] = s[6]; // fallthrough
    case 6: dst->u16s[5] = s[5]; // fallthrough
    case 5: dst->u16s[4] = s[4]; // fallthrough
    case 4: dst->u16s[3] = s[3]; // fallthrough
    case 3: dst->u16s[2] = s[2]; // fallthrough
    case 2: dst->u16s[1] = s[1]; // fallthrough
    case 1: dst->u16s[0] = s[0]; // fallthrough
    case 0: break;
    }
}

/* include/numkong/spatial/neon.h */
#if defined(__clang__)
#pragma clang attribute push(__attribute__((target("arch=armv8-a+simd"))), apply_to = function)
#elif defined(__GNUC__)
#pragma GCC push_options
#pragma GCC target("arch=armv8-a+simd")
#endif

NK_INTERNAL nk_f32_t nk_angular_normalize_f32_neon_(nk_f32_t ab, nk_f32_t a2, nk_f32_t b2) {
    if (a2 == 0 && b2 == 0) return 0;
    if (ab == 0) return 1;
    nk_f32_t squares_arr[2] = {a2, b2};
    float32x2_t squares_f32x2 = vld1_f32(squares_arr);
    // Unlike x86, Arm NEON manuals don't explicitly mention the accuracy of their `rsqrt` approximation.
    // Third-party research suggests that it's less accurate than SSE instructions, having an error of 1.5×2⁻¹².
    // One or two rounds of Newton-Raphson refinement are recommended to improve the accuracy.
    // https://github.com/lighttransport/embree-aarch64/issues/24
    // https://github.com/lighttransport/embree-aarch64/blob/3f75f8cb4e553d13dced941b5fefd4c826835a6b/common/math/math.h#L137-L145
    float32x2_t rsqrts_f32x2 = vrsqrte_f32(squares_f32x2);
    // Perform two rounds of Newton-Raphson refinement:
    // https://en.wikipedia.org/wiki/Newton%27s_method
    rsqrts_f32x2 = vmul_f32(rsqrts_f32x2, vrsqrts_f32(vmul_f32(squares_f32x2, rsqrts_f32x2), rsqrts_f32x2));
    rsqrts_f32x2 = vmul_f32(rsqrts_f32x2, vrsqrts_f32(vmul_f32(squares_f32x2, rsqrts_f32x2), rsqrts_f32x2));
    vst1_f32(squares_arr, rsqrts_f32x2);
    nk_f32_t result = 1 - ab * squares_arr[0] * squares_arr[1];
    return result < 0 ? 0 : result;
}

NK_INTERNAL nk_f64_t nk_angular_normalize_f64_neon_(nk_f64_t ab, nk_f64_t a2, nk_f64_t b2) {
    if (a2 == 0 && b2 == 0) return 0;
    if (ab == 0) return 1;
    nk_f64_t squares_arr[2] = {a2, b2};
    float64x2_t squares_f64x2 = vld1q_f64(squares_arr);

    // Unlike x86, Arm NEON manuals don't explicitly mention the accuracy of their `rsqrt` approximation.
    // Third-party research suggests that it's less accurate than SSE instructions, having an error of 1.5×2⁻¹².
    // One or two rounds of Newton-Raphson refinement are recommended to improve the accuracy.
    // https://github.com/lighttransport/embree-aarch64/issues/24
    // https://github.com/lighttransport/embree-aarch64/blob/3f75f8cb4e553d13dced941b5fefd4c826835a6b/common/math/math.h#L137-L145
    float64x2_t rsqrts_f64x2 = vrsqrteq_f64(squares_f64x2);
    // Perform three rounds of Newton-Raphson refinement for f64 precision (~48 bits):
    // https://en.wikipedia.org/wiki/Newton%27s_method
    rsqrts_f64x2 = vmulq_f64(rsqrts_f64x2, vrsqrtsq_f64(vmulq_f64(squares_f64x2, rsqrts_f64x2), rsqrts_f64x2));
    rsqrts_f64x2 = vmulq_f64(rsqrts_f64x2, vrsqrtsq_f64(vmulq_f64(squares_f64x2, rsqrts_f64x2), rsqrts_f64x2));
    rsqrts_f64x2 = vmulq_f64(rsqrts_f64x2, vrsqrtsq_f64(vmulq_f64(squares_f64x2, rsqrts_f64x2), rsqrts_f64x2));
    vst1q_f64(squares_arr, rsqrts_f64x2);
    nk_f64_t result = 1 - ab * squares_arr[0] * squares_arr[1];
    return result < 0 ? 0 : result;
}

#if NK_ACCUMULATE_F32_IN_F64
NK_PUBLIC void nk_angular_f32_neon(nk_f32_t const *a, nk_f32_t const *b, nk_size_t n, nk_f64_t *result) {
    // Accumulate in f64 for numerical stability (2 f32s per iteration, avoids slow vget_low/high)
    float64x2_t ab_f64x2 = vdupq_n_f64(0);
    float64x2_t a2_f64x2 = vdupq_n_f64(0);
    float64x2_t b2_f64x2 = vdupq_n_f64(0);
    nk_size_t i = 0;
    for (; i + 2 <= n; i += 2) {
        float32x2_t a_f32x2 = vld1_f32(a + i);
        float32x2_t b_f32x2 = vld1_f32(b + i);
        float64x2_t a_f64x2 = vcvt_f64_f32(a_f32x2);
        float64x2_t b_f64x2 = vcvt_f64_f32(b_f32x2);
        ab_f64x2 = vfmaq_f64(ab_f64x2, a_f64x2, b_f64x2);
        a2_f64x2 = vfmaq_f64(a2_f64x2, a_f64x2, a_f64x2);
        b2_f64x2 = vfmaq_f64(b2_f64x2, b_f64x2, b_f64x2);
    }
    nk_f64_t ab_f64 = vaddvq_f64(ab_f64x2);
    nk_f64_t a2_f64 = vaddvq_f64(a2_f64x2);
    nk_f64_t b2_f64 = vaddvq_f64(b2_f64x2);
    for (; i < n; ++i) {
        nk_f64_t ai = (nk_f64_t)a[i], bi = (nk_f64_t)b[i];
        ab_f64 += ai * bi, a2_f64 += ai * ai, b2_f64 += bi * bi;
    }
    *result = nk_angular_normalize_f64_neon_(ab_f64, a2_f64, b2_f64);
}
#else
NK_PUBLIC void nk_angular_f32_neon(nk_f32_t const *a, nk_f32_t const *b, nk_size_t n, nk_f64_t *result) {
    // f32 accumulation in two independent chains of three FMAs, like simsimd 6.5's `simsimd_cos_f32_neon`:
    // HNSW only orders distances, so the f64 accumulation (two converts + two f64 FMAs per 2 floats) bought
    // nothing it could use and halved the kernel's throughput on Graviton4 .
    float32x4_t ab0 = vdupq_n_f32(0), a20 = vdupq_n_f32(0), b20 = vdupq_n_f32(0);
    float32x4_t ab1 = vdupq_n_f32(0), a21 = vdupq_n_f32(0), b21 = vdupq_n_f32(0);
    nk_size_t i = 0;
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
    nk_f32_t ab = vaddvq_f32(vaddq_f32(ab0, ab1));
    nk_f32_t a2 = vaddvq_f32(vaddq_f32(a20, a21));
    nk_f32_t b2 = vaddvq_f32(vaddq_f32(b20, b21));
    for (; i < n; ++i) {
        nk_f32_t ai = a[i], bi = b[i];
        ab += ai * bi, a2 += ai * ai, b2 += bi * bi;
    }
    *result = nk_angular_normalize_f64_neon_((nk_f64_t)ab, (nk_f64_t)a2, (nk_f64_t)b2);
}
#endif // NK_ACCUMULATE_F32_IN_F64

/* ScyllaDB: Vector Store's entry point (src/kernel/ffi.rs), returning the distance as f32. */
float vs_cosine_f32_neon(nk_f32_t const *a, nk_f32_t const *b, nk_size_t n) {
    nk_f64_t result;
    nk_angular_f32_neon(a, b, n, &result);
    return (float)result;
}

#if defined(__clang__)
#pragma clang attribute pop
#elif defined(__GNUC__)
#pragma GCC pop_options
#endif

/* include/numkong/spatial/sve.h */
#if defined(__clang__)
#pragma clang attribute push(__attribute__((target("arch=armv8.2-a+sve"))), apply_to = function)
#elif defined(__GNUC__)
#pragma GCC push_options
#pragma GCC target("arch=armv8.2-a+sve")
#endif

#if NK_ACCUMULATE_F32_IN_F64
NK_PUBLIC void nk_angular_f32_sve(nk_f32_t const *a, nk_f32_t const *b, nk_size_t n, nk_f64_t *result) {
    nk_size_t i = 0;
    svfloat64_t ab_f64x = svdupq_n_f64(0.0, 0.0);
    svfloat64_t a2_f64x = svdupq_n_f64(0.0, 0.0);
    svfloat64_t b2_f64x = svdupq_n_f64(0.0, 0.0);
    for (; i < n; i += svcntw()) {
        svbool_t predicate_b32x = svwhilelt_b32_u64(i, n);
        svfloat32_t a_f32x = svld1_f32(predicate_b32x, a + i);
        svfloat32_t b_f32x = svld1_f32(predicate_b32x, b + i);
        nk_size_t remaining = n - i < svcntw() ? n - i : svcntw();

        // svcvt_f64_f32_x widens only even-indexed f32 elements; svext by 1 shifts odd into even.
        svbool_t pred_even_b64x = svwhilelt_b64_u64(0u, (remaining + 1) / 2);
        svfloat64_t a_even_f64x = svcvt_f64_f32_x(pred_even_b64x, a_f32x);
        svfloat64_t b_even_f64x = svcvt_f64_f32_x(pred_even_b64x, b_f32x);
        ab_f64x = svmla_f64_m(pred_even_b64x, ab_f64x, a_even_f64x, b_even_f64x);
        a2_f64x = svmla_f64_m(pred_even_b64x, a2_f64x, a_even_f64x, a_even_f64x);
        b2_f64x = svmla_f64_m(pred_even_b64x, b2_f64x, b_even_f64x, b_even_f64x);

        svbool_t pred_odd_b64x = svwhilelt_b64_u64(0u, remaining / 2);
        svfloat64_t a_odd_f64x = svcvt_f64_f32_x(pred_odd_b64x, svext_f32(a_f32x, a_f32x, 1));
        svfloat64_t b_odd_f64x = svcvt_f64_f32_x(pred_odd_b64x, svext_f32(b_f32x, b_f32x, 1));
        ab_f64x = svmla_f64_m(pred_odd_b64x, ab_f64x, a_odd_f64x, b_odd_f64x);
        a2_f64x = svmla_f64_m(pred_odd_b64x, a2_f64x, a_odd_f64x, a_odd_f64x);
        b2_f64x = svmla_f64_m(pred_odd_b64x, b2_f64x, b_odd_f64x, b_odd_f64x);
    }

    nk_f64_t ab_f64 = nk_svaddv_f64_(svptrue_b64(), ab_f64x);
    nk_f64_t a2_f64 = nk_svaddv_f64_(svptrue_b64(), a2_f64x);
    nk_f64_t b2_f64 = nk_svaddv_f64_(svptrue_b64(), b2_f64x);
    *result = nk_angular_normalize_f64_neon_(ab_f64, a2_f64, b2_f64);
}
#else
NK_PUBLIC void nk_angular_f32_sve(nk_f32_t const *a, nk_f32_t const *b, nk_size_t n, nk_f64_t *result) {
    // f32 accumulation in two independent chains of three FMAs (six accumulators), so that the
    // loop is not bound by the FMA latency on a 128-bit SVE core (Graviton4).
    svfloat32_t ab0 = svdup_n_f32(0.f), a20 = svdup_n_f32(0.f), b20 = svdup_n_f32(0.f);
    svfloat32_t ab1 = svdup_n_f32(0.f), a21 = svdup_n_f32(0.f), b21 = svdup_n_f32(0.f);
    nk_size_t const lanes = svcntw();
    nk_size_t i = 0;
    for (; i + 2 * lanes <= n; i += 2 * lanes) {
        svbool_t pg = svptrue_b32();
        svfloat32_t av0 = svld1_f32(pg, a + i), bv0 = svld1_f32(pg, b + i);
        svfloat32_t av1 = svld1_f32(pg, a + i + lanes), bv1 = svld1_f32(pg, b + i + lanes);
        ab0 = svmla_f32_x(pg, ab0, av0, bv0), a20 = svmla_f32_x(pg, a20, av0, av0), b20 = svmla_f32_x(pg, b20, bv0, bv0);
        ab1 = svmla_f32_x(pg, ab1, av1, bv1), a21 = svmla_f32_x(pg, a21, av1, av1), b21 = svmla_f32_x(pg, b21, bv1, bv1);
    }
    for (; i < n; i += lanes) {
        svbool_t pg = svwhilelt_b32_u64(i, n);
        svfloat32_t av = svld1_f32(pg, a + i), bv = svld1_f32(pg, b + i);
        ab0 = svmla_f32_m(pg, ab0, av, bv), a20 = svmla_f32_m(pg, a20, av, av), b20 = svmla_f32_m(pg, b20, bv, bv);
    }
    svbool_t all = svptrue_b32();
    nk_f32_t ab = svaddv_f32(all, svadd_f32_x(all, ab0, ab1));
    nk_f32_t a2 = svaddv_f32(all, svadd_f32_x(all, a20, a21));
    nk_f32_t b2 = svaddv_f32(all, svadd_f32_x(all, b20, b21));
    *result = nk_angular_normalize_f64_neon_((nk_f64_t)ab, (nk_f64_t)a2, (nk_f64_t)b2);
}
#endif // NK_ACCUMULATE_F32_IN_F64

/* ScyllaDB: Vector Store's entry point (src/kernel/ffi.rs), returning the distance as f32. */
float vs_cosine_f32_sve(nk_f32_t const *a, nk_f32_t const *b, nk_size_t n) {
    nk_f64_t result;
    nk_angular_f32_sve(a, b, n, &result);
    return (float)result;
}

#if defined(__clang__)
#pragma clang attribute pop
#elif defined(__GNUC__)
#pragma GCC pop_options
#endif

/* include/numkong/spatial/neonfhm.h of the fork (aa53affa): ScyllaDB's, no upstream counterpart */
#if defined(__clang__)
#pragma clang attribute push(__attribute__((target("arch=armv8.2-a+simd+fp16+fp16fml"))), apply_to = function)
#elif defined(__GNUC__)
#pragma GCC push_options
#pragma GCC target("arch=armv8.2-a+simd+fp16+fp16fml")
#endif

NK_PUBLIC void nk_angular_f16_neonfhm(nk_f16_t const *a, nk_f16_t const *b, nk_size_t n, nk_f32_t *result) {
    float16x8_t a_f16x8, b_f16x8;
    // The low and high halves of every vector feed separate accumulators, so each of the three sums
    // runs as two independent chains and a core with a 4-cycle FMLAL latency is not bound by it.
    float32x4_t ab_low_f32x4 = vdupq_n_f32(0), ab_high_f32x4 = vdupq_n_f32(0);
    float32x4_t a2_low_f32x4 = vdupq_n_f32(0), a2_high_f32x4 = vdupq_n_f32(0);
    float32x4_t b2_low_f32x4 = vdupq_n_f32(0), b2_high_f32x4 = vdupq_n_f32(0);
nk_angular_f16_neonfhm_cycle:
    if (n < 8) {
        nk_b128_vec_t a_vec, b_vec;
        nk_partial_load_b16x8_serial_(a, &a_vec, n);
        nk_partial_load_b16x8_serial_(b, &b_vec, n);
        a_f16x8 = vreinterpretq_f16_u16(a_vec.u16x8);
        b_f16x8 = vreinterpretq_f16_u16(b_vec.u16x8);
        n = 0;
    }
    else {
        a_f16x8 = vreinterpretq_f16_u16(vld1q_u16((nk_u16_t const *)a));
        b_f16x8 = vreinterpretq_f16_u16(vld1q_u16((nk_u16_t const *)b));
        a += 8, b += 8, n -= 8;
    }
    // FMLAL/FMLAL2: widening multiply-accumulate f16 → f32 of the elements 0-3 and 4-7
    ab_low_f32x4 = vfmlalq_low_f16(ab_low_f32x4, a_f16x8, b_f16x8);
    ab_high_f32x4 = vfmlalq_high_f16(ab_high_f32x4, a_f16x8, b_f16x8);
    a2_low_f32x4 = vfmlalq_low_f16(a2_low_f32x4, a_f16x8, a_f16x8);
    a2_high_f32x4 = vfmlalq_high_f16(a2_high_f32x4, a_f16x8, a_f16x8);
    b2_low_f32x4 = vfmlalq_low_f16(b2_low_f32x4, b_f16x8, b_f16x8);
    b2_high_f32x4 = vfmlalq_high_f16(b2_high_f32x4, b_f16x8, b_f16x8);
    if (n) goto nk_angular_f16_neonfhm_cycle;
    nk_f32_t ab = vaddvq_f32(vaddq_f32(ab_low_f32x4, ab_high_f32x4));
    nk_f32_t a2 = vaddvq_f32(vaddq_f32(a2_low_f32x4, a2_high_f32x4));
    nk_f32_t b2 = vaddvq_f32(vaddq_f32(b2_low_f32x4, b2_high_f32x4));
    *result = nk_angular_normalize_f32_neon_(ab, a2, b2);
}

/* ScyllaDB: Vector Store's entry point (src/kernel/ffi.rs), returning the distance. */
float vs_cosine_f16_neonfhm(nk_f16_t const *a, nk_f16_t const *b, nk_size_t n) {
    nk_f32_t result;
    nk_angular_f16_neonfhm(a, b, n, &result);
    return result;
}

#if defined(__clang__)
#pragma clang attribute pop
#elif defined(__GNUC__)
#pragma GCC pop_options
#endif

/* include/numkong/spatial/sve2.h of the fork (aa53affa): ScyllaDB's, no upstream counterpart */
#if defined(__clang__)
#pragma clang attribute push(__attribute__((target("arch=armv8.2-a+sve+sve2"))), apply_to = function)
#elif defined(__GNUC__)
#pragma GCC push_options
#pragma GCC target("arch=armv8.2-a+sve+sve2")
#endif

NK_PUBLIC void nk_angular_f16_sve2(nk_f16_t const *a_enum, nk_f16_t const *b_enum, nk_size_t n, nk_f32_t *result) {
    nk_size_t i = 0;
    // The bottom and top f16 lanes feed separate accumulators, so each of the three sums runs as
    // two independent chains and a core with a 4-cycle FMLALB/FMLALT latency is not bound by it.
    svfloat32_t ab_bot_f32x = svdup_n_f32(0.0f), ab_top_f32x = svdup_n_f32(0.0f);
    svfloat32_t a2_bot_f32x = svdup_n_f32(0.0f), a2_top_f32x = svdup_n_f32(0.0f);
    svfloat32_t b2_bot_f32x = svdup_n_f32(0.0f), b2_top_f32x = svdup_n_f32(0.0f);
    nk_f16_for_arm_simd_t const *a = (nk_f16_for_arm_simd_t const *)(a_enum);
    nk_f16_for_arm_simd_t const *b = (nk_f16_for_arm_simd_t const *)(b_enum);
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
    nk_f32_t ab_f32 = nk_svaddv_f32_(all_b32x, svadd_f32_x(all_b32x, ab_bot_f32x, ab_top_f32x));
    nk_f32_t a2_f32 = nk_svaddv_f32_(all_b32x, svadd_f32_x(all_b32x, a2_bot_f32x, a2_top_f32x));
    nk_f32_t b2_f32 = nk_svaddv_f32_(all_b32x, svadd_f32_x(all_b32x, b2_bot_f32x, b2_top_f32x));
    *result = nk_angular_normalize_f32_neon_(ab_f32, a2_f32, b2_f32);
}

/* ScyllaDB: Vector Store's entry point (src/kernel/ffi.rs), returning the distance. */
float vs_cosine_f16_sve2(nk_f16_t const *a, nk_f16_t const *b, nk_size_t n) {
    nk_f32_t result;
    nk_angular_f16_sve2(a, b, n, &result);
    return result;
}

#if defined(__clang__)
#pragma clang attribute pop
#elif defined(__GNUC__)
#pragma GCC pop_options
#endif

#endif /* __aarch64__ */

#if defined(__x86_64__)

#include <immintrin.h>

/* include/numkong/reduce/haswell.h and include/numkong/spatial/haswell.h */
#if defined(__clang__)
#pragma clang attribute push(__attribute__((target("avx2,f16c,fma,bmi,bmi2"))), apply_to = function)
#elif defined(__GNUC__)
#pragma GCC push_options
#pragma GCC target("avx2", "f16c", "fma", "bmi", "bmi2")
#endif

NK_INTERNAL nk_f32_t nk_reduce_add_f32x8_haswell_(__m256 sum_f32x8) {
    __m128 low_f32x4 = _mm256_castps256_ps128(sum_f32x8);
    __m128 high_f32x4 = _mm256_extractf128_ps(sum_f32x8, 1);
    __m128 sum_f32x4 = _mm_add_ps(low_f32x4, high_f32x4);
    sum_f32x4 = _mm_hadd_ps(sum_f32x4, sum_f32x4);
    sum_f32x4 = _mm_hadd_ps(sum_f32x4, sum_f32x4);
    return _mm_cvtss_f32(sum_f32x4);
}

NK_INTERNAL nk_f64_t nk_reduce_add_f64x4_haswell_(__m256d sum_f64x4) {
    __m128d low_f64x2 = _mm256_castpd256_pd128(sum_f64x4);
    __m128d high_f64x2 = _mm256_extractf128_pd(sum_f64x4, 1);
    __m128d sum_f64x2 = _mm_add_pd(low_f64x2, high_f64x2);
    sum_f64x2 = _mm_hadd_pd(sum_f64x2, sum_f64x2);
    return _mm_cvtsd_f64(sum_f64x2);
}

NK_INTERNAL nk_f64_t nk_angular_normalize_f64_haswell_(nk_f64_t ab, nk_f64_t a2, nk_f64_t b2) {

    // If both vectors have magnitude 0, the distance is 0.
    if (a2 == 0 && b2 == 0) return 0;
    // If any one of the vectors is 0, the square root of the product is 0,
    // the division is illformed, and the result is 1.
    else if (ab == 0) return 1;

    // Design note: We use exact `_mm_sqrt_pd` instead of fast rsqrt approximation.
    // The f32 `_mm_rsqrt_ps` has max relative error of 1.5 × 2⁻¹² (~11 bits precision).
    // Even with Newton-Raphson refinement (doubles precision to ~22-24 bits), this is
    // insufficient for f64's 52-bit mantissa, causing ULP errors in the hundreds of millions.
    // The `_mm_sqrt_pd` instruction has ~13 cycle latency but provides full f64 precision.
    // https://web.archive.org/web/20210208132927/http://assemblyrequired.crashworks.org/timing-square-root/
    __m128d squares_f64x2 = _mm_set_pd(a2, b2);
    __m128d sqrts_f64x2 = _mm_sqrt_pd(squares_f64x2);
    nk_f64_t a_sqrt = _mm_cvtsd_f64(_mm_unpackhi_pd(sqrts_f64x2, sqrts_f64x2));
    nk_f64_t b_sqrt = _mm_cvtsd_f64(sqrts_f64x2);
    nk_f64_t result = 1 - ab / (a_sqrt * b_sqrt);
    return result < 0 ? 0 : result;
}

NK_INTERNAL nk_f32_t nk_angular_normalize_f32_haswell_(nk_f32_t ab, nk_f32_t a2, nk_f32_t b2) {

    // If both vectors have magnitude 0, the distance is 0.
    if (a2 == 0.0f && b2 == 0.0f) return 0.0f;
    // If any one of the vectors is 0, the square root of the product is 0,
    // the division is illformed, and the result is 1.
    else if (ab == 0.0f) return 1.0f;

    // Load the squares into an __m128 register for single-precision floating-point operations
    __m128 squares_f32x4 = _mm_set_ps(a2, b2, a2, b2); // We replicate to make use of full register

    // Compute the reciprocal square root of the squares using `_mm_rsqrt_ps` (single-precision)
    __m128 rsqrts_f32x4 = _mm_rsqrt_ps(squares_f32x4);

    // Perform one iteration of Newton-Raphson refinement to improve the precision of rsqrt:
    // Formula: y' = y × (1.5 - 0.5 × x × y × y)
    __m128 half_f32x4 = _mm_set1_ps(0.5f);
    __m128 three_halves_f32x4 = _mm_set1_ps(1.5f);
    rsqrts_f32x4 = _mm_mul_ps(
        rsqrts_f32x4,
        _mm_sub_ps(three_halves_f32x4,
                   _mm_mul_ps(half_f32x4, _mm_mul_ps(squares_f32x4, _mm_mul_ps(rsqrts_f32x4, rsqrts_f32x4)))));

    // Extract the reciprocal square roots of a2 and b2 from the __m128 register
    nk_f32_t a2_reciprocal = _mm_cvtss_f32(_mm_shuffle_ps(rsqrts_f32x4, rsqrts_f32x4, _MM_SHUFFLE(0, 0, 0, 1)));
    nk_f32_t b2_reciprocal = _mm_cvtss_f32(rsqrts_f32x4);

    // Calculate the angular distance: 1 - dot_product × a2_reciprocal × b2_reciprocal
    nk_f32_t result = 1.0f - ab * a2_reciprocal * b2_reciprocal;
    return result < 0 ? 0 : result;
}

#if NK_ACCUMULATE_F32_IN_F64
NK_PUBLIC void nk_angular_f32_haswell(nk_f32_t const *a, nk_f32_t const *b, nk_size_t n, nk_f64_t *result) {
    // Upcast to f64 for higher precision accumulation
    __m256d dot_f64x4 = _mm256_setzero_pd();
    __m256d a_norm_sq_f64x4 = _mm256_setzero_pd();
    __m256d b_norm_sq_f64x4 = _mm256_setzero_pd();
    nk_size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m128 a_f32x4 = _mm_loadu_ps(a + i);
        __m128 b_f32x4 = _mm_loadu_ps(b + i);
        __m256d a_f64x4 = _mm256_cvtps_pd(a_f32x4);
        __m256d b_f64x4 = _mm256_cvtps_pd(b_f32x4);
        dot_f64x4 = _mm256_fmadd_pd(a_f64x4, b_f64x4, dot_f64x4);
        a_norm_sq_f64x4 = _mm256_fmadd_pd(a_f64x4, a_f64x4, a_norm_sq_f64x4);
        b_norm_sq_f64x4 = _mm256_fmadd_pd(b_f64x4, b_f64x4, b_norm_sq_f64x4);
    }

    nk_f64_t dot_f64 = nk_reduce_add_f64x4_haswell_(dot_f64x4);
    nk_f64_t a_norm_sq_f64 = nk_reduce_add_f64x4_haswell_(a_norm_sq_f64x4);
    nk_f64_t b_norm_sq_f64 = nk_reduce_add_f64x4_haswell_(b_norm_sq_f64x4);
    for (; i < n; ++i) {
        nk_f64_t a_f64 = a[i], b_f64 = b[i];
        dot_f64 += a_f64 * b_f64;
        a_norm_sq_f64 += a_f64 * a_f64;
        b_norm_sq_f64 += b_f64 * b_f64;
    }
    *result = nk_angular_normalize_f64_haswell_(dot_f64, a_norm_sq_f64, b_norm_sq_f64);
}
#else
NK_PUBLIC void nk_angular_f32_haswell(nk_f32_t const *a, nk_f32_t const *b, nk_size_t n, nk_f64_t *result) {
    // f32 accumulation in two independent chains of three FMAs on 8 lanes (six accumulators), like simsimd's
    // `simsimd_cos_f32_haswell`: three f32 FMAs per 8 floats, where the f64 path above converts a and b (4 lanes
    // each) and does three f64 FMAs on 4 lanes per 4 floats. HNSW only orders distances, so the f64 accumulation
    // bought nothing it could use (see `nk_angular_f32_skylake`).
    __m256 dot0_f32x8 = _mm256_setzero_ps(), a_norm_sq0_f32x8 = _mm256_setzero_ps(),
           b_norm_sq0_f32x8 = _mm256_setzero_ps();
    __m256 dot1_f32x8 = _mm256_setzero_ps(), a_norm_sq1_f32x8 = _mm256_setzero_ps(),
           b_norm_sq1_f32x8 = _mm256_setzero_ps();
    nk_size_t i = 0;
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
    nk_f32_t dot_f32 = nk_reduce_add_f32x8_haswell_(_mm256_add_ps(dot0_f32x8, dot1_f32x8));
    nk_f32_t a_norm_sq_f32 = nk_reduce_add_f32x8_haswell_(_mm256_add_ps(a_norm_sq0_f32x8, a_norm_sq1_f32x8));
    nk_f32_t b_norm_sq_f32 = nk_reduce_add_f32x8_haswell_(_mm256_add_ps(b_norm_sq0_f32x8, b_norm_sq1_f32x8));
    for (; i < n; ++i) {
        nk_f32_t a_f32 = a[i], b_f32 = b[i];
        dot_f32 += a_f32 * b_f32;
        a_norm_sq_f32 += a_f32 * a_f32;
        b_norm_sq_f32 += b_f32 * b_f32;
    }
    *result = (nk_f64_t)nk_angular_normalize_f32_haswell_(dot_f32, a_norm_sq_f32, b_norm_sq_f32);
}
#endif // NK_ACCUMULATE_F32_IN_F64

/* ScyllaDB: Vector Store's entry point (src/kernel/ffi.rs), returning the distance as f32. */
float vs_cosine_f32_haswell(nk_f32_t const *a, nk_f32_t const *b, nk_size_t n) {
    nk_f64_t result;
    nk_angular_f32_haswell(a, b, n, &result);
    return (float)result;
}

#if defined(__clang__)
#pragma clang attribute pop
#elif defined(__GNUC__)
#pragma GCC pop_options
#endif

/* include/numkong/reduce/skylake.h and include/numkong/spatial/skylake.h */
#if defined(__clang__)
#pragma clang attribute push(__attribute__((target("avx2,avx512f,avx512vl,avx512bw,avx512dq,f16c,fma,bmi,bmi2"))), apply_to = function)
#elif defined(__GNUC__)
#pragma GCC push_options
#pragma GCC target("avx2", "avx512f", "avx512vl", "avx512bw", "avx512dq", "f16c", "fma", "bmi", "bmi2")
#endif

NK_INTERNAL nk_f32_t nk_reduce_add_f32x16_skylake_(__m512 sum_f32x16) {
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

NK_INTERNAL nk_f64_t nk_angular_normalize_f64_skylake_(nk_f64_t ab, nk_f64_t a2, nk_f64_t b2) {

    // If both vectors have magnitude 0, the distance is 0.
    if (a2 == 0 && b2 == 0) return 0;
    // If any one of the vectors is 0, the square root of the product is 0,
    // the division is illformed, and the result is 1.
    else if (ab == 0) return 1;

    // Design note: We use exact `_mm_sqrt_pd` instead of `_mm_rsqrt14_pd` approximation.
    // The AVX-512 `_mm_rsqrt14_pd` has max relative error of 2⁻¹⁴ (~14 bits precision).
    // Even with Newton-Raphson refinement (doubles precision to ~28 bits), this is
    // insufficient for f64's 52-bit mantissa, causing ULP errors in the tens of millions.
    // The `_mm_sqrt_pd` instruction provides full f64 precision.
    //
    // Precision comparison for 1536-dimensional vectors:
    //      DType     rsqrt14+NR Error     Exact sqrt Error
    //      float64   1.35e-11 ± 1.85e-11  ~0 (2 ULP max)
    //
    // https://web.archive.org/web/20210208132927/http://assemblyrequired.crashworks.org/timing-square-root/
    __m128d squares_f64x2 = _mm_set_pd(a2, b2);
    __m128d sqrts_f64x2 = _mm_sqrt_pd(squares_f64x2);
    nk_f64_t a_sqrt = _mm_cvtsd_f64(_mm_unpackhi_pd(sqrts_f64x2, sqrts_f64x2));
    nk_f64_t b_sqrt = _mm_cvtsd_f64(sqrts_f64x2);
    nk_f64_t result = 1 - ab / (a_sqrt * b_sqrt);
    return result < 0 ? 0 : result;
}

#if NK_ACCUMULATE_F32_IN_F64
NK_PUBLIC void nk_angular_f32_skylake(nk_f32_t const *a, nk_f32_t const *b, nk_size_t n, nk_f64_t *result) {
    // Upcast to f64 for higher precision accumulation
    __m512d dot_f64x8 = _mm512_setzero_pd();
    __m512d a_norm_sq_f64x8 = _mm512_setzero_pd();
    __m512d b_norm_sq_f64x8 = _mm512_setzero_pd();
    __m256 a_f32x8, b_f32x8;

nk_angular_f32_skylake_cycle:
    if (n < 8) {
        __mmask8 mask = (__mmask8)_bzhi_u32(0xFFFFFFFF, n);
        a_f32x8 = _mm256_maskz_loadu_ps(mask, a);
        b_f32x8 = _mm256_maskz_loadu_ps(mask, b);
        n = 0;
    }
    else {
        a_f32x8 = _mm256_loadu_ps(a);
        b_f32x8 = _mm256_loadu_ps(b);
        a += 8, b += 8, n -= 8;
    }
    __m512d a_f64x8 = _mm512_cvtps_pd(a_f32x8);
    __m512d b_f64x8 = _mm512_cvtps_pd(b_f32x8);
    dot_f64x8 = _mm512_fmadd_pd(a_f64x8, b_f64x8, dot_f64x8);
    a_norm_sq_f64x8 = _mm512_fmadd_pd(a_f64x8, a_f64x8, a_norm_sq_f64x8);
    b_norm_sq_f64x8 = _mm512_fmadd_pd(b_f64x8, b_f64x8, b_norm_sq_f64x8);
    if (n) goto nk_angular_f32_skylake_cycle;

    nk_f64_t dot_f64 = _mm512_reduce_add_pd(dot_f64x8);
    nk_f64_t a_norm_sq_f64 = _mm512_reduce_add_pd(a_norm_sq_f64x8);
    nk_f64_t b_norm_sq_f64 = _mm512_reduce_add_pd(b_norm_sq_f64x8);
    *result = nk_angular_normalize_f64_skylake_(dot_f64, a_norm_sq_f64, b_norm_sq_f64);
}
#else
NK_PUBLIC void nk_angular_f32_skylake(nk_f32_t const *a, nk_f32_t const *b, nk_size_t n, nk_f64_t *result) {
    // f32 accumulation in two independent chains of three FMAs on 16 lanes (six accumulators), like simsimd's
    // `simsimd_cos_f32_skylake`: three f32 FMAs per 16 floats, where the f64 path above converts a and b (8 lanes
    // each) and does three f64 FMAs on 8 lanes per 8 floats. HNSW only orders distances, so the f64 accumulation
    // bought nothing it could use and on Sapphire Rapids answered 10% fewer searches per second than simsimd's.
    __m512 dot0_f32x16 = _mm512_setzero_ps(), a_norm_sq0_f32x16 = _mm512_setzero_ps(),
           b_norm_sq0_f32x16 = _mm512_setzero_ps();
    __m512 dot1_f32x16 = _mm512_setzero_ps(), a_norm_sq1_f32x16 = _mm512_setzero_ps(),
           b_norm_sq1_f32x16 = _mm512_setzero_ps();
    nk_size_t i = 0;
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
        __mmask16 mask = (__mmask16)_bzhi_u32(0xFFFF, n - i);
        __m512 a_f32x16 = _mm512_maskz_loadu_ps(mask, a + i), b_f32x16 = _mm512_maskz_loadu_ps(mask, b + i);
        dot1_f32x16 = _mm512_fmadd_ps(a_f32x16, b_f32x16, dot1_f32x16);
        a_norm_sq1_f32x16 = _mm512_fmadd_ps(a_f32x16, a_f32x16, a_norm_sq1_f32x16);
        b_norm_sq1_f32x16 = _mm512_fmadd_ps(b_f32x16, b_f32x16, b_norm_sq1_f32x16);
    }
    nk_f32_t dot_f32 = nk_reduce_add_f32x16_skylake_(_mm512_add_ps(dot0_f32x16, dot1_f32x16));
    nk_f32_t a_norm_sq_f32 = nk_reduce_add_f32x16_skylake_(_mm512_add_ps(a_norm_sq0_f32x16, a_norm_sq1_f32x16));
    nk_f32_t b_norm_sq_f32 = nk_reduce_add_f32x16_skylake_(_mm512_add_ps(b_norm_sq0_f32x16, b_norm_sq1_f32x16));
    *result = (nk_f64_t)nk_angular_normalize_f32_haswell_(dot_f32, a_norm_sq_f32, b_norm_sq_f32);
}
#endif // NK_ACCUMULATE_F32_IN_F64

/* ScyllaDB: Vector Store's entry point (src/kernel/ffi.rs), returning the distance as f32. */
float vs_cosine_f32_skylake(nk_f32_t const *a, nk_f32_t const *b, nk_size_t n) {
    nk_f64_t result;
    nk_angular_f32_skylake(a, b, n, &result);
    return (float)result;
}

#if defined(__clang__)
#pragma clang attribute pop
#elif defined(__GNUC__)
#pragma GCC pop_options
#endif

#endif /* __x86_64__ */
