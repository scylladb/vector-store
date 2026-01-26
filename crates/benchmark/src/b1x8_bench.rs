/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use itertools::Itertools;
use std::time::{Duration, Instant};
use usearch::b1x8;

trait F32ToB1x8Iterator: Iterator<Item = f32> + Sized {
    fn to_b1x8(self) -> Vec<b1x8> {
        let bytes: Vec<u8> = self
            .chunks(8)
            .into_iter()
            .map(|chunk| {
                chunk.enumerate().fold(0u8, |byte, (i, val)| {
                    if val > 0.0 {
                        byte | (1 << i)
                    } else {
                        byte
                    }
                })
            })
            .collect();

        b1x8::from_u8s(&bytes).to_vec()
    }
}

impl<I: Iterator<Item = f32>> F32ToB1x8Iterator for I {}

fn f32_to_b1x8_1(f32_vec: &[f32]) -> Vec<b1x8> {
    f32_vec.iter().copied().to_b1x8()
}

fn f32_to_b1x8_2(f32_vec: &[f32]) -> Vec<b1x8> {
    let bytes: Vec<u8> = f32_vec
        .chunks_exact(8)
        .map(|chunk| {
            chunk.iter().enumerate().fold(0u8, |byte, (i, &val)| {
                if val > 0.0 {
                    byte | (1 << i)
                } else {
                    byte
                }
            })
        })
        .collect();

    b1x8::from_u8s(&bytes).to_vec()
}

fn f32_to_b1x8_3(data: &[f32]) -> Vec<b1x8> {
    let bytes: Vec<u8> = data
        .chunks_exact(8)
        .map(|chunk| {
            let mut byte = 0u8;
            for (i, &val) in chunk.iter().enumerate() {
                if val > 0.0 {
                    byte |= 1 << i;
                }
            }
            byte
        })
        .collect();

    b1x8::from_u8s(&bytes).to_vec()
}

fn f32_to_b1x8_4(f32_vec: &[f32]) -> Vec<b1x8> {
    let mut bytes = vec![0u8; (f32_vec.len() + 7) / 8];

    for (i, &val) in f32_vec.iter().enumerate() {
        if val > 0.0 {
            bytes[i / 8] |= 1 << i;
        }
    }

    b1x8::from_u8s(&bytes).to_vec()
}

fn benchmark_function<F>(name: &str, f: F, data: &[f32], iterations: usize) -> Duration
where
    F: Fn(&[f32]) -> Vec<b1x8>,
{
    // Warmup
    for _ in 0..10 {
        let _ = f(data);
    }

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = f(data);
    }
    let elapsed = start.elapsed();

    let avg = elapsed / iterations as u32;
    println!(
        "{:20} | Total: {:>10.3}ms | Avg: {:>8.3}μs | Throughput: {:>10.2} MB/s",
        name,
        elapsed.as_secs_f64() * 1000.0,
        avg.as_secs_f64() * 1_000_000.0,
        (data.len() * 4 * iterations) as f64 / elapsed.as_secs_f64() / 1_000_000.0
    );

    elapsed
}

fn verify_correctness(size: usize) {
    let data: Vec<f32> = (0..size)
        .map(|i| if i % 3 == 0 { 1.0 } else { -1.0 })
        .collect();

    let result1 = f32_to_b1x8_1(&data);
    let result2 = f32_to_b1x8_2(&data);
    let result3 = f32_to_b1x8_3(&data);
    let result4 = f32_to_b1x8_4(&data);

    assert_eq!(
        result1.len(),
        result2.len(),
        "Results have different lengths"
    );
    assert_eq!(
        result1.len(),
        result3.len(),
        "Results have different lengths"
    );
    assert_eq!(
        result1.len(),
        result4.len(),
        "Results have different lengths"
    );

    // Convert to u8 for comparison
    let bytes1: &[u8] =
        unsafe { std::slice::from_raw_parts(result1.as_ptr() as *const u8, result1.len()) };
    let bytes2: &[u8] =
        unsafe { std::slice::from_raw_parts(result2.as_ptr() as *const u8, result2.len()) };
    let bytes3: &[u8] =
        unsafe { std::slice::from_raw_parts(result3.as_ptr() as *const u8, result3.len()) };
    let bytes4: &[u8] =
        unsafe { std::slice::from_raw_parts(result4.as_ptr() as *const u8, result4.len()) };

    assert_eq!(bytes1, bytes2, "Results differ between v1 and v2");
    assert_eq!(bytes1, bytes3, "Results differ between v1 and v3");
    assert_eq!(bytes1, bytes4, "Results differ between v1 and v4");

    println!("✓ Correctness verified for size {}", size);
}

fn main() {
    println!("=== F32 to B1x8 Conversion Benchmark ===\n");

    // Verify correctness first
    println!("Verifying correctness...");
    verify_correctness(1024);
    verify_correctness(1024 * 128);
    println!();

    let sizes = vec![
        ("Small (1KB)", 256),           // 256 * 4 bytes = 1KB
        ("Medium (128KB)", 32_768),     // 32KB * 4 = 128KB
        ("Large (1MB)", 262_144),       // 256KB * 4 = 1MB
        ("XLarge (16MB)", 4_194_304),   // 4M * 4 = 16MB
    ];

    for (label, size) in sizes {
        println!("=== {} ({} elements, {} bytes) ===", label, size, size * 4);

        // Generate test data
        let data: Vec<f32> = (0..size)
            .map(|i| if i % 2 == 0 { 1.0 } else { -1.0 })
            .collect();

        let iterations = match size {
            s if s < 10_000 => 10_000,
            s if s < 100_000 => 1_000,
            s if s < 1_000_000 => 100,
            _ => 10,
        };

        let t1 = benchmark_function("v1: iter+chunks", f32_to_b1x8_1, &data, iterations);
        let t2 = benchmark_function("v2: chunks_exact+fold", f32_to_b1x8_2, &data, iterations);
        let t3 = benchmark_function("v3: chunks_exact+for", f32_to_b1x8_3, &data, iterations);
        let t4 = benchmark_function("v4: preallocate+index", f32_to_b1x8_4, &data, iterations);

        println!();

        // Calculate relative performance
        let fastest = t1.min(t2).min(t3).min(t4);
        println!("Relative performance (vs fastest):");
        println!("  v1: {:.2}x", t1.as_secs_f64() / fastest.as_secs_f64());
        println!("  v2: {:.2}x", t2.as_secs_f64() / fastest.as_secs_f64());
        println!("  v3: {:.2}x", t3.as_secs_f64() / fastest.as_secs_f64());
        println!("  v4: {:.2}x", t4.as_secs_f64() / fastest.as_secs_f64());
        println!("\n{}\n", "=".repeat(80));
    }
}
