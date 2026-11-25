/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use itertools::Itertools;
use std::ops::RangeInclusive;

pub(crate) const SEED: u64 = 42;
const VOCABULARY: usize = 100_000;
const WORD_LEN: RangeInclusive<usize> = 3..=10;
const DOC_LEN: RangeInclusive<usize> = 50..=150;

/// A deterministic corpus: Zipf-distributed words over a fixed vocabulary of pseudo-words.
pub(crate) struct Corpus {
    rng: SplitMix64,
    words: Vec<String>,
    // Cumulative Zipf weights, for sampling by binary search.
    cdf: Vec<f64>,
}

impl Corpus {
    pub(crate) fn new(seed: u64) -> Self {
        let mut rng = SplitMix64(seed);
        let words = (0..VOCABULARY)
            .map(|_| {
                (0..rng.in_range(&WORD_LEN))
                    .map(|_| char::from(b'a' + rng.below(26) as u8))
                    .collect()
            })
            .collect();
        let cdf = (1..=VOCABULARY)
            .scan(0.0, |sum, rank| {
                *sum += 1.0 / rank as f64;
                Some(*sum)
            })
            .collect();
        Self { rng, words, cdf }
    }

    /// The word of the given Zipf rank; rank 0 is the most frequent one.
    pub(crate) fn word(&self, rank: usize) -> &str {
        &self.words[rank]
    }

    pub(crate) fn documents(&mut self, count: usize) -> Vec<String> {
        (0..count).map(|_| self.document()).collect()
    }

    fn document(&mut self) -> String {
        let total = *self.cdf.last().unwrap();
        (0..self.rng.in_range(&DOC_LEN))
            .map(|_| {
                let target = self.rng.unit() * total;
                let rank = self.cdf.partition_point(|&sum| sum < target);
                self.words[rank.min(VOCABULARY - 1)].as_str()
            })
            .join(" ")
    }
}

/// SplitMix64, to keep the corpus independent of the `rand` version.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn in_range(&mut self, range: &RangeInclusive<usize>) -> usize {
        range.start() + self.below((range.end() - range.start() + 1) as u64) as usize
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}
