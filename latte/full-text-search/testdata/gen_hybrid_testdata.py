#!/usr/bin/env python3
"""Regenerate the embedding fixtures for hybrid.rn.

documents.fbin holds one random vector per line of documents.tsv and
queries_natural.fbin one per line of queries_natural.tsv, in file order, so
record i of the fbin is the embedding of line i of the TSV. The vectors are
random: the fixture exercises the workload, not the ranking.
"""
import random
import struct
from pathlib import Path

OUT = Path(__file__).parent
DIMENSION = 4
SEED = 821


def line_count(path):
    return sum(1 for line in path.read_text().splitlines() if line.strip())


def write_fbin(path, count, rng):
    records = [rng.uniform(-1.0, 1.0) for _ in range(count * DIMENSION)]
    path.write_bytes(struct.pack("<II", count, DIMENSION) + struct.pack(f"<{len(records)}f", *records))


rng = random.Random(SEED)
write_fbin(OUT / "documents.fbin", line_count(OUT / "documents.tsv"), rng)
write_fbin(OUT / "queries_natural.fbin", line_count(OUT / "queries_natural.tsv"), rng)
