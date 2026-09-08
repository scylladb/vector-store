#!/usr/bin/env python3
"""Regenerate the fbin/ibin fixture files for fbin_dataset_test.rn."""
import struct
from pathlib import Path

OUT = Path(__file__).parent


def header(count, dim):
    return struct.pack("<II", count, dim)


def f32s(values):
    return struct.pack(f"<{len(values)}f", *values)


def u32s(values):
    return struct.pack(f"<{len(values)}I", *values)


# valid base/query file: 10 records x 4 dimensions, values 0.0, 0.5, 1.0, ...
good = header(10, 4) + f32s([i * 0.5 for i in range(40)])
(OUT / "data.fbin").write_bytes(good)

# unpatched prefix crop: header says 100 records, the file holds 10
(OUT / "data_cropped.fbin").write_bytes(header(100, 4) + f32s([i * 0.5 for i in range(40)]))

# trailing garbage: header says 10 records, 7 extra bytes appended
(OUT / "data_trailing.fbin").write_bytes(good + b"\x01" * 7)

# ground truth, ids only: 10 queries x k=5, ids 0..48 then u32::MAX (must decode unsigned)
ids = u32s(list(range(49)) + [4294967295])
(OUT / "gt_ids.ibin").write_bytes(header(10, 5) + ids)

# ground truth, official layout: ids plus appended f32 distances
(OUT / "gt_dist.bin").write_bytes(header(10, 5) + ids + f32s([0.1 * i for i in range(50)]))

# matches neither ground-truth layout: ids-only file with the last 3 bytes cut off
(OUT / "gt_bad.bin").write_bytes((header(10, 5) + ids)[:-3])
