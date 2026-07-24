#!/usr/bin/env python3
"""Benchmark orchestrator for ScyllaDB Full-Text Search (BM25).

Drives all phases via latte CLI calls, measures indexing throughput
with external wall-clock timing, and collects search latency + accuracy
from latte JSON reports.

Usage:
    python run_benchmark.py --node 127.0.0.1 --data-dir testdata/
    python run_benchmark.py --node 127.0.0.1 --data-dir prepared/nfcorpus/ --out /tmp/results.json
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time

def resolve_workload_path(provided):
    if provided is not None:
        return os.path.abspath(provided)
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "fts.rn")


def count_tsv_lines(path):
    count = 0
    with open(path) as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def discover_shards(data_dir):
    """Return sorted list of shard file paths, or empty list if no shards."""
    shard_dir = os.path.join(data_dir, "shards")
    if not os.path.isdir(shard_dir):
        return []
    shards = sorted(
        os.path.join(shard_dir, f)
        for f in os.listdir(shard_dir)
        if f.startswith("documents_") and f.endswith(".tsv")
    )
    return shards


def read_manifest(data_dir):
    path = os.path.join(data_dir, "manifest.json")
    if not os.path.isfile(path):
        return None
    with open(path) as f:
        return json.load(f)


def discover_query_sets(data_dir, filter_names=None):
    names = []
    try:
        for entry in sorted(os.listdir(data_dir)):
            if entry.startswith("queries_") and entry.endswith(".tsv"):
                name = entry[len("queries_"):-len(".tsv")]
                if name and (filter_names is None or name in filter_names):
                    names.append(name)
    except FileNotFoundError:
        pass
    return names


def parse_latte_report(path):
    if not os.path.isfile(path):
        return {}
    with open(path) as f:
        report = json.load(f)
    out = {}

    result = report.get("result") or report
    if not isinstance(result, dict):
        return out

    tp = result.get("req_throughput")
    if isinstance(tp, dict) and "value" in tp:
        out["qps"] = tp["value"]

    out["errors"] = result.get("error_count", 0)

    lat = result.get("request_latency")
    if isinstance(lat, dict):
        pcts = lat.get("percentiles")
        # latte order: [Min, P1, P2, P5, P10, P25, P50, P75, P90, P95, P98, P99, P99.9, P99.99, Max]
        if isinstance(pcts, list) and len(pcts) >= 15:
            out["latency_ms"] = {
                "min":   pcts[0]["value"],
                "p50":   pcts[6]["value"],
                "p90":   pcts[8]["value"],
                "p95":   pcts[9]["value"],
                "p99":   pcts[11]["value"],
                "p99.9": pcts[12]["value"],
                "max":   pcts[14]["value"],
            }

    cm = result.get("custom_metrics")
    if isinstance(cm, dict):
        flat = {}
        for name, metric in cm.items():
            dist = metric.get("distribution", {})
            entry = {"mean": dist.get("mean", {}).get("value")}
            std_err = dist.get("mean", {}).get("std_err")
            if std_err is not None:
                entry["std_err"] = std_err
            if name == "result_count":
                pcts = dist.get("percentiles")
                if isinstance(pcts, list) and len(pcts) >= 15:
                    entry["min"] = pcts[0]["value"]
                    entry["p50"] = pcts[6]["value"]
                    entry["p90"] = pcts[8]["value"]
                    entry["p95"] = pcts[9]["value"]
                    entry["p99"] = pcts[11]["value"]
                    entry["max"] = pcts[14]["value"]
            flat[name] = entry
        if flat:
            out["metrics"] = flat

    return out


def run_latte(cmd, label):
    quoted = []
    for a in cmd:
        if " " in str(a) or "=" in str(a):
            quoted.append(f"'{a}'")
        else:
            quoted.append(str(a))
    print(f"\n{'\u2500'*60}", flush=True)
    print(f"  Phase: {label}", flush=True)
    print(f"  $ {' '.join(quoted)}", flush=True)
    print(f"{'\u2500'*60}", flush=True)

    start = time.monotonic()
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    stdout = proc.stdout
    if stdout:
        for line in stdout:
            print(line, end="", flush=True)
    proc.wait()
    elapsed = time.monotonic() - start
    if proc.returncode != 0:
        print(f"\nERROR: {label} exited with code {proc.returncode}", file=sys.stderr)
        sys.exit(proc.returncode)
    return elapsed


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="ScyllaDB FTS Benchmark Orchestrator")
    p.add_argument("--node", required=True, help="ScyllaDB host[:port]")
    p.add_argument("--data-dir", required=True, help="Prepared dataset directory")
    p.add_argument("--out", default=None, help="Output summary path (default: <data-dir>/results.json)")
    p.add_argument("--latte-bin", default="latte", help="Path to latte binary (default: 'latte' in PATH)")
    p.add_argument("--latte-workload", default=None, help="Path to fts.rn (default: alongside this script)")
    p.add_argument("--keyspace", default="fts_bench")
    p.add_argument("--table", default="documents")
    p.add_argument("--index-name", default="documents_fts_idx")
    p.add_argument("--replication-factor", type=int, default=1)
    p.add_argument("--index-options", default="", help="CQL WITH OPTIONS string")
    p.add_argument("--load-threads", type=int, default=1)
    p.add_argument("--load-concurrency", type=int, default=64)
    p.add_argument("--retry-number", type=int, default=10, help="Max retries for index probe")
    p.add_argument("--retry-interval", default="1s", help="Latte retry interval for index probe (e.g. 500ms,5s)")
    p.add_argument("--max-index-wait", type=int, default=600, help="Max seconds to wait for index build (default: 600)")
    p.add_argument("--search-duration", default="60s", help="Per-query-set run duration")
    p.add_argument("--search-concurrency", type=int, default=32)
    p.add_argument("--limit", type=int, default=5, help="CQL LIMIT for search queries")
    p.add_argument("--search-warmup", default="10s")
    p.add_argument("--doc-limit", type=int, default=None, help="Maximum number of documents to load/index (applied as min(doc_count, doc_limit))")
    p.add_argument("--query-sets", default="all", help="Comma-separated subset or 'all'")
    p.add_argument("--skip-schema", action="store_true", help="Skip keyspace/table creation")
    p.add_argument("--skip-load", action="store_true", help="Skip schema + load (table pre-populated)")
    p.add_argument("--skip-search", action="store_true", help="Skip search phase")
    p.add_argument("--skip-build-index", action="store_true", help="Skip index build phase")
    p.add_argument("--drop-index", action="store_true", help="Drop the FTS index before schema creation")
    p.add_argument("--no-cleanup", action="store_true", help="Skip index cleanup after search (user manages state)")
    p.add_argument("--min-successful-probes", type=int, default=3,
                    help="Consecutive successful probes before index is considered ready (default: 3)")
    return p.parse_args(argv)


def main():
    args = parse_args()

    latte_bin = args.latte_bin
    if "/" in latte_bin:
        if not os.path.isfile(latte_bin) or not os.access(latte_bin, os.X_OK):
            print(f"ERROR: latte binary not found or not executable: {latte_bin}", file=sys.stderr)
            sys.exit(1)
    elif shutil.which(latte_bin) is None:
        print(f"ERROR: latte binary not found in PATH: {latte_bin}", file=sys.stderr)
        sys.exit(1)

    data_dir = os.path.abspath(args.data_dir)
    workload = resolve_workload_path(args.latte_workload)

    if not os.path.isdir(data_dir):
        print(f"ERROR: data directory not found: {data_dir}", file=sys.stderr)
        sys.exit(1)
    if not os.path.isfile(workload):
        print(f"ERROR: latte workload not found: {workload}", file=sys.stderr)
        sys.exit(1)

    out_path = args.out or os.path.join(data_dir, "results.json")
    out_dir = os.path.dirname(os.path.abspath(out_path))
    reports_dir = os.path.join(out_dir, "reports")
    os.makedirs(reports_dir, exist_ok=True)

    skip_schema = args.skip_schema or args.skip_load
    skip_load = args.skip_load

    # ── Document count ───────────────────────────────────────────────────
    manifest = read_manifest(data_dir)
    doc_path = os.path.join(data_dir, "documents.tsv")
    shards = discover_shards(data_dir)
    if os.path.isfile(doc_path):
        doc_count = count_tsv_lines(doc_path)
    elif shards:
        doc_count = sum(count_tsv_lines(s) for s in shards)
    else:
        doc_count = 0

    if manifest is None:
        print(f"Info: no manifest.json - counting {doc_count} documents from TSV")
    else:
        doc_count = manifest.get("documents") or manifest.get("document_count") or doc_count

    if args.doc_limit is not None:
        doc_count = min(doc_count, args.doc_limit)
        print(f"Doc limit applied: using {doc_count} documents")

    print(f"Target document count: {doc_count}")

    # ── Result structure ─────────────────────────────────────────────────
    results = {
        "data_dir": data_dir,
        "manifest": manifest,
        "document_count": doc_count,
        "cpu_count": os.cpu_count(),
        "index_options": args.index_options or "server_defaults",
        "phases": {},
    }

    # ══════════════════════════════════════════════════════════════════════
    # SCHEMA
    # ══════════════════════════════════════════════════════════════════════
    if not skip_schema:
        cmd = [
            latte_bin, "schema", str(workload), args.node,
            "-P", f'keyspace="{args.keyspace}"',
            "-P", f'table="{args.table}"',
            "-P", f"replication_factor={args.replication_factor}",
        ]
        if args.index_options:
            cmd += ["-P", f'index_options="{args.index_options}"']
        if args.drop_index:
            cmd += ["-P", "drop_index=true"]
        run_latte(cmd, "SCHEMA")

    # ══════════════════════════════════════════════════════════════════════
    # LOAD
    # ══════════════════════════════════════════════════════════════════════
    if not skip_load and doc_count > 0:
        total_elapsed = 0.0
        if shards:
            remaining = args.doc_limit if args.doc_limit is not None else None
            print(f"Loading {len(shards)} shards ...")
            for shard_path in shards:
                shard_name = os.path.basename(shard_path)
                shard_count = count_tsv_lines(shard_path)
                if shard_count == 0:
                    continue
                if remaining is not None:
                    load_count = min(shard_count, remaining)
                    remaining -= load_count
                else:
                    load_count = shard_count
                if load_count == 0:
                    break
                cmd = [
                    latte_bin, "run", "-f", "load", str(workload), args.node,
                    "-d", str(load_count),
                    "-P", f'fts_data_dir="{data_dir}"',
                    "-P", f'documents_file="shards/{shard_name}"',
                    "-P", f'keyspace="{args.keyspace}"',
                    "-P", f'table="{args.table}"',
                    "-P", 'target_column="body"',
                    "--threads", str(args.load_threads),
                    "--concurrency", str(args.load_concurrency),
                ]
                elapsed = run_latte(cmd, f"LOAD shard {shard_name}")
                total_elapsed += elapsed
                shard_throughput = round(load_count / elapsed, 1) if elapsed > 0 else 0.0
                print(f"  {shard_name}: {load_count} docs, {shard_throughput} docs/s")
            elapsed = total_elapsed
        else:
            cmd = [
                latte_bin, "run", "-f", "load", str(workload), args.node,
                "-d", str(doc_count),
                "-P", f'fts_data_dir="{data_dir}"',
                "-P", 'documents_file="documents.tsv"',
                "-P", f'keyspace="{args.keyspace}"',
                "-P", f'table="{args.table}"',
                "-P", 'target_column="body"',
                "--threads", str(args.load_threads),
                "--concurrency", str(args.load_concurrency),
            ]
            elapsed = run_latte(cmd, "LOAD")
        throughput = round(doc_count / elapsed, 1) if elapsed > 0 and doc_count > 0 else 0.0
        results["phases"]["load"] = {
            "elapsed_seconds": elapsed,
            "throughput_docs_per_sec": throughput,
        }
    elif not skip_load:
        print("Info: no documents to load, skipping LOAD phase")

    # ══════════════════════════════════════════════════════════════════════
    # BUILD INDEX
    # ══════════════════════════════════════════════════════════════════════
    if not args.skip_build_index:
        cmd = [
            latte_bin, "run", "-f", "build_index", str(workload), args.node,
            "-d", "1",
            "-P", f'keyspace="{args.keyspace}"',
            "-P", f'table="{args.table}"',
            "-P", 'target_column="body"',
            "-P", f'index_name="{args.index_name}"',
            "--retry-number", str(args.retry_number),
            "--retry-interval", args.retry_interval,
            "-P", f'max_index_wait_secs={args.max_index_wait}',
            "-P", f'min_successful_probes={args.min_successful_probes}',
            "-P", f'document_count={doc_count}',
            "--generate-report", "-o", os.path.join(reports_dir, "build_index.json"),
        ]
        if args.index_options:
            cmd += ["-P", f'index_options="{args.index_options}"']
        elapsed = run_latte(cmd, "BUILD INDEX")
        throughput = round(doc_count / elapsed, 1) if elapsed > 0 and doc_count > 0 else 0.0
        results["phases"]["build_index"] = {
            "elapsed_seconds": elapsed,
            "documents": doc_count,
            "indexing_throughput_docs_per_sec": throughput,
        }

    # ══════════════════════════════════════════════════════════════════════
    # SEARCH
    # ══════════════════════════════════════════════════════════════════════
    if not args.skip_search:
        if args.query_sets == "all":
            qs_names = discover_query_sets(data_dir)
        else:
            requested = {s.strip() for s in args.query_sets.split(",") if s.strip()}
            qs_names = discover_query_sets(data_dir, filter_names=requested)

        if qs_names:
            results["phases"]["search"] = {}
            for name in qs_names:
                qfile = f"queries_{name}.tsv"
                qpath = os.path.join(data_dir, qfile)
                if not os.path.isfile(qpath):
                    print(f"Warning: query file not found: {qpath}")
                    continue
                if os.path.getsize(qpath) == 0:
                    print(f"Warning: query file is empty, skipping: {qpath}")
                    continue

                rfile = f"qrels_{name}.tsv"
                has_qrels = os.path.isfile(os.path.join(data_dir, rfile))
                acc = "true" if has_qrels else "false"

                report_path = os.path.join(reports_dir, f"{name}.json")

                cmd = [
                    latte_bin, "run", "-f", "search", str(workload), args.node,
                    "-d", args.search_duration,
                    "--concurrency", str(args.search_concurrency),
                    "--warmup", args.search_warmup,
                    "--generate-report", "-o", str(report_path),
                    "-P", f'fts_data_dir="{data_dir}"',
                    "-P", f'queries_file="{qfile}"',
                    "-P", f'qrels_file="{rfile}"',
                    "-P", f"compute_accuracy={acc}",
                    "-P", f'keyspace="{args.keyspace}"',
                    "-P", f'table="{args.table}"',
                    "-P", 'target_column="body"',
                    "-P", f"search_limit={args.limit}",
                ]

                run_latte(cmd, f"SEARCH ({name})")

                parsed = parse_latte_report(report_path)
                parsed["report"] = os.path.relpath(report_path, out_dir)
                parsed["limit"] = args.limit
                results["phases"]["search"][name] = parsed
        else:
            print("Info: no query sets found in data directory")

    # ══════════════════════════════════════════════════════════════════════
    # WRITE RESULTS
    # ══════════════════════════════════════════════════════════════════════
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written -> {out_path}")

    # ══════════════════════════════════════════════════════════════════════
    # CLEANUP
    # ══════════════════════════════════════════════════════════════════════
    if not args.no_cleanup:
        cmd = [
            latte_bin, "schema", str(workload), args.node,
            "-P", f'keyspace="{args.keyspace}"',
            "-P", f'table="{args.table}"',
            "-P", "drop_index=true",
            "-P", f'index_name="{args.index_name}"',
        ]
        run_latte(cmd, "CLEANUP")

    # ══════════════════════════════════════════════════════════════════════
    # SUMMARY
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n{'═'*60}")
    print("  SUMMARY")
    print(f"{'═'*60}")
    bp = results["phases"].get("build_index")
    if bp:
        print(f"  Indexing throughput:   {bp['indexing_throughput_docs_per_sec']:>12.1f}  docs/sec")
        print(f"  Index build time:      {bp['elapsed_seconds']:>12.1f}  s")
    search_phases = results["phases"].get("search", {})
    for sname, sdata in search_phases.items():
        qps = sdata.get("qps", "N/A")
        errs = sdata.get("errors", "?")
        lt = sdata.get("latency_ms", {})
        parts = [f"QPS={qps}", f"limit={sdata.get('limit','?')}",
                 f"errors={errs}", f"p50={lt.get('p50','?')}ms", f"p99={lt.get('p99','?')}ms"]
        metrics = sdata.get("metrics", {})
        rc = metrics.get("result_count", {})
        if rc:
            rmean = rc.get("mean")
            rp50 = rc.get("p50")
            rp99 = rc.get("p99")
            if rmean is not None:
                parts.append(f"result_mean={rmean:.1f}")
            if rp50 is not None:
                parts.append(f"count_p50={rp50:.0f}")
            if rp99 is not None:
                parts.append(f"count_p99={rp99:.0f}")
        for mname in ("recall", "precision", "mrr", "ndcg"):
            if mname in metrics:
                mv = metrics[mname].get("mean")
                if mv is not None:
                    parts.append(f"{mname}={mv:.3f}")
                else:
                    parts.append(f"{mname}=N/A")
        print(f"  Search [{sname}]:  {' | '.join(parts)}")


if __name__ == "__main__":
    main()
