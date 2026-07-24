#!/usr/bin/env python3
"""Standalone offline utility for preparing FTS benchmark datasets.

Converts BEIR datasets (or custom TSV) into the prepared TSV layout,
optionally scales them (replicate or synthetic fragment-shuffle),
generates synthetic query sets, and splits into shards for multi-process loading.

Usage:
    python prepare_dataset.py --source beir --dataset scifact --out /tmp/scifact
    python prepare_dataset.py --source prepared --prepared-dir /tmp/scifact --scale 10 --out /tmp/scifact-10x
    python prepare_dataset.py --source custom --corpus my_docs.tsv --queries my_queries.tsv --out /tmp/mydata
    python prepare_dataset.py --source prepared --prepared-dir /tmp/scifact \\
        --scale-mode synthetic --target-docs 50000 --target-doc-size 256 --out /tmp/scifact-synth
"""

import argparse
import csv
import json
import os
import random
import re
import sys
import math
from collections import Counter, defaultdict
from typing import List


# ── Helpers ────────────────────────────────────────────────────────────────

TSV_FIELDS = ("doc_id", "body")
QREL_FIELDS = ("query_id", "doc_id", "relevance")
QUERY_FIELDS = ("query_id", "text")


def read_tsv(path, fields):
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f, fieldnames=fields, delimiter="\t")
        for row in reader:
            rows.append(row)
    return rows


def tsv_field(value):
    return str(value).replace("\t", " ").replace("\r", " ").replace("\n", " ")


def write_tsv(rows, path, fields):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        for row in rows:
            f.write("\t".join(tsv_field(row.get(f, "")) for f in fields) + "\n")


def write_tsv_simple(rows, path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        for row in rows:
            f.write("\t".join(tsv_field(value) for value in row) + "\n")


def count_lines(path):
    count = 0
    with open(path) as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def tokenize(text):
    return re.findall(r"[a-zA-Z0-9]+", text.lower())


def compute_doc_token_stats(documents):
    if not documents:
        return {}
    lengths = [len(tokenize(d["body"])) for d in documents]
    return _token_length_stats(lengths, label="per_doc")


def compute_query_token_stats(queries):
    if not queries:
        return {}
    lengths = [len(tokenize(sanitize_query_inverse(q["text"]))) for q in queries]
    return _token_length_stats(lengths, label="per_query")


def compute_qrels_stats(qrels):
    if not qrels:
        return {}
    grades_per_query = Counter()
    grade_distribution = Counter()
    for r in qrels:
        grades_per_query[r["query_id"]] += 1
        grade_distribution[r["relevance"]] += 1
    counts = list(grades_per_query.values())
    sorted_counts = sorted(counts)
    return {
        "total_entries": len(qrels),
        "queries_with_qrels": len(grades_per_query),
        "relevant_per_query": {
            "min": sorted_counts[0],
            "max": sorted_counts[-1],
            "mean": round(sum(counts) / len(counts), 1),
            "p50": _percentile_value(sorted_counts, 0.5),
        },
        "grade_distribution": dict(sorted(grade_distribution.items())),
    }


def _percentile_value(sorted_values, percentile):
    index = max(0, min(len(sorted_values) - 1, math.ceil(len(sorted_values) * percentile) - 1))
    return sorted_values[index]


def _token_length_stats(lengths, label="per_doc"):
    if not lengths:
        return {}
    total = sum(lengths)
    mean = total / len(lengths)
    sorted_lengths = sorted(lengths)
    p50 = _percentile_value(sorted_lengths, 0.5)
    p90 = _percentile_value(sorted_lengths, 0.9)
    return {
        "total_token_count": total,
        f"mean_tokens_{label}": round(mean, 1),
        f"p50_tokens_{label}": p50,
        f"p90_tokens_{label}": p90,
        f"max_tokens_{label}": sorted_lengths[-1],
    }


class ReservoirSample:
    """Maintains a fixed-size random sample of values for approximate statistics."""

    def __init__(self, capacity=100_000):
        self.capacity = capacity
        self._values = []
        self._count = 0

    def add(self, value):
        self._count += 1
        if len(self._values) < self.capacity:
            self._values.append(value)
        else:
            j = random.randint(0, self._count - 1)
            if j < self.capacity:
                self._values[j] = value

    @property
    def count(self):
        return self._count

    def stats(self):
        if not self._values:
            return {}
        s = sorted(self._values)
        n = len(s)
        return {
            "total_token_count": None,
            "sample_size": n,
            "total_docs_seen": self._count,
            "mean_tokens_per_doc": round(sum(s) / n, 1),
            "p50_tokens_per_doc": _percentile_value(s, 0.5),
            "p90_tokens_per_doc": _percentile_value(s, 0.9),
            "max_tokens_per_doc": s[-1],
        }


def sanitize_query_inverse(text):
    """Remove FTS escape backslashes for token-counting purposes."""
    return text.replace("\\", "")


# Escape characters that have special meaning in Tantivy's query grammar.
# Sources (tantivy-query-grammar/src/query_grammar.rs):
#   SPECIAL_CHARS (field name context, line 23):
#     '+', '^', '`', ':', '{', '}', '"', '\'', '[', ']', '(', ')', '!', '\\', '*', ' '
#   ESCAPE_IN_WORD / require_escape (line 46, 51):
#     same set plus '-'
#   Additional grammar-level triggers not in SPECIAL_CHARS:
#     '/' — regex query delimiter (/pattern/, line 699)
#     '~' — fuzzy/slop operator (term~, "phrase"~N)
#     '<', '>' — elastic unbounded range queries (<5, >=10, line 462)
# Space is intentionally omitted — it is the term separator in natural language queries.
FTS_ESCAPE = re.compile(r"([+\-^`:{}\"\'\[\]()!\\*~/<>])")

def sanitize_query(text, is_boolean=False):
    # Replace non-ASCII non-alphanumeric characters with space — they cannot be
    # indexed by SimpleTokenizer and may confuse Tantivy's query parser.
    # Non-ASCII alphanumeric characters (e.g. é, ñ) are preserved; they are
    # valid indexed tokens (lowercased by the tokenizer pipeline at index/query time).
    text = ''.join(c if c.isascii() or c.isalnum() else ' ' for c in text)
    if not is_boolean:
        # Remove Tantivy query operators from natural queries — they are parsed
        # case-sensitively at query-parse time (before tokenizer lowercasing).
        text = re.sub(r'\b(AND|OR|NOT|IN)\b', ' ', text)
        text = re.sub(r' +', ' ', text).strip()
    escaped = FTS_ESCAPE.sub(r"\\\1", text)
    if is_boolean:
        parts = re.split(r"(\s+(?:AND|OR|NOT)\s+)", escaped)
        result = []
        for part in parts:
            stripped = part.strip()
            if stripped in ("AND", "OR", "NOT"):
                result.append(stripped)
            else:
                result.append(part)
        return " ".join(r for r in result if r)
    return escaped


def read_queries_or_empty(path):
    if os.path.isfile(path):
        return read_tsv(path, QUERY_FIELDS)
    return []


def read_qrels_or_empty(path):
    if os.path.isfile(path):
        return read_tsv(path, QREL_FIELDS)
    return []


# ── Source loaders ─────────────────────────────────────────────────────────

def _list_beir_parquet_files(hub_id, config="corpus"):
    try:
        from huggingface_hub import list_repo_files
    except ImportError:
        try:
            from datasets.utils.hub import list_repo_files
        except ImportError:
            return None

    try:
        all_files = list_repo_files(hub_id, repo_type="dataset")
    except Exception:
        return None

    parquet_files = sorted(
        f for f in all_files
        if f.startswith(f"data/{config}/") and f.endswith(".parquet")
    ) or sorted(
        f for f in all_files
        if f.startswith(f"{config}/") and f.endswith(".parquet")
    )
    return parquet_files or None


def _get_beir_data_files(hub_id, parquet_files):
    return [
        f"https://huggingface.co/datasets/{hub_id}/resolve/main/{f}"
        for f in parquet_files
    ]


def _read_beir_config(dataset_name, config, split, max_rows=None):
    try:
        from datasets import load_dataset
    except ImportError:
        print("ERROR: the 'datasets' library is required for BEIR sources.", file=sys.stderr)
        print("Install with: pip install datasets", file=sys.stderr)
        sys.exit(1)

    hub_id = f"BeIR/{dataset_name}"

    parquet_files = _list_beir_parquet_files(hub_id, config=config)
    if parquet_files:
        data_files = _get_beir_data_files(hub_id, parquet_files)
        print(f"  Loading {config} from {hub_id} ({len(parquet_files)} parquet files) ...")
        ds = load_dataset("parquet", data_files=data_files, split="train")
        if max_rows is not None:
            ds = ds.select(range(min(max_rows, len(ds))))
        return ds

    print(f"  No parquet files found for '{config}'; trying load_dataset ...")
    load_split = split if split is not None else "train"
    try:
        ds = load_dataset(hub_id, config, split=load_split)
        if max_rows is not None:
            ds = ds.select(range(min(max_rows, len(ds))))
        return ds
    except Exception:
        pass

    try:
        ds = load_dataset(hub_id, config, split=load_split, trust_remote_code=True)
        if max_rows is not None:
            ds = ds.select(range(min(max_rows, len(ds))))
        return ds
    except Exception as e:
        print(f"  Failed to load '{config}': {e}", file=sys.stderr)
        return None


def corpus_from_dataset(corpus, max_docs):
    if max_docs is not None:
        corpus = corpus.select(range(min(max_docs, len(corpus))))

    documents = []
    for i, example in enumerate(corpus):
        text = example.get("text", "")
        if not text:
            title = example.get("title", "")
            text = title or ""
        if text:
            doc_id = example.get("_id", example.get("doc_id", f"doc_{i+1:06d}"))
            documents.append({"doc_id": doc_id, "body": text})
    return documents


def queries_from_dataset(ds, max_queries=None):
    if max_queries is not None:
        ds = ds.select(range(min(max_queries, len(ds))))
    queries = []
    for example in ds:
        qid = example.get("_id", example.get("query_id", ""))
        text = sanitize_query(example.get("text", ""))
        queries.append({"query_id": qid, "text": text})
    return queries


def qrels_from_dataset(ds):
    qrels = []
    for example in ds:
        qid = example.get("_id", example.get("query_id", ""))
        doc_id = example.get("corpus-id", example.get("doc_id", ""))
        score = example.get("score", 1)
        qrels.append({"query_id": qid, "doc_id": doc_id, "relevance": str(score)})
    return qrels


def _download_beir_qrels(dataset_name, split):
    qrels_hub_id = f"BeIR/{dataset_name}-qrels"

    if split is not None:
        filenames = [f"{split}.tsv"]
    else:
        try:
            from huggingface_hub import list_repo_files
        except ImportError:
            return None
        try:
            all_files = list_repo_files(qrels_hub_id, repo_type="dataset")
        except Exception:
            return None
        filenames = sorted(f for f in all_files if f.endswith(".tsv"))
        if not filenames:
            return None

    all_qrels = []
    for qrels_filename in filenames:
        try:
            from huggingface_hub import hf_hub_download
            path = hf_hub_download(repo_id=qrels_hub_id, filename=qrels_filename, repo_type="dataset")
        except Exception:
            continue
        with open(path, newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                qid = row.get("query-id", "").strip()
                doc_id = row.get("corpus-id", "").strip()
                score = row.get("score", "1").strip()
                if qid and doc_id:
                    all_qrels.append({"query_id": qid, "doc_id": doc_id, "relevance": score})

    if not all_qrels:
        return None

    if split is None:
        seen = set()
        deduped = []
        for r in all_qrels:
            key = (r["query_id"], r["doc_id"])
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        return deduped

    return all_qrels


def load_beir(dataset_name, split, max_docs=None, max_queries=None):
    hub_id = f"BeIR/{dataset_name}"
    print(f"  Dataset: {hub_id}")

    documents = corpus_from_dataset(
        _read_beir_config(dataset_name, "corpus", split, max_rows=max_docs),
        max_docs=None,
    )

    queries = []
    qrels = []
    queries_ds = _read_beir_config(dataset_name, "queries", split, max_rows=max_queries)
    if queries_ds is not None:
        queries = queries_from_dataset(queries_ds, max_queries=None)

    qrels_ds = _read_beir_config(dataset_name, "qrels", split)
    if qrels_ds is not None:
        qrels = qrels_from_dataset(qrels_ds)
    else:
        print(f"  No qrels config found; trying {hub_id}-qrels on HuggingFace ...")
        qrels = _download_beir_qrels(dataset_name, split) or []

    if qrels:
        qids = {r["query_id"] for r in qrels}
        queries = [q for q in queries if q["query_id"] in qids]

    if max_queries is not None and qrels:
        qids = {q["query_id"] for q in queries}
        qrels = [r for r in qrels if r["query_id"] in qids]

    return documents, queries, qrels


def load_beir_local(beir_dir, split, max_docs=None):
    corpus_path = os.path.join(beir_dir, "corpus.jsonl")
    if not os.path.isfile(corpus_path):
        print(f"ERROR: BEIR corpus not found: {corpus_path}", file=sys.stderr)
        sys.exit(1)

    documents = []
    with open(corpus_path) as f:
        for i, line in enumerate(f):
            if max_docs is not None and i >= max_docs:
                break
            entry = json.loads(line)
            doc_id = entry.get("_id") or entry.get("doc_id") or f"doc_{i+1:06d}"
            text = entry.get("text", "")
            documents.append({"doc_id": doc_id, "body": text})

    return documents


def load_custom_tsv(corpus_path, max_docs=None):
    documents = []
    with open(corpus_path, newline="") as f:
        reader = csv.DictReader(f, fieldnames=TSV_FIELDS, delimiter="\t")
        for i, row in enumerate(reader):
            if max_docs is not None and i >= max_docs:
                break
            doc_id = row.get("doc_id", "").strip()
            body = row.get("body", "").strip()
            if body:
                documents.append({"doc_id": doc_id or f"doc_{i+1:06d}", "body": body})
    return documents


def load_from_prepared_dir(prepared_dir):
    doc_path = os.path.join(prepared_dir, "documents.tsv")
    if not os.path.isfile(doc_path):
        print(f"ERROR: no documents.tsv in {prepared_dir}", file=sys.stderr)
        sys.exit(1)

    documents = read_tsv(doc_path, TSV_FIELDS)
    for i, doc in enumerate(documents):
        if not doc.get("doc_id", "").strip():
            doc["doc_id"] = f"doc_{i+1:06d}"

    queries = read_queries_or_empty(os.path.join(prepared_dir, "queries_natural.tsv"))
    qrels = read_qrels_or_empty(os.path.join(prepared_dir, "qrels_natural.tsv"))

    return documents, queries, qrels


# ── Scaling ────────────────────────────────────────────────────────────────

def scale_documents(documents, scale_factor):
    if scale_factor <= 1:
        return documents, 1

    scaled = []
    for doc in documents:
        for copy_num in range(scale_factor):
            new_id = f"{doc['doc_id']}_copy_{copy_num}" if copy_num > 0 else doc["doc_id"]
            scaled.append({"doc_id": new_id, "body": doc["body"]})
    return scaled, scale_factor


def scale_qrels(qrels, scale_factor):
    if scale_factor <= 1 or not qrels:
        return qrels
    scaled = []
    for qrel in qrels:
        for copy_num in range(scale_factor):
            doc_id = qrel["doc_id"]
            if copy_num > 0:
                doc_id = f"{doc_id}_copy_{copy_num}"
            scaled.append({
                "query_id": qrel["query_id"],
                "doc_id": doc_id,
                "relevance": qrel["relevance"],
            })
    return scaled


# ── Synthetic document scaling (fragment shuffle) ──────────────────────────

SENTENCE_BREAK = re.compile(r"(?<=[.!?])\s+")
PARAGRAPH_BREAK = re.compile(r"\n\s*\n")


def count_tokens(text):
    return len(text.split())


def _auto_fragment_unit(documents):
    sample = [d["body"] for d in documents[:100] if d.get("body")]
    if not sample:
        return "word"
    avg = sum(count_tokens(t) for t in sample) / len(sample)
    if avg > 500:
        return "paragraph"
    elif avg >= 50:
        return "sentence"
    return "word"


def _fragment_documents(documents, unit):
    fragments = []
    for doc in documents:
        text = doc.get("body", "").strip()
        if not text:
            continue
        if unit == "paragraph":
            parts = PARAGRAPH_BREAK.split(text) or [text]
        elif unit == "word":
            parts = text.split()
        else:
            parts = SENTENCE_BREAK.split(text) or [text]
        fragments.extend(p.strip() for p in parts if p.strip())
    return fragments


def scale_synthetic(documents, scale_factor=1, target_doc_size=None,
                    fragment_unit="auto", target_docs=None):
    if not documents:
        return [], 1

    original_count = len(documents)

    if target_docs is not None:
        target_count = target_docs
    else:
        target_count = max(original_count, int(original_count * scale_factor))

    if target_doc_size is None:
        total_tokens = sum(count_tokens(d["body"]) for d in documents)
        target_doc_size = total_tokens // original_count if original_count else 100

    if fragment_unit == "auto":
        fragment_unit = _auto_fragment_unit(documents)

    fragments = _fragment_documents(documents, fragment_unit)
    if not fragments:
        print("  WARNING: no fragments produced, returning originals unchanged")
        return documents, 1

    print(f"  Fragment unit: {fragment_unit} ({len(fragments)} fragments from "
          f"{original_count} docs, target ~{target_doc_size} tok/doc)")

    random.shuffle(fragments)

    new_docs = []
    for i in range(target_count):
        doc_id = f"synth_{i:06d}"
        parts = []
        size = 0
        target_min = max(1, int(target_doc_size * 0.8))

        while size < target_min:
            frag = random.choice(fragments)
            parts.append(frag)
            size += count_tokens(frag)

        new_docs.append({"doc_id": doc_id, "body": " ".join(parts)})

    effective_scale = target_count / original_count if original_count else 1
    return new_docs, effective_scale


def scale_synthetic_streaming(out_path, documents, scale_factor=1, target_doc_size=None,
                              fragment_unit="auto", target_docs=None, stats_capacity=100_000,
                              phrase_sample_capacity=100, num_shards=1):
    """Like scale_synthetic but writes documents directly to file, avoiding O(target_docs) memory.

    Also computes term doc frequencies and phrase samples during generation so that
    synthetic query generation never needs to re-read the file.

    When num_shards >= 2, writes directly to shard files instead of a single file.

    Returns (count, effective_scale, token_stats_dict, doc_freq, total_docs, phrase_samples).
    """
    if not documents:
        return 0, 1, {}, {}, 0, []

    original_count = len(documents)

    if target_docs is not None:
        target_count = target_docs
    else:
        target_count = max(original_count, int(original_count * scale_factor))

    if target_doc_size is None:
        total_tokens = sum(count_tokens(d["body"]) for d in documents)
        target_doc_size = total_tokens // original_count if original_count else 100

    if fragment_unit == "auto":
        fragment_unit = _auto_fragment_unit(documents)

    fragments = _fragment_documents(documents, fragment_unit)
    if not fragments:
        print("  WARNING: no fragments produced, returning empty")
        return 0, 1, {}, {}, 0, []

    print(f"  Fragment unit: {fragment_unit} ({len(fragments)} fragments from "
          f"{original_count} docs, target ~{target_doc_size} tok/doc)")

    random.shuffle(fragments)

    reservoir = ReservoirSample(stats_capacity)
    doc_freq = Counter()
    phrase_samples = []
    phrase_count = 0
    target_min = max(1, int(target_doc_size * 0.8))

    if num_shards >= 2:
        shard_dir = os.path.join(os.path.dirname(out_path) or ".", "shards")
        os.makedirs(shard_dir, exist_ok=True)
        shard_size = math.ceil(target_count / num_shards)
        shard_idx = 0
        fh = open(os.path.join(shard_dir, f"documents_{shard_idx:03d}.tsv"), "w")

        try:
            for i in range(target_count):
                doc_id = f"synth_{i:06d}"
                parts = []
                size = 0
                while size < target_min:
                    frag = random.choice(fragments)
                    parts.append(frag)
                    size += count_tokens(frag)
                body = " ".join(parts)
                fh.write(f"{tsv_field(doc_id)}\t{tsv_field(body)}\n")
                reservoir.add(size)

                # Advance to next shard when this one is full
                if (i + 1) % shard_size == 0 and shard_idx < num_shards - 1:
                    fh.close()
                    shard_idx += 1
                    fh = open(os.path.join(shard_dir, f"documents_{shard_idx:03d}.tsv"), "w")

                terms = set(tokenize(body))
                for t in terms:
                    doc_freq[t] += 1

                words = re.findall(r"[a-zA-Z0-9]+", body)
                if len(words) >= 2:
                    phrase_count += 1
                    n = random.randint(2, min(4, len(words)))
                    start = random.randint(0, len(words) - n)
                    phrase_words = words[start:start+n]
                    if _is_useful_phrase(phrase_words):
                        phrase = " ".join(phrase_words)
                        if len(phrase_samples) < phrase_sample_capacity:
                            phrase_samples.append(phrase)
                        else:
                            j = random.randint(0, phrase_count - 1)
                            if j < phrase_sample_capacity:
                                phrase_samples[j] = phrase
        finally:
            fh.close()
    else:
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        with open(out_path, "w") as f:
            for i in range(target_count):
                doc_id = f"synth_{i:06d}"
                parts = []
                size = 0
                while size < target_min:
                    frag = random.choice(fragments)
                    parts.append(frag)
                    size += count_tokens(frag)
                body = " ".join(parts)
                f.write(f"{tsv_field(doc_id)}\t{tsv_field(body)}\n")
                reservoir.add(size)

                terms = set(tokenize(body))
                for t in terms:
                    doc_freq[t] += 1

                words = re.findall(r"[a-zA-Z0-9]+", body)
                if len(words) >= 2:
                    phrase_count += 1
                    n = random.randint(2, min(4, len(words)))
                    start = random.randint(0, len(words) - n)
                    phrase_words = words[start:start+n]
                    if _is_useful_phrase(phrase_words):
                        phrase = " ".join(phrase_words)
                        if len(phrase_samples) < phrase_sample_capacity:
                            phrase_samples.append(phrase)
                        else:
                            j = random.randint(0, phrase_count - 1)
                            if j < phrase_sample_capacity:
                                phrase_samples[j] = phrase

    effective_scale = target_count / original_count if original_count else 1
    return target_count, effective_scale, reservoir.stats(), doc_freq, target_count, phrase_samples


# ── Synthetic query generation ─────────────────────────────────────────────

# Tantivy English stop words — mirrors the Lucene EnglishAnalyzer list used by
# StopWordFilter::new(Language::English) in tantivy 0.26.x.
# Source: tantivy/src/tokenizer/stop_word_filter/mod.rs
# Upstream: https://github.com/apache/lucene/blob/d5d6dc079395c47cd6d12dcce3bcfdd2c7d9dc63/lucene/analysis/common/src/java/org/apache/lucene/analysis/en/EnglishAnalyzer.java#L46
TANTIVY_ENGLISH_STOP_WORDS = frozenset({
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in",
    "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the",
    "their", "then", "there", "these", "they", "this", "to", "was", "will", "with",
})


def compute_term_stats(documents):
    doc_freq = Counter()
    total_docs = len(documents)
    for doc in documents:
        terms = set(tokenize(doc["body"]))
        for t in terms:
            doc_freq[t] += 1
    return doc_freq, total_docs


def bucket_terms(doc_freq, total_docs, count_per_bucket=5):
    """Classify terms into rare / medium / common by document-frequency ratio.

    Thresholds scale with average document size so that short-doc corpora
    still produce meaningful buckets:

        common :  DF/total_docs  >  0.10 × scale
        medium :  DF/total_docs  >  0.01 × scale
        rare    :  everything else

    where ``scale = min(1.0, avg_tokens / 250)`` and ``avg_tokens`` is the
    mean number of whitespace-split tokens per document (estimated from the
    doc_freq totals).

    Stopwords and single-character tokens are excluded from all buckets.

    After initial classification, if a bucket contains fewer than
    *count_per_bucket* terms the shortfall is filled by promoting the most
    frequent terms from the next-lower bucket.  A warning is logged when
    this fallback fires.
    """
    # Estimate average tokens per document.  We approximate by summing
    # the document frequencies of every term (each doc contributes +1 per
    # unique term it contains) and dividing by the number of documents.
    # This over-estimates the token count because a single document may
    # share terms, but it correlates well enough with actual doc size to
    # drive the scaling.
    total_unique_terms = sum(doc_freq.values())
    avg_tokens = total_unique_terms / total_docs if total_docs else 50

    scale = min(1.0, avg_tokens / 250)
    common_threshold = 0.10 * scale
    medium_threshold = 0.01 * scale

    # Classify
    filtered = [
        (term, freq)
        for term, freq in doc_freq.items()
        if len(term) >= 2 and term not in TANTIVY_ENGLISH_STOP_WORDS
    ]
    filtered.sort(key=lambda x: x[1], reverse=True)

    common = []
    medium = []
    rare = []
    for term, freq in filtered:
        ratio = freq / total_docs if total_docs else 0
        if ratio > common_threshold:
            common.append(term)
        elif ratio > medium_threshold:
            medium.append(term)
        else:
            rare.append(term)

    # Ensure minimum bucket sizes by promoting from the next-lower bucket.
    min_common = count_per_bucket
    min_medium = count_per_bucket * 3

    if len(common) < min_common:
        shortage = min_common - len(common)
        print(f"  WARNING: only {len(common)} terms met the common threshold "
              f"({common_threshold:.4f}); promoting {shortage} from medium")
        medium.sort(key=lambda t: doc_freq[t], reverse=True)
        common.extend(medium[:shortage])
        medium = medium[shortage:]

    if len(medium) < min_medium:
        shortage = min_medium - len(medium)
        print(f"  WARNING: only {len(medium)} terms met the medium threshold "
              f"({medium_threshold:.4f}); promoting {shortage} from rare")
        rare.sort(key=lambda t: doc_freq[t], reverse=True)
        medium.extend(rare[:shortage])
        rare = rare[shortage:]

    return rare, medium, common


def _resolve_pool(rare, medium, common, pool_name):
    """Map a pool name string to the corresponding term list."""
    pools = {
        "rare": rare,
        "medium": medium,
        "common": common,
        "medium+common": medium + common,
        "rare+medium": rare + medium,
    }
    if pool_name not in pools:
        raise ValueError(f"Unknown boolean pool name: {pool_name!r}. "
                         f"Valid options: {list(pools.keys())}")
    return pools[pool_name]


def generate_term_queries(terms, count, bucket_name):
    queries = []
    for i in range(min(count, len(terms))):
        qid = f"q_synth_term_{bucket_name}_{i+1:04d}"
        queries.append({"query_id": qid, "text": sanitize_query(terms[i])})
    return queries


def _is_useful_phrase(words):
    """Return True if *words* contains at least two non-stopword tokens."""
    meaningful = [w for w in words
                  if len(w) >= 2 and w.lower() not in TANTIVY_ENGLISH_STOP_WORDS]
    return len(meaningful) >= 2


def generate_phrase_queries(documents, count):
    phrases = []
    for doc in documents:
        if not doc["body"]:
            continue
        words = re.findall(r"[a-zA-Z0-9]+", doc["body"])
        if len(words) < 2:
            continue
        n = random.randint(2, min(4, len(words)))
        start = random.randint(0, len(words) - n)
        phrase_words = words[start:start+n]
        if not _is_useful_phrase(phrase_words):
            continue
        phrases.append(" ".join(phrase_words))

    random.shuffle(phrases)
    queries = []
    for i in range(min(count, len(phrases))):
        qid = f"q_synth_phrase_{i+1:04d}"
        queries.append({"query_id": qid, "text": sanitize_query(phrases[i])})
    return queries


def generate_boolean_queries(term_pool, count, operator):
    queries = []
    pool = term_pool[:]
    random.shuffle(pool)
    for i in range(min(count, len(pool) // 2)):
        a, b = pool[2*i], pool[2*i+1]
        qid = f"q_synth_boolean_{operator.lower()}_{i+1:04d}"
        if operator.upper() == "NOT":
            text = f"{a} NOT {b}"
        else:
            text = f"{a} {operator.upper()} {b}"
        queries.append({"query_id": qid, "text": sanitize_query(text, is_boolean=True)})
    return queries


def generate_synthetic_queries(documents, count_per_bucket=5,
                                boolean_and_pool="medium+common",
                                boolean_or_pool="rare",
                                boolean_not_pool="medium+common"):
    doc_freq, total_docs = compute_term_stats(documents)
    rare, medium, common = bucket_terms(doc_freq, total_docs, count_per_bucket)

    random.shuffle(rare)
    random.shuffle(medium)
    random.shuffle(common)

    all_queries = {}

    term_rare = generate_term_queries(rare, count_per_bucket, "rare")
    term_medium = generate_term_queries(medium, count_per_bucket, "medium")
    term_common = generate_term_queries(common, count_per_bucket, "common")

    all_queries["term_rare"] = term_rare
    all_queries["term_medium"] = term_medium
    all_queries["term_common"] = term_common

    all_queries["phrase"] = generate_phrase_queries(documents, count_per_bucket)

    and_pool = _resolve_pool(rare, medium, common, boolean_and_pool)
    or_pool = _resolve_pool(rare, medium, common, boolean_or_pool)
    not_pool = _resolve_pool(rare, medium, common, boolean_not_pool)

    random.shuffle(and_pool)
    random.shuffle(or_pool)
    random.shuffle(not_pool)

    all_queries["boolean_and"] = generate_boolean_queries(and_pool, count_per_bucket, "AND")
    all_queries["boolean_or"] = generate_boolean_queries(or_pool, count_per_bucket, "OR")
    all_queries["boolean_not"] = generate_boolean_queries(not_pool, count_per_bucket, "NOT")

    return all_queries


def generate_phrase_queries_from_list(phrases, count):
    """Generate phrase queries from a pre-sampled list of phrases."""
    random.shuffle(phrases)
    queries = []
    for i in range(min(count, len(phrases))):
        qid = f"q_synth_phrase_{i+1:04d}"
        queries.append({"query_id": qid, "text": sanitize_query(phrases[i])})
    return queries


def generate_synthetic_queries_from_stats(doc_freq, total_docs, phrase_samples,
                                          count_per_bucket=5,
                                          boolean_and_pool="medium+common",
                                          boolean_or_pool="rare",
                                          boolean_not_pool="medium+common"):
    """Generate all synthetic queries from pre-computed term stats and phrase samples.

    No file I/O needed — uses doc_freq and phrase_samples computed during streaming.
    """
    rare, medium, common = bucket_terms(doc_freq, total_docs, count_per_bucket)

    random.shuffle(rare)
    random.shuffle(medium)
    random.shuffle(common)

    all_queries = {}
    all_queries["term_rare"] = generate_term_queries(rare, count_per_bucket, "rare")
    all_queries["term_medium"] = generate_term_queries(medium, count_per_bucket, "medium")
    all_queries["term_common"] = generate_term_queries(common, count_per_bucket, "common")
    all_queries["phrase"] = generate_phrase_queries_from_list(phrase_samples, count_per_bucket)

    and_pool = _resolve_pool(rare, medium, common, boolean_and_pool)
    or_pool = _resolve_pool(rare, medium, common, boolean_or_pool)
    not_pool = _resolve_pool(rare, medium, common, boolean_not_pool)

    random.shuffle(and_pool)
    random.shuffle(or_pool)
    random.shuffle(not_pool)

    all_queries["boolean_and"] = generate_boolean_queries(and_pool, count_per_bucket, "AND")
    all_queries["boolean_or"] = generate_boolean_queries(or_pool, count_per_bucket, "OR")
    all_queries["boolean_not"] = generate_boolean_queries(not_pool, count_per_bucket, "NOT")

    return all_queries


# ── Sharding ───────────────────────────────────────────────────────────────

def write_shards(documents, num_shards, out_dir):
    if num_shards < 2:
        return

    shard_dir = os.path.join(out_dir, "shards")
    os.makedirs(shard_dir, exist_ok=True)

    shard_size = math.ceil(len(documents) / num_shards)
    for shard_idx in range(num_shards):
        start = shard_idx * shard_size
        end = min(start + shard_size, len(documents))
        chunk = documents[start:end]
        if not chunk:
            continue
        shard_path = os.path.join(shard_dir, f"documents_{shard_idx:03d}.tsv")
        write_tsv(chunk, shard_path, TSV_FIELDS)


# ── Manifest ───────────────────────────────────────────────────────────────

def write_manifest(out_dir, metadata):
    path = os.path.join(out_dir, "manifest.json")
    with open(path, "w") as f:
        json.dump(metadata, f, indent=2)


# ── CLI ────────────────────────────────────────────────────────────────────

def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Prepare FTS benchmark dataset from BEIR, custom TSV, or existing prepared data."
    )
    p.add_argument("--source", required=True,
                    choices=["beir", "beir-local", "custom", "prepared"],
                    help="Data source type")
    p.add_argument("--dataset", help="BEIR dataset name (for --source beir)")
    p.add_argument("--split", default=None, help="BEIR dataset split (default: all data)")
    p.add_argument("--beir-dir", help="Local BEIR directory (for --source beir-local)")
    p.add_argument("--corpus", help="Custom corpus TSV path (for --source custom)")
    p.add_argument("--queries", default=None, help="Custom queries TSV path (for --source custom)")
    p.add_argument("--qrels", default=None, help="Custom qrels TSV path (for --source custom)")
    p.add_argument("--prepared-dir", help="Existing prepared data directory (for --source prepared)")
    p.add_argument("--out", required=True, help="Output directory")
    p.add_argument("--scale", type=int, default=1, help="Document scale factor (default: 1, no scaling)")
    p.add_argument("--scale-mode", choices=["replicate", "synthetic"], default="replicate",
                    help="Scaling mode: replicate (copy docs), synthetic (fragment shuffle)")
    p.add_argument("--fragment-unit", choices=["auto", "sentence", "paragraph", "word"],
                    default="auto", help="Fragment granularity for synthetic scaling")
    p.add_argument("--target-doc-size", type=int, default=None,
                    help="Target tokens per synthetic document (default: corpus average)")
    p.add_argument("--target-docs", type=int, default=None,
                    help="Exact synthetic document count (instead of --scale)")
    p.add_argument("--max-docs", type=int, default=None, help="Cap original documents before scaling")
    p.add_argument("--max-queries", type=int, default=None, help="Cap natural queries (BEIR source only)")
    p.add_argument("--shards", type=int, default=1, help="Split documents into N shard files (default: 1, no split)")
    p.add_argument("--synthetic-queries", type=int, default=0,
                    help="Synthetic queries per bucket (default: 0, disabled)")
    p.add_argument("--boolean-and-pool",
                    choices=["rare", "medium", "common", "medium+common", "rare+medium"],
                    default="medium+common",
                    help="Term pool for AND boolean queries (default: medium+common)")
    p.add_argument("--boolean-or-pool",
                    choices=["rare", "medium", "common", "medium+common", "rare+medium"],
                    default="rare",
                    help="Term pool for OR boolean queries (default: rare)")
    p.add_argument("--boolean-not-pool",
                    choices=["rare", "medium", "common", "medium+common", "rare+medium"],
                    default="medium+common",
                    help="Term pool for NOT boolean queries (default: medium+common)")
    p.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    return p.parse_args(argv)


def main():
    args = parse_args()
    random.seed(args.seed)

    out_dir = os.path.abspath(args.out)
    os.makedirs(out_dir, exist_ok=True)

    documents = []
    queries = []
    qrels = []
    source_metadata = {}

    # ── Load source ────────────────────────────────────────────────────────
    if args.source == "beir":
        if not args.dataset:
            print("ERROR: --dataset is required for --source beir", file=sys.stderr)
            sys.exit(1)
        label = f"split '{args.split}'" if args.split else "all splits"
        print(f"Loading BEIR dataset '{args.dataset}' ({label}) ...")
        documents, queries, qrels = load_beir(args.dataset, args.split, max_docs=args.max_docs, max_queries=args.max_queries)
        source_metadata = {"source": "beir", "dataset": args.dataset, "split": args.split or "all"}

    elif args.source == "beir-local":
        if not args.beir_dir:
            print("ERROR: --beir-dir is required for --source beir-local", file=sys.stderr)
            sys.exit(1)
        print(f"Loading BEIR-local from {args.beir_dir} ...")
        documents = load_beir_local(args.beir_dir, args.split, max_docs=args.max_docs)
        source_metadata = {"source": "beir-local", "beir_dir": args.beir_dir, "split": args.split}

    elif args.source == "custom":
        if not args.corpus:
            print("ERROR: --corpus is required for --source custom", file=sys.stderr)
            sys.exit(1)
        print(f"Loading custom corpus from {args.corpus} ...")
        documents = load_custom_tsv(args.corpus, max_docs=args.max_docs)
        if args.queries:
            queries = read_queries_or_empty(args.queries)
        if args.qrels:
            qrels = read_qrels_or_empty(args.qrels)
        source_metadata = {"source": "custom", "corpus": args.corpus}

    elif args.source == "prepared":
        if not args.prepared_dir:
            print("ERROR: --prepared-dir is required for --source prepared", file=sys.stderr)
            sys.exit(1)
        print(f"Loading prepared data from {args.prepared_dir} ...")
        documents, queries, qrels = load_from_prepared_dir(args.prepared_dir)
        if args.max_docs is not None and args.max_docs < len(documents):
            documents = documents[:args.max_docs]
        source_metadata = {"source": "prepared", "prepared_dir": args.prepared_dir}

    if not documents:
        print("ERROR: no documents loaded", file=sys.stderr)
        sys.exit(1)

    original_doc_count = len(documents)
    print(f"  Loaded {original_doc_count} documents")

    # ── Scale ──────────────────────────────────────────────────────────────
    fragment_unit_used = None
    doc_path = os.path.join(out_dir, "documents.tsv")
    synthetic_doc_count = None
    doc_stats = {}
    doc_freq = {}
    total_docs = 0
    phrase_samples = []

    if args.scale_mode == "synthetic":
        if args.target_docs is None and args.scale <= 1:
            print("ERROR: --scale-mode synthetic requires --target-docs or --scale > 1",
                  file=sys.stderr)
            sys.exit(1)
        target_docs = args.target_docs if args.target_docs is not None else (
            len(documents) * args.scale)
        if args.fragment_unit == "auto":
            fragment_unit_used = _auto_fragment_unit(documents)
        else:
            fragment_unit_used = args.fragment_unit
        (synthetic_doc_count, applied_scale, doc_stats,
         doc_freq, total_docs, phrase_samples) = scale_synthetic_streaming(
            doc_path,
            documents,
            scale_factor=args.scale,
            target_doc_size=args.target_doc_size,
            fragment_unit=fragment_unit_used,
            target_docs=target_docs,
            num_shards=args.shards,
        )
        qrels = []
        print(f"  Generated {synthetic_doc_count} synthetic documents "
              f"(fragment={fragment_unit_used}, target_doc_size={args.target_doc_size or 'auto'})")
        # Free source documents — no longer needed after streaming write
        documents = []
        if doc_stats:
            print(f"  Token stats (sampled): mean={doc_stats.get('mean_tokens_per_doc')}, "
                  f"p50={doc_stats.get('p50_tokens_per_doc')}, "
                  f"p90={doc_stats.get('p90_tokens_per_doc')}, max={doc_stats.get('max_tokens_per_doc')}")
    else:
        documents, applied_scale = scale_documents(documents, args.scale)
        if applied_scale > 1:
            print(f"  Scaled {applied_scale}x -> {len(documents)} documents")
        if qrels and applied_scale > 1:
            qrels = scale_qrels(qrels, applied_scale)

        # ── Document token statistics ─────────────────────────────────────
        doc_stats = compute_doc_token_stats(documents)
        if doc_stats:
            print(f"  Token stats: mean={doc_stats['mean_tokens_per_doc']}, p50={doc_stats['p50_tokens_per_doc']}, "
                  f"p90={doc_stats['p90_tokens_per_doc']}, max={doc_stats['max_tokens_per_doc']}")

        # ── Write documents ───────────────────────────────────────────────
        write_tsv(documents, doc_path, TSV_FIELDS)
        print(f"  Wrote {len(documents)} documents -> {doc_path}")

    # ── Write natural queries / qrels (if any) ────────────────────────────
    if queries:
        qpath = os.path.join(out_dir, "queries_natural.tsv")
        write_tsv(queries, qpath, QUERY_FIELDS)
        print(f"  Wrote {len(queries)} natural queries -> {qpath}")

    if qrels:
        rpath = os.path.join(out_dir, "qrels_natural.tsv")
        write_tsv(qrels, rpath, QREL_FIELDS)
        print(f"  Wrote {len(qrels)} qrels -> {rpath}")

    # ── Generate synthetic queries ────────────────────────────────────────
    if args.synthetic_queries > 0 and original_doc_count > 0:
        print(f"  Generating synthetic queries ({args.synthetic_queries} per bucket) ...")
        if args.scale_mode == "synthetic":
            synthetic = generate_synthetic_queries_from_stats(
                doc_freq, total_docs, phrase_samples,
                count_per_bucket=args.synthetic_queries,
                boolean_and_pool=args.boolean_and_pool,
                boolean_or_pool=args.boolean_or_pool,
                boolean_not_pool=args.boolean_not_pool,
            )
        else:
            synthetic = generate_synthetic_queries(
                documents, count_per_bucket=args.synthetic_queries,
                boolean_and_pool=args.boolean_and_pool,
                boolean_or_pool=args.boolean_or_pool,
                boolean_not_pool=args.boolean_not_pool,
            )
        for bucket_name, qs in synthetic.items():
            if not qs:
                continue
            fname = f"queries_{bucket_name}.tsv"
            qpath = os.path.join(out_dir, fname)
            write_tsv(qs, qpath, QUERY_FIELDS)
            print(f"    {fname}: {len(qs)} queries")
    else:
        synthetic = {}

    # ── Shards ────────────────────────────────────────────────────────────
    if args.shards >= 2:
        if args.scale_mode == "synthetic":
            print(f"  Wrote {args.shards} shards -> {out_dir}/shards/")
        else:
            write_shards(documents, args.shards, out_dir)
            print(f"  Split into {args.shards} shards -> {out_dir}/shards/")

    # ── Query token statistics ──────────────────────────────────────────
    query_stats = {}
    if queries:
        qs_stats = compute_query_token_stats(queries)
        query_stats["natural"] = qs_stats
        if qs_stats:
            print(f"  Query token stats (natural): mean={qs_stats['mean_tokens_per_query']}, "
                  f"p50={qs_stats['p50_tokens_per_query']}, max={qs_stats['max_tokens_per_query']}")

    for bucket_name, qs in synthetic.items():
        if qs:
            qs_stats = compute_query_token_stats(qs)
            query_stats[bucket_name] = qs_stats
            if qs_stats:
                print(f"  Query token stats ({bucket_name}): mean={qs_stats['mean_tokens_per_query']}, "
                      f"p50={qs_stats['p50_tokens_per_query']}, max={qs_stats['max_tokens_per_query']}")

    # ── Qrels statistics ────────────────────────────────────────────────
    qrels_stats = compute_qrels_stats(qrels)
    if qrels_stats:
        rpq = qrels_stats["relevant_per_query"]
        print(f"  Qrels: {qrels_stats['total_entries']} entries, {qrels_stats['queries_with_qrels']} queries, "
              f"relevant/query: max={rpq['max']}, mean={rpq['mean']}, p50={rpq['p50']}")

    # ── Manifest ──────────────────────────────────────────────────────────
    final_doc_count = synthetic_doc_count if synthetic_doc_count is not None else len(documents)
    has_qrels = bool(qrels)
    manifest = {
        **source_metadata,
        "documents": final_doc_count,
        "original_doc_count": original_doc_count,
        "scale_mode": args.scale_mode,
        "scale_factor": applied_scale,
        "has_qrels": has_qrels,
        "synthetic_queries": args.synthetic_queries > 0,
        "synthetic_query_count_per_bucket": args.synthetic_queries,
        "num_shards": args.shards,
        "doc_token_stats": doc_stats,
    }

    if args.scale_mode == "synthetic":
        manifest["fragment_unit"] = args.fragment_unit if args.fragment_unit != "auto" else fragment_unit_used

    if queries:
        manifest["queries_natural"] = len(queries)
    if qrels:
        manifest["qrels_natural"] = len(qrels)
        manifest["qrels_stats"] = qrels_stats

    for bucket_name in ("term_rare", "term_medium", "term_common", "phrase",
                        "boolean_and", "boolean_or", "boolean_not"):
        if bucket_name in synthetic:
            manifest[f"queries_{bucket_name}"] = len(synthetic[bucket_name])

    for bucket_name, stats in query_stats.items():
        if stats:
            manifest[f"queries_{bucket_name}_token_stats"] = stats

    write_manifest(out_dir, manifest)
    print(f"  Wrote manifest -> {os.path.join(out_dir, 'manifest.json')}")

    print(f"\nDone. Dataset ready in: {out_dir}")


if __name__ == "__main__":
    main()
