#!/usr/bin/env python3
"""Regenerate the fixture files for tsv_dataset_test.rn.

documents.tsv, the queries and the qrels are the hand-written smoke-test
corpus and are not generated here.
"""
from pathlib import Path

OUT = Path(__file__).parent

# The line layouts that shift byte offsets - a blank line and a final line
# with no terminator - and the shapes only the TSV indexer has: a body
# containing a tab (kept whole), an empty body (kept: title-only documents
# are legitimate) and a multi-byte body (offsets are sums of byte lengths).
# tsv_dataset_test.rn holds the same five documents as its expected values.
(OUT / "docs_layouts.tsv").write_bytes(
    (
        "doc_a\tfirst document\n"
        "doc_tab\tbody with\tan embedded tab\n"
        "\n"
        "doc_empty\t\n"
        "doc_utf8\tzażółć gęślą jaźń 中文 \U0001f642\n"
        "doc_last\tno newline after this one"
    ).encode()
)

# a line that is not blank and has no tab
(OUT / "docs_no_tab.tsv").write_bytes(b"doc_a\tfirst document\nno tab here\n")

# a body with no id before its tab
(OUT / "docs_no_id.tsv").write_bytes(b"doc_a\tfirst document\n\torphan body\n")
