/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Answering a LIKE query: walking the candidates of a [`LikePlan`] and returning only the keys
//! whose value matches the pattern.

use super::actor::PatternLikeR;
use super::like_pattern::LikePattern;
use super::like_plan::LikePlan;
use super::tantivy::PatternBackend;
use super::tantivy::TEXT_FIELD;
use super::tantivy::unframed;
use crate::IndexKey;
use crate::Limit;
use crate::PrimaryKey;
use crate::table::PrimaryId;
use crate::table::TableSearch;
use crate::tantivy_common::IndexState;
use crate::tantivy_common::PRIMARY_ID_FIELD;
use crate::tantivy_common::find_partition_id;
use anyhow::anyhow;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::RwLock;
use tantivy::DocId;
use tantivy::DocSet;
use tantivy::SegmentReader;
use tantivy::TERMINATED;
use tantivy::TantivyDocument;
use tantivy::columnar::Column;
use tantivy::fastfield::AliveBitSet;
use tantivy::query::EnableScoring;
use tantivy::query::Weight;
use tantivy::schema::Field;
use tantivy::schema::Value;
use tantivy::store::StoreReader;

/// Candidates are verified in index order, mostly one after another within a block, so a
/// single cached block of the document store is enough.
const STORE_CACHE_BLOCKS: usize = 1;

pub(super) fn handle_like(
    state: &IndexState<PatternBackend>,
    table: &RwLock<impl TableSearch>,
    index_key: &IndexKey,
    plan: &LikePlan,
    limit: Limit,
) -> PatternLikeR {
    let partition_id =
        find_partition_id::<PatternBackend>(table.read().unwrap().deref(), index_key)?;
    collect_matches(state, plan, (*limit.as_ref()).into(), |primary_id| {
        table.read().unwrap().primary_key(partition_id, primary_id)
    })
}

/// Walks the candidates in index order, unscored, and returns the keys of the first `limit`
/// that match and still `resolve` to a row.
///
/// The walk stops as soon as `limit` keys are found, so a frequent gram does not pay for its
/// whole posting list. Candidates are resolved during the walk, so a document of a row that is
/// already gone does not take the place of a match found further on.
fn collect_matches(
    state: &IndexState<PatternBackend>,
    plan: &LikePlan,
    limit: usize,
    resolve: impl FnMut(PrimaryId) -> Option<PrimaryKey>,
) -> anyhow::Result<Vec<PrimaryKey>> {
    let text_field = state.schema().get_field(TEXT_FIELD).unwrap();
    let searcher = state.searcher();
    let weight = plan
        .query(text_field)
        .weight(EnableScoring::disabled_from_searcher(&searcher))
        .map_err(|e| anyhow!("pattern: failed to build the query: {e}"))?;
    let mut found = FoundKeys::new(limit, resolve);
    for segment in searcher.segment_readers() {
        if found.is_full() {
            break;
        }
        walk_segment(segment, weight.as_ref(), text_field, plan, &mut found)?;
    }
    Ok(found.keys)
}

fn walk_segment<R: FnMut(PrimaryId) -> Option<PrimaryKey>>(
    segment: &SegmentReader,
    weight: &dyn Weight,
    text_field: Field,
    plan: &LikePlan,
    found: &mut FoundKeys<R>,
) -> anyhow::Result<()> {
    let mut scorer = weight
        .scorer(segment, 1.0)
        .map_err(|e| anyhow!("pattern: failed to run the query: {e}"))?;
    let mut docs = SegmentDocs::open(segment, text_field)?;
    let mut doc_id = scorer.doc();
    while doc_id != TERMINATED && !found.is_full() {
        add_if_matches(&mut docs, doc_id, plan, found)?;
        doc_id = scorer.advance();
    }
    Ok(())
}

fn add_if_matches<R: FnMut(PrimaryId) -> Option<PrimaryKey>>(
    docs: &mut SegmentDocs,
    doc_id: DocId,
    plan: &LikePlan,
    found: &mut FoundKeys<R>,
) -> anyhow::Result<()> {
    if !docs.is_alive(doc_id) {
        return Ok(());
    }
    let primary_id = docs.primary_id(doc_id)?;
    if docs.matches(doc_id, plan)? {
        found.add(primary_id);
    }
    Ok(())
}

/// The documents of one segment: the primary ids come from their columnar field, and the
/// document store holding the values is opened only once a candidate needs verifying.
struct SegmentDocs<'a> {
    segment: &'a SegmentReader,
    alive: Option<&'a AliveBitSet>,
    primary_ids: Column<u64>,
    text_field: Field,
    store: Option<StoreReader>,
}

impl<'a> SegmentDocs<'a> {
    fn open(segment: &'a SegmentReader, text_field: Field) -> anyhow::Result<Self> {
        let primary_ids = segment
            .fast_fields()
            .u64(PRIMARY_ID_FIELD)
            .map_err(|e| anyhow!("pattern: failed to open the primary id column: {e}"))?;
        Ok(Self {
            segment,
            alive: segment.alive_bitset(),
            primary_ids,
            text_field,
            store: None,
        })
    }

    fn is_alive(&self, doc_id: DocId) -> bool {
        self.alive.is_none_or(|alive| alive.is_alive(doc_id))
    }

    fn primary_id(&self, doc_id: DocId) -> anyhow::Result<PrimaryId> {
        self.primary_ids
            .first(doc_id)
            .map(PrimaryId::from)
            .ok_or_else(|| anyhow!("pattern: missing primary_id in the column"))
    }

    fn matches(&mut self, doc_id: DocId, plan: &LikePlan) -> anyhow::Result<bool> {
        plan.verify_with()
            .map_or(Ok(true), |pattern| self.value_matches(doc_id, pattern))
    }

    fn value_matches(&mut self, doc_id: DocId, pattern: &LikePattern) -> anyhow::Result<bool> {
        let doc: TantivyDocument = self
            .store()?
            .get(doc_id)
            .map_err(|e| anyhow!("pattern: failed to retrieve doc: {e}"))?;
        Ok(doc
            .get_first(self.text_field)
            .and_then(|value| value.as_str())
            .map(unframed)
            .transpose()?
            .is_some_and(|value| pattern.matches(value)))
    }

    fn store(&mut self) -> anyhow::Result<&StoreReader> {
        let store = match self.store.take() {
            Some(store) => store,
            None => self
                .segment
                .get_store_reader(STORE_CACHE_BLOCKS)
                .map_err(|e| anyhow!("pattern: failed to open the document store: {e}"))?,
        };
        Ok(self.store.insert(store))
    }
}

/// The keys found so far, each primary id taken into account once, as a row may be indexed by
/// more than one document.
struct FoundKeys<R> {
    limit: usize,
    resolve: R,
    seen: HashSet<PrimaryId>,
    keys: Vec<PrimaryKey>,
}

impl<R: FnMut(PrimaryId) -> Option<PrimaryKey>> FoundKeys<R> {
    fn new(limit: usize, resolve: R) -> Self {
        Self {
            limit,
            resolve,
            seen: HashSet::new(),
            keys: Vec::new(),
        }
    }

    fn is_full(&self) -> bool {
        self.keys.len() >= self.limit
    }

    fn add(&mut self, primary_id: PrimaryId) {
        if !self.seen.insert(primary_id) {
            return;
        }
        if let Some(key) = (self.resolve)(primary_id) {
            self.keys.push(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryError;
    use crate::memory::Allocate;
    use crate::pattern_index::PatternIndex;
    use crate::pattern_index::PatternIndexExt;
    use crate::pattern_index::tantivy::test_support::add_doc;
    use crate::pattern_index::tantivy::test_support::make_sender;
    use crate::pattern_index::tantivy::test_support::make_sender_with;
    use crate::pattern_index::tantivy::test_support::rm_doc;
    use crate::tantivy_common::test_support::make_index_key;
    use crate::tantivy_common::test_support::make_table_resolving;
    use crate::tantivy_common::test_support::make_table_with_keys;
    use crate::tantivy_common::test_support::primary_key_of;
    use rstest::rstest;
    use scylla::value::CqlValue;
    use std::num::NonZeroUsize;
    use std::time::Duration;
    use tokio::sync::mpsc;

    async fn make_sender_with_docs(docs: &[(u64, &str)]) -> mpsc::Sender<PatternIndex> {
        let sender = make_sender(make_table_with_keys());
        add_docs(&sender, docs).await;
        sender
    }

    async fn add_docs(sender: &mpsc::Sender<PatternIndex>, docs: &[(u64, impl AsRef<str>)]) {
        for (primary, content) in docs {
            add_doc(sender, *primary, content.as_ref()).await;
        }
    }

    async fn like_page(
        sender: &mpsc::Sender<PatternIndex>,
        pattern: &str,
        limit: usize,
    ) -> PatternLikeR {
        sender
            .like(
                make_index_key(),
                pattern.into(),
                NonZeroUsize::new(limit).unwrap().into(),
            )
            .await
    }

    /// The ids of the rows matching `pattern`, sorted, so tests do not depend on index order.
    async fn like_ids(sender: &mpsc::Sender<PatternIndex>, pattern: &str) -> Vec<i64> {
        let mut ids: Vec<i64> = like_page(sender, pattern, 100)
            .await
            .unwrap()
            .into_iter()
            .map(|key| match key.get(0).unwrap() {
                CqlValue::BigInt(id) => id,
                other => panic!("unexpected primary key value {other:?}"),
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    #[rstest]
    #[case::single_char("%宫%", &[6, 7, 8])]
    #[case::two_chars_infix_but_not_a_different_word("%将军%", &[1, 2, 3])]
    #[case::order_sensitive("%南宫%", &[6, 7])]
    #[case::longer_than_max_gram("%将军来了%", &[3])]
    #[case::prefix("将%", &[3, 4])]
    #[case::suffix("%将军", &[1, 2])]
    #[case::exact("宫南", &[8])]
    #[case::any_one("_将军", &[1, 2])]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_matches_cjk_values(#[case] pattern: &str, #[case] expected: &[i64]) {
        let sender = make_sender_with_docs(&[
            (1, "宇将军"),
            (2, "李将军"),
            (3, "将军来了"),
            (4, "将领"),
            (5, "元帅"),
            (6, "南宫月"),
            (7, "小南宫粉丝团"),
            (8, "宫南"),
        ])
        .await;

        assert_eq!(like_ids(&sender, pattern).await, expected);
    }

    #[rstest]
    #[case::prefix("AB-%", &[1, 2])]
    #[case::infix("%024%", &[1, 4, 5])]
    #[case::any_one_at_the_end("%-_", &[2, 3])]
    #[case::suffix("%-AB", &[5])]
    #[case::substring("%AB%", &[1, 2, 4, 5])]
    #[case::exact("XAB-1024", &[4])]
    #[case::exact_is_the_whole_value("AB-1024", &[])]
    #[case::case_matters("ab-%", &[3])]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_matches_latin_values(#[case] pattern: &str, #[case] expected: &[i64]) {
        let sender = make_sender_with_docs(&[
            (1, "AB-1024-XL"),
            (2, "AB-2048-S"),
            (3, "ab-4096-M"),
            (4, "XAB-1024"),
            (5, "CD-1024-AB"),
        ])
        .await;

        assert_eq!(like_ids(&sender, pattern).await, expected);
    }

    #[rstest]
    #[case::lowercase_pattern("ab-%", &[1, 2, 3])]
    #[case::uppercase_pattern("AB-%", &[1, 2, 3])]
    #[case::mixed_case_exact("Ab-4096-m", &[3])]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_on_case_insensitive_index_ignores_case(
        #[case] pattern: &str,
        #[case] expected: &[i64],
    ) {
        let sender = make_sender_with(make_table_with_keys(), Allocate::Can, false);
        add_docs(
            &sender,
            &[
                (1, "AB-1024-XL"),
                (2, "AB-2048-S"),
                (3, "ab-4096-M"),
                (4, "XAB-1024"),
                (5, "CD-1024-AB"),
            ],
        )
        .await;

        assert_eq!(like_ids(&sender, pattern).await, expected);
    }

    #[rstest]
    #[case::sigma_alone("%Σ%")]
    #[case::sigma_ending_the_literal("%ΟΣ%")]
    #[case::mixed_case("%Οσ%")]
    #[case::prefix("ΟΣ%")]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_on_case_insensitive_index_finds_a_sigma_wherever_it_stands(
        #[case] pattern: &str,
    ) {
        let sender = make_sender_with(make_table_with_keys(), Allocate::Can, false);
        add_docs(&sender, &[(1, "ΟΣ"), (2, "ΟΣΑ"), (3, "ΟΣ.Α")]).await;

        assert_eq!(like_ids(&sender, pattern).await, vec![1, 2, 3]);
    }

    #[rstest]
    #[case::cjk("将军来 军来了", "将军来了", "%将军来了%")]
    #[case::latin("abcd cde", "abcde", "%abcde%")]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_rejects_a_value_holding_all_grams_apart(
        #[case] decoy: &str,
        #[case] value: &str,
        #[case] pattern: &str,
    ) {
        let sender = make_sender_with_docs(&[(1, decoy), (2, value)]).await;

        assert_eq!(like_ids(&sender, pattern).await, vec![2]);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn walk_continues_past_false_positives_until_limit() {
        // Rows 1-20 hold every gram of `abcde` but not `abcde` itself.
        // Every row has its own segment and segments are walked in no fixed order, so many decoys
        // make it likely that some are walked before the match.
        let decoys: Vec<(u64, String)> = (1..=20).map(|id| (id, format!("abcd{id}cde"))).collect();
        let sender = make_sender(make_table_with_keys());
        add_docs(&sender, &decoys).await;
        add_doc(&sender, 21, "xxabcdexx").await;

        let keys = like_page(&sender, "%abcde%", 1).await.unwrap();

        assert_eq!(keys, vec![primary_key_of(PrimaryId::from(21))]);
    }

    #[rstest]
    #[case::latin_values(&[(1, "abc"), (2, "abc"), (3, "abc")], "%abc%")]
    #[case::one_cjk_character_over_cjk_values(
        &[
            (1, "宇将军"),
            (2, "李将军"),
            (3, "将军来了"),
            (4, "将领"),
            (5, "元帅"),
            (6, "南宫月"),
            (7, "小南宫粉丝团"),
            (8, "宫南"),
        ],
        "%宫%"
    )]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_respects_limit(#[case] docs: &[(u64, &str)], #[case] pattern: &str) {
        let sender = make_sender_with_docs(docs).await;

        assert_eq!(like_page(&sender, pattern, 2).await.unwrap().len(), 2);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn stale_ids_do_not_take_the_place_of_a_match() {
        // Rows 1-3 are gone from the table; only row 4 resolves to a key.
        let table = make_table_resolving(|primary_id| {
            (u64::from(primary_id) > 3).then(|| primary_key_of(primary_id))
        });
        let sender = make_sender(table);
        add_docs(&sender, &[(1, "abc"), (2, "abc"), (3, "abc"), (4, "abc")]).await;

        let keys = like_page(&sender, "%abc%", 1).await.unwrap();

        assert_eq!(keys, vec![primary_key_of(PrimaryId::from(4))]);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_returns_a_row_indexed_twice_once() {
        let sender = make_sender_with_docs(&[(1, "abc"), (1, "abc")]).await;

        assert_eq!(like_ids(&sender, "%abc%").await, vec![1]);
    }

    #[rstest]
    #[case::escaped_percent(r"%\%", &[1])]
    #[case::unescaped_percent("100%", &[1, 2])]
    #[case::escaped_underscore(r"a\_b", &[3])]
    #[case::unescaped_underscore("a_b", &[3, 4, 5])]
    #[case::escaped_backslash(r"%\\%", &[5])]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_treats_escaped_wildcards_as_literals(
        #[case] pattern: &str,
        #[case] expected: &[i64],
    ) {
        let sender =
            make_sender_with_docs(&[(1, "100%"), (2, "100"), (3, "a_b"), (4, "axb"), (5, r"a\b")])
                .await;

        assert_eq!(like_ids(&sender, pattern).await, expected);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn empty_pattern_matches_only_the_empty_value() {
        let sender = make_sender_with_docs(&[(1, ""), (2, "a"), (3, " ")]).await;

        assert_eq!(like_ids(&sender, "").await, vec![1]);
    }

    #[rstest]
    #[case::any_one("a_b", &[1])]
    #[case::any_run("a%b", &[1, 2])]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn wildcards_match_a_newline(#[case] pattern: &str, #[case] expected: &[i64]) {
        let sender = make_sender_with_docs(&[(1, "a\nb"), (2, "a\n\nb")]).await;

        assert_eq!(like_ids(&sender, pattern).await, expected);
    }

    #[rstest]
    #[case::exact_against_a_value_ending_with_the_end_mark("x", &[1])]
    #[case::empty_against_the_marks_swapped("", &[])]
    #[case::substring_of_the_start_mark("%\u{2}%", &[3, 4])]
    #[case::substring_of_the_end_mark("%\u{3}%", &[2, 4])]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_verifies_values_holding_frame_marks(
        #[case] pattern: &str,
        #[case] expected: &[i64],
    ) {
        let sender =
            make_sender_with_docs(&[(1, "x"), (2, "x\u{3}"), (3, "a\u{2}b"), (4, "\u{3}\u{2}")])
                .await;

        assert_eq!(like_ids(&sender, pattern).await, expected);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_excludes_a_removed_document() {
        let sender = make_sender_with_docs(&[(1, "abc"), (2, "abcd")]).await;
        rm_doc(&sender, 2).await;

        assert_eq!(like_ids(&sender, "abc%").await, vec![1]);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_before_any_document_returns_nothing() {
        let sender = make_sender(make_table_with_keys());

        assert!(like_ids(&sender, "%abc%").await.is_empty());
    }

    #[rstest]
    #[case::before_any_document(&[])]
    #[case::with_documents(&[(1, "abc")])]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_without_literal_characters_is_a_query_error(#[case] docs: &[(u64, &str)]) {
        let sender = make_sender_with_docs(docs).await;

        let err = like_page(&sender, "%_%", 10).await.unwrap_err();

        assert!(err.downcast_ref::<QueryError>().is_some(), "got: {err}");
    }
}
