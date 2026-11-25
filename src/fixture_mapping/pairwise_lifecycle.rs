//! Shared invariants for the pairwise mapping generation lifecycle.

use crate::fixture_mapping::{fingerprint_serializable, Fingerprint};
use clickhouse::{Client, Row};
use sea_query::{Alias, Expr, Func, MysqlQueryBuilder, Order, Query, UnionType};
use serde::{de::Error as _, Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::HashSet,
    sync::mpsc::{self, RecvTimeoutError, Sender},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::click_house::ClickHouse;
use uuid::Uuid;

/// Shared format identifier for a fresh football or basketball pairwise mapping.
pub const MAPPING_FORMAT_ID: &str = "poisson_pairwise_mapping/v1";
/// Matching and assignment rules used by the aligned fresh bootstrap.
pub const MAPPING_RULE_VERSION: u8 = 1;
/// Group assembly rules used by the aligned fresh bootstrap.
pub const MAPPING_ASSEMBLY_VERSION: u8 = 1;
/// Persisted raw and rectified evidence format used by the fresh bootstrap.
pub const MAPPING_EVIDENCE_FORMAT_VERSION: u8 = 1;
/// Canonical direct-signature format used by the fresh bootstrap.
pub const MAPPING_DIRECT_SIGNATURE_VERSION: u8 = 1;
const CONTENT_DIGEST_FORMAT: &str = "clickhouse-multiset-v1";
const PAIR_PROGRESS_INTERVAL: Duration = Duration::from_secs(60);
const PAIR_PROGRESS_THREAD_NAME: &str = "fixture-mapping-pair-progress";
/// Shared phase label for the provider-pair mapping loop.
pub const DIRECT_MAPPING_PROGRESS_PHASE: &str = "Direct mapping";

/// Reports that a provider pair is still being processed during long direct mapping.
#[derive(Default)]
pub struct PairProgressHeartbeat {
    stop: Option<Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

impl PairProgressHeartbeat {
    /// Logs pair start and periodically reports progress until the value is dropped.
    pub fn start(
        click_house: &ClickHouse<'_>,
        phase: &'static str,
        pair_number: usize,
        total_pairs: usize,
        source1: String,
        source2: String,
        pair_started: Instant,
    ) -> Self {
        let logger = click_house.get_logger();
        let logger_name = logger.get_logger_name().to_owned();
        logger.log_info(&format!(
            "{phase} started pair {pair_number}/{total_pairs}: {source1}/{source2}"
        ));

        let (stop, receiver) = mpsc::channel();
        let heartbeat_source1 = source1.clone();
        let heartbeat_source2 = source2.clone();
        let worker = thread::Builder::new()
            .name(PAIR_PROGRESS_THREAD_NAME.to_owned())
            .spawn(move || loop {
                match receiver.recv_timeout(PAIR_PROGRESS_INTERVAL) {
                    Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                    Err(RecvTimeoutError::Timeout) => log::info!(
                        target: logger_name.as_str(),
                        "{phase} still working: pair {pair_number}/{total_pairs} ({heartbeat_source1}/{heartbeat_source2}), completed {}/{} pairs, pair elapsed {:.1}s",
                        pair_number.saturating_sub(1),
                        total_pairs,
                        pair_started.elapsed().as_secs_f64(),
                    ),
                }
            });

        match worker {
            Ok(worker) => Self {
                stop: Some(stop),
                worker: Some(worker),
            },
            Err(error) => {
                logger.log_warn(&format!(
                    "Unable to start {phase} heartbeat for {source1}/{source2}: {error}"
                ));
                Self::default()
            }
        }
    }
}

impl Drop for PairProgressHeartbeat {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
pub(super) const GENERATIONS_TABLE: &str = "mapping_pairwise_generations";

/// Evidence revision metadata persisted in a mapping generation manifest.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct EvidenceRevision {
    /// First provider in the mapped pair.
    pub source1: String,
    /// Second provider in the mapped pair.
    pub source2: String,
    /// Generation that owns the evidence rows.
    pub generation: Uuid,
    /// Expected number of raw evidence rows in that generation.
    pub rows: u64,
    /// Order-independent digest of the raw evidence rows.
    #[serde(default)]
    pub raw_digest: String,
    /// Expected number of rectified edges for the pair.
    #[serde(default)]
    pub rectified_rows: u64,
    /// Order-independent digest of the rectified edges.
    #[serde(default)]
    pub rectified_digest: String,
}

#[derive(Default, Deserialize, Serialize)]
pub struct Manifest {
    pub version: u8,
    #[serde(default)]
    pub format_id: String,
    #[serde(default)]
    pub assembly_version: u8,
    #[serde(default)]
    pub evidence_format_version: u8,
    pub inputs: Vec<String>,
    #[serde(default)]
    pub direct_signatures: Vec<String>,
    pub inventory: Vec<String>,
    pub revisions: Vec<EvidenceRevision>,
    #[serde(default)]
    pub evidence_available: Option<bool>,
    #[serde(default)]
    pub restored_from: Option<Uuid>,
    #[serde(default)]
    pub first_fixture_entity_generation: Option<Uuid>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_stage_generation: Option<Uuid>,
}

#[derive(Deserialize, Serialize)]
pub struct AttemptInputs<'a> {
    pub rule: u8,
    #[serde(default)]
    pub format_id: std::borrow::Cow<'a, str>,
    #[serde(default)]
    pub assembly_version: u8,
    #[serde(default)]
    pub evidence_format_version: u8,
    pub inputs: std::borrow::Cow<'a, [String]>,
    pub inventory: std::borrow::Cow<'a, [String]>,
    pub direct: std::borrow::Cow<'a, [String]>,
    pub reevaluate_source: Option<String>,
    pub withdrawal: Option<std::borrow::Cow<'a, str>>,
}

impl AttemptInputs<'_> {
    pub fn changed(&self, old: &Self, providers: &[String]) -> String {
        let mut reasons = Vec::new();
        if self.rule != old.rule {
            reasons.push(format!("mapping rule {} → {}", old.rule, self.rule));
        }
        if self.format_id != old.format_id {
            reasons.push("pairwise mapping format changed".to_owned());
        }
        if self.assembly_version != old.assembly_version {
            reasons.push(format!(
                "fixture group assembly {} → {}",
                old.assembly_version, self.assembly_version
            ));
        }
        if self.evidence_format_version != old.evidence_format_version {
            reasons.push(format!(
                "evidence format {} → {}",
                old.evidence_format_version, self.evidence_format_version
            ));
        }
        for (label, previous, current) in [
            ("normalized inputs", &old.inputs, &self.inputs),
            ("fixture key inventories", &old.inventory, &self.inventory),
        ] {
            let changed: Vec<_> = providers
                .iter()
                .enumerate()
                .filter(|(i, _)| previous.get(*i) != current.get(*i))
                .map(|(_, name)| name.as_str())
                .collect();
            if !changed.is_empty() {
                reasons.push(format!("{label} changed for {}", changed.join(", ")));
            }
        }
        let changed_pairs = self
            .direct
            .iter()
            .zip(old.direct.iter())
            .filter(|(a, b)| a != b)
            .count()
            + self.direct.len().abs_diff(old.direct.len());
        if changed_pairs != 0 {
            reasons.push(format!("{changed_pairs} pair signatures changed"));
        }
        if self.reevaluate_source != old.reevaluate_source {
            reasons.push("source reevaluation option changed".to_owned());
        }
        if self.withdrawal != old.withdrawal {
            reasons.push("identity withdrawal option changed".to_owned());
        }
        if reasons.is_empty() {
            reasons.push("run signature differs despite identical saved inputs".to_owned());
        }
        reasons.join("; ")
    }
}

// A repaired checkpoint supersedes earlier metadata for the same phase.
#[derive(Deserialize, Serialize)]
pub struct SnapshotCheckpoint {
    pub created_at: i64,
    pub generation: Uuid,
    pub counts: SnapshotCounts,
}

#[derive(Deserialize, Serialize, PartialEq, Eq)]
pub struct SnapshotCounts {
    pub members: u64,
    pub keys: u64,
    pub orientations: u64,
    pub conflicts: u64,
}

#[derive(Serialize)]
pub struct RectifiedCheckpoint<'a> {
    pub created_at: i64,
    #[serde(flatten)]
    pub revision: &'a EvidenceRevision,
}

#[derive(Deserialize, Serialize)]
pub struct RowCheckpoint {
    pub created_at: i64,
    pub generation: Uuid,
    pub rows: u64,
}

impl Manifest {
    pub fn has_current_rule(&self) -> bool {
        self.format_id == MAPPING_FORMAT_ID && self.version == MAPPING_RULE_VERSION
    }

    pub fn matches_inputs(
        &self,
        direct: &[String],
        inventory: &[String],
        inputs: &[String],
    ) -> bool {
        // This run's own learned links can change the next set of pair signatures
        // even when the provider inputs have not changed. Consume them when
        // inputs change or a caller requests reevaluation. Restore clears the
        // signatures, which forces reconciliation on the next run.
        self.direct_signatures.len() == direct.len()
            && self.inventory == inventory
            && self.inputs == inputs
            && self.evidence_format_version == MAPPING_EVIDENCE_FORMAT_VERSION
            && self.assembly_version == MAPPING_ASSEMBLY_VERSION
    }
}

/// Identify every direct pair that can change evidence for this target pair.
/// None represents a change in the target pair's own direct rows.
pub fn changed_evidence_witnesses<S: Copy + Eq>(
    pair: (usize, usize),
    pairs: &[(usize, usize)],
    dirty: &[bool],
    sources: &[S],
) -> Vec<Option<S>> {
    let (i, j) = pair;
    let mut changed = Vec::new();
    for (&(a, b), &dirty) in pairs.iter().zip(dirty) {
        if !dirty {
            continue;
        }
        let witness = match (a == i || a == j, b == i || b == j) {
            (true, true) => None,
            (true, false) => Some(sources[b]),
            (false, true) => Some(sources[a]),
            (false, false) => continue,
        };
        if !changed.contains(&witness) {
            changed.push(witness);
        }
    }
    changed
}

/// Encodes ClickHouse aggregate hashes as an order-independent row-set digest.
pub fn multiset_digest(rows: u64, hash_sum: u64, hash_xor: u64) -> String {
    format!("{CONTENT_DIGEST_FORMAT}:{rows}:{hash_sum:016x}:{hash_xor:016x}")
}

/// Returns the first sequence number for an empty mapping and advances later generations.
pub fn next_mapping_sequence(previous: Option<i64>) -> Option<i64> {
    previous.unwrap_or_default().checked_add(1)
}

/// Rejects a manifest from another mapping format even if its numeric version is 1.
pub fn require_mapping_format(format_id: &str, version: u8) -> clickhouse::error::Result<()> {
    if format_id != MAPPING_FORMAT_ID || version != MAPPING_RULE_VERSION {
        return Err(clickhouse::error::Error::custom(
            "existing mapping state has no compatible version 1 format identifier; back up and clear the mapping namespace before bootstrapping",
        ));
    }
    Ok(())
}

/// Initializes or resumes an uncommitted version 1 mapping under the writer lease.
pub async fn prepare_mapping_bootstrap(client: &Client) -> clickhouse::error::Result<()> {
    #[derive(Row, Deserialize)]
    struct MappingTable {
        name: String,
    }

    let mut tables = Query::select();
    tables
        .from((Alias::new("system"), Alias::new("tables")))
        .and_where(
            Expr::col(Alias::new("database"))
                .eq(Func::cust(Alias::new("currentDatabase")).args([])),
        );
    let marker = tables
        .clone()
        .column(Alias::new("comment"))
        .and_where(Expr::col(Alias::new("name")).eq(GENERATIONS_TABLE))
        .and_where(Expr::col(Alias::new("engine")).eq("MergeTree"))
        .to_string(MysqlQueryBuilder);
    if client
        .query(&marker)
        .fetch_optional::<String>()
        .await?
        .as_deref()
        == Some(MAPPING_FORMAT_ID)
    {
        return Ok(());
    }

    let prefix = Func::cust(Alias::new("startsWith")).args([
        Expr::col(Alias::new("name")).into(),
        Expr::val("mapping_").into(),
    ]);
    let query = tables
        .column(Alias::new("name"))
        .and_where(Expr::expr(prefix).eq(true))
        .and_where(Expr::col(Alias::new("engine")).ne("View"))
        .order_by(Alias::new("name"), Order::Asc)
        .limit(1)
        .to_string(MysqlQueryBuilder);
    let dirty: Option<MappingTable> = client.query(&query).fetch_optional().await?;
    if let Some(table) = dirty {
        return Err(clickhouse::error::Error::Custom(format!(
            "version 1 mapping bootstrap requires no existing physical mapping tables; {} already exists",
            table.name,
        )));
    }
    // The marker and the first table are created by the same DDL statement.
    client
        .query(&super::pairwise_storage::generations_ddl()?)
        .execute()
        .await
}

/// Verify that a manifest contains one nonempty evidence revision for every
/// expected provider pair in the exact order consumed by pairwise assembly.
pub fn revisions_are_complete(
    revisions: &[EvidenceRevision],
    expected_pairs: &[(String, String)],
) -> bool {
    if revisions.len() != expected_pairs.len() {
        return false;
    }
    revisions
        .iter()
        .zip(expected_pairs)
        .all(|(revision, expected)| {
            !revision.generation.is_nil()
                && revision.source1 < revision.source2
                && revision.source1 == expected.0
                && revision.source2 == expected.1
                && !revision.raw_digest.is_empty()
                && !revision.rectified_digest.is_empty()
        })
}

/// Whether a completed direct pair can be reused for the current run.
pub fn should_reuse_direct(
    reusable: bool,
    repair: bool,
    interrupted: bool,
    previous_signature: Option<&str>,
    current_signature: &str,
) -> bool {
    reusable && !repair && !interrupted && previous_signature == Some(current_signature)
}

/// Fingerprint direct-pair inputs after canonicalizing unordered learned links
/// and withdrawn-link rows. Link values should canonicalize any nested witness
/// collections before being passed here.
pub fn canonical_direct_signature<L, W>(
    format_version: u8,
    rule_version: u8,
    providers: [&str; 2],
    inputs: [&str; 2],
    links: &[L],
    withdrawals: &[W],
) -> Result<Fingerprint, serde_json::Error>
where
    L: Ord + Serialize,
    W: Ord + Serialize,
{
    let mut canonical_links: Vec<_> = links.iter().collect();
    canonical_links.sort_unstable();
    let mut canonical_withdrawals: Vec<_> = withdrawals.iter().collect();
    canonical_withdrawals.sort_unstable();
    fingerprint_serializable(&(
        format_version,
        rule_version,
        providers,
        inputs,
        canonical_links,
        canonical_withdrawals,
    ))
}

/// Shared eligibility gate for an independent fixture-pair witness used to
/// learn an entity link. The caller supplies its kickoff tolerance result and
/// can exclude date-only providers.
pub fn independent_fixture_witness_allowed(
    assisted: bool,
    kickoff_within_learning_tier: bool,
    date_only_input: bool,
) -> bool {
    !assisted && kickoff_within_learning_tier && !date_only_input
}

#[derive(Clone, Copy)]
struct Candidate<R, P> {
    rank: R,
    other: usize,
    reversed: bool,
    payload: Option<P>,
    ambiguous: bool,
}

/// Resolve an unranked candidate set to mutually unique endpoint pairs.
/// Duplicate copies of the same oriented pair count once; endpoint ties and
/// conflicting orientations are rejected on both sides.
pub fn mutually_unique_pairs(
    left_count: usize,
    right_count: usize,
    candidates: impl IntoIterator<Item = (usize, usize, bool)>,
) -> Vec<(usize, usize, bool)> {
    let mut choices = MutualBestChoices::<(), ()>::new(left_count, right_count);
    for (left, right, reversed) in candidates {
        if left < left_count && right < right_count {
            choices.add(left, right, reversed, (), None);
        }
    }
    choices
        .mutual()
        .map(|(left, right, reversed, _, _)| (left, right, reversed))
        .collect()
}

/// Best-ranked candidate for each fixture on both sides of a pairwise match.
///
/// Callers own the rank and payload definitions. The shared accumulator owns
/// tie handling and only emits mutually unique choices with matching ranks and
/// orientations.
pub struct MutualBestChoices<R, P> {
    left: Vec<Option<Candidate<R, P>>>,
    right: Vec<Option<Candidate<R, P>>>,
}

impl<R, P> MutualBestChoices<R, P>
where
    R: Copy + Ord,
    P: Copy,
{
    pub fn new(left_count: usize, right_count: usize) -> Self {
        Self {
            left: vec![None; left_count],
            right: vec![None; right_count],
        }
    }

    pub fn add(&mut self, left: usize, right: usize, reversed: bool, rank: R, payload: Option<P>) {
        Self::record(&mut self.left[left], right, reversed, rank, payload);
        Self::record(&mut self.right[right], left, reversed, rank, payload);
    }

    fn record(
        best: &mut Option<Candidate<R, P>>,
        other: usize,
        reversed: bool,
        rank: R,
        payload: Option<P>,
    ) {
        match best {
            Some(saved) if saved.rank.cmp(&rank) == Ordering::Greater => {}
            Some(saved) if saved.rank == rank => {
                saved.ambiguous |= saved.other != other || saved.reversed != reversed;
                if saved.payload.is_none() {
                    saved.payload = payload;
                }
            }
            _ => {
                *best = Some(Candidate {
                    rank,
                    other,
                    reversed,
                    payload,
                    ambiguous: false,
                });
            }
        }
    }

    pub fn mutual(&self) -> impl Iterator<Item = (usize, usize, bool, R, Option<P>)> + '_ {
        self.left
            .iter()
            .enumerate()
            .filter_map(|(left, candidate)| {
                let candidate = candidate.as_ref()?;
                let reverse = self.right[candidate.other].as_ref()?;
                (!candidate.ambiguous
                    && !reverse.ambiguous
                    && reverse.other == left
                    && reverse.reversed == candidate.reversed
                    && reverse.rank == candidate.rank)
                    .then_some((
                        left,
                        candidate.other,
                        candidate.reversed,
                        candidate.rank,
                        candidate.payload,
                    ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_one_bootstrap_starts_at_sequence_one_and_uses_one_shared_format() {
        assert_eq!(MAPPING_RULE_VERSION, 1);
        assert_eq!(MAPPING_ASSEMBLY_VERSION, 1);
        assert_eq!(MAPPING_EVIDENCE_FORMAT_VERSION, 1);
        assert_eq!(MAPPING_DIRECT_SIGNATURE_VERSION, 1);
        assert_eq!(next_mapping_sequence(None), Some(1));
        assert_eq!(next_mapping_sequence(Some(1)), Some(2));
        assert_eq!(MAPPING_FORMAT_ID, "poisson_pairwise_mapping/v1");
        assert!(require_mapping_format("", 1).is_err());
        assert!(require_mapping_format(MAPPING_FORMAT_ID, 1).is_ok());
        let ddl = super::super::pairwise_storage::generations_ddl().unwrap();
        assert!(ddl.ends_with(&format!("COMMENT '{MAPPING_FORMAT_ID}'")));
    }

    #[test]
    fn multiset_digest_is_stable_for_the_same_aggregate_values() {
        assert_eq!(multiset_digest(12, 34, 56), multiset_digest(12, 34, 56));
        assert_ne!(multiset_digest(12, 34, 56), multiset_digest(12, 34, 57));
    }

    #[test]
    fn direct_pair_reuse_requires_matching_signatures_and_no_repair() {
        let signature = "same-inputs";
        assert!(should_reuse_direct(
            true,
            false,
            false,
            Some(signature),
            signature
        ));
        assert!(!should_reuse_direct(
            false,
            false,
            false,
            Some(signature),
            signature
        ));
        assert!(!should_reuse_direct(
            true,
            true,
            false,
            Some(signature),
            signature
        ));
        assert!(!should_reuse_direct(
            true,
            false,
            true,
            Some(signature),
            signature
        ));
        assert!(!should_reuse_direct(
            true,
            false,
            false,
            Some("previous-inputs"),
            signature
        ));
    }

    #[test]
    fn mutual_choices_reject_ties_and_rank_each_endpoint_independently() {
        let mut choices = MutualBestChoices::<(u8, u8), &'static str>::new(3, 3);
        choices.add(0, 0, false, (1, 0), Some("lower"));
        choices.add(0, 1, false, (2, 0), Some("higher"));
        choices.add(1, 2, true, (2, 0), Some("other"));

        let pairs: Vec<_> = choices.mutual().collect();
        assert_eq!(
            pairs,
            vec![
                (0, 1, false, (2, 0), Some("higher")),
                (1, 2, true, (2, 0), Some("other")),
            ]
        );

        let mut tied = MutualBestChoices::<u8, ()>::new(1, 2);
        tied.add(0, 0, false, 1, None);
        tied.add(0, 1, false, 1, None);
        assert_eq!(tied.mutual().count(), 0);
    }

    #[test]
    fn unranked_candidates_require_mutual_uniqueness_and_ignore_duplicate_rows() {
        let candidates = [
            (0, 0, false),
            (0, 0, false),
            (0, 1, false),
            (1, 2, true),
            (1, 2, true),
            (2, 3, false),
            (3, 3, true),
        ];
        let forward = mutually_unique_pairs(4, 4, candidates);
        let reverse = mutually_unique_pairs(4, 4, candidates.into_iter().rev());
        assert_eq!(forward, vec![(1, 2, true)]);
        assert_eq!(forward, reverse);
    }

    #[test]
    fn independent_witnesses_require_exact_unassisted_precise_inputs() {
        assert!(independent_fixture_witness_allowed(false, true, false));
        assert!(!independent_fixture_witness_allowed(true, true, false));
        assert!(!independent_fixture_witness_allowed(false, false, false));
        assert!(!independent_fixture_witness_allowed(false, true, true));
    }

    #[test]
    fn direct_signatures_ignore_link_and_withdrawal_order() {
        let links = [(1u8, "home"), (2u8, "away")];
        let reversed_links = [(2u8, "away"), (1u8, "home")];
        let withdrawals = [("team", "7"), ("competition", "9")];
        let reversed_withdrawals = [("competition", "9"), ("team", "7")];
        let first = canonical_direct_signature(
            1,
            4,
            ["left", "right"],
            ["input-a", "input-b"],
            &links,
            &withdrawals,
        )
        .unwrap();
        let reordered = canonical_direct_signature(
            1,
            4,
            ["left", "right"],
            ["input-a", "input-b"],
            &reversed_links,
            &reversed_withdrawals,
        )
        .unwrap();
        assert_eq!(first, reordered);
    }

    #[test]
    fn revision_metadata_requires_each_expected_pair_once() {
        let first = EvidenceRevision {
            source1: "basketball_reference".to_owned(),
            source2: "nba".to_owned(),
            generation: Uuid::new_v4(),
            rows: 10,
            raw_digest: "raw-a".to_owned(),
            rectified_rows: 8,
            rectified_digest: "edges-a".to_owned(),
        };
        let second = EvidenceRevision {
            source1: "nba".to_owned(),
            source2: "sportradar".to_owned(),
            generation: Uuid::new_v4(),
            rows: 4,
            raw_digest: "raw-b".to_owned(),
            rectified_rows: 3,
            rectified_digest: "edges-b".to_owned(),
        };
        let expected = vec![
            (first.source1.clone(), first.source2.clone()),
            (second.source1.clone(), second.source2.clone()),
        ];
        assert!(revisions_are_complete(
            &[first.clone(), second.clone()],
            &expected
        ));
        assert!(!revisions_are_complete(
            &[second.clone(), first.clone()],
            &expected
        ));
        assert!(!revisions_are_complete(&[first.clone(), first], &expected));
        assert!(!revisions_are_complete(&[second], &expected[..1]));
    }
}

#[derive(Row, Deserialize, Serialize)]
pub struct Head {
    #[serde(with = "clickhouse::serde::uuid")]
    pub generation: Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub parent_generation: Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub snapshot_generation: Uuid,
    pub sequence: i64,
    pub details: String,
}

pub async fn head(client: &Client) -> clickhouse::error::Result<Option<Head>> {
    let sql = Query::select()
        .columns(Head::COLUMN_NAMES.iter().map(|column| Alias::new(*column)))
        .from(Alias::new(GENERATIONS_TABLE))
        .order_by(Alias::new("sequence"), Order::Desc)
        .limit(1)
        .to_string(MysqlQueryBuilder);
    client.query(&sql).fetch_optional().await
}

/// Call with the mapping writer lease held, after dependent outputs have been staged.
pub async fn publish_head(client: &Client, candidate: &Head) -> clickhouse::error::Result<()> {
    let parent = head(client).await?;
    let actual = parent.as_ref().map_or(Uuid::nil(), |row| row.generation);
    let expected_sequence = next_mapping_sequence(parent.as_ref().map(|row| row.sequence));
    if actual != candidate.parent_generation || expected_sequence != Some(candidate.sequence) {
        return Err(clickhouse::error::Error::custom(
            "mapping parent or sequence changed before publication",
        ));
    }
    let mut insert = client.insert(GENERATIONS_TABLE)?;
    insert.write(candidate).await?;
    insert.end().await
}

/// Check that the current lookup rows are a one-to-one projection of the
/// committed wide memberships for every configured provider.
pub async fn lookup_matches_memberships(
    client: &Client,
    snapshot: Uuid,
    provider_names: &[String],
) -> clickhouse::error::Result<bool> {
    const MEMBERSHIPS_TABLE: &str = "mapping_pairwise_memberships";
    const LOOKUP_TABLE: &str = "mapping_pairwise_lookup";
    const SOURCE: &str = "source";
    const FIXTURE: &str = "source_fixture_id";
    const CENTRAL_FIXTURE: &str = "central_fixture_id";
    const EXPECTED_FLAG: &str = "expected_row";
    const LOOKUP_FLAG: &str = "lookup_row";
    const EXPECTED_COUNT: &str = "expected_count";
    const LOOKUP_COUNT: &str = "lookup_count";
    const EXPECTED_ID: &str = "expected_id";
    const LOOKUP_ID: &str = "lookup_id";
    const ROWS_ALIAS: &str = "fixture_lookup_rows";
    const MISMATCHES_ALIAS: &str = "fixture_lookup_mismatches";

    if provider_names.is_empty()
        || provider_names.iter().collect::<HashSet<_>>().len() != provider_names.len()
    {
        return Err(clickhouse::error::Error::custom(
            "fixture lookup validation requires unique provider names",
        ));
    }

    for provider in provider_names {
        let expected = Query::select()
            .expr_as(Expr::val(provider.as_str()), Alias::new(SOURCE))
            .expr_as(
                Expr::col(Alias::new(provider.as_str())),
                Alias::new(FIXTURE),
            )
            .expr_as(
                Func::cust(Alias::new("toString")).arg(Expr::col(Alias::new(CENTRAL_FIXTURE))),
                Alias::new(CENTRAL_FIXTURE),
            )
            .expr_as(Expr::val(1_i64), Alias::new(EXPECTED_FLAG))
            .expr_as(Expr::val(0_i64), Alias::new(LOOKUP_FLAG))
            .from(Alias::new(MEMBERSHIPS_TABLE))
            .and_where(
                Expr::col(Alias::new("generation"))
                    .eq(Func::cust(Alias::new("toUUID")).arg(snapshot.to_string())),
            )
            .and_where(Expr::col(Alias::new(provider.as_str())).is_not_null())
            .to_owned();
        let actual = Query::select()
            .columns([SOURCE, FIXTURE].map(Alias::new))
            .expr_as(
                Func::cust(Alias::new("toString")).arg(Expr::col(Alias::new(CENTRAL_FIXTURE))),
                Alias::new(CENTRAL_FIXTURE),
            )
            .expr_as(Expr::val(0_i64), Alias::new(EXPECTED_FLAG))
            .expr_as(Expr::val(1_i64), Alias::new(LOOKUP_FLAG))
            .from(Alias::new(LOOKUP_TABLE))
            .and_where(
                Expr::col(Alias::new("generation"))
                    .eq(Func::cust(Alias::new("toUUID")).arg(snapshot.to_string())),
            )
            .and_where(Expr::col(Alias::new(SOURCE)).eq(provider.as_str()))
            .to_owned();
        let mut rows = expected;
        rows.union(UnionType::All, actual);

        let mut mismatches = Query::select();
        let expected_flag = Expr::col(Alias::new(EXPECTED_FLAG));
        let lookup_flag = Expr::col(Alias::new(LOOKUP_FLAG));
        mismatches
            .columns([SOURCE, FIXTURE].map(Alias::new))
            .expr_as(Func::sum(expected_flag.clone()), Alias::new(EXPECTED_COUNT))
            .expr_as(Func::sum(lookup_flag.clone()), Alias::new(LOOKUP_COUNT))
            .expr_as(
                Func::cust(Alias::new("minIf")).args([
                    Expr::col(Alias::new(CENTRAL_FIXTURE)).into(),
                    expected_flag.eq(1_i64).into(),
                ]),
                Alias::new(EXPECTED_ID),
            )
            .expr_as(
                Func::cust(Alias::new("minIf")).args([
                    Expr::col(Alias::new(CENTRAL_FIXTURE)).into(),
                    lookup_flag.eq(1_i64).into(),
                ]),
                Alias::new(LOOKUP_ID),
            )
            .from_subquery(rows, Alias::new(ROWS_ALIAS))
            .group_by_columns([Alias::new(SOURCE), Alias::new(FIXTURE)]);
        mismatches.and_having(
            Expr::col(Alias::new(EXPECTED_COUNT))
                .ne(1_i64)
                .or(Expr::col(Alias::new(LOOKUP_COUNT)).ne(1_i64))
                .or(Expr::col(Alias::new(EXPECTED_ID)).ne(Expr::col(Alias::new(LOOKUP_ID)))),
        );

        let query = Query::select()
            .expr(Func::count(Expr::col(sea_query::Asterisk)))
            .from_subquery(mismatches, Alias::new(MISMATCHES_ALIAS))
            .to_string(MysqlQueryBuilder);
        if client.query(&query).fetch_one::<u64>().await? != 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Return baseline fixture keys from the trusted lookup, or from wide memberships
/// when lookup integrity has failed.
pub fn snapshot_fixture_keys(
    snapshot: Uuid,
    provider: &str,
    lookup_is_valid: bool,
) -> sea_query::SelectStatement {
    const GENERATION: &str = "generation";
    const MEMBERSHIPS_TABLE: &str = "mapping_pairwise_memberships";
    const LOOKUP_TABLE: &str = "mapping_pairwise_lookup";
    const SOURCE: &str = "source";
    const FIXTURE: &str = "source_fixture_id";
    const CENTRAL_FIXTURE: &str = "central_fixture_id";

    if lookup_is_valid {
        Query::select()
            .columns([GENERATION, SOURCE, FIXTURE, CENTRAL_FIXTURE].map(Alias::new))
            .from(Alias::new(LOOKUP_TABLE))
            .and_where(
                Expr::col(Alias::new(GENERATION))
                    .eq(Func::cust(Alias::new("toUUID")).arg(snapshot.to_string())),
            )
            .and_where(Expr::col(Alias::new(SOURCE)).eq(provider))
            .order_by(Alias::new(FIXTURE), Order::Asc)
            .to_owned()
    } else {
        Query::select()
            .column(Alias::new(GENERATION))
            .expr_as(Expr::val(provider), Alias::new(SOURCE))
            .expr_as(Expr::col(Alias::new(provider)), Alias::new(FIXTURE))
            .column(Alias::new(CENTRAL_FIXTURE))
            .from(Alias::new(MEMBERSHIPS_TABLE))
            .and_where(
                Expr::col(Alias::new(GENERATION))
                    .eq(Func::cust(Alias::new("toUUID")).arg(snapshot.to_string())),
            )
            .and_where(Expr::col(Alias::new(provider)).is_not_null())
            .order_by(Alias::new(FIXTURE), Order::Asc)
            .to_owned()
    }
}

/// Read all requested fixture keys from the same committed membership snapshot.
pub async fn lookup_pinned<S: Copy + Eq + std::hash::Hash>(
    client: &Client,
    snapshot: Uuid,
    keys: &[(S, String)],
    name: impl Fn(S) -> String,
) -> clickhouse::error::Result<fxhash::FxHashMap<(S, String), Uuid>> {
    const LOOKUP_TABLE: &str = "mapping_pairwise_lookup";
    const GENERATION: &str = "generation";
    const SOURCE: &str = "source";
    const BATCH_SIZE: usize = 500;
    #[derive(Row, Deserialize)]
    struct Lookup {
        source_fixture_id: String,
        #[serde(with = "clickhouse::serde::uuid")]
        central_fixture_id: Uuid,
    }
    let mut by_source: fxhash::FxHashMap<S, Vec<&str>> = Default::default();
    for (source, key) in keys {
        by_source.entry(*source).or_default().push(key);
    }
    let mut result = fxhash::FxHashMap::default();
    let generation = Func::cust(Alias::new("toUUID")).arg(snapshot.to_string());
    let fixture = Alias::new(Lookup::COLUMN_NAMES[0]);
    for (source, ids) in by_source {
        let provider = name(source);
        for batch in ids.chunks(BATCH_SIZE) {
            let query = Query::select()
                .columns(
                    Lookup::COLUMN_NAMES
                        .iter()
                        .map(|column| Alias::new(*column)),
                )
                .from(Alias::new(LOOKUP_TABLE))
                .and_where(Expr::col(Alias::new(GENERATION)).eq(generation.clone()))
                .and_where(Expr::col(Alias::new(SOURCE)).eq(&provider))
                .and_where(Expr::col(fixture.clone()).is_in(batch.iter().copied()))
                .to_string(MysqlQueryBuilder);
            for row in client.query(&query).fetch_all::<Lookup>().await? {
                result.insert((source, row.source_fixture_id), row.central_fixture_id);
            }
        }
    }
    Ok(result)
}

/// Select the current committed generation for a public mapping view.
pub fn latest() -> sea_query::SelectStatement {
    Query::select()
        .column(Alias::new("generation"))
        .from(Alias::new(GENERATIONS_TABLE))
        .order_by(Alias::new("sequence"), Order::Desc)
        .limit(1)
        .to_owned()
}
