//! Deterministic, service-free fixture matching and per-fixture assessment reuse.
//!
//! Callers keep provider records and published identities. This module borrows a
//! compact view of each fixture, learns team relationships from unambiguous
//! seed identities, and resolves mutually unique best matches. Sport-specific
//! time, score, period, and orientation rules are supplied by [`MatchingPolicy`].

pub mod assessment_codec;
pub mod pairwise_evidence;
pub mod pairwise_groups;
pub mod pairwise_lifecycle;
pub mod pairwise_storage;
pub mod publication;
pub mod registry;

use fxhash::{FxHashMap, FxHashSet};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{hash::Hash, sync::Arc};
use unicode_normalization::{char::is_combining_mark, UnicodeNormalization};

const REQUIRED_SUPPORT: usize = 3;
const COUNTRY_COMPETITION_SCOPE: usize = 0;
const COUNTRY_SCOPE: usize = 1;
const COMPETITION_SCOPE: usize = 2;
const TEAM_SCOPE: usize = 3;
const SCOPE_COUNT: usize = 4;
const STRICT_CONTEXT_TIER: u8 = 0;
const ABSENT_EDGE_VALUE: u8 = u8::MAX;
const POLICY_FINGERPRINT_MARKER: u8 = 1;
const LEGACY_MAPPING_OUTPUT_VERSION: u8 = 1;

/// The SHA-256 representation used by fixture inputs and assessment records.
pub type Fingerprint = [u8; 32];

/// Number of evidence tiers represented by the cache format.
pub const EVIDENCE_TIER_COUNT: u8 = 12;

/// Version used by football's existing assessment-key format.
///
/// Keep this at `1` while the shared engine preserves that format. Bump it when
/// an incompatible matching change must invalidate persisted assessments.
pub const MAPPING_OUTPUT_VERSION: u8 = 1;

/// Coarse timestamp precision supplied with a fixture view.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum TimestampPrecision {
    /// The provider supplies a measured kickoff or event timestamp.
    Exact,
    /// The provider supplies a calendar date without a reliable time of day.
    DateOnly,
}

/// Relative evidence strength used to rank otherwise eligible fixture pairs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvidenceStrength {
    /// Scores and time both support the pair.
    ScoreAndTime,
    /// Time supports the pair.
    Time,
    /// Scores support the pair.
    Score,
}

impl EvidenceStrength {
    fn rank(self) -> u8 {
        match self {
            Self::ScoreAndTime => 0,
            Self::Time => 1,
            Self::Score => 2,
        }
    }

    fn from_rank(rank: u8) -> Option<Self> {
        match rank {
            0 => Some(Self::ScoreAndTime),
            1 => Some(Self::Time),
            2 => Some(Self::Score),
            _ => None,
        }
    }
}

/// Context scope under which a pair of team relationships was learned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatchScope {
    /// Country and competition both agree.
    CountryCompetition,
    /// Country agrees.
    Country,
    /// Competition agrees.
    Competition,
    /// Team relationships agree without a shared context.
    Team,
}

impl MatchScope {
    fn index(self) -> usize {
        match self {
            Self::CountryCompetition => COUNTRY_COMPETITION_SCOPE,
            Self::Country => COUNTRY_SCOPE,
            Self::Competition => COMPETITION_SCOPE,
            Self::Team => TEAM_SCOPE,
        }
    }

    fn from_index(index: usize) -> Option<Self> {
        match index {
            COUNTRY_COMPETITION_SCOPE => Some(Self::CountryCompetition),
            COUNTRY_SCOPE => Some(Self::Country),
            COMPETITION_SCOPE => Some(Self::Competition),
            TEAM_SCOPE => Some(Self::Team),
            _ => None,
        }
    }
}

/// Indexes one-to-one alias conflicts by scope for constant-time candidate checks.
///
/// Each input item is `(scope, left identity, right identity)`. An identity conflicts
/// when the same left identity maps to multiple right identities, or vice versa,
/// within one scope.
pub struct AliasConflictIndex<Scope, Id> {
    right_by_left: FxHashMap<Scope, FxHashMap<Id, Option<Id>>>,
    left_by_right: FxHashMap<Scope, FxHashMap<Id, Option<Id>>>,
}

impl<Scope, Id> AliasConflictIndex<Scope, Id>
where
    Scope: Clone + Eq + Hash,
    Id: Clone + Eq + Hash,
{
    /// Builds the index in one pass over the discovered aliases.
    pub fn new(aliases: impl IntoIterator<Item = (Scope, Id, Id)>) -> Self {
        let mut index = Self {
            right_by_left: FxHashMap::default(),
            left_by_right: FxHashMap::default(),
        };
        for (scope, left, right) in aliases {
            Self::record_target(
                &mut index.right_by_left,
                scope.clone(),
                left.clone(),
                right.clone(),
            );
            Self::record_target(&mut index.left_by_right, scope, right, left);
        }
        index
    }

    /// Returns whether either side conflicts with a different identity in this scope.
    pub fn conflicts(&self, scope: &Scope, left: &Id, right: &Id) -> bool {
        Self::target_conflicts(&self.right_by_left, scope, left, right)
            || Self::target_conflicts(&self.left_by_right, scope, right, left)
    }

    fn record_target(
        targets: &mut FxHashMap<Scope, FxHashMap<Id, Option<Id>>>,
        scope: Scope,
        source: Id,
        target: Id,
    ) {
        match targets.entry(scope).or_default().entry(source) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(Some(target));
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if entry
                    .get()
                    .as_ref()
                    .is_some_and(|existing| existing != &target)
                {
                    entry.insert(None);
                }
            }
        }
    }

    fn target_conflicts(
        targets: &FxHashMap<Scope, FxHashMap<Id, Option<Id>>>,
        scope: &Scope,
        source: &Id,
        target: &Id,
    ) -> bool {
        targets
            .get(scope)
            .and_then(|by_source| by_source.get(source))
            .is_some_and(|existing| existing.as_ref().is_none_or(|existing| existing != target))
    }
}

/// A team's raw provider identity, optional canonical identity, and seed name.
///
/// `provider_id` is always kept separate from `canonical_id`: matching results
/// are positions into the caller's input, and the caller remains responsible
/// for publishing its original source keys.
#[derive(Debug)]
pub struct TeamView<'a, Id> {
    /// Provider-local team ID used in learned relationships and candidate lookup.
    pub provider_id: &'a Id,
    /// Optional reviewed identity used to join canonical seed identities.
    pub canonical_id: Option<&'a Id>,
    /// Provider-normalized name used only by exact-name completion.
    pub match_name: Option<&'a str>,
    /// Caller-normalized team name. Set to `None` when the name is unusable.
    pub seed_name: Option<&'a str>,
}

impl<Id> Copy for TeamView<'_, Id> {}

impl<Id> Clone for TeamView<'_, Id> {
    fn clone(&self) -> Self {
        *self
    }
}

/// Borrowed matching input for one provider fixture.
///
/// The raw provider IDs are retained independently from optional canonical
/// context and team identities. The caller should fingerprint the complete
/// serialized input it already uses for cache invalidation, including aliases,
/// precision, and every policy input that can change a decision.
#[derive(Clone, Copy, Debug)]
pub struct FixtureView<'a, Id, Period> {
    /// Raw provider fixture ID. Repeated or unknown IDs are excluded.
    pub fixture_id: &'a Id,
    /// Raw provider country ID.
    pub country_id: &'a Id,
    /// Optional canonical country ID used for matching context.
    pub canonical_country_id: Option<&'a Id>,
    /// Optional canonical country identity used to build alias-based seeds.
    pub canonical_seed_country_id: Option<&'a Id>,
    /// Raw provider competition ID.
    pub competition_id: &'a Id,
    /// Optional canonical competition ID used for matching context.
    pub canonical_competition_id: Option<&'a Id>,
    /// Optional canonical competition identity used to build alias-based seeds.
    pub canonical_seed_competition_id: Option<&'a Id>,
    /// Home and away provider IDs plus caller-supplied canonical seeds.
    pub teams: [TeamView<'a, Id>; 2],
    /// Normalized country name used to build exact-name seeds.
    pub seed_country_name: Option<&'a str>,
    /// Normalized competition name used to build exact-name seeds.
    pub seed_competition_name: Option<&'a str>,
    /// Kickoff or event timestamp in the caller's chosen unit.
    pub kickoff: i64,
    /// Precision of `kickoff`, interpreted by the caller's policy.
    pub timestamp_precision: TimestampPrecision,
    /// Sport-specific period or phase value.
    pub period: &'a Period,
    /// Optional home and away scores.
    pub score: Option<[i32; 2]>,
    /// Fingerprint of the caller's complete serialized fixture input.
    pub input_fingerprint: Fingerprint,
}

/// Caller-supplied eligibility rules for a pair of fixtures.
///
/// `max_candidate_window` must be at least as large as every value returned by
/// `candidate_window`. Both windows are inclusive and use the same unit as the
/// fixture timestamps. `evidence_strength` owns score, period, precision, and
/// sport-specific time compatibility. Cache callers should pass a stable
/// policy fingerprint to [`resolve_cached`] whenever a policy change can alter
/// a cached decision.
pub trait MatchingPolicy<Id, Period> {
    /// Returns whether an ID is a usable fixture, context, or team identity.
    fn id_is_known(&self, id: &Id) -> bool;

    /// Broad window used to find nearby records in the time index.
    fn max_candidate_window(&self) -> u64;

    /// Inclusive candidate window for this fixture pair.
    fn candidate_window(
        &self,
        left: &FixtureView<'_, Id, Period>,
        right: &FixtureView<'_, Id, Period>,
    ) -> u64;

    /// Returns the strongest evidence for this orientation, if any.
    fn evidence_strength(
        &self,
        left: &FixtureView<'_, Id, Period>,
        right: &FixtureView<'_, Id, Period>,
        reversed: bool,
    ) -> Option<EvidenceStrength>;

    /// Returns whether this pair supplies strict evidence for learning a team
    /// relationship. The default preserves football's existing rule. Callers
    /// with broader matching windows can override it to keep learning stricter.
    fn supports_team_learning(
        &self,
        left: &FixtureView<'_, Id, Period>,
        right: &FixtureView<'_, Id, Period>,
    ) -> bool {
        self.evidence_strength(left, right, false) == Some(EvidenceStrength::ScoreAndTime)
    }

    /// Controls reversed home/away evidence. Football-compatible defaults keep
    /// reversals limited to score-and-time evidence in strict context.
    fn allow_reversed_home_away(
        &self,
        _left: &FixtureView<'_, Id, Period>,
        _right: &FixtureView<'_, Id, Period>,
        scope: MatchScope,
        strength: EvidenceStrength,
    ) -> bool {
        scope == MatchScope::CountryCompetition && strength == EvidenceStrength::ScoreAndTime
    }
}

/// A resolved pair, represented by positions in the input slices.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AcceptedMatch {
    /// Position in the left fixture slice.
    pub left: usize,
    /// Position in the right fixture slice.
    pub right: usize,
    /// Scope that supplied the highest-ranked team links.
    pub scope: MatchScope,
    /// Strength of the matching evidence.
    pub strength: EvidenceStrength,
    /// Whether the right fixture's home and away teams were reversed.
    pub reversed: bool,
}

/// An exact-name match proposed after the relationship-based resolver finishes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactNameMatch {
    /// Position in the left fixture slice.
    pub left: usize,
    /// Position in the right fixture slice.
    pub right: usize,
    /// Whether the right fixture's home and away teams were reversed.
    pub reversed: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ExactNameKey(String, String, i64);

#[derive(Default)]
struct ExactNameCandidate {
    left: Option<(usize, bool)>,
    right: Option<(usize, bool)>,
    ambiguous: bool,
}

/// Folds punctuation and diacritics while retaining team-name qualifiers.
pub fn fold_team_name(value: &str) -> String {
    value
        .nfd()
        .flat_map(char::to_lowercase)
        .filter(|character| !is_combining_mark(*character) && character.is_alphanumeric())
        .collect()
}

fn exact_name_key<Id, Period>(
    fixture: &FixtureView<'_, Id, Period>,
) -> Option<(ExactNameKey, bool)> {
    if fixture.timestamp_precision != TimestampPrecision::Exact {
        return None;
    }
    let home = fold_team_name(fixture.teams[0].match_name?);
    let away = fold_team_name(fixture.teams[1].match_name?);
    if home.is_empty() || away.is_empty() || home == away {
        return None;
    }
    if home <= away {
        Some((ExactNameKey(home, away, fixture.kickoff), false))
    } else {
        Some((ExactNameKey(away, home, fixture.kickoff), true))
    }
}

fn ambiguous_exact_name_candidates<Id, Period>(
    fixtures: &[FixtureView<'_, Id, Period>],
    selected: impl Iterator<Item = usize>,
    time_window: u64,
) -> FxHashSet<usize> {
    let mut selected_by_name = FxHashMap::<String, FxHashMap<String, Vec<(i64, usize)>>>::default();
    let mut selected_index_keys = FxHashMap::<usize, ExactNameKey>::default();
    let mut exact_key_counts = FxHashMap::<ExactNameKey, usize>::default();
    for index in selected {
        let Some((key, _)) = exact_name_key(&fixtures[index]) else {
            continue;
        };
        if selected_index_keys.contains_key(&index) {
            continue;
        }
        selected_by_name
            .entry(key.0.clone())
            .or_default()
            .entry(key.1.clone())
            .or_default()
            .push((key.2, index));
        exact_key_counts.entry(key.clone()).or_default();
        selected_index_keys.insert(index, key);
    }
    for by_away in selected_by_name.values_mut() {
        for rows in by_away.values_mut() {
            rows.sort_unstable();
        }
    }

    let mut ambiguous = FxHashSet::default();
    let time_window = i64::try_from(time_window).unwrap_or(i64::MAX);
    for (index, fixture) in fixtures.iter().enumerate() {
        let Some((key, _)) = exact_name_key(fixture) else {
            continue;
        };
        if let Some(count) = exact_key_counts.get_mut(&key) {
            *count += 1;
        }
        let Some(candidates) = selected_by_name
            .get(&key.0)
            .and_then(|by_away| by_away.get(&key.1))
        else {
            continue;
        };
        let earliest = key.2.saturating_sub(time_window);
        let latest = key.2.saturating_add(time_window);
        let start = candidates.partition_point(|&(kickoff, _)| kickoff < earliest);
        for &(_, selected_index) in candidates[start..]
            .iter()
            .take_while(|&&(kickoff, _)| kickoff <= latest)
        {
            if index != selected_index {
                ambiguous.insert(selected_index);
            }
        }
    }
    for (index, key) in selected_index_keys {
        if exact_key_counts.get(&key).is_some_and(|&count| count > 1) {
            ambiguous.insert(index);
        }
    }
    ambiguous
}

fn unique_fixture_ids<Id: Eq + Hash, Period>(
    fixtures: &[FixtureView<'_, Id, Period>],
    policy: &impl MatchingPolicy<Id, Period>,
) -> Vec<bool> {
    let mut counts = FxHashMap::<&Id, usize>::default();
    for fixture in fixtures {
        *counts.entry(fixture.fixture_id).or_default() += 1;
    }
    fixtures
        .iter()
        .map(|fixture| policy.id_is_known(fixture.fixture_id) && counts[fixture.fixture_id] == 1)
        .collect()
}

/// Adds mutually unique exact-name candidates that agree on exact kickoff and score.
///
/// `existing` contains pairs already accepted by stronger or scoped passes. Such pairs
/// remain occupied and also make a duplicate exact-name candidate ambiguous.
pub fn complete_exact_team_names<Id, Period, Policy>(
    left: &[FixtureView<'_, Id, Period>],
    right: &[FixtureView<'_, Id, Period>],
    existing: &[(usize, usize)],
    policy: &Policy,
    nearby_duplicate_window: u64,
    accepts: impl Fn(usize, usize, bool) -> bool,
) -> Vec<ExactNameMatch>
where
    Id: Eq + Hash + Serialize,
    Period: Eq,
    Policy: MatchingPolicy<Id, Period>,
{
    let left_unique = unique_fixture_ids(left, policy);
    let right_unique = unique_fixture_ids(right, policy);
    let occupied_left: FxHashSet<_> = existing.iter().map(|(left, _)| *left).collect();
    let occupied_right: FxHashSet<_> = existing.iter().map(|(_, right)| *right).collect();
    let mut candidates = FxHashMap::<ExactNameKey, ExactNameCandidate>::default();
    for (index, fixture) in left.iter().enumerate() {
        if occupied_left.contains(&index) || !left_unique[index] {
            continue;
        }
        let Some((key, flipped)) = exact_name_key(fixture) else {
            continue;
        };
        let candidate = candidates.entry(key).or_default();
        candidate.ambiguous |= candidate.left.replace((index, flipped)).is_some();
    }
    for &(left_index, _) in existing {
        if let Some((key, _)) = exact_name_key(&left[left_index]) {
            if let Some(candidate) = candidates.get_mut(&key) {
                candidate.ambiguous = true;
            }
        }
    }
    for (index, fixture) in right.iter().enumerate() {
        if !right_unique[index] {
            continue;
        }
        let Some((key, flipped)) = exact_name_key(fixture) else {
            continue;
        };
        if let Some(candidate) = candidates.get_mut(&key) {
            candidate.ambiguous |= occupied_right.contains(&index)
                || candidate.right.replace((index, flipped)).is_some();
        }
    }
    let mut proposals: Vec<_> = candidates
        .into_values()
        .filter_map(|candidate| {
            (!candidate.ambiguous)
                .then_some(candidate.left.zip(candidate.right))
                .flatten()
        })
        .filter_map(
            |((left_index, left_flipped), (right_index, right_flipped))| {
                let reversed = left_flipped ^ right_flipped;
                (policy.evidence_strength(&left[left_index], &right[right_index], reversed)
                    == Some(EvidenceStrength::ScoreAndTime)
                    && accepts(left_index, right_index, reversed))
                .then_some(ExactNameMatch {
                    left: left_index,
                    right: right_index,
                    reversed,
                })
            },
        )
        .collect();
    if proposals.is_empty() {
        return Vec::new();
    }

    // Exact names must not override contradictory provider entity IDs. Build
    // the target sets from all exact proposals in each source-context pair so
    // one fixture cannot hide a split identity seen in another fixture.
    let mut left_targets =
        FxHashMap::<((String, String, String, String), String), FxHashSet<String>>::default();
    let mut right_targets =
        FxHashMap::<((String, String, String, String), String), FxHashSet<String>>::default();
    for proposal in &proposals {
        let left_fixture = &left[proposal.left];
        let right_fixture = &right[proposal.right];
        let Some(context) = exact_name_context(left_fixture, right_fixture) else {
            continue;
        };
        let mut right_teams = right_fixture.teams;
        if proposal.reversed {
            right_teams.swap(0, 1);
        }
        for (left_team, right_team) in left_fixture.teams.into_iter().zip(right_teams) {
            let left_id = left_team
                .canonical_id
                .filter(|id| policy.id_is_known(id))
                .or_else(|| {
                    policy
                        .id_is_known(left_team.provider_id)
                        .then_some(left_team.provider_id)
                });
            let right_id = right_team
                .canonical_id
                .filter(|id| policy.id_is_known(id))
                .or_else(|| {
                    policy
                        .id_is_known(right_team.provider_id)
                        .then_some(right_team.provider_id)
                });
            let Some((left_id, right_id)) = left_id.zip(right_id) else {
                continue;
            };
            let (Some(left_key), Some(right_key)) = (
                serde_json::to_string(left_id).ok(),
                serde_json::to_string(right_id).ok(),
            ) else {
                continue;
            };
            left_targets
                .entry((context.clone(), left_key.clone()))
                .or_default()
                .insert(right_key.clone());
            right_targets
                .entry((context.clone(), right_key))
                .or_default()
                .insert(left_key);
        }
    }
    proposals.retain(|proposal| {
        let left_fixture = &left[proposal.left];
        let right_fixture = &right[proposal.right];
        let Some(context) = exact_name_context(left_fixture, right_fixture) else {
            return true;
        };
        let mut right_teams = right_fixture.teams;
        if proposal.reversed {
            right_teams.swap(0, 1);
        }
        left_fixture
            .teams
            .into_iter()
            .zip(right_teams)
            .all(|(left_team, right_team)| {
                let left_id = left_team
                    .canonical_id
                    .filter(|id| policy.id_is_known(id))
                    .or_else(|| {
                        policy
                            .id_is_known(left_team.provider_id)
                            .then_some(left_team.provider_id)
                    });
                let right_id = right_team
                    .canonical_id
                    .filter(|id| policy.id_is_known(id))
                    .or_else(|| {
                        policy
                            .id_is_known(right_team.provider_id)
                            .then_some(right_team.provider_id)
                    });
                let Some((left_id, right_id)) = left_id.zip(right_id) else {
                    return true;
                };
                let (Some(left_key), Some(right_key)) = (
                    serde_json::to_string(left_id).ok(),
                    serde_json::to_string(right_id).ok(),
                ) else {
                    return true;
                };
                left_targets
                    .get(&(context.clone(), left_key.clone()))
                    .is_none_or(|targets| targets.len() == 1 && targets.contains(&right_key))
                    && right_targets
                        .get(&(context.clone(), right_key.clone()))
                        .is_none_or(|sources| sources.len() == 1 && sources.contains(&left_key))
            })
    });
    if proposals.is_empty() {
        return Vec::new();
    }

    let ambiguous_left = ambiguous_exact_name_candidates(
        left,
        proposals.iter().map(|candidate| candidate.left),
        nearby_duplicate_window,
    );
    let ambiguous_right = ambiguous_exact_name_candidates(
        right,
        proposals.iter().map(|candidate| candidate.right),
        nearby_duplicate_window,
    );
    let mut matches: Vec<_> = proposals
        .into_iter()
        .filter(|candidate| {
            !ambiguous_left.contains(&candidate.left) && !ambiguous_right.contains(&candidate.right)
        })
        .collect();
    matches.sort_unstable_by_key(|candidate| (candidate.left, candidate.right));
    matches
}

fn exact_name_context<Id, Period>(
    left: &FixtureView<'_, Id, Period>,
    right: &FixtureView<'_, Id, Period>,
) -> Option<(String, String, String, String)>
where
    Id: Serialize,
{
    let dimension = |identity: Option<&Id>, name: Option<&str>| {
        identity
            .and_then(|value| serde_json::to_string(value).ok())
            .or_else(|| name.map(str::to_owned))
    };
    Some((
        dimension(
            left.canonical_seed_country_id.or(left.canonical_country_id),
            left.seed_country_name,
        )?,
        dimension(
            right
                .canonical_seed_country_id
                .or(right.canonical_country_id),
            right.seed_country_name,
        )?,
        dimension(
            left.canonical_seed_competition_id
                .or(left.canonical_competition_id),
            left.seed_competition_name,
        )?,
        dimension(
            right
                .canonical_seed_competition_id
                .or(right.canonical_competition_id),
            right.seed_competition_name,
        )?,
    ))
}

impl AcceptedMatch {
    /// Cache tier used for this match, with stronger evidence and narrower
    /// context scopes ranked first.
    pub fn tier(self) -> u8 {
        self.strength.rank() * SCOPE_COUNT as u8 + self.scope.index() as u8
    }
}

/// Counters useful to callers when reporting matcher behavior.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MappingStats {
    /// Number of candidate pairs assessed after cache lookup.
    pub comparisons: usize,
    /// Number of global learning rounds.
    pub learning_rounds: usize,
    /// Number of valid left fixtures whose previous assessment was retained.
    pub retained_assessments: usize,
    /// Number of assessments loaded from cache.
    pub reused_assessments: usize,
    /// Number of assessments recomputed.
    pub recomputed_assessments: usize,
    /// Number of fixtures involved in a best-tier tie.
    pub ambiguous: usize,
    /// Number of quarantined team relationships.
    pub quarantined: usize,
    /// Number of duplicated or unknown fixture identities.
    pub conflicting_identities: usize,
    /// Number of entries in the right-side team index.
    pub index_entries: usize,
    /// Number of input fixture records.
    pub fixture_records: usize,
    /// Approximate bytes used by the right-side index vectors.
    pub index_bytes: usize,
    /// Largest combined candidate and learning-proposal set observed.
    pub peak_candidate_pairs: usize,
    /// Number of right-side candidates emitted by the team index for assessment.
    pub candidates_examined: u64,
    /// Largest set of distinct right-side candidates emitted for one left fixture.
    pub peak_nearby_candidates: usize,
    /// Largest combined candidate, proposal, and support set retained in one round.
    pub peak_retained_entries: usize,
    /// Number of frozen trusted relations supplied by the caller.
    pub frozen_relations: usize,
}

/// Output from one deterministic fixture resolution pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MappingResult {
    /// Mutually unique best-tier pairs in stable input-index order.
    pub pairs: Vec<AcceptedMatch>,
    /// Run counters.
    pub stats: MappingStats,
}

/// Errors from invalid prepared inputs or serializing deterministic signatures.
#[derive(Debug, thiserror::Error)]
pub enum MappingError {
    /// The prepared fingerprints do not describe the provided fixture views.
    #[error("prepared fingerprints do not match the supplied fixtures")]
    PreparedInputsMismatch,
    /// A typed identity could not be serialized for a stable relationship signature.
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    /// A caller-supplied candidate or retention limit was exceeded.
    #[error("candidate budget exceeded during {stage}: observed={observed}, limit={limit}")]
    CandidateBudgetExceeded {
        /// Enumeration or retention structure that crossed its limit.
        stage: &'static str,
        /// Observed count when the limit was crossed.
        observed: u64,
        /// Configured limit.
        limit: u64,
    },
}

/// Edge retained for one fixture assessment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EvidenceEdge {
    /// Fingerprint of the right fixture.
    pub right_fingerprint: Fingerprint,
    /// Matching tier, or `None` when the edge only supports learning.
    pub tier: Option<u8>,
    /// Whether a candidate match is reversed.
    pub reversed: bool,
    /// Home/away anchor used when this edge proposes a learned relation.
    pub proposal_home_anchor: Option<bool>,
}

/// Cached assessment data exposed for caller-owned persistence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Assessment {
    /// Fingerprint of the left fixture input.
    pub input_fingerprint: Fingerprint,
    /// Ordered candidate and learning edges.
    pub edges: Vec<EvidenceEdge>,
    /// Ordered SHA-256 checksum of `edges`.
    pub checksum: Fingerprint,
}

impl Assessment {
    /// Checks the edge encoding and checksum before accepting persisted data.
    pub fn is_valid(&self) -> bool {
        self.edges.iter().all(|edge| {
            edge.tier.is_none_or(|tier| tier < EVIDENCE_TIER_COUNT)
                && (edge.tier.is_some() || edge.proposal_home_anchor.is_some())
                && (!edge.reversed || edge.tier.is_some())
        }) && evidence_checksum(&self.edges) == self.checksum
    }
}

/// In-memory assessment cache. Database access and row formats stay with callers.
#[derive(Clone, Debug, Default)]
pub struct ProcessingState {
    assessments: FxHashMap<Fingerprint, Assessment>,
    changed: FxHashSet<Fingerprint>,
    used: FxHashSet<Fingerprint>,
}

impl ProcessingState {
    fn get_for_candidates(
        &mut self,
        key: &Fingerprint,
        input_fingerprint: &Fingerprint,
        eligible_right: &FxHashSet<Fingerprint>,
        edge_is_valid: impl Fn(&EvidenceEdge) -> bool,
    ) -> Option<&Assessment> {
        self.used.insert(*key);
        self.assessments.get(key).filter(|saved| {
            saved.is_valid()
                && saved.input_fingerprint == *input_fingerprint
                && saved.edges.iter().all(|edge| {
                    eligible_right.contains(&edge.right_fingerprint) && edge_is_valid(edge)
                })
        })
    }

    /// Stores a newly computed assessment and marks its key for persistence.
    pub fn insert(
        &mut self,
        key: Fingerprint,
        input_fingerprint: Fingerprint,
        edges: Vec<EvidenceEdge>,
    ) {
        self.used.insert(key);
        self.changed.insert(key);
        let checksum = evidence_checksum(&edges);
        self.assessments.insert(
            key,
            Assessment {
                input_fingerprint,
                edges,
                checksum,
            },
        );
    }

    /// Loads a persisted assessment only when its fixed-width evidence is valid.
    pub fn load_assessment(&mut self, key: Fingerprint, assessment: Assessment) -> bool {
        if !assessment.is_valid() {
            return false;
        }
        self.assessments.insert(key, assessment);
        true
    }

    /// Assessment keys changed during the most recent resolution call.
    pub fn changed_keys(&self) -> &FxHashSet<Fingerprint> {
        &self.changed
    }

    /// Assessment keys used during the most recent resolution call.
    pub fn used_keys(&self) -> &FxHashSet<Fingerprint> {
        &self.used
    }

    /// All currently retained assessments, for caller-side cache inspection.
    pub fn assessments(&self) -> &FxHashMap<Fingerprint, Assessment> {
        &self.assessments
    }

    /// Iterates changed assessments in a stable key order for deterministic saves.
    pub fn changed_assessments(&self) -> Vec<(Fingerprint, &Assessment)> {
        let mut keys: Vec<_> = self.changed.iter().copied().collect();
        keys.sort_unstable();
        keys.into_iter()
            .filter_map(|key| self.assessments.get(&key).map(|value| (key, value)))
            .collect()
    }

    fn begin_run(&mut self, valid_right: &FxHashSet<Fingerprint>) {
        self.changed.clear();
        self.used.clear();
        self.assessments.retain(|_, assessment| {
            assessment.is_valid()
                && assessment
                    .edges
                    .iter()
                    .all(|edge| valid_right.contains(&edge.right_fingerprint))
        });
    }
}

/// Fixed-width, ordered edge checksum compatible with the original football cache.
pub fn evidence_checksum(edges: &[EvidenceEdge]) -> Fingerprint {
    let mut hash = Sha256::new();
    hash.update((edges.len() as u64).to_le_bytes());
    for edge in edges {
        hash.update(edge.right_fingerprint);
        hash.update([
            edge.tier.unwrap_or(ABSENT_EDGE_VALUE),
            u8::from(edge.reversed),
            edge.proposal_home_anchor
                .map_or(ABSENT_EDGE_VALUE, u8::from),
        ]);
    }
    hash.finalize().into()
}

/// SHA-256 fingerprint for serialized caller input.
pub fn fingerprint_serializable<T: Serialize>(value: &T) -> Result<Fingerprint, serde_json::Error> {
    let mut bytes = Vec::new();
    serde_json::to_writer(&mut bytes, value)?;
    Ok(fingerprint(&bytes))
}

/// SHA-256 fingerprint for a byte sequence.
pub fn fingerprint(bytes: &[u8]) -> Fingerprint {
    Sha256::digest(bytes).into()
}

/// Lowercase hexadecimal encoding used by the assessment table adapter.
pub fn fingerprint_hex(value: &Fingerprint) -> String {
    let value: sha2::digest::Output<Sha256> = (*value).into();
    format!("{value:x}")
}

/// Parses the fixed-width lowercase or uppercase hexadecimal fingerprint format.
pub fn parse_fingerprint_hex(value: &str) -> Option<Fingerprint> {
    if value.len() != 64 || !value.is_ascii() {
        return None;
    }
    let mut result = [0; 32];
    for (index, byte) in result.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(result)
}

/// Prepared stable fixture fingerprints and right-side lookup indices.
#[derive(Clone, Debug)]
pub struct PreparedInputs {
    left: Arc<[Fingerprint]>,
    right: Arc<[Fingerprint]>,
    right_indices: FxHashMap<Fingerprint, usize>,
}

impl PreparedInputs {
    /// Constructs prepared inputs from caller-computed fingerprints.
    pub fn new(left: impl Into<Arc<[Fingerprint]>>, right: impl Into<Arc<[Fingerprint]>>) -> Self {
        let left: Arc<[Fingerprint]> = left.into();
        let right: Arc<[Fingerprint]> = right.into();
        let right_indices = right
            .iter()
            .enumerate()
            .map(|(index, fingerprint)| (*fingerprint, index))
            .collect();
        Self {
            left,
            right,
            right_indices,
        }
    }

    /// Returns the fingerprints in left input order.
    pub fn left_fingerprints(&self) -> &[Fingerprint] {
        &self.left
    }

    /// Returns the fingerprints in right input order.
    pub fn right_fingerprints(&self) -> &[Fingerprint] {
        &self.right
    }
}

/// Creates prepared fingerprints from the complete serializable caller inputs.
pub fn prepare<T: Serialize>(left: &[T], right: &[T]) -> Result<PreparedInputs, serde_json::Error> {
    let left = fixture_fingerprints(left)?;
    let right = fixture_fingerprints(right)?;
    Ok(PreparedInputs::new(left, right))
}

/// Fingerprints records in their existing serialized field order.
pub fn fixture_fingerprints<T: Serialize>(
    records: &[T],
) -> Result<Vec<Fingerprint>, serde_json::Error> {
    fixture_fingerprints_iter(records)
}

/// Serialize adapter-prepared inputs in iterator order without cloning the rows.
pub fn fixture_fingerprints_iter<T: Serialize>(
    records: impl IntoIterator<Item = T>,
) -> Result<Vec<Fingerprint>, serde_json::Error> {
    let mut bytes = Vec::new();
    records
        .into_iter()
        .map(|record| {
            bytes.clear();
            serde_json::to_writer(&mut bytes, &record)?;
            Ok(fingerprint(&bytes))
        })
        .collect()
}

/// Stream a sorted snapshot into its digest without buffering the serialized rows.
pub fn ordered_snapshot_fingerprint<T: Serialize>(
    rows: &[T],
) -> Result<Fingerprint, serde_json::Error> {
    let mut digest = Sha256::new();
    serde_json::to_writer(&mut digest, &rows)?;
    Ok(digest.finalize().into())
}

/// Fingerprints a provider snapshot as an order-independent multiset.
pub fn fixture_snapshot_fingerprint<T: Serialize>(
    records: &[T],
) -> Result<Fingerprint, serde_json::Error> {
    let mut row_fingerprints = fixture_fingerprints(records)?;
    row_fingerprints.sort_unstable();
    let mut hash = Sha256::new();
    hash.update((row_fingerprints.len() as u64).to_le_bytes());
    for row_fingerprint in row_fingerprints {
        hash.update(row_fingerprint);
    }
    Ok(hash.finalize().into())
}

/// Optional limits on enumeration and ranking memory for one resolution pass.
///
/// The limits are applied where the shared team index emits candidates and
/// where ranking, proposal, and review state is retained. A sparse window with
/// many unrelated timestamp combinations is therefore cheap while a dense
/// neighborhood fails with a measured identity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CandidateBudget {
    /// Maximum right-side candidates emitted across the pass.
    pub max_candidates_examined: Option<u64>,
    /// Maximum combined candidate, proposal, and support entries retained.
    pub max_retained_entries: Option<usize>,
}

/// Caller controls for one deterministic resolution pass.
///
/// Defaults preserve the original shared behavior: local exact-name and
/// canonical seeding are enabled and locally supported relations may be
/// promoted. Registry callers pass a frozen relation set, disable local
/// seeding, and disable promotion during final resolution.
#[derive(Clone, Copy, Debug)]
pub struct ResolverOptions<'a, Id> {
    /// Trusted relations that replace all locally-derived seeds for this pass.
    pub frozen_relations: Option<&'a FxHashSet<Relation<Id>>>,
    /// Whether to derive exact-name and canonical seeds from this batch.
    pub seed_local_relations: bool,
    /// Whether relations supported by this batch may be promoted.
    pub promote_local_relations: bool,
    /// Optional enumeration and retention limits.
    pub candidate_budget: CandidateBudget,
}

impl<Id> Default for ResolverOptions<'_, Id> {
    fn default() -> Self {
        Self {
            frozen_relations: None,
            seed_local_relations: true,
            promote_local_relations: true,
            candidate_budget: CandidateBudget::default(),
        }
    }
}

fn check_budget(
    stage: &'static str,
    observed: u64,
    limit: Option<u64>,
) -> Result<(), MappingError> {
    if let Some(limit) = limit.filter(|limit| observed > *limit) {
        return Err(MappingError::CandidateBudgetExceeded {
            stage,
            observed,
            limit,
        });
    }
    Ok(())
}

/// Resolves all fixture views without reusing prior assessments.
pub fn resolve<Id, Period, Policy>(
    left: &[FixtureView<'_, Id, Period>],
    right: &[FixtureView<'_, Id, Period>],
    policy: &Policy,
) -> Result<MappingResult, MappingError>
where
    Id: Clone + Eq + Hash + Ord + Serialize,
    Policy: MatchingPolicy<Id, Period>,
{
    resolve_with_options(left, right, policy, ResolverOptions::default())
}

/// Resolves all fixture views using caller-supplied relation and budget controls.
pub fn resolve_with_options<Id, Period, Policy>(
    left: &[FixtureView<'_, Id, Period>],
    right: &[FixtureView<'_, Id, Period>],
    policy: &Policy,
    options: ResolverOptions<'_, Id>,
) -> Result<MappingResult, MappingError>
where
    Id: Clone + Eq + Hash + Ord + Serialize,
    Policy: MatchingPolicy<Id, Period>,
{
    let prepared = PreparedInputs::new(
        left.iter()
            .map(|fixture| fixture.input_fingerprint)
            .collect::<Vec<_>>(),
        right
            .iter()
            .map(|fixture| fixture.input_fingerprint)
            .collect::<Vec<_>>(),
    );
    resolve_rounds(left, right, &prepared, policy, None, None, true, options)
}

/// Resolves fixtures with selective assessment reuse and returns changed cache entries
/// through `cache.changed_assessments()`.
///
/// `policy_fingerprint` is included in assessment keys when present. Passing
/// `None` preserves the original football key byte-for-byte, so that caller
/// must include all policy inputs in its existing fixture fingerprint or cache
/// version.
pub fn resolve_cached<Id, Period, Policy>(
    left: &[FixtureView<'_, Id, Period>],
    right: &[FixtureView<'_, Id, Period>],
    prepared: &PreparedInputs,
    policy: &Policy,
    cache: &mut ProcessingState,
    policy_fingerprint: Option<&Fingerprint>,
) -> Result<MappingResult, MappingError>
where
    Id: Clone + Eq + Hash + Ord + Serialize,
    Policy: MatchingPolicy<Id, Period>,
{
    resolve_cached_with_options(
        left,
        right,
        prepared,
        policy,
        cache,
        policy_fingerprint,
        ResolverOptions::default(),
    )
}

/// Resolves fixtures with assessment reuse plus caller-supplied relation and
/// budget controls.
#[allow(clippy::too_many_arguments)]
pub fn resolve_cached_with_options<Id, Period, Policy>(
    left: &[FixtureView<'_, Id, Period>],
    right: &[FixtureView<'_, Id, Period>],
    prepared: &PreparedInputs,
    policy: &Policy,
    cache: &mut ProcessingState,
    policy_fingerprint: Option<&Fingerprint>,
    options: ResolverOptions<'_, Id>,
) -> Result<MappingResult, MappingError>
where
    Id: Clone + Eq + Hash + Ord + Serialize,
    Policy: MatchingPolicy<Id, Period>,
{
    if prepared.left.as_ref() != left_fingerprints(left).as_slice()
        || prepared.right.as_ref() != right_fingerprints(right).as_slice()
    {
        return Err(MappingError::PreparedInputsMismatch);
    }
    let valid_right: FxHashSet<_> = prepared.right.iter().copied().collect();
    cache.begin_run(&valid_right);
    let result = resolve_rounds(
        left,
        right,
        prepared,
        policy,
        Some(cache),
        policy_fingerprint,
        false,
        options,
    )?;
    let used = cache.used.clone();
    cache.assessments.retain(|key, _| used.contains(key));
    Ok(result)
}

fn left_fingerprints<Id, Period>(fixtures: &[FixtureView<'_, Id, Period>]) -> Vec<Fingerprint> {
    fixtures
        .iter()
        .map(|fixture| fixture.input_fingerprint)
        .collect::<Vec<_>>()
}

fn right_fingerprints<Id, Period>(fixtures: &[FixtureView<'_, Id, Period>]) -> Vec<Fingerprint> {
    left_fingerprints(fixtures)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
struct FixtureIndex(usize);

/// Canonical matching context shared by a fixture pair.
///
/// The tuple is `(left country, right country, left competition, right
/// competition)`. Callers building a frozen relation set use the same shape.
pub type Context<Id> = (Id, Id, Id, Id);
/// A trusted provider-team relation with its canonical pair context.
pub type Relation<Id> = (Context<Id>, Id, Id);
type Pair = (usize, usize);
type PairAssessment<Id> = (Option<(u8, AcceptedMatch)>, Option<(Relation<Id>, bool)>);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum SeedKey<Id> {
    Exact(String, String, String),
    CanonicalTeam(Id, Id, Id),
}

type TeamIdentity<Id> = (Id, Id, Id, Option<Id>);
type Names<Id> = FxHashMap<SeedKey<Id>, FxHashSet<TeamIdentity<Id>>>;

fn effective_context<Id, Period, Policy>(
    fixture: &FixtureView<'_, Id, Period>,
    policy: &Policy,
) -> (Id, Id)
where
    Id: Clone,
    Policy: MatchingPolicy<Id, Period>,
{
    let (country, competition) = effective_context_refs(fixture, policy);
    (country.clone(), competition.clone())
}

fn effective_context_refs<'a, Id, Period, Policy>(
    fixture: &FixtureView<'a, Id, Period>,
    policy: &Policy,
) -> (&'a Id, &'a Id)
where
    Policy: MatchingPolicy<Id, Period>,
{
    let country = fixture
        .canonical_country_id
        .filter(|id| policy.id_is_known(id))
        .unwrap_or(fixture.country_id);
    let competition = fixture
        .canonical_competition_id
        .filter(|id| policy.id_is_known(id))
        .unwrap_or(fixture.competition_id);
    (country, competition)
}

fn context<Id, Period, Policy>(
    left: &FixtureView<'_, Id, Period>,
    right: &FixtureView<'_, Id, Period>,
    policy: &Policy,
) -> Context<Id>
where
    Id: Clone,
    Policy: MatchingPolicy<Id, Period>,
{
    let (left_country, left_competition) = effective_context(left, policy);
    let (right_country, right_competition) = effective_context(right, policy);
    (
        left_country,
        right_country,
        left_competition,
        right_competition,
    )
}

fn known_context<Id, Period, Policy>(context: &Context<Id>, policy: &Policy) -> bool
where
    Policy: MatchingPolicy<Id, Period>,
{
    policy.id_is_known(&context.0)
        && policy.id_is_known(&context.1)
        && policy.id_is_known(&context.2)
        && policy.id_is_known(&context.3)
}

fn valid_records<Id, Period, Policy>(
    records: &[FixtureView<'_, Id, Period>],
    stats: &mut MappingStats,
    policy: &Policy,
) -> Vec<bool>
where
    Id: Clone + Eq + Hash,
    Policy: MatchingPolicy<Id, Period>,
{
    let mut counts = FxHashMap::default();
    for fixture in records {
        *counts.entry(fixture.fixture_id).or_insert(0usize) += 1;
    }
    stats.conflicting_identities += counts.values().filter(|count| **count > 1).count();
    records
        .iter()
        .map(|fixture| policy.id_is_known(fixture.fixture_id) && counts[&fixture.fixture_id] == 1)
        .collect()
}

fn canonical_seed_context<'a, Id, Period, Policy>(
    fixture: &'a FixtureView<'_, Id, Period>,
    policy: &Policy,
) -> (Option<&'a Id>, Option<&'a Id>)
where
    Policy: MatchingPolicy<Id, Period>,
{
    let country = fixture
        .canonical_seed_country_id
        .or(fixture.canonical_country_id)
        .filter(|id| policy.id_is_known(id));
    let competition = fixture
        .canonical_seed_competition_id
        .or(fixture.canonical_competition_id)
        .filter(|id| policy.id_is_known(id));
    (country, competition)
}

fn canonical_context_matches<Id, Period, Policy>(
    left: &FixtureView<'_, Id, Period>,
    right: &FixtureView<'_, Id, Period>,
    policy: &Policy,
) -> bool
where
    Id: Eq,
    Policy: MatchingPolicy<Id, Period>,
{
    let (left_country, left_competition) = canonical_seed_context(left, policy);
    let (right_country, right_competition) = canonical_seed_context(right, policy);
    left_country
        .zip(right_country)
        .is_some_and(|(left, right)| left == right)
        && left_competition
            .zip(right_competition)
            .is_some_and(|(left, right)| left == right)
}

fn same_known_identity<Id, Period, Policy>(
    left: Option<&Id>,
    right: Option<&Id>,
    policy: &Policy,
) -> bool
where
    Id: Eq,
    Policy: MatchingPolicy<Id, Period>,
{
    left.zip(right).is_some_and(|(left, right)| {
        policy.id_is_known(left) && policy.id_is_known(right) && left == right
    })
}

fn names<Id, Period, Policy>(
    records: &[FixtureView<'_, Id, Period>],
    valid: &[bool],
    policy: &Policy,
) -> Names<Id>
where
    Id: Clone + Eq + Hash,
    Policy: MatchingPolicy<Id, Period>,
{
    let mut output = Names::default();
    for (_, fixture) in records
        .iter()
        .enumerate()
        .filter(|(index, _)| valid[*index])
    {
        let (country, competition) = effective_context(fixture, policy);
        if !policy.id_is_known(&country) || !policy.id_is_known(&competition) {
            continue;
        }
        for team in fixture.teams {
            if !policy.id_is_known(team.provider_id) {
                continue;
            }
            let identity = (
                country.clone(),
                competition.clone(),
                team.provider_id.clone(),
                team.canonical_id
                    .filter(|id| policy.id_is_known(id))
                    .cloned(),
            );
            if let (Some(country_name), Some(competition_name), Some(team_name)) = (
                fixture.seed_country_name,
                fixture.seed_competition_name,
                team.seed_name,
            ) {
                if !country_name.is_empty() && !competition_name.is_empty() && !team_name.is_empty()
                {
                    output
                        .entry(SeedKey::Exact(
                            country_name.to_owned(),
                            competition_name.to_owned(),
                            team_name.to_owned(),
                        ))
                        .or_default()
                        .insert(identity.clone());
                }
            }
            let (canonical_country, canonical_competition) =
                canonical_seed_context(fixture, policy);
            if let (Some(canonical_country), Some(canonical_competition), Some(canonical_team)) = (
                canonical_country,
                canonical_competition,
                team.canonical_id.filter(|id| policy.id_is_known(id)),
            ) {
                output
                    .entry(SeedKey::CanonicalTeam(
                        canonical_country.clone(),
                        canonical_competition.clone(),
                        canonical_team.clone(),
                    ))
                    .or_default()
                    .insert(identity);
            }
        }
    }
    output
}

fn seeds<Id, Period, Policy>(
    left: &[FixtureView<'_, Id, Period>],
    right: &[FixtureView<'_, Id, Period>],
    left_valid: &[bool],
    right_valid: &[bool],
    policy: &Policy,
) -> FxHashSet<Relation<Id>>
where
    Id: Clone + Eq + Hash,
    Policy: MatchingPolicy<Id, Period>,
{
    let left_names = names(left, left_valid, policy);
    let right_names = names(right, right_valid, policy);
    let mut output = FxHashSet::default();
    for (key, left_identities) in left_names {
        if left_identities.len() != 1 {
            continue;
        }
        let Some(right_identities) = right_names.get(&key).filter(|ids| ids.len() == 1) else {
            continue;
        };
        let Some(left_identity) = left_identities.iter().next() else {
            continue;
        };
        let Some(right_identity) = right_identities.iter().next() else {
            continue;
        };
        if matches!(key, SeedKey::Exact(..))
            && left_identity
                .3
                .as_ref()
                .zip(right_identity.3.as_ref())
                .is_some_and(|(left, right)| left != right)
        {
            continue;
        }
        output.insert((
            (
                left_identity.0.clone(),
                right_identity.0.clone(),
                left_identity.1.clone(),
                right_identity.1.clone(),
            ),
            left_identity.2.clone(),
            right_identity.2.clone(),
        ));
    }
    output
}

fn conflicts<Id: Clone + Eq + Hash>(
    relations: &FxHashSet<Relation<Id>>,
) -> FxHashSet<Relation<Id>> {
    let mut left_links: FxHashMap<_, FxHashSet<_>> = FxHashMap::default();
    let mut right_links: FxHashMap<_, FxHashSet<_>> = FxHashMap::default();
    for (context, left, right) in relations {
        left_links.entry((context, left)).or_default().insert(right);
        right_links
            .entry((context, right))
            .or_default()
            .insert(left);
    }
    relations
        .iter()
        .filter(|(context, left, right)| {
            left_links[&(context, left)].len() > 1 || right_links[&(context, right)].len() > 1
        })
        .cloned()
        .collect()
}

type BlockedTeamIndex<Id> = FxHashMap<Context<Id>, (FxHashSet<Id>, FxHashSet<Id>)>;

fn blocked_team_index<Id: Clone + Eq + Hash>(
    blocked: &FxHashSet<Relation<Id>>,
) -> BlockedTeamIndex<Id> {
    let mut index = BlockedTeamIndex::default();
    for (context, left, right) in blocked {
        let (left_teams, right_teams) = index.entry(context.clone()).or_default();
        left_teams.insert(left.clone());
        right_teams.insert(right.clone());
    }
    index
}

fn blocked_fixture<Id: Eq + Hash>(
    index: &BlockedTeamIndex<Id>,
    context: &Context<Id>,
    left: &FixtureView<'_, Id, impl Sized>,
    right: &FixtureView<'_, Id, impl Sized>,
) -> bool {
    index.get(context).is_some_and(|(left_teams, right_teams)| {
        left_teams.contains(left.teams[0].provider_id)
            || left_teams.contains(left.teams[1].provider_id)
            || right_teams.contains(right.teams[0].provider_id)
            || right_teams.contains(right.teams[1].provider_id)
    })
}

type ScopeSideContext<Id> = (Option<Id>, Option<Id>);
type ScopedRelation<Id> = (Option<(Id, Id)>, Option<(Id, Id)>, Id, Id);
type TrustedScopeIndex<Id> =
    FxHashMap<ScopeSideContext<Id>, FxHashMap<ScopeSideContext<Id>, FxHashMap<Id, Id>>>;

struct Trusted<Id> {
    maps: Vec<TrustedScopeIndex<Id>>,
}

impl<Id> Default for Trusted<Id> {
    fn default() -> Self {
        Self {
            maps: (0..SCOPE_COUNT).map(|_| FxHashMap::default()).collect(),
        }
    }
}

impl<Id> Trusted<Id>
where
    Id: Clone + Eq + Hash + Ord + Serialize,
{
    fn build(relations: &FxHashSet<Relation<Id>>) -> Self {
        let mut relations_by_scope: Vec<FxHashSet<ScopedRelation<Id>>> =
            (0..SCOPE_COUNT).map(|_| FxHashSet::default()).collect();
        for ((left_country, right_country, left_competition, right_competition), left, right) in
            relations
        {
            for scope in 0..SCOPE_COUNT {
                relations_by_scope[scope].insert((
                    (scope == COUNTRY_COMPETITION_SCOPE || scope == COUNTRY_SCOPE)
                        .then(|| (left_country.clone(), right_country.clone())),
                    (scope == COUNTRY_COMPETITION_SCOPE || scope == COMPETITION_SCOPE)
                        .then(|| (left_competition.clone(), right_competition.clone())),
                    left.clone(),
                    right.clone(),
                ));
            }
        }
        let mut output = Self::default();
        for (scope, mut map) in relations_by_scope.into_iter().enumerate() {
            let mut left_links: FxHashMap<_, FxHashSet<_>> = FxHashMap::default();
            let mut right_links: FxHashMap<_, FxHashSet<_>> = FxHashMap::default();
            for (country, competition, left, right) in map.iter() {
                left_links
                    .entry((country.clone(), competition.clone(), left.clone()))
                    .or_default()
                    .insert(right.clone());
                right_links
                    .entry((country.clone(), competition.clone(), right.clone()))
                    .or_default()
                    .insert(left.clone());
            }
            map.retain(|(country, competition, left, right)| {
                left_links[&(country.clone(), competition.clone(), left.clone())].len() == 1
                    && right_links[&(country.clone(), competition.clone(), right.clone())].len()
                        == 1
            });
            for (country, competition, left, right) in map {
                let left_context = (
                    country.as_ref().map(|(left, _)| left.clone()),
                    competition.as_ref().map(|(left, _)| left.clone()),
                );
                let right_context = (
                    country.as_ref().map(|(_, right)| right.clone()),
                    competition.as_ref().map(|(_, right)| right.clone()),
                );
                output.maps[scope]
                    .entry(left_context)
                    .or_default()
                    .entry(right_context)
                    .or_default()
                    .insert(left, right);
            }
        }
        output
    }

    fn signatures(&self) -> Result<FxHashMap<Id, Fingerprint>, serde_json::Error> {
        let mut by_team: FxHashMap<&Id, Vec<_>> = FxHashMap::default();
        for (scope, map) in self.maps.iter().enumerate() {
            for (left_context, by_right_context) in map {
                for (right_context, linked_teams) in by_right_context {
                    for (left, right) in linked_teams {
                        let relation_context = (
                            left_context.0.as_ref().zip(right_context.0.as_ref()),
                            left_context.1.as_ref().zip(right_context.1.as_ref()),
                        );
                        by_team
                            .entry(left)
                            .or_default()
                            .push((scope, (relation_context.0, relation_context.1, left, right)));
                    }
                }
            }
        }
        by_team
            .into_iter()
            .map(|(team, mut relations)| {
                relations.sort_unstable();
                Ok(((*team).clone(), fingerprint_serializable(&relations)?))
            })
            .collect()
    }
}

fn assess<Id, Period, Policy>(
    left_index: usize,
    right_index: usize,
    left: &FixtureView<'_, Id, Period>,
    right: &FixtureView<'_, Id, Period>,
    is_blocked: bool,
    links: &Trusted<Id>,
    policy: &Policy,
) -> PairAssessment<Id>
where
    Id: Clone + Eq + Hash + Ord + Serialize,
    Policy: MatchingPolicy<Id, Period>,
{
    if is_blocked {
        return (None, None);
    }
    let context = context(left, right, policy);
    let scoped_contexts: [ScopeSideContext<Id>; SCOPE_COUNT] = std::array::from_fn(|scope| {
        (
            (scope == COUNTRY_COMPETITION_SCOPE || scope == COUNTRY_SCOPE)
                .then(|| context.0.clone()),
            (scope == COUNTRY_COMPETITION_SCOPE || scope == COMPETITION_SCOPE)
                .then(|| context.2.clone()),
        )
    });
    let scoped_right_contexts: [ScopeSideContext<Id>; SCOPE_COUNT] = std::array::from_fn(|scope| {
        (
            (scope == COUNTRY_COMPETITION_SCOPE || scope == COUNTRY_SCOPE)
                .then(|| context.1.clone()),
            (scope == COUNTRY_COMPETITION_SCOPE || scope == COMPETITION_SCOPE)
                .then(|| context.3.clone()),
        )
    });
    let scoped_maps: [Option<&FxHashMap<Id, Id>>; SCOPE_COUNT] = std::array::from_fn(|scope| {
        links.maps[scope]
            .get(&scoped_contexts[scope])
            .and_then(|right_contexts| right_contexts.get(&scoped_right_contexts[scope]))
    });
    let linked_home: [Option<&Id>; SCOPE_COUNT] = std::array::from_fn(|scope| {
        scoped_maps[scope].and_then(|team_links| team_links.get(left.teams[0].provider_id))
    });
    let linked_away: [Option<&Id>; SCOPE_COUNT] = std::array::from_fn(|scope| {
        scoped_maps[scope].and_then(|team_links| team_links.get(left.teams[1].provider_id))
    });

    let mut candidate: Option<(u8, AcceptedMatch)> = None;
    let mut proposal = None;
    let canonical_scope_matches = canonical_context_matches(left, right, policy);
    for reversed in [false, true] {
        let (right_home, right_away) = if reversed {
            (right.teams[1].provider_id, right.teams[0].provider_id)
        } else {
            (right.teams[0].provider_id, right.teams[1].provider_id)
        };
        for scope_index in 0..SCOPE_COUNT {
            let canonical_scope =
                scope_index == COUNTRY_COMPETITION_SCOPE && canonical_scope_matches;
            let home = linked_home[scope_index].is_some_and(|linked| linked == right_home)
                || (canonical_scope
                    && policy.id_is_known(left.teams[0].provider_id)
                    && policy.id_is_known(right_home)
                    && same_known_identity(
                        left.teams[0].canonical_id,
                        if reversed {
                            right.teams[1].canonical_id
                        } else {
                            right.teams[0].canonical_id
                        },
                        policy,
                    ));
            let away = linked_away[scope_index].is_some_and(|linked| linked == right_away)
                || (canonical_scope
                    && policy.id_is_known(left.teams[1].provider_id)
                    && policy.id_is_known(right_away)
                    && same_known_identity(
                        left.teams[1].canonical_id,
                        if reversed {
                            right.teams[0].canonical_id
                        } else {
                            right.teams[1].canonical_id
                        },
                        policy,
                    ));
            let scope = MatchScope::from_index(scope_index).expect("scope index is bounded");
            if home && away {
                if let Some(strength) = policy.evidence_strength(left, right, reversed) {
                    if !reversed || policy.allow_reversed_home_away(left, right, scope, strength) {
                        let tier = strength.rank() * SCOPE_COUNT as u8 + scope_index as u8;
                        let pair = AcceptedMatch {
                            left: left_index,
                            right: right_index,
                            scope,
                            strength,
                            reversed,
                        };
                        if candidate
                            .as_ref()
                            .is_none_or(|(previous_tier, _)| tier < *previous_tier)
                        {
                            candidate = Some((tier, pair));
                        }
                    }
                }
            }
            if scope_index == COUNTRY_COMPETITION_SCOPE
                && !reversed
                && home != away
                && policy.supports_team_learning(left, right)
                && known_context::<Id, Period, Policy>(&context, policy)
            {
                let (left_id, right_id) = if home {
                    (left.teams[1].provider_id, right.teams[1].provider_id)
                } else {
                    (left.teams[0].provider_id, right.teams[0].provider_id)
                };
                if policy.id_is_known(left_id) && policy.id_is_known(right_id) {
                    proposal = Some(((context.clone(), left_id.clone(), right_id.clone()), home));
                }
            }
            if candidate
                .as_ref()
                .is_some_and(|(tier, _)| *tier == STRICT_CONTEXT_TIER)
            {
                return (candidate, proposal);
            }
        }
    }
    (candidate, proposal)
}

fn unique_best(candidates: &[(Pair, u8)]) -> (Vec<Pair>, usize) {
    let mut left: FxHashMap<usize, (u8, FxHashSet<usize>)> = FxHashMap::default();
    let mut right: FxHashMap<usize, (u8, FxHashSet<usize>)> = FxHashMap::default();
    for &((left_index, right_index), tier) in candidates {
        for (map, key, value) in [
            (&mut left, left_index, right_index),
            (&mut right, right_index, left_index),
        ] {
            let entry = map
                .entry(key)
                .or_insert_with(|| (tier, FxHashSet::default()));
            if tier < entry.0 {
                entry.0 = tier;
                entry.1.clear();
            }
            if tier == entry.0 {
                entry.1.insert(value);
            }
        }
    }
    let ambiguous = left
        .values()
        .chain(right.values())
        .filter(|(_, candidates)| candidates.len() > 1)
        .count();
    let mut accepted = Vec::new();
    for (&left_index, (_, right_candidates)) in &left {
        if right_candidates.len() == 1 {
            let right_index = *right_candidates.iter().next().expect("one candidate");
            if right[&right_index].1.len() == 1 && right[&right_index].1.contains(&left_index) {
                accepted.push((left_index, right_index));
            }
        }
    }
    accepted.sort_unstable();
    (accepted, ambiguous)
}

fn schedule_refresh(fixtures: &[usize], scheduled: &mut [bool], pending: &mut Vec<usize>) {
    for &fixture in fixtures {
        if !scheduled[fixture] {
            scheduled[fixture] = true;
            pending.push(fixture);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_rounds<Id, Period, Policy>(
    left: &[FixtureView<'_, Id, Period>],
    right: &[FixtureView<'_, Id, Period>],
    prepared: &PreparedInputs,
    policy: &Policy,
    mut cache: Option<&mut ProcessingState>,
    policy_fingerprint: Option<&Fingerprint>,
    replay_all: bool,
    options: ResolverOptions<'_, Id>,
) -> Result<MappingResult, MappingError>
where
    Id: Clone + Eq + Hash + Ord + Serialize,
    Policy: MatchingPolicy<Id, Period>,
{
    let mut stats = MappingStats {
        fixture_records: left.len() + right.len(),
        frozen_relations: options.frozen_relations.map_or(0, FxHashSet::len),
        ..MappingStats::default()
    };
    let left_valid = valid_records(left, &mut stats, policy);
    let right_valid = valid_records(right, &mut stats, policy);
    let mut index: FxHashMap<Id, Vec<FixtureIndex>> = FxHashMap::default();
    let mut canonical_index: FxHashMap<Id, Vec<FixtureIndex>> = FxHashMap::default();
    for (right_index, fixture) in right
        .iter()
        .enumerate()
        .filter(|(index, _)| right_valid[*index])
    {
        for team in fixture.teams {
            if policy.id_is_known(team.provider_id) {
                index
                    .entry(team.provider_id.clone())
                    .or_default()
                    .push(FixtureIndex(right_index));
                stats.index_entries += 1;
            }
            if let Some(canonical_id) = team.canonical_id.filter(|id| policy.id_is_known(id)) {
                canonical_index
                    .entry(canonical_id.clone())
                    .or_default()
                    .push(FixtureIndex(right_index));
                stats.index_entries += 1;
            }
        }
    }
    for records in index.values_mut().chain(canonical_index.values_mut()) {
        records.sort_unstable_by_key(|index| (right[index.0].kickoff, right[index.0].fixture_id));
        records.dedup();
    }
    stats.index_bytes = index
        .values()
        .chain(canonical_index.values())
        .map(|records| records.capacity() * std::mem::size_of::<FixtureIndex>())
        .sum();

    let mut trusted = if let Some(frozen) = options.frozen_relations {
        frozen.clone()
    } else if options.seed_local_relations {
        seeds(left, right, &left_valid, &right_valid, policy)
    } else {
        FxHashSet::default()
    };
    let mut blocked = conflicts(&trusted);
    trusted.retain(|relation| !blocked.contains(relation));
    let initial = trusted.clone();

    let mut left_by_team = FxHashMap::<Id, Vec<usize>>::default();
    let mut left_by_context = FxHashMap::<(Id, Id), Vec<usize>>::default();
    let mut pending = Vec::new();
    for (left_index, fixture) in left
        .iter()
        .enumerate()
        .filter(|(index, _)| left_valid[*index])
    {
        pending.push(left_index);
        if !replay_all {
            for team in fixture.teams {
                left_by_team
                    .entry(team.provider_id.clone())
                    .or_default()
                    .push(left_index);
            }
            let (country, competition) = effective_context(fixture, policy);
            left_by_context
                .entry((country, competition))
                .or_default()
                .push(left_index);
        }
    }
    let valid_left_count = pending.len();
    let mut scheduled = left_valid.clone();
    let mut previous_signatures = FxHashMap::default();
    let mut proposals: FxHashMap<Pair, Relation<Id>> = FxHashMap::default();
    let mut candidates: FxHashMap<Pair, (u8, AcceptedMatch)> = FxHashMap::default();
    let mut nearby = FxHashSet::default();
    let mut nearby_ordered = Vec::new();
    let mut candidate_statuses = Vec::new();

    loop {
        stats.learning_rounds += 1;
        let links = Trusted::build(&trusted);
        let blocked_teams = blocked_team_index(&blocked);
        let signatures = links.signatures()?;
        if stats.learning_rounds > 1 {
            if replay_all {
                pending.extend((0..left.len()).filter(|&index| left_valid[index]));
                candidates.clear();
                proposals.clear();
            } else {
                for (team, fixtures) in &left_by_team {
                    if signatures.get(team) != previous_signatures.get(team) {
                        schedule_refresh(fixtures, &mut scheduled, &mut pending);
                    }
                }
                candidates.retain(|(index, _), _| !scheduled[*index]);
                proposals.retain(|(index, _), _| !scheduled[*index]);
            }
        }

        stats.retained_assessments += valid_left_count - pending.len();
        let mut candidate_targets: Vec<(&Id, &ScopeSideContext<Id>)> = Vec::new();
        for left_index in pending.drain(..) {
            scheduled[left_index] = false;
            let fixture = &left[left_index];
            let (left_country, left_competition) = effective_context_refs(fixture, policy);
            let scope_left_contexts: [ScopeSideContext<Id>; SCOPE_COUNT] =
                std::array::from_fn(|scope| {
                    (
                        (scope == COUNTRY_COMPETITION_SCOPE || scope == COUNTRY_SCOPE)
                            .then(|| left_country.clone()),
                        (scope == COUNTRY_COMPETITION_SCOPE || scope == COMPETITION_SCOPE)
                            .then(|| left_competition.clone()),
                    )
                });
            let allowance = policy.max_candidate_window().min(i64::MAX as u64) as i64;
            let lower_kickoff = fixture.kickoff.saturating_sub(allowance);
            let upper_kickoff = fixture.kickoff.saturating_add(allowance);
            candidate_targets.clear();
            for team in fixture.teams {
                for (scope, left_context) in scope_left_contexts.iter().enumerate() {
                    if let Some(right_contexts) = links.maps[scope].get(left_context) {
                        for (right_context, team_links) in right_contexts {
                            if let Some(target) = team_links.get(team.provider_id) {
                                candidate_targets.push((target, right_context));
                            }
                        }
                    }
                }
                if let Some(canonical_id) = team.canonical_id.filter(|id| policy.id_is_known(id)) {
                    if let Some(records) = canonical_index.get(canonical_id) {
                        let lower =
                            records.partition_point(|index| right[index.0].kickoff < lower_kickoff);
                        let upper = records
                            .partition_point(|index| right[index.0].kickoff <= upper_kickoff);
                        for index in &records[lower..upper] {
                            if canonical_context_matches(fixture, &right[index.0], policy) {
                                nearby.insert(index.0);
                            }
                        }
                    }
                }
            }
            candidate_targets.sort_unstable_by(
                |(left_team, left_context), (right_team, right_context)| {
                    left_team
                        .cmp(right_team)
                        .then_with(|| left_context.cmp(right_context))
                },
            );
            candidate_targets.dedup_by(|candidate, previous| {
                candidate.0 == previous.0 && candidate.1 == previous.1
            });
            let mut target_start = 0;
            while target_start < candidate_targets.len() {
                let target = candidate_targets[target_start].0;
                let mut target_end = target_start + 1;
                while target_end < candidate_targets.len()
                    && candidate_targets[target_end].0 == target
                {
                    target_end += 1;
                }
                if let Some(records) = index.get(target) {
                    let lower =
                        records.partition_point(|index| right[index.0].kickoff < lower_kickoff);
                    let upper =
                        records.partition_point(|index| right[index.0].kickoff <= upper_kickoff);
                    for index in &records[lower..upper] {
                        let (right_country, right_competition) =
                            effective_context_refs(&right[index.0], policy);
                        let context_matches = candidate_targets[target_start..target_end]
                            .iter()
                            .any(|(_, context)| {
                                context
                                    .0
                                    .as_ref()
                                    .is_none_or(|country| country == right_country)
                                    && context
                                        .1
                                        .as_ref()
                                        .is_none_or(|competition| competition == right_competition)
                            });
                        if context_matches {
                            nearby.insert(index.0);
                        }
                    }
                }
                target_start = target_end;
            }

            nearby_ordered.clear();
            nearby_ordered.extend(nearby.drain());
            nearby_ordered.sort_unstable_by_key(|index| &prepared.right[*index]);
            stats.candidates_examined = stats
                .candidates_examined
                .saturating_add(nearby_ordered.len() as u64);
            check_budget(
                "team_index_emission",
                stats.candidates_examined,
                options.candidate_budget.max_candidates_examined,
            )?;
            stats.peak_nearby_candidates = stats.peak_nearby_candidates.max(nearby_ordered.len());
            let mut eligible_right = FxHashSet::default();
            candidate_statuses.clear();
            for &right_index in &nearby_ordered {
                let right_fixture = &right[right_index];
                let in_window = fixture.kickoff.abs_diff(right_fixture.kickoff)
                    <= policy.candidate_window(fixture, right_fixture);
                let is_blocked = blocked_fixture(
                    &blocked_teams,
                    &context(fixture, right_fixture, policy),
                    fixture,
                    right_fixture,
                );
                candidate_statuses.push((is_blocked, in_window));
                if in_window && !is_blocked {
                    eligible_right.insert(prepared.right[right_index]);
                }
            }
            let fingerprint = assessment_key(
                &prepared.left[left_index],
                fixture.teams[0].provider_id,
                fixture.teams[1].provider_id,
                &signatures,
                nearby_ordered.iter().zip(&candidate_statuses).map(
                    |(&right_index, &(is_blocked, _))| (&prepared.right[right_index], is_blocked),
                ),
                policy_fingerprint,
            );
            let saved = cache.as_deref_mut().and_then(|state| {
                state.get_for_candidates(
                    &fingerprint,
                    &prepared.left[left_index],
                    &eligible_right,
                    |edge| {
                        if !edge.reversed {
                            return true;
                        }
                        let Some(tier) = edge.tier else {
                            return false;
                        };
                        let Some(&right_index) =
                            prepared.right_indices.get(&edge.right_fingerprint)
                        else {
                            return false;
                        };
                        let Some(strength) = EvidenceStrength::from_rank(tier / SCOPE_COUNT as u8)
                        else {
                            return false;
                        };
                        let Some(scope) =
                            MatchScope::from_index((tier % SCOPE_COUNT as u8) as usize)
                        else {
                            return false;
                        };
                        let right_fixture = &right[right_index];
                        policy.evidence_strength(fixture, right_fixture, true) == Some(strength)
                            && policy.allow_reversed_home_away(
                                fixture,
                                right_fixture,
                                scope,
                                strength,
                            )
                    },
                )
            });
            let mut new_edges = Vec::new();
            let edges = if let Some(saved) = saved {
                stats.reused_assessments += 1;
                &saved.edges
            } else {
                stats.recomputed_assessments += 1;
                for (&right_index, &(is_blocked, in_window)) in
                    nearby_ordered.iter().zip(&candidate_statuses)
                {
                    let right_fixture = &right[right_index];
                    if !in_window {
                        continue;
                    }
                    stats.comparisons += 1;
                    let (candidate, proposal) = assess(
                        left_index,
                        right_index,
                        fixture,
                        right_fixture,
                        is_blocked,
                        &links,
                        policy,
                    );
                    if candidate.is_some() || proposal.is_some() {
                        new_edges.push(EvidenceEdge {
                            right_fingerprint: prepared.right[right_index],
                            tier: candidate.map(|(tier, _)| tier),
                            reversed: candidate.is_some_and(|(_, pair)| pair.reversed),
                            proposal_home_anchor: proposal.map(|(_, home_anchor)| home_anchor),
                        });
                    }
                }
                &new_edges
            };

            for edge in edges {
                let Some(&right_index) = prepared.right_indices.get(&edge.right_fingerprint) else {
                    continue;
                };
                if let Some(tier) = edge.tier {
                    let Some(strength) = EvidenceStrength::from_rank(tier / SCOPE_COUNT as u8)
                    else {
                        continue;
                    };
                    let Some(scope) = MatchScope::from_index((tier % SCOPE_COUNT as u8) as usize)
                    else {
                        continue;
                    };
                    candidates.insert(
                        (left_index, right_index),
                        (
                            tier,
                            AcceptedMatch {
                                left: left_index,
                                right: right_index,
                                scope,
                                strength,
                                reversed: edge.reversed,
                            },
                        ),
                    );
                }
                if let Some(home_anchor) = edge.proposal_home_anchor {
                    let (left_id, right_id) = if home_anchor {
                        (
                            fixture.teams[1].provider_id,
                            right[right_index].teams[1].provider_id,
                        )
                    } else {
                        (
                            fixture.teams[0].provider_id,
                            right[right_index].teams[0].provider_id,
                        )
                    };
                    proposals.insert(
                        (left_index, right_index),
                        (
                            context(fixture, &right[right_index], policy),
                            left_id.clone(),
                            right_id.clone(),
                        ),
                    );
                }
            }
            if saved.is_none() {
                if let Some(state) = cache.as_deref_mut() {
                    state.insert(fingerprint, prepared.left[left_index], new_edges);
                }
            }
        }
        previous_signatures = signatures;

        stats.peak_candidate_pairs = stats
            .peak_candidate_pairs
            .max(candidates.len() + proposals.len());
        let mut strict: Vec<_> = candidates
            .iter()
            .filter(|(_, (tier, _))| *tier == STRICT_CONTEXT_TIER)
            .map(|(&pair, _)| (pair, 0))
            .collect();
        strict.extend(proposals.keys().map(|&pair| (pair, 0)));
        let (unique, _) = unique_best(&strict);
        let mut support: FxHashMap<Relation<Id>, FxHashSet<Pair>> = FxHashMap::default();
        for pair in unique {
            if let Some(relation) = proposals.get(&pair) {
                support.entry(relation.clone()).or_default().insert(pair);
            }
        }
        let retained = candidates
            .len()
            .saturating_add(proposals.len())
            .saturating_add(support.len());
        stats.peak_retained_entries = stats.peak_retained_entries.max(retained);
        check_budget(
            "retained_ranking_state",
            retained as u64,
            options
                .candidate_budget
                .max_retained_entries
                .map(|limit| limit as u64),
        )?;
        let mut observed = trusted.clone();
        observed.extend(support.keys().cloned());
        let bad = conflicts(&observed);
        let mut new_blocks = blocked.clone();
        new_blocks.extend(bad);
        for relation in &observed {
            if blocked_teams.get(&relation.0).is_some_and(|(left, right)| {
                left.contains(&relation.1) || right.contains(&relation.2)
            }) {
                new_blocks.insert(relation.clone());
            }
        }
        if new_blocks != blocked {
            for ((country, _, competition, _), _, _) in new_blocks.difference(&blocked) {
                if let Some(fixtures) = left_by_context.get(&(country.clone(), competition.clone()))
                {
                    schedule_refresh(fixtures, &mut scheduled, &mut pending);
                }
            }
            blocked = new_blocks;
            trusted = initial
                .iter()
                .filter(|relation| !blocked.contains(*relation))
                .cloned()
                .collect();
            continue;
        }
        let mut next = trusted.clone();
        if options.promote_local_relations {
            for (relation, evidence) in support {
                if evidence.len() >= REQUIRED_SUPPORT && !blocked.contains(&relation) {
                    next.insert(relation);
                }
            }
        }
        if next != trusted {
            trusted = next;
            continue;
        }

        let tiers: Vec<_> = candidates
            .iter()
            .map(|(&pair, (tier, _))| (pair, *tier))
            .collect();
        let (pairs, ambiguous) = unique_best(&tiers);
        stats.ambiguous = ambiguous;
        stats.quarantined = blocked.len();
        return Ok(MappingResult {
            pairs: pairs.into_iter().map(|pair| candidates[&pair].1).collect(),
            stats,
        });
    }
}

fn assessment_key<'a, Id: Eq + Hash>(
    input: &Fingerprint,
    home: &Id,
    away: &Id,
    signatures: &FxHashMap<Id, Fingerprint>,
    nearby: impl Iterator<Item = (&'a Fingerprint, bool)>,
    policy_fingerprint: Option<&Fingerprint>,
) -> Fingerprint {
    let mut hash = Sha256::new();
    if MAPPING_OUTPUT_VERSION != LEGACY_MAPPING_OUTPUT_VERSION {
        hash.update([MAPPING_OUTPUT_VERSION]);
    }
    hash.update(input);
    if let Some(policy_fingerprint) = policy_fingerprint {
        hash.update([POLICY_FINGERPRINT_MARKER]);
        hash.update(policy_fingerprint);
    }
    for team in [home, away] {
        hash.update([u8::from(signatures.contains_key(team))]);
        hash.update(signatures.get(team).unwrap_or(&[0; 32]));
    }
    for (record, blocked) in nearby {
        hash.update(record);
        hash.update([u8::from(blocked)]);
    }
    hash.finalize().into()
}

/// Converts a name to the snake-case form used for exact-name seeds.
pub fn normalize_to_snake_case(value: &str) -> String {
    let replaced = value.replace('&', "and");
    let trimmed = replaced.trim();
    let characters: Vec<char> = trimmed.chars().collect();
    let mut result = String::with_capacity(trimmed.len());
    for (index, character) in characters.iter().copied().enumerate() {
        if index > 0 {
            let previous = characters[index - 1];
            let boundary = (character.is_uppercase() && previous.is_lowercase())
                || (character.is_uppercase()
                    && index + 1 < characters.len()
                    && characters[index + 1].is_lowercase()
                    && previous.is_uppercase())
                || (character.is_alphabetic() && previous.is_numeric())
                || (character.is_numeric() && previous.is_alphabetic());
            if boundary && !result.is_empty() && !result.ends_with('_') {
                result.push('_');
            }
        }
        if character.is_alphanumeric() {
            result.push(character.to_ascii_lowercase());
        } else if !result.is_empty() && !result.ends_with('_') {
            result.push('_');
        }
    }
    while result.ends_with('_') {
        result.pop();
    }
    result
}

/// Trims a trailing season year or year range from a normalized competition name.
pub fn trim_year_suffix(value: &str) -> String {
    let parts: Vec<&str> = value.split_whitespace().collect();
    if parts.len() < 2 {
        return value.to_owned();
    }
    let Some(last) = parts.last() else {
        return value.to_owned();
    };
    let is_year = last.len() == 4 && last.chars().all(|character| character.is_ascii_digit());
    let is_year_range = (last.len() == 9 || last.len() == 7 || last.len() == 5) && {
        let sub_parts: Vec<&str> = last.split('/').collect();
        sub_parts.len() == 2
            && (sub_parts[0].len() == 4 || sub_parts[0].len() == 2)
            && sub_parts[0]
                .chars()
                .all(|character| character.is_ascii_digit())
            && (sub_parts[1].len() == 4 || sub_parts[1].len() == 2)
            && sub_parts[1]
                .chars()
                .all(|character| character.is_ascii_digit())
    };
    if is_year || is_year_range {
        parts[..parts.len() - 1].join(" ")
    } else {
        value.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_COUNTRY_NAME: &str = "test_country";
    const TEST_COMPETITION_NAME: &str = "test_league";
    const EXACT_WINDOW: u64 = 60;
    const DATE_ONLY_WINDOW: u64 = 86_400;
    const MAX_SCORE_WINDOW: u64 = 30;
    const TIED_ID: &str = "tied_fixture";

    #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
    enum TestId {
        Number(i32),
        Text(String),
    }

    impl TestId {
        fn number(value: i32) -> Self {
            Self::Number(value)
        }

        fn text(value: impl Into<String>) -> Self {
            Self::Text(value.into())
        }
    }

    #[derive(Clone, Debug, Serialize)]
    struct TestTeam {
        id: TestId,
        canonical_id: Option<TestId>,
        seed_name: String,
    }

    #[derive(Clone, Debug, Serialize)]
    struct TestFixture {
        id: TestId,
        country_id: TestId,
        canonical_country_id: Option<TestId>,
        canonical_seed_country_id: Option<TestId>,
        competition_id: TestId,
        canonical_competition_id: Option<TestId>,
        canonical_seed_competition_id: Option<TestId>,
        teams: [TestTeam; 2],
        kickoff: i64,
        timestamp_precision: TimestampPrecision,
        period: u8,
        score: Option<[i32; 2]>,
    }

    fn fixture(
        id: impl Into<String>,
        source: i32,
        home_id: i32,
        away_id: i32,
        home_name: impl Into<String>,
        away_name: impl Into<String>,
        kickoff: i64,
    ) -> TestFixture {
        TestFixture {
            id: TestId::text(id),
            country_id: TestId::number(source),
            canonical_country_id: None,
            canonical_seed_country_id: None,
            competition_id: TestId::number(source + 100),
            canonical_competition_id: None,
            canonical_seed_competition_id: None,
            teams: [
                TestTeam {
                    id: TestId::number(home_id),
                    canonical_id: None,
                    seed_name: home_name.into(),
                },
                TestTeam {
                    id: TestId::number(away_id),
                    canonical_id: None,
                    seed_name: away_name.into(),
                },
            ],
            kickoff,
            timestamp_precision: TimestampPrecision::Exact,
            period: 1,
            score: Some([2, 1]),
        }
    }

    fn view(fixture: &TestFixture) -> FixtureView<'_, TestId, u8> {
        FixtureView {
            fixture_id: &fixture.id,
            country_id: &fixture.country_id,
            canonical_country_id: fixture.canonical_country_id.as_ref(),
            canonical_seed_country_id: fixture.canonical_seed_country_id.as_ref(),
            competition_id: &fixture.competition_id,
            canonical_competition_id: fixture.canonical_competition_id.as_ref(),
            canonical_seed_competition_id: fixture.canonical_seed_competition_id.as_ref(),
            teams: [
                TeamView {
                    provider_id: &fixture.teams[0].id,
                    canonical_id: fixture.teams[0].canonical_id.as_ref(),
                    match_name: (!fixture.teams[0].seed_name.is_empty())
                        .then_some(fixture.teams[0].seed_name.as_str()),
                    seed_name: (!fixture.teams[0].seed_name.is_empty())
                        .then_some(fixture.teams[0].seed_name.as_str()),
                },
                TeamView {
                    provider_id: &fixture.teams[1].id,
                    canonical_id: fixture.teams[1].canonical_id.as_ref(),
                    match_name: (!fixture.teams[1].seed_name.is_empty())
                        .then_some(fixture.teams[1].seed_name.as_str()),
                    seed_name: (!fixture.teams[1].seed_name.is_empty())
                        .then_some(fixture.teams[1].seed_name.as_str()),
                },
            ],
            seed_country_name: Some(TEST_COUNTRY_NAME),
            seed_competition_name: Some(TEST_COMPETITION_NAME),
            kickoff: fixture.kickoff,
            timestamp_precision: fixture.timestamp_precision,
            period: &fixture.period,
            score: fixture.score,
            input_fingerprint: fingerprint_serializable(fixture).expect("fixture serializes"),
        }
    }

    fn views(fixtures: &[TestFixture]) -> Vec<FixtureView<'_, TestId, u8>> {
        fixtures.iter().map(view).collect()
    }

    fn reference_ambiguous_exact_name_candidates(
        fixtures: &[FixtureView<'_, TestId, u8>],
        selected: impl Iterator<Item = usize>,
        time_window: u64,
    ) -> FxHashSet<usize> {
        let mut selected_by_name = FxHashMap::<(String, String), Vec<(i64, usize)>>::default();
        let mut selected_indices = FxHashSet::default();
        for index in selected {
            let fixture = &fixtures[index];
            let Some((key, _)) = exact_name_key(fixture) else {
                continue;
            };
            selected_indices.insert(index);
            selected_by_name
                .entry((key.0, key.1))
                .or_default()
                .push((fixture.kickoff, index));
        }
        for rows in selected_by_name.values_mut() {
            rows.sort_unstable();
        }

        let mut ambiguous = FxHashSet::default();
        let time_window = i64::try_from(time_window).unwrap_or(i64::MAX);
        for (index, fixture) in fixtures.iter().enumerate() {
            let Some((key, _)) = exact_name_key(fixture) else {
                continue;
            };
            let Some(candidates) = selected_by_name.get(&(key.0, key.1)) else {
                continue;
            };
            let earliest = fixture.kickoff.saturating_sub(time_window);
            let latest = fixture.kickoff.saturating_add(time_window);
            let start = candidates.partition_point(|&(kickoff, _)| kickoff < earliest);
            for &(_, selected_index) in candidates[start..]
                .iter()
                .take_while(|&&(kickoff, _)| kickoff <= latest)
            {
                if index != selected_index {
                    ambiguous.insert(selected_index);
                }
            }
        }
        for index in selected_indices {
            if let Some((key, _)) = exact_name_key(&fixtures[index]) {
                if fixtures.iter().enumerate().any(|(other, fixture)| {
                    other != index
                        && exact_name_key(fixture).is_some_and(|(candidate, _)| candidate == key)
                }) {
                    ambiguous.insert(index);
                }
            }
        }
        ambiguous
    }

    #[derive(Clone, Copy)]
    struct TestPolicy;

    impl MatchingPolicy<TestId, u8> for TestPolicy {
        fn id_is_known(&self, id: &TestId) -> bool {
            match id {
                TestId::Number(value) => *value > 0,
                TestId::Text(value) => !value.trim().is_empty() && value != "0",
            }
        }

        fn max_candidate_window(&self) -> u64 {
            DATE_ONLY_WINDOW
        }

        fn candidate_window(
            &self,
            left: &FixtureView<'_, TestId, u8>,
            right: &FixtureView<'_, TestId, u8>,
        ) -> u64 {
            if left.timestamp_precision == TimestampPrecision::DateOnly
                || right.timestamp_precision == TimestampPrecision::DateOnly
            {
                DATE_ONLY_WINDOW
            } else {
                EXACT_WINDOW
            }
        }

        fn evidence_strength(
            &self,
            left: &FixtureView<'_, TestId, u8>,
            right: &FixtureView<'_, TestId, u8>,
            reversed: bool,
        ) -> Option<EvidenceStrength> {
            if left.period != right.period {
                return None;
            }
            let date_only = left.timestamp_precision == TimestampPrecision::DateOnly
                || right.timestamp_precision == TimestampPrecision::DateOnly;
            let time_window = if date_only {
                DATE_ONLY_WINDOW
            } else {
                EXACT_WINDOW
            };
            let score_time_window = if date_only { DATE_ONLY_WINDOW } else { 5 };
            let delta = left.kickoff.abs_diff(right.kickoff);
            let scores_match = left.score.zip(right.score).is_some_and(|(left, right)| {
                if reversed {
                    left[0] == right[1] && left[1] == right[0]
                } else {
                    left == right
                }
            });
            if scores_match && delta <= score_time_window {
                Some(EvidenceStrength::ScoreAndTime)
            } else if !reversed && delta <= time_window {
                Some(EvidenceStrength::Time)
            } else if !reversed && scores_match && delta <= MAX_SCORE_WINDOW {
                Some(EvidenceStrength::Score)
            } else {
                None
            }
        }
    }

    struct RejectReversedPolicy;

    impl MatchingPolicy<TestId, u8> for RejectReversedPolicy {
        fn id_is_known(&self, id: &TestId) -> bool {
            TestPolicy.id_is_known(id)
        }

        fn max_candidate_window(&self) -> u64 {
            TestPolicy.max_candidate_window()
        }

        fn candidate_window(
            &self,
            left: &FixtureView<'_, TestId, u8>,
            right: &FixtureView<'_, TestId, u8>,
        ) -> u64 {
            TestPolicy.candidate_window(left, right)
        }

        fn evidence_strength(
            &self,
            left: &FixtureView<'_, TestId, u8>,
            right: &FixtureView<'_, TestId, u8>,
            reversed: bool,
        ) -> Option<EvidenceStrength> {
            TestPolicy.evidence_strength(left, right, reversed)
        }

        fn allow_reversed_home_away(
            &self,
            _left: &FixtureView<'_, TestId, u8>,
            _right: &FixtureView<'_, TestId, u8>,
            _scope: MatchScope,
            _strength: EvidenceStrength,
        ) -> bool {
            false
        }
    }

    #[test]
    fn test_duplicate_fixture_ids_are_excluded() {
        let first = fixture("duplicate", 1, 10, 11, "home", "away", 0);
        let mut duplicate = fixture("duplicate", 1, 10, 11, "home", "away", 0);
        duplicate.teams[0].id = TestId::number(10);
        let right = fixture("right", 2, 20, 21, "home", "away", 0);
        let left_records = [first, duplicate];
        let right_records = [right];
        let left = views(&left_records);
        let right = views(&right_records);

        let result = resolve(&left, &right, &TestPolicy).expect("resolution succeeds");

        assert!(result.pairs.is_empty());
        assert_eq!(result.stats.conflicting_identities, 1);
    }

    #[test]
    fn test_mutual_best_tier_tie_remains_ambiguous() {
        let left_fixture = fixture("left", 1, 10, 11, "home", "away", 0);
        let right_first = fixture("right_a", 2, 20, 21, "home", "away", 0);
        let mut right_second = fixture(TIED_ID, 2, 20, 21, "home", "away", 0);
        right_second.id = TestId::text(TIED_ID);
        let left_records = [left_fixture];
        let right_records = [right_first, right_second];
        let left = views(&left_records);
        let right = views(&right_records);

        let result = resolve(&left, &right, &TestPolicy).expect("resolution succeeds");

        assert!(result.pairs.is_empty());
        assert!(result.stats.ambiguous > 0);
    }

    #[test]
    fn test_duplicate_candidate_edges_do_not_create_false_ambiguity() {
        let (pairs, ambiguous) = unique_best(&[((0, 0), 0), ((0, 0), 0), ((1, 0), 1)]);
        assert_eq!(pairs, [(0, 0)]);
        assert_eq!(ambiguous, 0);
    }

    #[test]
    fn test_conflicting_seed_links_are_quarantined() {
        let left = vec![
            fixture("left_a", 1, 10, 11, "shared", "away_a", 0),
            fixture("left_b", 1, 10, 12, "shared", "away_b", 100),
        ];
        let right = vec![
            fixture("right_a", 2, 20, 21, "shared", "away_a", 0),
            fixture("right_b", 2, 22, 23, "shared", "away_b", 100),
        ];
        let left = views(&left);
        let right = views(&right);

        let result = resolve(&left, &right, &TestPolicy).expect("resolution succeeds");

        assert!(result.pairs.is_empty());
        assert!(result.stats.quarantined > 0);
    }

    #[test]
    fn test_reversed_home_away_evidence_is_preserved() {
        let left_fixture = fixture("left", 1, 10, 11, "home", "away", 0);
        let mut right_fixture = fixture("right", 2, 21, 20, "away", "home", 0);
        right_fixture.score = Some([1, 2]);
        let left_records = [left_fixture];
        let right_records = [right_fixture];
        let left = views(&left_records);
        let right = views(&right_records);

        let result = resolve(&left, &right, &TestPolicy).expect("resolution succeeds");

        assert_eq!(result.pairs.len(), 1);
        assert!(result.pairs[0].reversed);
        assert_eq!(result.pairs[0].scope, MatchScope::CountryCompetition);
    }

    #[test]
    fn test_strict_learning_requires_three_unique_anchors() {
        let left: Vec<_> = (0..3)
            .map(|index| {
                fixture(
                    format!("left_{index}"),
                    1,
                    10 + index,
                    20,
                    format!("anchor_{index}"),
                    format!("left_team_{index}"),
                    i64::from(index) * 100,
                )
            })
            .collect();
        let right: Vec<_> = (0..3)
            .map(|index| {
                fixture(
                    format!("right_{index}"),
                    2,
                    30 + index,
                    40,
                    format!("anchor_{index}"),
                    format!("right_team_{index}"),
                    i64::from(index) * 100,
                )
            })
            .collect();
        let left = views(&left);
        let right = views(&right);

        let result = resolve(&left, &right, &TestPolicy).expect("resolution succeeds");

        assert_eq!(result.pairs.len(), 3);
        assert!(result.stats.learning_rounds >= 2);
    }

    #[test]
    fn test_canonical_team_ids_match_when_provider_seed_ids_are_ambiguous() {
        let mut historical = fixture(
            "historical",
            1,
            10,
            11,
            "liberty",
            "fever",
            -(DATE_ONLY_WINDOW as i64) * 3,
        );
        let mut current = fixture("current", 1, 20, 21, "liberty", "fever", 0);
        let mut target = fixture("target", 2, 30, 31, "liberty", "fever", 0);

        for record in [&mut historical, &mut current, &mut target] {
            record.canonical_seed_country_id = Some(TestId::text("canonical_us"));
            record.canonical_seed_competition_id = Some(TestId::text("canonical_wnba"));
            record.teams[0].canonical_id = Some(TestId::text("liberty"));
            record.teams[1].canonical_id = Some(TestId::text("fever"));
            for team in &mut record.teams {
                team.seed_name.clear();
            }
        }

        let left_records = [historical, current];
        let right_records = [target];
        let left = views(&left_records);
        let right = views(&right_records);
        let result = resolve(&left, &right, &TestPolicy).expect("canonical match resolves");

        assert_eq!(result.pairs.len(), 1);
        assert_eq!(result.pairs[0].left, 1);
        assert_eq!(result.pairs[0].right, 0);
    }

    #[test]
    fn test_cached_reversed_edge_rechecks_active_policy() {
        let left_records = [fixture("left", 1, 10, 11, "home", "away", 0)];
        let mut right_fixture = fixture("right", 2, 20, 21, "away", "home", 0);
        right_fixture.score = Some([1, 2]);
        let right_records = [right_fixture];
        let left = views(&left_records);
        let right = views(&right_records);
        let prepared = PreparedInputs::new(
            left.iter()
                .map(|fixture| fixture.input_fingerprint)
                .collect::<Vec<_>>(),
            right
                .iter()
                .map(|fixture| fixture.input_fingerprint)
                .collect::<Vec<_>>(),
        );
        let mut state = ProcessingState::default();
        let initial = resolve_cached(&left, &right, &prepared, &TestPolicy, &mut state, None)
            .expect("initial reversed match resolves");
        assert_eq!(initial.pairs.len(), 1);
        assert!(initial.pairs[0].reversed);

        let rejected = resolve_cached(
            &left,
            &right,
            &prepared,
            &RejectReversedPolicy,
            &mut state,
            None,
        )
        .expect("changed reversal policy resolves");
        let replay = resolve(&left, &right, &RejectReversedPolicy)
            .expect("full replay with changed policy resolves");

        assert!(rejected.pairs.is_empty());
        assert_eq!(rejected.pairs, replay.pairs);
        assert_eq!(rejected.stats.reused_assessments, 0);
        assert!(rejected.stats.recomputed_assessments > 0);
    }

    #[test]
    fn test_canonical_seed_identities_work_without_provider_names() {
        let mut left_fixture = fixture("left", 1, 10, 11, "", "", 0);
        let mut right_fixture = fixture("right", 2, 20, 21, "", "", 0);
        for fixture in [&mut left_fixture, &mut right_fixture] {
            fixture.canonical_country_id = Some(TestId::text("country_key"));
            fixture.canonical_competition_id = Some(TestId::text("league_key"));
            fixture.teams[0].canonical_id = Some(TestId::text("home_key"));
            fixture.teams[1].canonical_id = Some(TestId::text("away_key"));
        }
        let left_records = [left_fixture];
        let right_records = [right_fixture];
        let left = views(&left_records);
        let right = views(&right_records);

        let result = resolve(&left, &right, &TestPolicy).expect("resolution succeeds");

        assert_eq!(result.pairs.len(), 1);
    }

    #[test]
    fn exact_name_completion_requires_unique_exact_score_and_time() {
        let left_fixture = fixture("left", 1, 1, 2, "Café United", "City U21", 1_000);
        let mut right_fixture = fixture("right", 2, 3, 4, "City U21", "Cafe United", 1_000);
        right_fixture.score = Some([1, 2]);
        let left = [view(&left_fixture)];
        let right = [view(&right_fixture)];
        let matches =
            complete_exact_team_names(&left, &right, &[], &TestPolicy, EXACT_WINDOW, |_, _, _| {
                true
            });
        assert_eq!(
            matches,
            [ExactNameMatch {
                left: 0,
                right: 0,
                reversed: true,
            }]
        );

        let nearby_duplicate = fixture("duplicate", 2, 5, 6, "City U21", "Cafe United", 1_020);
        let ambiguous_right = [view(&right_fixture), view(&nearby_duplicate)];
        assert!(complete_exact_team_names(
            &left,
            &ambiguous_right,
            &[],
            &TestPolicy,
            EXACT_WINDOW,
            |_, _, _| true,
        )
        .is_empty());

        let mut date_only_left = left_fixture;
        date_only_left.timestamp_precision = TimestampPrecision::DateOnly;
        assert!(complete_exact_team_names(
            &[view(&date_only_left)],
            &right,
            &[],
            &TestPolicy,
            EXACT_WINDOW,
            |_, _, _| true,
        )
        .is_empty());
    }

    #[test]
    fn test_alias_conflict_index_is_scoped_and_bidirectional() {
        let index = AliasConflictIndex::new([
            ("league_a", "left_a", "right_a"),
            ("league_a", "left_a", "right_b"),
            ("league_a", "left_b", "right_a"),
            ("league_b", "left_a", "right_b"),
            ("league_b", "left_a", "right_b"),
        ]);

        assert!(index.conflicts(&"league_a", &"left_a", &"right_a"));
        assert!(index.conflicts(&"league_a", &"left_b", &"right_a"));
        assert!(!index.conflicts(&"league_b", &"left_a", &"right_b"));
        assert!(index.conflicts(&"league_b", &"left_a", &"right_a"));
    }

    #[test]
    fn test_optimized_exact_name_ambiguity_matches_reference() {
        let mut fixtures = vec![
            fixture("selected", 1, 10, 11, "Home", "Away", 1_000),
            fixture("near_boundary", 1, 12, 13, "Away", "Home", 1_060),
            fixture("exact_duplicate", 1, 14, 15, "Home", "Away", 1_000),
            fixture("same_names_far_away", 1, 16, 17, "Home", "Away", 10_000),
            fixture("different_pair", 1, 18, 19, "Home", "Else", 1_000),
            fixture("date_only", 1, 20, 21, "Home", "Away", 1_000),
        ];
        fixtures[5].timestamp_precision = TimestampPrecision::DateOnly;
        let fixtures = views(&fixtures);
        let selected = [0, 0, 3, 4, 5];

        let expected = reference_ambiguous_exact_name_candidates(
            &fixtures,
            selected.iter().copied(),
            EXACT_WINDOW,
        );
        let actual = ambiguous_exact_name_candidates(&fixtures, selected.into_iter(), EXACT_WINDOW);

        assert_eq!(actual, expected);
    }

    #[test]
    fn exact_name_completion_rejects_cross_fixture_identity_splits_per_context() {
        let mut left = vec![
            fixture("left_a", 1, 10, 11, "Alpha", "Old Town", 1_000),
            fixture("left_b", 1, 10, 12, "Alpha", "New Town", 2_000),
            fixture("left_other_league", 1, 10, 13, "Alpha", "Metro", 3_000),
        ];
        let mut right = vec![
            fixture("right_a", 2, 20, 21, "Alpha", "Old Town", 1_000),
            fixture("right_b", 2, 22, 23, "Alpha", "New Town", 2_000),
            fixture("right_other_league", 2, 24, 25, "Alpha", "Metro", 3_000),
        ];
        for fixture in left.iter_mut().take(2).chain(right.iter_mut().take(2)) {
            fixture.canonical_competition_id = Some(TestId::text("league_a"));
        }
        left[2].canonical_competition_id = Some(TestId::text("league_b"));
        right[2].canonical_competition_id = Some(TestId::text("league_b"));

        let left = views(&left);
        let right = views(&right);
        let matches =
            complete_exact_team_names(&left, &right, &[], &TestPolicy, EXACT_WINDOW, |_, _, _| {
                true
            });

        // The shared provider home-team ID maps to two IDs in league A, so
        // those proposals are unsafe. The same IDs may map independently in
        // league B because its country/competition context is separate.
        assert_eq!(
            matches,
            [ExactNameMatch {
                left: 2,
                right: 2,
                reversed: false,
            }]
        );
    }

    #[test]
    fn test_date_only_and_exact_timestamps_use_caller_policy() {
        let left_exact = fixture("left_exact", 1, 10, 11, "home", "away", 0);
        let mut right_exact = fixture("right_exact", 2, 20, 21, "home", "away", 3_600);
        right_exact.score = None;
        let mut left_date = fixture("left_date", 1, 10, 11, "home", "away", 0);
        let mut right_date = fixture("right_date", 2, 20, 21, "home", "away", 3_600);
        left_date.score = None;
        right_date.score = None;
        left_date.timestamp_precision = TimestampPrecision::DateOnly;
        right_date.timestamp_precision = TimestampPrecision::DateOnly;
        let exact_left_records = [left_exact];
        let exact_right_records = [right_exact];
        let date_left_records = [left_date];
        let date_right_records = [right_date];
        let exact_left = views(&exact_left_records);
        let exact_right = views(&exact_right_records);
        let date_left = views(&date_left_records);
        let date_right = views(&date_right_records);

        let exact = resolve(&exact_left, &exact_right, &TestPolicy).expect("exact resolves");
        let date = resolve(&date_left, &date_right, &TestPolicy).expect("date-only resolves");

        assert!(exact.pairs.is_empty());
        assert_eq!(date.pairs.len(), 1);

        let mut boundary_left = fixture("boundary_left", 1, 10, 11, "home", "away", 0);
        let mut boundary_right = fixture(
            "boundary_right",
            2,
            20,
            21,
            "home",
            "away",
            EXACT_WINDOW as i64,
        );
        boundary_left.score = None;
        boundary_right.score = None;
        let boundary_left_records = [boundary_left];
        let boundary_right_records = [boundary_right];
        let boundary_left = views(&boundary_left_records);
        let boundary_right = views(&boundary_right_records);
        assert_eq!(
            resolve(&boundary_left, &boundary_right, &TestPolicy)
                .expect("inclusive boundary resolves")
                .pairs
                .len(),
            1
        );
    }

    #[test]
    fn test_cache_reuse_and_selective_reassessment_match_full_replay() {
        let left_records = vec![fixture("left", 1, 10, 11, "home", "away", 0)];
        let right_records = vec![fixture("right", 2, 20, 21, "home", "away", 0)];
        let left = views(&left_records);
        let right = views(&right_records);
        let prepared = PreparedInputs::new(
            left.iter()
                .map(|fixture| fixture.input_fingerprint)
                .collect::<Vec<_>>(),
            right
                .iter()
                .map(|fixture| fixture.input_fingerprint)
                .collect::<Vec<_>>(),
        );
        let policy_fingerprint = fingerprint(b"policy-v1");
        let mut state = ProcessingState::default();
        let cold = resolve_cached(
            &left,
            &right,
            &prepared,
            &TestPolicy,
            &mut state,
            Some(&policy_fingerprint),
        )
        .expect("cold resolution succeeds");
        let warm = resolve_cached(
            &left,
            &right,
            &prepared,
            &TestPolicy,
            &mut state,
            Some(&policy_fingerprint),
        )
        .expect("warm resolution succeeds");
        let full = resolve(&left, &right, &TestPolicy).expect("full replay succeeds");

        assert!(!cold.pairs.is_empty());
        assert_eq!(warm.pairs, full.pairs);
        assert!(warm.stats.reused_assessments > 0);

        let changed_policy = fingerprint(b"policy-v2");
        let refreshed = resolve_cached(
            &left,
            &right,
            &prepared,
            &TestPolicy,
            &mut state,
            Some(&changed_policy),
        )
        .expect("policy change resolution succeeds");
        assert_eq!(refreshed.pairs, full.pairs);
        assert!(refreshed.stats.recomputed_assessments > 0);
        assert!(!state.changed_assessments().is_empty());

        let mut corrected_right_records = right_records;
        corrected_right_records[0].kickoff = 1_000;
        let corrected_right = views(&corrected_right_records);
        let corrected_prepared = PreparedInputs::new(
            left.iter()
                .map(|fixture| fixture.input_fingerprint)
                .collect::<Vec<_>>(),
            corrected_right
                .iter()
                .map(|fixture| fixture.input_fingerprint)
                .collect::<Vec<_>>(),
        );
        let selective = resolve_cached(
            &left,
            &corrected_right,
            &corrected_prepared,
            &TestPolicy,
            &mut state,
            Some(&policy_fingerprint),
        )
        .expect("corrected resolution succeeds");
        let full =
            resolve(&left, &corrected_right, &TestPolicy).expect("corrected replay succeeds");
        assert_eq!(selective.pairs, full.pairs);
        assert!(selective.stats.recomputed_assessments > 0);
    }

    #[test]
    fn test_cached_edges_must_still_point_to_eligible_fixtures() {
        let left_records = [fixture("left", 1, 10, 11, "home", "away", 0)];
        let right_records = [
            fixture("right", 2, 20, 21, "home", "away", 0),
            fixture("unrelated", 2, 30, 31, "other_home", "other_away", 0),
        ];
        let left = views(&left_records);
        let right = views(&right_records);
        let prepared = PreparedInputs::new(
            left.iter()
                .map(|fixture| fixture.input_fingerprint)
                .collect::<Vec<_>>(),
            right
                .iter()
                .map(|fixture| fixture.input_fingerprint)
                .collect::<Vec<_>>(),
        );
        let mut state = ProcessingState::default();
        let first = resolve_cached(&left, &right, &prepared, &TestPolicy, &mut state, None)
            .expect("initial resolution succeeds");
        assert_eq!(first.pairs.len(), 1);

        let key = *state
            .changed_keys()
            .iter()
            .next()
            .expect("assessment saved");
        let assessment = state.assessments.get_mut(&key).expect("assessment exists");
        assert_eq!(assessment.edges.len(), 1);
        assessment.edges[0].right_fingerprint = right[1].input_fingerprint;
        assessment.checksum = evidence_checksum(&assessment.edges);

        let checked = resolve_cached(&left, &right, &prepared, &TestPolicy, &mut state, None)
            .expect("stale edge is reassessed");
        let full = resolve(&left, &right, &TestPolicy).expect("full replay succeeds");

        assert_eq!(checked.pairs, full.pairs);
        assert_eq!(checked.stats.reused_assessments, 0);
        assert!(checked.stats.recomputed_assessments > 0);
    }

    #[test]
    fn test_legacy_assessment_key_and_ordered_edge_checksum_are_stable() {
        let input = [1; 32];
        let home = TestId::number(1);
        let away = TestId::number(2);
        let signatures = FxHashMap::from_iter([(home.clone(), [3; 32])]);
        let nearby = [([4; 32], false), ([5; 32], true)];
        let key = assessment_key(
            &input,
            &home,
            &away,
            &signatures,
            nearby
                .iter()
                .map(|(fingerprint, blocked)| (fingerprint, *blocked)),
            None,
        );
        let mut legacy_key = Sha256::new();
        legacy_key.update(input);
        for team in [&home, &away] {
            legacy_key.update([u8::from(signatures.contains_key(team))]);
            legacy_key.update(signatures.get(team).unwrap_or(&[0; 32]));
        }
        for (fingerprint, blocked) in &nearby {
            legacy_key.update(fingerprint);
            legacy_key.update([u8::from(*blocked)]);
        }
        let legacy_key: Fingerprint = legacy_key.finalize().into();
        assert_eq!(key, legacy_key);

        let edges = [
            EvidenceEdge {
                right_fingerprint: [6; 32],
                tier: Some(2),
                reversed: false,
                proposal_home_anchor: None,
            },
            EvidenceEdge {
                right_fingerprint: [7; 32],
                tier: None,
                reversed: false,
                proposal_home_anchor: Some(true),
            },
        ];
        let mut ordered_checksum = Sha256::new();
        ordered_checksum.update(2_u64.to_le_bytes());
        ordered_checksum.update([6; 32]);
        ordered_checksum.update([2, 0, ABSENT_EDGE_VALUE]);
        ordered_checksum.update([7; 32]);
        ordered_checksum.update([ABSENT_EDGE_VALUE, 0, 1]);
        let expected_checksum: Fingerprint = ordered_checksum.finalize().into();
        assert_eq!(evidence_checksum(&edges), expected_checksum);
        assert_ne!(
            evidence_checksum(&edges),
            evidence_checksum(&[edges[1], edges[0]])
        );

        let reversed_nonzero_tier = Assessment {
            input_fingerprint: input,
            edges: vec![EvidenceEdge {
                right_fingerprint: [9; 32],
                tier: Some(4),
                reversed: true,
                proposal_home_anchor: None,
            }],
            checksum: evidence_checksum(&[EvidenceEdge {
                right_fingerprint: [9; 32],
                tier: Some(4),
                reversed: true,
                proposal_home_anchor: None,
            }]),
        };
        assert!(reversed_nonzero_tier.is_valid());

        let mut cache = ProcessingState::default();
        let corrupt = Assessment {
            input_fingerprint: input,
            edges: edges.to_vec(),
            checksum: [0; 32],
        };
        assert!(!cache.load_assessment([8; 32], corrupt));
    }

    #[test]
    fn test_name_normalization_and_season_suffix_helpers() {
        assert_eq!(normalize_to_snake_case("CamelCase"), "camel_case");
        assert_eq!(normalize_to_snake_case("HTMLParser"), "html_parser");
        assert_eq!(normalize_to_snake_case("Hello World"), "hello_world");
        assert_eq!(normalize_to_snake_case("   Spaces   "), "spaces");
        assert_eq!(
            normalize_to_snake_case("Multiple   Spaces"),
            "multiple_spaces"
        );
        assert_eq!(normalize_to_snake_case("kebab-case"), "kebab_case");
        assert_eq!(
            normalize_to_snake_case("already_snake_case"),
            "already_snake_case"
        );
        assert_eq!(normalize_to_snake_case("123Number"), "123_number");
        assert_eq!(
            normalize_to_snake_case("Mix of Spaces andCamelCase"),
            "mix_of_spaces_and_camel_case"
        );
        assert_eq!(trim_year_suffix("premier league 2024"), "premier league");
        assert_eq!(
            trim_year_suffix("premier league 2024/2025"),
            "premier league"
        );
        assert_eq!(trim_year_suffix("premier league 2024/25"), "premier league");
        assert_eq!(trim_year_suffix("premier league 2"), "premier league 2");
        assert_eq!(trim_year_suffix("champion league"), "champion league");
        assert_eq!(trim_year_suffix("2024"), "2024");
    }
}
