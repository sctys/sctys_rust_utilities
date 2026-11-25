//! Pure publication diffing shared by fixture mapping callers.

use std::{collections::HashMap, hash::Hash};

/// One row from the current published mapping state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExistingMapping<Key> {
    /// Raw source fixture identity used as the storage key.
    pub key: Key,
    /// Whether the latest stored row is active.
    pub is_active: bool,
    /// Fingerprint of the first source fixture.
    pub input_fingerprint1: String,
    /// Fingerprint of the second source fixture.
    pub input_fingerprint2: String,
    /// Published match source.
    pub match_source: String,
    /// Published match level.
    pub match_level: String,
    /// Published match condition.
    pub match_condition: String,
}

/// A desired mapping with its caller-owned accepted-pair value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DesiredMapping<Key, Pair> {
    /// Raw source fixture identity used as the storage key.
    pub key: Key,
    /// Accepted pair returned to the caller when this row needs an upsert.
    pub pair: Pair,
    /// Fingerprint of the first source fixture.
    pub input_fingerprint1: String,
    /// Fingerprint of the second source fixture.
    pub input_fingerprint2: String,
    /// Published match source.
    pub match_source: String,
    /// Published match level.
    pub match_level: String,
    /// Published match condition.
    pub match_condition: String,
}

/// Changes required to make the stored state agree with desired direct mappings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationChanges<Key, Pair> {
    /// Desired pairs whose rows must be inserted or replaced.
    pub upserts: Vec<Pair>,
    /// Active raw fixture keys whose rows must be withdrawn.
    pub withdrawals: Vec<Key>,
    /// Whether downstream fixture reconciliation must inspect the full inventory.
    pub requires_full_reconcile: bool,
}

/// Compares direct mappings using raw fixture keys and the complete published decision.
///
/// Upserts follow `desired` order. Withdrawals follow the order of the provided existing
/// rows. A new key does not require a full reconciliation. Replacements, reactivations,
/// and withdrawals do.
pub fn direct_changes<Key, Pair>(
    desired: &[DesiredMapping<Key, Pair>],
    existing: Vec<ExistingMapping<Key>>,
) -> PublicationChanges<Key, Pair>
where
    Key: Clone + Eq + Hash,
    Pair: Copy,
{
    let mut previous: HashMap<Key, ExistingMapping<Key>> = HashMap::default();
    let mut existing_order = Vec::with_capacity(existing.len());
    for row in existing {
        existing_order.push(row.key.clone());
        previous.insert(row.key.clone(), row);
    }

    let mut upserts = Vec::new();
    let mut replaced_existing = false;
    for row in desired {
        let old = previous.remove(&row.key);
        let agrees = old.as_ref().is_some_and(|old| {
            old.is_active
                && old.input_fingerprint1 == row.input_fingerprint1
                && old.input_fingerprint2 == row.input_fingerprint2
                && old.match_source == row.match_source
                && old.match_level == row.match_level
                && old.match_condition == row.match_condition
        });
        if old.is_some() && !agrees {
            replaced_existing = true;
        }
        if !agrees {
            upserts.push(row.pair);
        }
    }

    let mut withdrawals = Vec::new();
    for key in existing_order {
        if let Some(old) = previous.remove(&key) {
            if old.is_active {
                withdrawals.push(key);
            }
        }
    }
    let requires_full_reconcile = replaced_existing || !withdrawals.is_empty();

    PublicationChanges {
        upserts,
        withdrawals,
        requires_full_reconcile,
    }
}

/// Error returned when publication timestamps can no longer be strictly increased.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("mapping publication timestamp overflow")]
pub struct TimestampOverflow;

/// Returns `now` or one second after the latest stored timestamp, whichever is later.
pub fn next_timestamp(
    existing: impl IntoIterator<Item = i64>,
    now: i64,
) -> Result<i64, TimestampOverflow> {
    let Some(latest) = existing.into_iter().max() else {
        return Ok(now);
    };
    latest
        .checked_add(1)
        .map(|next| next.max(now))
        .ok_or(TimestampOverflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn desired(key: &str, pair: usize, fingerprint: &str) -> DesiredMapping<String, usize> {
        DesiredMapping {
            key: key.to_owned(),
            pair,
            input_fingerprint1: fingerprint.to_owned(),
            input_fingerprint2: "right".to_owned(),
            match_source: "direct".to_owned(),
            match_level: "score_time".to_owned(),
            match_condition: "normal".to_owned(),
        }
    }

    fn existing(key: &str, fingerprint: &str, is_active: bool) -> ExistingMapping<String> {
        ExistingMapping {
            key: key.to_owned(),
            is_active,
            input_fingerprint1: fingerprint.to_owned(),
            input_fingerprint2: "right".to_owned(),
            match_source: "direct".to_owned(),
            match_level: "score_time".to_owned(),
            match_condition: "normal".to_owned(),
        }
    }

    #[test]
    fn new_mapping_does_not_request_full_reconciliation() {
        let changes = direct_changes(&[desired("new", 3, "left")], Vec::new());
        assert_eq!(changes.upserts, [3]);
        assert!(changes.withdrawals.is_empty());
        assert!(!changes.requires_full_reconcile);
    }

    #[test]
    fn replacements_reactivations_and_withdrawals_request_full_reconciliation() {
        let changes = direct_changes(
            &[
                desired("replace", 1, "corrected"),
                desired("reactivate", 2, "old"),
            ],
            vec![
                existing("replace", "old", true),
                existing("reactivate", "old", false),
                existing("withdraw", "old", true),
            ],
        );
        assert_eq!(changes.upserts, [1, 2]);
        assert_eq!(changes.withdrawals, ["withdraw"]);
        assert!(changes.requires_full_reconcile);
    }

    #[test]
    fn unchanged_mapping_does_not_need_an_upsert() {
        let changes = direct_changes(
            &[desired("same", 1, "left")],
            vec![existing("same", "left", true)],
        );
        assert!(changes.upserts.is_empty());
        assert!(!changes.requires_full_reconcile);
    }

    #[test]
    fn timestamp_is_monotonic_and_reports_overflow() {
        assert_eq!(next_timestamp([], 10), Ok(10));
        assert_eq!(next_timestamp([9, 10], 10), Ok(11));
        assert_eq!(next_timestamp([9, 10], 20), Ok(20));
        assert_eq!(next_timestamp([i64::MAX], 20), Err(TimestampOverflow));
    }
}

/// A current direct row and the fingerprints used to locate its fixture inputs.
pub struct DirectWitness<'a, Row> {
    pub row: &'a Row,
    pub active: bool,
    pub assisted: bool,
    pub fingerprints: [&'a str; 2],
}

/// Recovers only independent direct rows whose fingerprints identify unique
/// current fixtures. The adapter validates provider keys, scores and orientation.
pub fn recover_direct_witnesses<'a, Row: 'a>(
    candidates: impl IntoIterator<Item = DirectWitness<'a, Row>>,
    expected_active: u64,
    left: &[super::Fingerprint],
    right: &[super::Fingerprint],
) -> Option<Vec<(&'a Row, usize, usize)>> {
    use super::{parse_fingerprint_hex, Fingerprint};
    use fxhash::FxHashMap;

    let mut active_count = 0;
    let mut independent = Vec::new();
    for candidate in candidates.into_iter().filter(|candidate| candidate.active) {
        active_count += 1;
        if !candidate.assisted {
            independent.push((
                candidate.row,
                parse_fingerprint_hex(candidate.fingerprints[0])?,
                parse_fingerprint_hex(candidate.fingerprints[1])?,
            ));
        }
    }
    if active_count != expected_active {
        return None;
    }

    let locate = |mut needed: FxHashMap<Fingerprint, Option<usize>>,
                  fingerprints: &[Fingerprint]| {
        for (index, hash) in fingerprints.iter().enumerate() {
            if let Some(entry) = needed.get_mut(hash) {
                if entry.replace(index).is_some() {
                    return None;
                }
            }
        }
        Some(needed)
    };
    let left_indices = locate(
        independent
            .iter()
            .map(|(_, hash, _)| (*hash, None))
            .collect(),
        left,
    )?;
    let right_indices = locate(
        independent
            .iter()
            .map(|(_, _, hash)| (*hash, None))
            .collect(),
        right,
    )?;
    independent
        .into_iter()
        .map(|(row, left_hash, right_hash)| {
            Some((row, left_indices[&left_hash]?, right_indices[&right_hash]?))
        })
        .collect()
}
