//! Database-independent encoding for persisted fixture assessments.

use crate::fixture_mapping::{Assessment, EvidenceEdge, Fingerprint};
use fxhash::FxHashSet;
use std::collections::HashMap;

/// Row index reserved for an assessment header.
pub const HEADER_INDEX: i64 = 0;
/// Value used for an absent tier or home anchor.
pub const ABSENT: i8 = -1;

/// The assessment fields stored in one persistence row.
///
/// Callers add their source-pair and period columns to their local row types.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssessmentRow {
    /// Fingerprint of the left fixture input.
    pub input_fingerprint: String,
    /// Fingerprint identifying the assessment.
    pub assessment_fingerprint: String,
    /// Header index or one-based edge slot.
    pub row_index: i64,
    /// Number of edges described by the header.
    pub result_count: i64,
    /// Ordered edge checksum stored as lowercase hexadecimal.
    pub result_checksum: String,
    /// Right fixture fingerprint for an edge row, empty for the header.
    pub right_fingerprint: String,
    /// Candidate tier or [`ABSENT`].
    pub candidate_tier: i8,
    /// Whether the edge is reversed, encoded as zero or one.
    pub reversed: u8,
    /// Home anchor value or [`ABSENT`].
    pub proposal_home_anchor: i8,
}

/// An assessment is too large to represent in the persisted signed row count.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("assessment contains more edges than the persisted row count can represent")]
pub struct AssessmentTooLarge;

/// Encodes an assessment as a header followed by ordered edge rows.
pub fn encode(
    key: &Fingerprint,
    assessment: &Assessment,
) -> Result<Vec<AssessmentRow>, AssessmentTooLarge> {
    let result_count = i64::try_from(assessment.edges.len()).map_err(|_| AssessmentTooLarge)?;
    let input_fingerprint = crate::fixture_mapping::fingerprint_hex(&assessment.input_fingerprint);
    let assessment_fingerprint = crate::fixture_mapping::fingerprint_hex(key);
    let result_checksum = crate::fixture_mapping::fingerprint_hex(&assessment.checksum);
    let header = AssessmentRow {
        input_fingerprint,
        assessment_fingerprint,
        row_index: HEADER_INDEX,
        result_count,
        result_checksum,
        right_fingerprint: String::new(),
        candidate_tier: ABSENT,
        reversed: 0,
        proposal_home_anchor: ABSENT,
    };
    let mut rows = Vec::with_capacity(assessment.edges.len() + 1);
    rows.push(header.clone());
    for (slot, edge) in assessment.edges.iter().enumerate() {
        let row_index = i64::try_from(slot + 1).map_err(|_| AssessmentTooLarge)?;
        rows.push(AssessmentRow {
            row_index,
            right_fingerprint: crate::fixture_mapping::fingerprint_hex(&edge.right_fingerprint),
            candidate_tier: edge.tier.map_or(ABSENT, |tier| tier as i8),
            reversed: u8::from(edge.reversed),
            proposal_home_anchor: edge.proposal_home_anchor.map_or(ABSENT, i8::from),
            ..header.clone()
        });
    }
    Ok(rows)
}

/// Decodes one assessment group. Invalid, incomplete, or conflicting rows are cache misses.
/// Identical duplicate rows are accepted, matching ClickHouse `ReplacingMergeTree` reads.
pub fn decode(rows: &[AssessmentRow]) -> Option<(Fingerprint, Assessment)> {
    let mut slots: HashMap<i64, AssessmentRow> = HashMap::default();
    for row in rows {
        if let Some(previous) = slots.get(&row.row_index) {
            if previous != row {
                return None;
            }
        } else {
            slots.insert(row.row_index, row.clone());
        }
    }

    let header = slots.remove(&HEADER_INDEX)?;
    let result_count = usize::try_from(header.result_count).ok()?;
    if result_count != slots.len()
        || !header.right_fingerprint.is_empty()
        || header.candidate_tier != ABSENT
        || header.proposal_home_anchor != ABSENT
        || header.reversed != 0
    {
        return None;
    }
    let input_fingerprint =
        crate::fixture_mapping::parse_fingerprint_hex(&header.input_fingerprint)?;
    let assessment_fingerprint =
        crate::fixture_mapping::parse_fingerprint_hex(&header.assessment_fingerprint)?;
    let checksum = crate::fixture_mapping::parse_fingerprint_hex(&header.result_checksum)?;

    let mut edges = Vec::with_capacity(result_count);
    let mut endpoints = FxHashSet::default();
    for slot in 1..=header.result_count {
        let row = slots.remove(&slot)?;
        if row.input_fingerprint != header.input_fingerprint
            || row.assessment_fingerprint != header.assessment_fingerprint
            || row.result_count != header.result_count
            || row.result_checksum != header.result_checksum
            || !(ABSENT..crate::fixture_mapping::EVIDENCE_TIER_COUNT as i8)
                .contains(&row.candidate_tier)
            || !(-1..=1).contains(&row.proposal_home_anchor)
            || row.reversed > 1
            || row.right_fingerprint.is_empty()
            || !endpoints.insert(row.right_fingerprint.clone())
        {
            return None;
        }
        let right_fingerprint =
            crate::fixture_mapping::parse_fingerprint_hex(&row.right_fingerprint)?;
        edges.push(EvidenceEdge {
            right_fingerprint,
            tier: (row.candidate_tier != ABSENT).then_some(row.candidate_tier as u8),
            reversed: row.reversed == 1,
            proposal_home_anchor: (row.proposal_home_anchor != ABSENT)
                .then_some(row.proposal_home_anchor == 1),
        });
    }

    let assessment = Assessment {
        input_fingerprint,
        edges,
        checksum,
    };
    assessment
        .is_valid()
        .then_some((assessment_fingerprint, assessment))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture_mapping::evidence_checksum;

    fn sample() -> (Fingerprint, Assessment) {
        let key = [1; 32];
        let edges = vec![
            EvidenceEdge {
                right_fingerprint: [2; 32],
                tier: Some(3),
                reversed: false,
                proposal_home_anchor: None,
            },
            EvidenceEdge {
                right_fingerprint: [4; 32],
                tier: None,
                reversed: false,
                proposal_home_anchor: Some(true),
            },
        ];
        let assessment = Assessment {
            input_fingerprint: [7; 32],
            checksum: evidence_checksum(&edges),
            edges,
        };
        (key, assessment)
    }

    #[test]
    fn encoded_rows_round_trip_without_reordering_edges() {
        let (key, assessment) = sample();
        let rows = encode(&key, &assessment).unwrap();
        assert_eq!(rows[0].row_index, HEADER_INDEX);
        assert_eq!(rows[0].result_count, 2);
        assert_eq!(rows[1].row_index, 1);
        assert_eq!(rows[1].candidate_tier, 3);
        assert_eq!(rows[2].row_index, 2);
        assert_eq!(rows[2].proposal_home_anchor, 1);
        assert_eq!(decode(&rows), Some((key, assessment)));
    }

    #[test]
    fn identical_duplicates_are_accepted_but_conflicts_are_cache_misses() {
        let (key, assessment) = sample();
        let mut rows = encode(&key, &assessment).unwrap();
        rows.push(rows[1].clone());
        assert_eq!(decode(&rows), Some((key, assessment)));
        rows.last_mut().unwrap().reversed = 1;
        assert!(decode(&rows).is_none());
    }

    #[test]
    fn incomplete_or_corrupt_groups_are_cache_misses() {
        let (key, assessment) = sample();
        let mut rows = encode(&key, &assessment).unwrap();
        rows.pop();
        assert!(decode(&rows).is_none());

        let mut rows = encode(&key, &assessment).unwrap();
        rows[1].candidate_tier = crate::fixture_mapping::EVIDENCE_TIER_COUNT as i8;
        assert!(decode(&rows).is_none());

        let mut rows = encode(&key, &assessment).unwrap();
        rows[1].result_checksum = crate::fixture_mapping::fingerprint_hex(&[9; 32]);
        assert!(decode(&rows).is_none());
    }
}
