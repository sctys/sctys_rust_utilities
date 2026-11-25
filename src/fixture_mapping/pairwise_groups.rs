//! Assemble rectified fixture links without rerunning provider matching.
use fxhash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MAX_PROVIDERS: usize = 128;
const MEMBERSHIP_NAMESPACE: Uuid = Uuid::from_u128(0x420a4914_4185_51ef_b47a_eb81e6197149);
const MEMBERSHIP_VERSION: u8 = 1;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Membership {
    pub id: Uuid,
    pub providers: Vec<Option<String>>,
}

/// Return every unordered provider pair in canonical index order.
pub fn pair_schedule(provider_count: usize) -> Vec<(usize, usize)> {
    (0..provider_count)
        .flat_map(|left| (left + 1..provider_count).map(move |right| (left, right)))
        .collect()
}

impl Membership {
    pub fn new<const PROVIDER_COUNT: usize>(
        providers: [Option<String>; PROVIDER_COUNT],
        names: &[String],
    ) -> Self {
        let mut bytes = vec![MEMBERSHIP_VERSION];
        for (name, fixture) in names.iter().zip(&providers) {
            if let Some(fixture) = fixture {
                for value in [name, fixture] {
                    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
                    bytes.extend_from_slice(value.as_bytes());
                }
            }
        }
        Self {
            id: Uuid::new_v5(&MEMBERSHIP_NAMESPACE, &bytes),
            providers: providers.into(),
        }
    }
}

/// A disjoint set with provider masks and home/away parity relative to the root.
pub struct Groups {
    nodes: Vec<Node>,
    timestamp_gap_seconds: Option<u64>,
    timestamp_ranges: Option<Vec<TimestampRange>>,
}

struct Node {
    parent: u32,
    providers: u128,
    source_provider: u128,
    flipped: bool,
    invalid: bool,
    rank: u8,
}

#[derive(Clone, Copy, Default)]
struct TimestampRange {
    earliest: Option<i64>,
    latest: Option<i64>,
}

struct ComponentLink {
    left: u32,
    right: u32,
    reversed: bool,
    votes: usize,
    source_links: Vec<usize>,
}

impl Groups {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            timestamp_gap_seconds: None,
            timestamp_ranges: None,
        }
    }

    /// Build groups that reject a connected component when its precise
    /// timestamps span more than `max_gap_seconds`. Unknown timestamps are
    /// ignored, so callers can omit synthetic or date-only values.
    pub fn with_timestamp_gap(max_gap_seconds: u64) -> Self {
        Self {
            nodes: Vec::new(),
            timestamp_gap_seconds: Some(max_gap_seconds),
            timestamp_ranges: Some(Vec::new()),
        }
    }

    pub fn with_size(size: u32) -> Self {
        let mut groups = Self {
            nodes: Vec::with_capacity(size as usize),
            timestamp_gap_seconds: None,
            timestamp_ranges: None,
        };
        for _ in 0..size {
            groups.push(0);
        }
        groups
    }

    pub fn push(&mut self, providers: u128) -> u32 {
        self.push_component(providers, None, false)
    }

    pub fn push_with_timestamp(&mut self, providers: u128, timestamp: Option<i64>) -> u32 {
        let range = timestamp.map(|timestamp| TimestampRange {
            earliest: Some(timestamp),
            latest: Some(timestamp),
        });
        self.push_component(providers, range, false)
    }

    fn push_component(
        &mut self,
        providers: u128,
        timestamp_range: Option<TimestampRange>,
        invalid: bool,
    ) -> u32 {
        let id = u32::try_from(self.nodes.len()).expect("fixture index exceeds UInt32");
        self.nodes.push(Node {
            parent: id,
            providers,
            source_provider: providers,
            flipped: false,
            invalid,
            rank: 0,
        });
        if let Some(ranges) = &mut self.timestamp_ranges {
            ranges.push(timestamp_range.unwrap_or_default());
        }
        id
    }

    pub fn find(&mut self, id: u32) -> (u32, bool) {
        let node = &self.nodes[id as usize];
        if node.parent == id {
            return (id, false);
        }
        let flipped = node.flipped;
        let (root, parent_flipped) = self.find(node.parent);
        self.nodes[id as usize].parent = root;
        self.nodes[id as usize].flipped = flipped ^ parent_flipped;
        (root, flipped ^ parent_flipped)
    }

    pub fn join(&mut self, left: u32, right: u32, reversed: bool) {
        let (mut left, a) = self.find(left);
        let (mut right, b) = self.find(right);
        let inconsistent = a ^ b ^ reversed;
        if left == right {
            self.nodes[left as usize].invalid |= inconsistent;
            return;
        }
        if self.nodes[left as usize].rank < self.nodes[right as usize].rank {
            std::mem::swap(&mut left, &mut right);
        }
        if self.nodes[left as usize].rank == self.nodes[right as usize].rank {
            self.nodes[left as usize].rank += 1;
        }
        let overlap = self.nodes[left as usize].providers & self.nodes[right as usize].providers;
        let timestamp_range = self.timestamp_ranges.as_ref().map(|ranges| {
            let left_range = ranges[left as usize];
            let right_range = ranges[right as usize];
            TimestampRange {
                earliest: match (left_range.earliest, right_range.earliest) {
                    (Some(left), Some(right)) => Some(left.min(right)),
                    (left, right) => left.or(right),
                },
                latest: match (left_range.latest, right_range.latest) {
                    (Some(left), Some(right)) => Some(left.max(right)),
                    (left, right) => left.or(right),
                },
            }
        });
        let timestamp_conflict = self.timestamp_gap_seconds.is_some_and(|max_gap| {
            timestamp_range
                .and_then(|range| range.earliest.zip(range.latest))
                .is_some_and(|(earliest, latest)| earliest.abs_diff(latest) > max_gap)
        });
        self.nodes[left as usize].providers |= self.nodes[right as usize].providers;
        self.nodes[left as usize].invalid |=
            overlap != 0 || self.nodes[right as usize].invalid || timestamp_conflict;
        if let (Some(ranges), Some(timestamp_range)) = (&mut self.timestamp_ranges, timestamp_range)
        {
            ranges[left as usize] = timestamp_range;
        }
        self.nodes[right as usize].parent = left;
        self.nodes[right as usize].flipped = inconsistent;
    }

    /// Resolve one support level on top of all stronger groups. When several
    /// provider pairs support the same component merge, that consensus breaks
    /// ties between otherwise equal-strength merges.
    pub fn apply_level(&mut self, links: &[(u32, u32, bool)]) -> Vec<usize> {
        let mut pending: Vec<_> = (0..links.len()).collect();
        let mut rejected = Vec::new();

        while !pending.is_empty() {
            let mut component_pairs =
                FxHashMap::<(u32, u32), FxHashMap<(u128, u128), [Vec<usize>; 2]>>::default();
            let mut handled = FxHashSet::default();

            for &index in &pending {
                let (a, b, reversed) = links[index];
                let left_provider = self.nodes[a as usize].source_provider;
                let right_provider = self.nodes[b as usize].source_provider;
                let (left, left_flip) = self.find(a);
                let (right, right_flip) = self.find(b);
                let reversed = reversed ^ left_flip ^ right_flip;

                if left == right {
                    if reversed {
                        rejected.push(index);
                    }
                    handled.insert(index);
                    continue;
                }

                let (left, right) = if left < right {
                    (left, right)
                } else {
                    (right, left)
                };
                let provider_pair = if left_provider <= right_provider {
                    (left_provider, right_provider)
                } else {
                    (right_provider, left_provider)
                };
                component_pairs
                    .entry((left, right))
                    .or_default()
                    .entry(provider_pair)
                    .or_insert_with(|| std::array::from_fn(|_| Vec::new()))[usize::from(reversed)]
                .push(index);
            }

            let mut proposals = Vec::with_capacity(component_pairs.len());
            for ((left, right), provider_pairs) in component_pairs {
                let mut votes = [0usize; 2];
                let mut source_links: [Vec<usize>; 2] = std::array::from_fn(|_| Vec::new());

                for indices in provider_pairs.into_values() {
                    match (indices[0].is_empty(), indices[1].is_empty()) {
                        (false, false) => {
                            let conflicting: Vec<_> = indices.into_iter().flatten().collect();
                            rejected.extend(conflicting.iter().copied());
                            handled.extend(conflicting);
                        }
                        (false, true) => {
                            votes[0] += 1;
                            source_links[0].extend(indices[0].iter().copied());
                        }
                        (true, false) => {
                            votes[1] += 1;
                            source_links[1].extend(indices[1].iter().copied());
                        }
                        (true, true) => unreachable!("provider-pair vote has no links"),
                    }
                }

                if votes[0] == votes[1] {
                    rejected.extend(source_links.iter().flatten().copied());
                    handled.extend(source_links.into_iter().flatten());
                    continue;
                }

                let winning_orientation = usize::from(votes[1] > votes[0]);
                rejected.extend(source_links[1 - winning_orientation].iter().copied());
                handled.extend(source_links[1 - winning_orientation].iter().copied());
                proposals.push(ComponentLink {
                    left,
                    right,
                    reversed: winning_orientation != 0,
                    votes: votes[winning_orientation],
                    source_links: std::mem::take(&mut source_links[winning_orientation]),
                });
            }

            let Some(strongest_vote_count) = proposals.iter().map(|proposal| proposal.votes).max()
            else {
                pending.retain(|index| !handled.contains(index));
                continue;
            };

            let strongest: Vec<_> = proposals
                .into_iter()
                .filter(|proposal| proposal.votes == strongest_vote_count)
                .collect();
            let strongest_links: Vec<_> = strongest
                .iter()
                .map(|proposal| (proposal.left, proposal.right, proposal.reversed))
                .collect();
            let rejected_proposals = self.apply_unweighted_level(&strongest_links);
            for &proposal_index in &rejected_proposals {
                rejected.extend(strongest[proposal_index].source_links.iter().copied());
            }
            for proposal in strongest {
                handled.extend(proposal.source_links);
            }
            pending.retain(|index| !handled.contains(index));
        }

        rejected.sort_unstable();
        rejected.dedup();
        rejected
    }

    fn apply_unweighted_level(&mut self, links: &[(u32, u32, bool)]) -> Vec<usize> {
        let mut trial = self
            .timestamp_gap_seconds
            .map_or_else(Self::new, Self::with_timestamp_gap);
        let mut roots = FxHashMap::default();
        for &(a, b, reversed) in links {
            let (left, left_flip) = self.find(a);
            let (right, right_flip) = self.find(b);
            let left = if let Some(&trial_root) = roots.get(&left) {
                trial_root
            } else {
                let node = &self.nodes[left as usize];
                let timestamp_range = self
                    .timestamp_ranges
                    .as_ref()
                    .map(|ranges| ranges[left as usize]);
                let trial_root = trial.push_component(
                    node.providers,
                    timestamp_range,
                    self.timestamp_gap_seconds.is_some() && node.invalid,
                );
                roots.insert(left, trial_root);
                trial_root
            };
            let right = if let Some(&trial_root) = roots.get(&right) {
                trial_root
            } else {
                let node = &self.nodes[right as usize];
                let timestamp_range = self
                    .timestamp_ranges
                    .as_ref()
                    .map(|ranges| ranges[right as usize]);
                let trial_root = trial.push_component(
                    node.providers,
                    timestamp_range,
                    self.timestamp_gap_seconds.is_some() && node.invalid,
                );
                roots.insert(right, trial_root);
                trial_root
            };
            trial.join(left, right, reversed ^ left_flip ^ right_flip);
        }
        let mut rejected = Vec::new();
        for (index, &(a, _, _)) in links.iter().enumerate() {
            let (left, _) = self.find(a);
            // The main groups must stay frozen until every trial outcome has been read.
            let (root, _) = trial.find(roots[&left]);
            if trial.nodes[root as usize].invalid {
                rejected.push(index);
            }
        }
        let mut rejected_indices = rejected.iter().copied().peekable();
        for (index, &(a, b, reversed)) in links.iter().enumerate() {
            if rejected_indices.next_if_eq(&index).is_none() {
                self.join(a, b, reversed);
            }
        }
        rejected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pair_schedule_is_unordered_and_stable() {
        assert_eq!(
            pair_schedule(4),
            [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]
        );
        assert!(pair_schedule(1).is_empty());
    }

    #[test]
    fn conflicts_within_a_vote_level_do_not_break_stronger_groups() {
        let mut groups = Groups::new();
        let a = groups.push(1);
        let other_a = groups.push(1);
        let b = groups.push(2);
        let c = groups.push(4);
        assert!(groups.apply_level(&[(a, b, true)]).is_empty());
        assert_eq!(
            groups.apply_level(&[(b, c, true), (c, other_a, false)]),
            [0, 1]
        );
        assert!(groups.apply_level(&[(b, c, true)]).is_empty());
        assert_eq!(groups.find(a), groups.find(c));
        assert_ne!(groups.find(a).0, groups.find(other_a).0);
    }

    #[test]
    fn precise_timestamp_span_rejects_every_link_in_an_equal_support_component() {
        let mut groups = Groups::with_timestamp_gap(3_600);
        let exact_early = groups.push_with_timestamp(1, Some(0));
        let date_only = groups.push_with_timestamp(2, None);
        let exact_late = groups.push_with_timestamp(4, Some(7_200));

        assert_eq!(
            groups.apply_level(&[
                (exact_early, date_only, false),
                (date_only, exact_late, false)
            ]),
            [0, 1]
        );
        assert_ne!(groups.find(exact_early).0, groups.find(date_only).0);
        assert_ne!(groups.find(date_only).0, groups.find(exact_late).0);
    }

    #[test]
    fn precise_timestamp_gap_boundary_is_inclusive() {
        let mut groups = Groups::with_timestamp_gap(3_600);
        let early = groups.push_with_timestamp(1, Some(0));
        let late = groups.push_with_timestamp(2, Some(3_600));

        assert!(groups.apply_level(&[(early, late, false)]).is_empty());
        assert_eq!(groups.find(early).0, groups.find(late).0);
    }

    #[test]
    fn timestamp_gap_is_opt_in_for_existing_callers() {
        let mut groups = Groups::new();
        let early = groups.push_with_timestamp(1, Some(0));
        let late = groups.push_with_timestamp(2, Some(7_200));

        assert!(groups.apply_level(&[(early, late, false)]).is_empty());
        assert_eq!(groups.find(early).0, groups.find(late).0);
    }

    #[test]
    fn orientation_contradiction_rejects_all_equal_votes_in_any_order() {
        for edges in [
            [(0, 1, false), (1, 2, false), (0, 2, true)],
            [(0, 2, true), (1, 2, false), (0, 1, false)],
        ] {
            let mut groups = Groups::new();
            for provider in 0..3 {
                groups.push(1u128 << provider);
            }
            assert_eq!(groups.apply_level(&edges), [0, 1, 2]);
            for provider in 0..3 {
                assert_eq!(groups.find(provider), (provider, false));
            }
        }
    }

    #[test]
    fn ids_follow_membership() {
        let names = ["a".to_owned(), "b".to_owned()];
        let mut providers: [Option<String>; 2] = std::array::from_fn(|_| None);
        providers[0] = Some("01".to_owned());
        let original = Membership::new(providers.clone(), &names);
        providers[1] = Some("1".to_owned());
        assert_ne!(Membership::new(providers.clone(), &names).id, original.id);
        providers[1] = None;
        assert_eq!(Membership::new(providers, &names), original);
    }
}
