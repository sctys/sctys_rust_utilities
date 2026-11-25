//! Orientation-aware component merging for fixture registries.

use fxhash::FxHashMap;
use std::hash::Hash;

/// Registry metadata supplied for one provider fixture.
#[derive(Clone, Debug)]
pub struct RegistryFixture<GroupId, const PERIOD_COUNT: usize> {
    /// A caller-assigned one-bit provider identity.
    pub provider_bit: u128,
    /// Exact kickoff timestamp, or `None` when only a date is available.
    pub kickoff: Option<i64>,
    /// Scores indexed by the caller's standard periods.
    pub scores: [Option<(i32, i32)>; PERIOD_COUNT],
    /// Approved split assignments keyed by their split identity.
    pub split_groups: FxHashMap<GroupId, GroupId>,
}

struct Component<GroupId, Evidence, const PERIOD_COUNT: usize> {
    provider_mask: u128,
    members: usize,
    min_kickoff: Option<i64>,
    max_kickoff: Option<i64>,
    scores: [Option<(i32, i32)>; PERIOD_COUNT],
    provider_conflict: bool,
    score_conflict: bool,
    evidence_samples: Vec<Evidence>,
    split_groups: FxHashMap<GroupId, GroupId>,
}

/// Disjoint sets that retain relative home/away orientation and registry metadata.
pub struct DisjointSets<GroupId, Evidence, const PERIOD_COUNT: usize> {
    parents: Vec<usize>,
    reversed_to_parent: Vec<bool>,
    components: Vec<Option<Component<GroupId, Evidence, PERIOD_COUNT>>>,
    max_kickoff_spread: u64,
    evidence_sample_limit: usize,
}

impl<GroupId, Evidence, const PERIOD_COUNT: usize> DisjointSets<GroupId, Evidence, PERIOD_COUNT>
where
    GroupId: Clone + Eq + Hash,
    Evidence: Clone,
{
    /// Creates singleton components using caller-prepared provider bits and fixture metadata.
    pub fn new(
        fixtures: &[RegistryFixture<GroupId, PERIOD_COUNT>],
        max_kickoff_spread: u64,
        evidence_sample_limit: usize,
    ) -> Self {
        let components = fixtures
            .iter()
            .map(|fixture| {
                Some(Component {
                    provider_mask: fixture.provider_bit,
                    members: 1,
                    min_kickoff: fixture.kickoff,
                    max_kickoff: fixture.kickoff,
                    scores: fixture.scores,
                    provider_conflict: false,
                    score_conflict: false,
                    evidence_samples: Vec::new(),
                    split_groups: fixture.split_groups.clone(),
                })
            })
            .collect();
        Self {
            parents: (0..fixtures.len()).collect(),
            reversed_to_parent: vec![false; fixtures.len()],
            components,
            max_kickoff_spread,
            evidence_sample_limit,
        }
    }

    /// Returns the component root and whether this fixture is reversed to that root.
    pub fn find(&mut self, node: usize) -> (usize, bool) {
        let parent = self.parents[node];
        if parent == node {
            return (node, false);
        }
        let (root, parent_reversed) = self.find(parent);
        let reversed = self.reversed_to_parent[node] ^ parent_reversed;
        self.parents[node] = root;
        self.reversed_to_parent[node] = reversed;
        (root, reversed)
    }

    /// Adds new evidence and merges components when all mapping rules agree.
    pub fn connect(
        &mut self,
        left: usize,
        right: usize,
        reversed: bool,
        evidence: Evidence,
    ) -> bool {
        let (left_root, left_reversed) = self.find(left);
        let (right_root, right_reversed) = self.find(right);
        if left_root == right_root {
            let consistent = left_reversed ^ right_reversed == reversed;
            if consistent {
                if let Some(component) = self.components[left_root].as_mut() {
                    if component.evidence_samples.len() < self.evidence_sample_limit {
                        component.evidence_samples.push(evidence);
                    }
                }
            }
            return consistent;
        }

        let left_component = self.components[left_root]
            .as_ref()
            .expect("every disjoint-set root has a component");
        let right_component = self.components[right_root]
            .as_ref()
            .expect("every disjoint-set root has a component");
        let root_reversed = left_reversed ^ right_reversed ^ reversed;
        if left_component.provider_conflict
            || right_component.provider_conflict
            || left_component.provider_mask & right_component.provider_mask != 0
        {
            return false;
        }

        let min_kickoff = left_component
            .min_kickoff
            .into_iter()
            .chain(right_component.min_kickoff)
            .min();
        let max_kickoff = left_component
            .max_kickoff
            .into_iter()
            .chain(right_component.max_kickoff)
            .max();
        if min_kickoff
            .zip(max_kickoff)
            .is_some_and(|(min, max)| max.abs_diff(min) > self.max_kickoff_spread)
        {
            return false;
        }

        let Some(merged_split_groups) =
            merge_split_groups(&left_component.split_groups, &right_component.split_groups)
        else {
            return false;
        };
        let (parent_root, child_root, parent_component, child_component) =
            if left_component.members >= right_component.members {
                (left_root, right_root, left_component, right_component)
            } else {
                (right_root, left_root, right_component, left_component)
            };
        let mut merged_scores = parent_component.scores;
        let mut score_conflict = left_component.score_conflict || right_component.score_conflict;
        merge_scores(
            &mut merged_scores,
            child_component.scores,
            root_reversed,
            &mut score_conflict,
        );
        if score_conflict {
            return false;
        }

        let merged = Component {
            provider_mask: left_component.provider_mask | right_component.provider_mask,
            members: left_component.members + right_component.members,
            min_kickoff,
            max_kickoff,
            scores: merged_scores,
            provider_conflict: false,
            score_conflict,
            evidence_samples: left_component
                .evidence_samples
                .iter()
                .chain(right_component.evidence_samples.iter())
                .cloned()
                .chain(std::iter::once(evidence))
                .take(self.evidence_sample_limit)
                .collect(),
            split_groups: merged_split_groups,
        };
        self.parents[child_root] = parent_root;
        self.reversed_to_parent[child_root] = root_reversed;
        self.components[parent_root] = Some(merged);
        self.components[child_root] = None;
        true
    }

    /// Rebuilds a stored component link without applying new-evidence time or conflict gates.
    pub fn restore_link(&mut self, left: usize, right: usize, reversed: bool) -> bool {
        let (left_root, left_reversed) = self.find(left);
        let (right_root, right_reversed) = self.find(right);
        if left_root == right_root {
            return left_reversed ^ right_reversed == reversed;
        }

        let root_reversed = left_reversed ^ right_reversed ^ reversed;
        let left_component = self.components[left_root]
            .as_ref()
            .expect("every disjoint-set root has a component");
        let right_component = self.components[right_root]
            .as_ref()
            .expect("every disjoint-set root has a component");
        let (parent_root, child_root, parent_component, child_component) =
            if left_component.members >= right_component.members {
                (left_root, right_root, left_component, right_component)
            } else {
                (right_root, left_root, right_component, left_component)
            };
        let mut scores = parent_component.scores;
        let mut score_conflict = left_component.score_conflict || right_component.score_conflict;
        merge_scores(
            &mut scores,
            child_component.scores,
            root_reversed,
            &mut score_conflict,
        );
        let Some(split_groups) = merge_split_groups(
            &parent_component.split_groups,
            &child_component.split_groups,
        ) else {
            return false;
        };

        let merged = Component {
            provider_mask: parent_component.provider_mask | child_component.provider_mask,
            members: parent_component.members + child_component.members,
            // Preserve the stored-state restore behavior for mixed precision components.
            min_kickoff: parent_component
                .min_kickoff
                .min(child_component.min_kickoff),
            max_kickoff: parent_component
                .max_kickoff
                .max(child_component.max_kickoff),
            scores,
            provider_conflict: left_component.provider_conflict
                || right_component.provider_conflict
                || left_component.provider_mask & right_component.provider_mask != 0,
            score_conflict,
            evidence_samples: parent_component
                .evidence_samples
                .iter()
                .chain(child_component.evidence_samples.iter())
                .take(self.evidence_sample_limit)
                .cloned()
                .collect(),
            split_groups,
        };
        self.parents[child_root] = parent_root;
        self.reversed_to_parent[child_root] = root_reversed;
        self.components[parent_root] = Some(merged);
        self.components[child_root] = None;
        true
    }

    /// Evidence associated with a root component, in stable merge order.
    pub fn evidence_samples(&self, root: usize) -> &[Evidence] {
        self.components[root]
            .as_ref()
            .map_or(&[], |component| component.evidence_samples.as_slice())
    }
}

fn merge_split_groups<GroupId: Clone + Eq + Hash>(
    left: &FxHashMap<GroupId, GroupId>,
    right: &FxHashMap<GroupId, GroupId>,
) -> Option<FxHashMap<GroupId, GroupId>> {
    let mut merged = left.clone();
    for (split_id, group_id) in right {
        if merged
            .get(split_id)
            .is_some_and(|existing_group| existing_group != group_id)
        {
            return None;
        }
        merged.insert(split_id.clone(), group_id.clone());
    }
    Some(merged)
}

fn merge_scores<const PERIOD_COUNT: usize>(
    merged: &mut [Option<(i32, i32)>; PERIOD_COUNT],
    child: [Option<(i32, i32)>; PERIOD_COUNT],
    reversed: bool,
    score_conflict: &mut bool,
) {
    for (index, value) in child.iter().enumerate() {
        let Some((home, away)) = value else {
            continue;
        };
        let score = if reversed {
            (*away, *home)
        } else {
            (*home, *away)
        };
        if let Some(existing) = merged[index] {
            *score_conflict |= existing != score;
        } else {
            merged[index] = Some(score);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(
        provider_bit: u128,
        kickoff: Option<i64>,
        score: Option<(i32, i32)>,
    ) -> RegistryFixture<i32, 1> {
        RegistryFixture {
            provider_bit,
            kickoff,
            scores: [score],
            split_groups: FxHashMap::default(),
        }
    }

    #[test]
    fn connect_tracks_orientation_and_rejects_provider_score_and_time_conflicts() {
        let fixtures = [
            fixture(1, Some(100), Some((1, 0))),
            fixture(2, Some(110), Some((0, 1))),
            fixture(4, Some(121), Some((1, 0))),
        ];
        let mut sets = DisjointSets::<i32, bool, 1>::new(&fixtures, 20, 2);
        assert!(sets.connect(0, 1, true, true));
        assert_eq!(sets.find(1), (0, true));
        assert!(!sets.connect(0, 2, false, false));

        let duplicate_provider = [fixture(1, Some(1), None), fixture(1, Some(1), None)];
        let mut sets = DisjointSets::<i32, bool, 1>::new(&duplicate_provider, 100, 2);
        assert!(!sets.connect(0, 1, false, false));

        let score_conflict = [
            fixture(1, Some(1), Some((1, 0))),
            fixture(2, Some(1), Some((2, 0))),
        ];
        let mut sets = DisjointSets::<i32, bool, 1>::new(&score_conflict, 100, 2);
        assert!(!sets.connect(0, 1, false, false));
    }

    #[test]
    fn restore_keeps_existing_mixed_precision_bounds_and_conflict_state() {
        let fixtures = [
            fixture(1, Some(100), None),
            fixture(2, None, None),
            fixture(4, Some(150), None),
        ];
        let mut sets = DisjointSets::<i32, (), 1>::new(&fixtures, 49, 2);
        assert!(sets.restore_link(0, 1, false));
        // The restored `None` lower bound is preserved. Only the current exact kickoff is
        // compared with the restored maximum, so this stored link remains acceptable.
        assert!(sets.connect(0, 2, false, ()));
    }

    #[test]
    fn conflicting_manual_splits_block_new_component_merges() {
        let mut left = fixture(1, Some(100), None);
        left.split_groups.insert(7, 10);
        let mut right = fixture(2, Some(100), None);
        right.split_groups.insert(7, 11);
        let mut sets = DisjointSets::<i32, (), 1>::new(&[left, right], 60, 2);
        assert!(!sets.connect(0, 1, false, ()));
        assert_ne!(sets.find(0).0, sets.find(1).0);
    }

    #[test]
    fn evidence_samples_keep_merge_order_and_respect_the_limit() {
        let fixtures = [
            fixture(1, Some(100), None),
            fixture(2, Some(100), None),
            fixture(4, Some(100), None),
        ];
        let mut sets = DisjointSets::<i32, bool, 1>::new(&fixtures, 60, 1);
        assert!(sets.connect(0, 1, false, true));
        assert!(sets.connect(1, 2, false, false));
        assert_eq!(sets.evidence_samples(0), [true]);
    }
}
