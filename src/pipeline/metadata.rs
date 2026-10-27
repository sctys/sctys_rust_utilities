//! Focused identities and stable algorithms independent of application enums.
use std::{collections::HashSet, hash::Hash};

pub const CUTOFF_DATE_DATASET_MARKER: &str = "cutoff_date";
pub const SCRAPER_LOG_DATASET_MARKER: &str = "_scraper_log";
pub const SCRAPED_DATASET_MARKER: &str = "_scraped";

pub trait SourceIdentity {
    fn source_name(&self) -> String;
    fn prefix(&self) -> String;
}

pub trait DatasetMetadata {
    type Source: SourceIdentity;
    fn source(&self) -> Self::Source;
    fn dataset_name(&self) -> String;
    fn is_bookkeeping(&self) -> bool;
}

pub trait ModuleMetadata {
    type Source: SourceIdentity;
    type Dataset: DatasetMetadata<Source = Self::Source>;
    fn source(&self) -> Self::Source;
    fn module_name(&self) -> String;
    fn datasets(&self) -> Vec<Self::Dataset>;
}

pub fn active_modules<M: PartialEq>(
    modules: impl IntoIterator<Item = M>,
    inactive: impl IntoIterator<Item = M>,
) -> Vec<M> {
    let inactive: Vec<_> = inactive.into_iter().collect();
    modules
        .into_iter()
        .filter(|module| !inactive.contains(module))
        .collect()
}

pub fn ordered_datasets<M, D: Copy + Eq + Hash>(
    modules: &[M],
    datasets: impl Fn(&M) -> Vec<D>,
) -> Vec<D> {
    let mut seen = HashSet::new();
    modules
        .iter()
        .flat_map(datasets)
        .filter(|dataset| seen.insert(*dataset))
        .collect()
}

/// Collects values whose configured name contains a marker, leaving the
/// application's enum and collection type with the caller.
pub fn values_with_name_marker<T, C: FromIterator<T>>(
    values: impl IntoIterator<Item = T>,
    marker: &str,
    name: impl Fn(&T) -> String,
) -> C {
    values
        .into_iter()
        .filter(|value| name(value).contains(marker))
        .collect()
}

pub fn is_bookkeeping_dataset_name(name: &str) -> bool {
    [
        CUTOFF_DATE_DATASET_MARKER,
        SCRAPER_LOG_DATASET_MARKER,
        SCRAPED_DATASET_MARKER,
    ]
    .iter()
    .any(|marker| name.contains(marker))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_ordered_active_modules_and_dataset_expansion() {
        assert_eq!(active_modules(['C', 'B', 'A', 'B'], ['A']), ['C', 'B', 'B']);
        assert_eq!(
            ordered_datasets(&['B', 'A', 'B'], |module| if *module == 'B' {
                vec![2, 1]
            } else {
                vec![1, 3]
            }),
            [2, 1, 3]
        );
        assert!(ordered_datasets::<char, usize>(&[], |_| vec![1]).is_empty());
    }
}
