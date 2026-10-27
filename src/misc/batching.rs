//! Borrowed, ordered batches limited by row count and estimated memory size.
//!
//! Estimates are not serialized request-size guarantees. Callers own limits,
//! buffers, error context, and any retry policy; insertion may partially succeed.

use std::{future::Future, iter::FusedIterator, ops::Range};

/// Positive batching limits. Construction rejects either zero limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchLimits {
    max_rows: usize,
    max_bytes: usize,
}

/// A zero limit would prevent an iterator from making progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidBatchLimits {
    #[error("batch row limit must be greater than zero")]
    ZeroRows,
    #[error("batch byte limit must be greater than zero")]
    ZeroBytes,
}

impl BatchLimits {
    pub fn new(max_rows: usize, max_bytes: usize) -> Result<Self, InvalidBatchLimits> {
        if max_rows == 0 {
            return Err(InvalidBatchLimits::ZeroRows);
        }
        if max_bytes == 0 {
            return Err(InvalidBatchLimits::ZeroBytes);
        }
        Ok(Self {
            max_rows,
            max_bytes,
        })
    }

    pub fn max_rows(self) -> usize {
        self.max_rows
    }

    pub fn max_bytes(self) -> usize {
        self.max_bytes
    }
}

/// Lazy, non-overlapping ranges covering a slice without cloning its records.
///
/// Exact byte boundaries are included. A row exceeding the byte limit is yielded
/// alone so processing always advances. Each visited row is estimated once,
/// including a cached estimate when a row starts the next batch.
pub struct BatchRanges<'a, T, F> {
    data: &'a [T],
    limits: BatchLimits,
    estimate: F,
    next_start: usize,
    next_row_bytes: Option<usize>,
}

impl<'a, T, F: FnMut(&T) -> usize> BatchRanges<'a, T, F> {
    /// ```
    /// use sctys_rust_utilities::misc::batching::{BatchLimits, BatchRanges};
    /// let sizes = [4, 6, 11, 1];
    /// let limits = BatchLimits::new(3, 10)?;
    /// let ranges: Vec<_> = BatchRanges::new(&sizes, limits, |size| *size).collect();
    /// assert_eq!(ranges, vec![0..2, 2..3, 3..4]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(data: &'a [T], limits: BatchLimits, estimate: F) -> Self {
        Self {
            data,
            limits,
            estimate,
            next_start: 0,
            next_row_bytes: None,
        }
    }
}

impl<T, F: FnMut(&T) -> usize> Iterator for BatchRanges<'_, T, F> {
    type Item = Range<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_start == self.data.len() {
            return None;
        }
        let start = self.next_start;
        let mut end = start;
        let mut bytes = 0usize;
        while end < self.data.len() && end - start < self.limits.max_rows {
            let row_bytes = self
                .next_row_bytes
                .take()
                .unwrap_or_else(|| (self.estimate)(&self.data[end]));
            // Subtraction also detects sums above usize::MAX without overflow.
            if end > start && row_bytes > self.limits.max_bytes.saturating_sub(bytes) {
                self.next_row_bytes = Some(row_bytes);
                break;
            }
            bytes = bytes.saturating_add(row_bytes);
            end += 1;
            if row_bytes > self.limits.max_bytes {
                break;
            }
        }
        self.next_start = end;
        Some(start..end)
    }
}

impl<T, F: FnMut(&T) -> usize> FusedIterator for BatchRanges<'_, T, F> {}

/// Executes ranges sequentially, returning the first insertion error unchanged.
///
/// Does not clear buffers, retry, or undo earlier successful inserts. Ranges are
/// taken from the supplied iterator (normally [`BatchRanges`]) without prefetching
/// another batch after failure. The callback can borrow the original slice.
pub async fn insert_batches<I, F, Fut, E>(batches: I, mut insert: F) -> Result<(), E>
where
    I: IntoIterator<Item = Range<usize>>,
    F: FnMut(Range<usize>) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    for range in batches {
        insert(range).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, future::ready};

    fn ranges(sizes: &[usize], rows: usize, bytes: usize) -> Vec<Range<usize>> {
        BatchRanges::new(sizes, BatchLimits::new(rows, bytes).unwrap(), |n| *n).collect()
    }

    #[test]
    fn test_limits_reject_zero() {
        assert_eq!(BatchLimits::new(0, 10), Err(InvalidBatchLimits::ZeroRows));
        assert_eq!(BatchLimits::new(10, 0), Err(InvalidBatchLimits::ZeroBytes));
        assert!(BatchLimits::new(0, 0).is_err());
        let limits = BatchLimits::new(2, 10).unwrap();
        assert_eq!((limits.max_rows(), limits.max_bytes()), (2, 10));
    }

    #[test]
    fn test_empty_and_count_boundaries() {
        assert!(ranges(&[], 2, 10).is_empty());
        assert_eq!(ranges(&[1; 5], 2, 10), vec![0..2, 2..4, 4..5]);
        assert_eq!(ranges(&[0; 4], 2, 10), vec![0..2, 2..4]);
    }

    #[test]
    fn test_byte_boundaries_and_oversized_rows() {
        assert_eq!(ranges(&[4, 6, 1, 9], 10, 10), vec![0..2, 2..4]);
        assert_eq!(ranges(&[4, 7, 3], 10, 10), vec![0..1, 1..3]);
        assert_eq!(
            ranges(&[11, 0, 12, 1], 10, 10),
            vec![0..1, 1..2, 2..3, 3..4]
        );
        assert_eq!(ranges(&[10, 0, 1], 10, 10), vec![0..2, 2..3]);
    }

    #[test]
    fn test_saturating_arithmetic() {
        assert_eq!(
            ranges(&[usize::MAX, 1, 0], 10, usize::MAX),
            vec![0..1, 1..3]
        );
        assert_eq!(ranges(&[usize::MAX, usize::MAX], 10, 10), vec![0..1, 1..2]);
        assert_eq!(ranges(&[usize::MAX - 1, 1], 10, usize::MAX), vec![0..2]);
    }

    #[test]
    fn test_coverage_order_and_single_estimation() {
        let sizes = [0, 5, 6, 1, 11, 0, 2, 3, 4];
        for rows in 1..=sizes.len() {
            for bytes in 1..=12 {
                let mut estimated = Vec::new();
                let batches: Vec<_> =
                    BatchRanges::new(&sizes, BatchLimits::new(rows, bytes).unwrap(), |n| {
                        estimated.push(*n);
                        *n
                    })
                    .collect();
                assert_eq!(estimated, sizes);
                let indices: Vec<_> = batches.iter().cloned().flatten().collect();
                assert_eq!(indices, (0..sizes.len()).collect::<Vec<_>>());
                for range in batches {
                    assert!(!range.is_empty() && range.len() <= rows);
                    assert!(range.len() == 1 || sizes[range].iter().sum::<usize>() <= bytes);
                }
            }
        }
        let mut empty = BatchRanges::new(&[] as &[usize], BatchLimits::new(1, 1).unwrap(), |n| *n);
        assert_eq!(empty.next(), None);
        assert_eq!(empty.next(), None);
    }

    #[tokio::test]
    async fn test_sequential_insertion_and_early_failure_preserve_buffer() {
        let mut data = Vec::with_capacity(8);
        data.extend([1; 5]);
        let capacity = data.capacity();
        for fail_at in [None, Some(0), Some(1), Some(2)] {
            let calls = Cell::new(0);
            let estimates = Cell::new(0);
            let mut inserted = Vec::new();
            let batches = BatchRanges::new(&data, BatchLimits::new(2, 10).unwrap(), |_| {
                estimates.set(estimates.get() + 1);
                1
            });
            let result = insert_batches(batches, |range| {
                let index = calls.get();
                calls.set(index + 1);
                inserted.push(range);
                ready(if fail_at == Some(index) {
                    Err("insert failed")
                } else {
                    Ok(())
                })
            })
            .await;
            let expected = [0..2, 2..4, 4..5];
            let count = fail_at.map_or(3, |index| index + 1);
            assert_eq!(inserted, expected[..count]);
            assert_eq!(result, fail_at.map_or(Ok(()), |_| Err("insert failed")));
            assert_eq!(estimates.get(), (count * 2).min(5));
            assert_eq!(data, [1; 5]);
            assert_eq!(data.capacity(), capacity);
        }
    }

    #[tokio::test]
    async fn test_empty_insertion_does_not_call_backend() {
        insert_batches(
            std::iter::empty(),
            |_| -> std::future::Ready<Result<(), ()>> { panic!("empty batches must not insert") },
        )
        .await
        .unwrap();
    }
}
