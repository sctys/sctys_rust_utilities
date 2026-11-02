//! Per-run scraper-log buffering and workflow-owned deduplication.
//!
//! Flush ready batches after persistence and at input boundaries, and flush any
//! remainder before finalization or returning an error. Only enqueue success
//! logs after their scraped records have been persisted. This buffer is not a
//! durable queue: cancellation or process termination can lose pending logs.

use super::{parse::RowSink, storage::ClickHouseSink};
use crate::{click_house::ClickHouse, logger::ProjectLogger};
use clickhouse::Row;
use serde::Serialize;
use std::{collections::HashSet, future::Future, time::Duration};

/// Flushes ready scraper logs, or a final remainder when `force` is true.
///
/// Network errors and timeouts pause the caller until storage recovers, using
/// delays of 1, 2, 4, 8, 16, then 30 seconds between attempts. Other errors are
/// returned immediately. Only the retained log batch is retried; callers must
/// enqueue logs once, after persisting their response records.
///
/// Ambiguous writes can duplicate rows, so workflow-owned deduplication remains
/// necessary. Cancelling this future retains rows only while the batch remains
/// alive; this helper does not provide durable recovery after process exit.
///
/// # Errors
/// Returns non-network ClickHouse insertion errors without retrying them.
pub async fn flush_clickhouse_logs<R>(
    logs: &mut ScraperLogBatch<R>,
    force: bool,
    sink: &mut ClickHouseSink<'_, '_>,
    logger: &ProjectLogger,
) -> Result<(), clickhouse::error::Error>
where
    R: Serialize + Row + std::fmt::Debug + Send + Sync,
{
    let table = sink.table.clone();
    let retries = flush_logs_with_retry(logs, force, sink, |attempt, error, delay| {
        logger.log_warn(&format!(
            "Unable to flush scraper logs to {table} (attempt {attempt}); scraping for this module is paused, retrying in {delay:?}: {error:?}"
        ));
    })
    .await?;
    if retries > 0 {
        logger.log_info(&format!(
            "Flushed scraper logs to {table} after {retries} retries; resuming scraping"
        ));
    }
    Ok(())
}

async fn flush_logs_with_retry<R: Sync, S: RowSink<R, Error = clickhouse::error::Error>>(
    logs: &mut ScraperLogBatch<R>,
    force: bool,
    sink: &mut S,
    mut on_retry: impl FnMut(usize, &clickhouse::error::Error, Duration) + Send,
) -> Result<usize, clickhouse::error::Error> {
    if logs.is_empty() || (!force && !logs.is_ready()) {
        return Ok(0);
    }
    let mut delay = Duration::from_secs(1);
    let mut retries = 0usize;
    loop {
        match logs.flush(sink).await {
            Ok(()) => return Ok(retries),
            Err(error)
                if matches!(
                    error,
                    clickhouse::error::Error::Network(_) | clickhouse::error::Error::TimedOut
                ) =>
            {
                retries = retries.saturating_add(1);
                on_retry(retries, &error, delay);
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2).min(Duration::from_secs(30));
            }
            Err(error) => return Err(error),
        }
    }
}

/// Identifies the table whose final deduplication failed.
#[derive(Debug, thiserror::Error)]
#[error("Unable to deduplicate scraper log table {table}: {source}")]
pub struct LogDeduplicationError<E> {
    pub table: String,
    #[source]
    pub source: E,
}

/// A scraping failure skips deduplication; a deduplication failure retains its table.
#[derive(Debug, thiserror::Error)]
pub enum ScrapeGroupError<S, D> {
    #[error("Scraper group failed: {0}")]
    Scrape(#[source] S),
    #[error(transparent)]
    Deduplicate(#[from] LogDeduplicationError<D>),
}

/// Deduplicates distinct table names once each, in first-seen order.
///
/// Stops on the first error without retrying or rerunning earlier tables. Names
/// must identify tables within the same database/client context.
pub async fn deduplicate_log_tables<I, F, Fut, E>(
    tables: I,
    mut deduplicate: F,
) -> Result<(), LogDeduplicationError<E>>
where
    I: IntoIterator<Item = String>,
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    let mut seen = HashSet::new();
    for table in tables {
        if seen.insert(table.clone()) {
            deduplicate(table.clone())
                .await
                .map_err(|source| LogDeduplicationError { table, source })?;
        }
    }
    Ok(())
}

/// Runs a scraping workflow to completion before deduplicating its log tables.
///
/// Every job must finish persistence and flush its logs before returning. For
/// concurrent jobs, the supplied future must join all jobs (including their
/// flushes), rather than detach them. Errors and panics skip final deduplication;
/// the original scraper error is preserved. Notifications remain caller-owned.
///
/// ```
/// use sctys_rust_utilities::pipeline::scrape_log::{run_scrape_group, ScrapeGroupError};
/// # async fn example() -> Result<(), ScrapeGroupError<&'static str, &'static str>> {
/// let output = run_scrape_group(
///     async { Ok::<_, &'static str>(42) }, // Await every scraper and flush here.
///     ["shared_log", "shared_log"].map(String::from),
///     |_table| async { Ok::<_, &'static str>(()) }, // Deduplicate this table.
/// ).await?;
/// assert_eq!(output, 42);
/// # Ok(())
/// # }
/// ```
pub async fn run_scrape_group<T, S, D, I, F, Fut>(
    scrape: impl Future<Output = Result<T, S>>,
    tables: I,
    deduplicate: F,
) -> Result<T, ScrapeGroupError<S, D>>
where
    I: IntoIterator<Item = String>,
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<(), D>>,
{
    let result = scrape.await.map_err(ScrapeGroupError::Scrape)?;
    deduplicate_log_tables(tables, deduplicate).await?;
    Ok(result)
}

/// ClickHouse adapter for a workflow's explicit log-deduplication step.
pub async fn deduplicate_clickhouse_log_tables(
    click_house: &ClickHouse<'_>,
    client: &clickhouse::Client,
    tables: impl IntoIterator<Item = String>,
) -> Result<(), LogDeduplicationError<clickhouse::error::Error>> {
    deduplicate_log_tables(tables, |table| async move {
        click_house.deduplication_on_table(client, &table).await
    })
    .await
}

/// ClickHouse adapter for a scraping group followed by one deduplication step.
pub async fn run_scrape_group_with_clickhouse<T, S>(
    scrape: impl Future<Output = Result<T, S>>,
    tables: impl IntoIterator<Item = String>,
    click_house: &ClickHouse<'_>,
    client: &clickhouse::Client,
) -> Result<T, ScrapeGroupError<S, clickhouse::error::Error>> {
    run_scrape_group(scrape, tables, |table| async move {
        click_house.deduplication_on_table(client, &table).await
    })
    .await
}

/// Conservative default for scraper logs, balancing insert overhead and freshness.
pub const DEFAULT_SCRAPER_LOG_BATCH_SIZE: usize = 100;

/// An ordered log buffer. A failed insert retains every pending row for recovery.
///
/// The threshold triggers a flush of the whole buffer; it is not a hard maximum
/// request size. One response may produce more rows than the configured size.
pub struct ScraperLogBatch<R> {
    rows: Vec<R>,
    batch_size: usize,
}

impl<R> Default for ScraperLogBatch<R> {
    fn default() -> Self {
        Self::new(DEFAULT_SCRAPER_LOG_BATCH_SIZE)
    }
}

impl<R> ScraperLogBatch<R> {
    /// A size of one restores immediate flushing at persistence/input boundaries.
    /// Zero is normalized to one so a configuration mistake cannot disable writes.
    pub fn new(batch_size: usize) -> Self {
        Self {
            rows: Vec::new(),
            batch_size: batch_size.max(1),
        }
    }

    pub fn push(&mut self, row: R) {
        self.rows.push(row);
    }

    pub fn extend(&mut self, rows: impl IntoIterator<Item = R>) {
        self.rows.extend(rows);
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn is_ready(&self) -> bool {
        self.rows.len() >= self.batch_size
    }
}

impl<R: Sync> ScraperLogBatch<R> {
    pub async fn flush_if_ready<S: RowSink<R>>(&mut self, sink: &mut S) -> Result<(), S::Error> {
        if self.is_ready() {
            self.flush(sink).await?;
        }
        Ok(())
    }

    /// Empty buffers do not issue inserts. Rows are cleared only after success.
    /// An ambiguous backend error may have committed rows; retries can duplicate
    /// them, so callers must retain their existing deduplication policy.
    pub async fn flush<S: RowSink<R>>(&mut self, sink: &mut S) -> Result<(), S::Error> {
        if !self.rows.is_empty() {
            sink.insert(&self.rows).await?;
            self.rows.clear();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::{cell::RefCell, collections::VecDeque, future::ready};

    #[tokio::test]
    async fn test_group_waits_for_all_jobs_and_flushes_then_deduplicates_once() {
        let events = RefCell::new(Vec::new());
        let scrape = async {
            tokio::join!(
                async {
                    events.borrow_mut().push("scrape:a".to_string());
                    tokio::task::yield_now().await;
                    events.borrow_mut().push("flush:a".to_string());
                },
                async {
                    events.borrow_mut().push("scrape:b".to_string());
                    tokio::task::yield_now().await;
                    events.borrow_mut().push("flush:b".to_string());
                }
            );
            Ok::<_, &'static str>(42)
        };
        let result = run_scrape_group(
            scrape,
            ["shared", "shared", "other", "shared", "other"].map(String::from),
            |table| {
                events.borrow_mut().push(format!("dedup:{table}"));
                ready(Ok::<_, &'static str>(()))
            },
        )
        .await
        .unwrap();
        assert_eq!(result, 42);
        let events = events.into_inner();
        assert!(events[..4].contains(&"flush:a".to_string()));
        assert!(events[..4].contains(&"flush:b".to_string()));
        assert_eq!(&events[4..], ["dedup:shared", "dedup:other"]);
    }

    #[tokio::test]
    async fn test_scrape_or_flush_error_skips_group_deduplication() {
        for failure in ["scrape failed", "flush failed"] {
            let result = run_scrape_group(
                ready(Err::<(), _>(failure)),
                ["shared".to_string()],
                |_| -> std::future::Ready<Result<(), &'static str>> {
                    panic!("deduplication must not run after a scraper/flush error");
                },
            )
            .await;
            assert!(matches!(result, Err(ScrapeGroupError::Scrape(error)) if error == failure));
        }
    }

    #[tokio::test]
    async fn test_deduplication_failure_names_table_and_stops_later_steps() {
        let mut visited = Vec::new();
        let result = run_scrape_group(
            ready(Ok::<_, &'static str>(())),
            ["first", "first", "failed", "last"].map(String::from),
            |table| {
                let result = if table == "failed" {
                    Err("database failed")
                } else {
                    Ok(())
                };
                visited.push(table);
                ready(result)
            },
        )
        .await;
        assert_eq!(visited, ["first", "failed"]);
        let Err(ScrapeGroupError::Deduplicate(error)) = result else {
            panic!("expected a deduplication error");
        };
        assert_eq!(error.table, "failed");
        assert_eq!(error.source, "database failed");
    }

    #[tokio::test]
    async fn test_empty_table_list_still_runs_scrape_workflow() {
        let result = run_scrape_group(
            ready(Ok::<_, &'static str>(42)),
            [],
            |_| -> std::future::Ready<Result<(), &'static str>> {
                panic!("no tables to deduplicate");
            },
        )
        .await
        .unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    #[should_panic(expected = "scraper panic")]
    async fn test_group_propagates_panics_without_deduplication() {
        let _ = run_scrape_group(
            std::future::poll_fn(|_| -> std::task::Poll<Result<(), &'static str>> {
                panic!("scraper panic");
            }),
            ["shared".to_string()],
            |_| -> std::future::Ready<Result<(), &'static str>> {
                panic!("deduplication must not run after a panic");
            },
        )
        .await;
    }

    #[derive(Default)]
    struct Sink {
        batches: Vec<Vec<u32>>,
        fail: bool,
    }

    #[async_trait]
    impl RowSink<u32> for Sink {
        type Error = &'static str;
        async fn insert(&mut self, rows: &[u32]) -> Result<(), Self::Error> {
            if self.fail {
                return Err("insert failed");
            }
            self.batches.push(rows.to_vec());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_threshold_and_final_remainder_preserve_order() {
        let mut logs = ScraperLogBatch::new(3);
        let mut sink = Sink::default();
        for row in 0..8 {
            logs.push(row);
            logs.flush_if_ready(&mut sink).await.unwrap();
        }
        assert_eq!(sink.batches, [vec![0, 1, 2], vec![3, 4, 5]]);
        logs.flush(&mut sink).await.unwrap();
        logs.flush(&mut sink).await.unwrap();
        assert_eq!(sink.batches, [vec![0, 1, 2], vec![3, 4, 5], vec![6, 7]]);
        assert!(logs.is_empty());
    }

    #[tokio::test]
    async fn test_failed_flush_retains_rows_for_recovery() {
        let mut logs = ScraperLogBatch::new(2);
        logs.extend([1, 2, 3]);
        let mut sink = Sink {
            fail: true,
            ..Sink::default()
        };
        assert_eq!(logs.flush_if_ready(&mut sink).await, Err("insert failed"));
        assert!(logs.is_ready());
        sink.fail = false;
        logs.flush(&mut sink).await.unwrap();
        assert_eq!(sink.batches, [vec![1, 2, 3]]);
        assert!(logs.is_empty());
    }

    #[tokio::test]
    async fn test_module_sizes_and_independent_buffers() {
        let mut first = ScraperLogBatch::new(1);
        let mut second = ScraperLogBatch::new(3);
        let mut sink = Sink::default();
        first.push(1);
        second.extend([2, 3]);
        first.flush_if_ready(&mut sink).await.unwrap();
        second.flush_if_ready(&mut sink).await.unwrap();
        assert_eq!(sink.batches, [vec![1]]);
        second.flush(&mut sink).await.unwrap();
        assert_eq!(sink.batches, [vec![1], vec![2, 3]]);
    }

    #[test]
    fn test_default_and_zero_size() {
        let mut logs = ScraperLogBatch::<u32>::default();
        logs.extend(0..(DEFAULT_SCRAPER_LOG_BATCH_SIZE - 1) as u32);
        assert!(!logs.is_ready());
        logs.push(99);
        assert!(logs.is_ready());
        let mut immediate = ScraperLogBatch::new(0);
        immediate.push(1);
        assert!(immediate.is_ready());
    }

    #[derive(Default)]
    struct LogSink {
        errors: VecDeque<clickhouse::error::Error>,
        attempts: Vec<Vec<u32>>,
    }

    #[async_trait]
    impl RowSink<u32> for LogSink {
        type Error = clickhouse::error::Error;

        async fn insert(&mut self, rows: &[u32]) -> Result<(), Self::Error> {
            self.attempts.push(rows.to_vec());
            match self.errors.pop_front() {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }
    }

    #[tokio::test]
    async fn test_retry_flush_respects_threshold_and_forces_final_remainder() {
        let mut logs = ScraperLogBatch::new(2);
        logs.push(42);
        let mut sink = LogSink::default();
        flush_logs_with_retry(&mut logs, false, &mut sink, |_, _, _| {})
            .await
            .unwrap();
        assert!(sink.attempts.is_empty());
        assert!(!logs.is_empty());
        flush_logs_with_retry(&mut logs, true, &mut sink, |_, _, _| {})
            .await
            .unwrap();
        assert_eq!(sink.attempts, [vec![42]]);
        assert!(logs.is_empty());
    }

    fn connection_error() -> clickhouse::error::Error {
        clickhouse::error::Error::Network(Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connection refused",
        )))
    }

    #[tokio::test(start_paused = true)]
    async fn test_log_flush_recovers_without_losing_or_reenqueueing_rows() {
        let mut logs = ScraperLogBatch::new(2);
        logs.extend([1, 2, 3]);
        let mut sink = LogSink {
            errors: [connection_error(), clickhouse::error::Error::TimedOut].into(),
            ..LogSink::default()
        };
        let mut retries = Vec::new();
        flush_logs_with_retry(&mut logs, true, &mut sink, |attempt, _, _| {
            retries.push(attempt);
        })
        .await
        .unwrap();
        assert_eq!(retries, [1, 2]);
        assert_eq!(sink.attempts, vec![vec![1, 2, 3]; 3]);
        assert!(logs.is_empty());
        // A later boundary/final flush must not write the successful batch again.
        flush_logs_with_retry(&mut logs, true, &mut sink, |_, _, _| {
            panic!("empty batch must not retry");
        })
        .await
        .unwrap();
        assert_eq!(sink.attempts.len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn test_log_flush_survives_prolonged_outage_and_caps_backoff() {
        let mut logs = ScraperLogBatch::new(1);
        logs.push(42);
        let mut sink = LogSink {
            errors: (0..12).map(|_| connection_error()).collect(),
            ..LogSink::default()
        };
        let start = tokio::time::Instant::now();
        let mut delays = Vec::new();
        let retries = flush_logs_with_retry(&mut logs, true, &mut sink, |_, _, delay| {
            delays.push(delay.as_secs())
        })
        .await
        .unwrap();
        assert_eq!(retries, 12);
        assert_eq!(delays, [1, 2, 4, 8, 16, 30, 30, 30, 30, 30, 30, 30]);
        assert_eq!(start.elapsed(), Duration::from_secs(241));
        assert_eq!(sink.attempts, vec![vec![42]; 13]);
        assert!(logs.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn test_cancelling_wait_retains_pending_batch() {
        let mut logs = ScraperLogBatch::new(1);
        logs.push(42);
        let mut sink = LogSink {
            errors: [connection_error()].into(),
            ..LogSink::default()
        };
        let result = tokio::time::timeout(
            Duration::from_millis(500),
            flush_logs_with_retry(&mut logs, true, &mut sink, |_, _, _| {}),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(sink.attempts, [vec![42]]);
        assert!(!logs.is_empty());
        flush_logs_with_retry(&mut logs, true, &mut sink, |_, _, _| {})
            .await
            .unwrap();
        assert_eq!(sink.attempts.last(), Some(&vec![42]));
        assert!(logs.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn test_permanent_error_after_network_failures_preserves_batch() {
        let mut logs = ScraperLogBatch::new(1);
        logs.push(42);
        let mut sink = LogSink {
            errors: [
                connection_error(),
                clickhouse::error::Error::BadResponse("access denied".into()),
            ]
            .into(),
            ..LogSink::default()
        };
        let error = flush_logs_with_retry(&mut logs, true, &mut sink, |_, _, _| {})
            .await
            .unwrap_err();
        assert!(matches!(error, clickhouse::error::Error::BadResponse(_)));
        assert_eq!(sink.attempts, vec![vec![42]; 2]);
        assert!(!logs.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn test_log_flush_does_not_retry_database_errors() {
        let mut logs = ScraperLogBatch::new(1);
        logs.push(42);
        let mut sink = LogSink {
            errors: [clickhouse::error::Error::BadResponse(
                "unknown table".into(),
            )]
            .into(),
            ..LogSink::default()
        };
        let error = flush_logs_with_retry(&mut logs, true, &mut sink, |_, _, _| {
            panic!("database errors must fail immediately");
        })
        .await
        .unwrap_err();
        assert!(matches!(error, clickhouse::error::Error::BadResponse(_)));
        assert_eq!(sink.attempts, [vec![42]]);
        assert!(!logs.is_empty());
    }
}
