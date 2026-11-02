# Scraper log batching

`pipeline::scrape_log` provides an ordered log buffer and separate deduplication
helpers. Applications supply log rows, persistence sinks, table names, and
workflow boundaries.

## Buffer configuration

`ScraperLogBatch::default()` uses `DEFAULT_SCRAPER_LOG_BATCH_SIZE`, currently
**100 rows**. Pass a custom threshold to `ScraperLogBatch::new(batch_size)`:

```rust
use sctys_rust_utilities::pipeline::scrape_log::ScraperLogBatch;

let mut logs = ScraperLogBatch::<LogRow>::new(500);
```

The threshold counts buffered rows. Zero is normalized to one. A threshold of
one makes any nonempty buffer ready to flush; insertion still happens only when
the caller invokes a flush method.

## Buffer operations

- `push(row)` and `extend(rows)` append rows in order.
- `is_ready()` checks whether the row threshold has been reached.
- `flush_if_ready(&mut sink)` inserts the buffer only when it is ready.
- `flush(&mut sink)` inserts any remaining rows, regardless of the threshold.
- `is_empty()` checks whether any rows remain buffered.

Sinks implement `pipeline::parse::RowSink`. The buffer skips empty inserts and
clears rows only after the sink reports success. The threshold triggers insertion
of the whole buffer; it is not a hard maximum request size.

Callers choose when to check the threshold and when to flush a final remainder.
They also own persistence ordering and error recovery. For example, enqueue
success logs after their corresponding records have been persisted, and flush
remaining logs before completing a workflow.

The buffer has no timer or durable storage. Cancellation, panic, or process
termination can lose pending rows. A failed insert retains the rows, but the
backend may already have committed them; retrying can therefore create duplicates.

## Separate deduplication steps

`deduplicate_log_tables(table_names, callback)` invokes a fallible callback once
per distinct table name, in first-seen order. It stops at the first failure and
returns `LogDeduplicationError`, preserving the table name and underlying error.

`deduplicate_clickhouse_log_tables(click_house, &client, table_names)` adapts this
operation to the existing ClickHouse deduplication utility. Table names must
belong to the supplied database/client context. This operation does not flush
buffers: callers must first finish and flush every writer using those tables.

Applications own module-to-table mappings, scheduling, notifications, and retry
policy. Deduplication helpers neither send notifications nor retry failures.

## Optional group coordination

`run_scrape_group(scrape_future, table_names, callback)` awaits a fallible
scraping workflow, then deduplicates its distinct table names. It returns the
workflow's output on success. `run_scrape_group_with_clickhouse` provides the
ClickHouse adapter for the same sequence.

Both group helpers:

- Wait for the supplied workflow to finish before deduplication.
- Skip deduplication if that workflow returns an error or panics.
- Deduplicate repeated table names once, in first-seen order.
- Stop at the first deduplication failure and preserve its table name.

The supplied future must await every job, including its final flush, rather than
detach jobs. The helpers do not catch panics or roll back earlier writes.
`ScrapeGroupError` distinguishes workflow failures from deduplication failures.

## Validation

The unit tests use an in-memory sink and need no database or credentials:

```sh
cargo test --lib pipeline::scrape_log::tests
```

They cover thresholds, final remainders, independent buffers, custom sizes, zero
normalization, ordering, empty inserts, buffer retention after failure, group
completion ordering, repeated tables, error propagation, and panic propagation.
