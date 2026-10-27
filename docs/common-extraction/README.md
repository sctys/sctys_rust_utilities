# Shared framework implementation

The initial framework implementation focused on this library. A follow-up on
October 24, 2026, updated the three application `common` crates to adopt the
shared row, record-key, query, time-window, path, file-name, and insert-time
conventions described below. This is a focused consumer rollout, not completion
of the plan's full phase G removal audit.

## Implementation ownership

| Concern | Library implementation | Caller supplies |
| --- | --- | --- |
| Roots, layouts and file names | `project::paths`, `project::files` | Project/root identities, layout, templates, legacy prefix and backup-root choice |
| Staged initialization and secret failures | `project::services` | Logger settings, secret selectors, chosen services; owners outlive borrowed contexts |
| Module and dataset contracts | `pipeline::metadata` | Concrete source/module/dataset types and declarations |
| Stable active filtering and dataset expansion | `metadata::{active_modules, ordered_datasets}` | Ordered lists and associations |
| Selection and parse sequencing | Existing `ModuleSelection`, `run_parse_steps`, plus lending `run_parse_with` | Explicit selection, resume flag, fallible module hooks |
| Record/input/log/column contracts and generation | `pipeline::records`, `CutoffDateRecord`, exported `scraper_family!` and `scraper_family_with_adapters!` | Domain fields, cutoff policy and filename parsers; exceptional handwritten implementations use the same traits |
| File loading, metadata attachment and upload loop | `records::{load_file, load_and_upload, import_records}` | Filename/folder interpretation and exceptional payload decoder |
| Collection CRUD, indexes and date backup/removal | `storage::{CollectionStore, backup_collection_by_modified_date}` | Existing client, database/collection and modified-key names |
| Table loading/insertion/query/creation/finalization | `storage::{TableStore, ClickHouseSink}` | Existing clients, table/schema/window metadata and business queries |
| Export backend as well as stage ordering | `storage::{DatasetExportSpec, StorageExport}`, existing `export_dataset` | Independent row/export bounds, staging replacement, paths and destinations |
| Input filtering, requests, retries, replay and counters | `scrape::run_scraper` | Focused discovery, interpretation, transformation and persistence hooks |
| Transport/session setup and physical requests | `transport::TransportSession` | Transport/browser/proxy/Cloudflare settings; provider state stays per run |
| Cursor, input/output memory accounting and multi-output final flush | `parse::{parse_stream, parse_stream_and_then, parse_module, TypedOutput, OutputGroup}` | Typed output declarations, decoders, estimators, sinks, thresholds and finalizer |
| Borrowed insertion and owned-buffer clearing | `parse::{insert_borrowed, flush_owned, clear_after_success}` | Limits, estimator and sink; buffers remain intact after a failed insertion |
| Compression/upload/archive/cleanup sequence | `archive::{create_archive, create_archive_async, run_archive, StorageArchive}` | Explicit archive member paths, selected entries, cleanup paths and destinations |
| Import directory traversal | `archive::discover_import_files` | Folder depth and date/file predicates |
| Run modes and one error-notification boundary | `runner::{run, report_result}` | Domain operation hooks, run mode and removal exclusions |
| Report text and configured Slack routing | `reporting` | Names, route/channel and local termination decision |
| Scheduled log-count reporting | `reporting::{report_scrapes, ClickHouseLogCounts}` | Selected modules, cutoff, clock, table/column identities and legacy boolean predicate shape |

## Shared conventions found in downstream code

The downstream common crates repeated these values and message formats. The
library now owns them so consumers can import one definition:

| Convention | Shared API |
| --- | --- |
| Mongo record/log keys | `pipeline::records::{MODIFIED_FIELD, DATA_FIELD, SCRAPED_FIELD, INSERT_TIME_FIELD, CommonScraperKeys}` and `CollectionStore::new` |
| Common scraper record-key enum | `pipeline::records::CommonScraperKeys` |
| Cutoff-date row schema | `pipeline::records::{CutoffDateRecord, CUTOFF_DATE_FIELD, INSERT_TIME_FIELD}` |
| Bookkeeping dataset markers | `pipeline::metadata::{CUTOFF_DATE_DATASET_MARKER, SCRAPER_LOG_DATASET_MARKER, SCRAPED_DATASET_MARKER}` |
| Root environment names and layout labels | `project::paths` constants and `Layout::standard(project, backup_root)` |
| S3 buckets and archive folders | `project::cloud_storage` constants |
| Archive templates and Parquet extension | `project::files` constants and `files::is_parquet_file` |
| Slack secret category and channel selectors | `project::services::slack_channel_selectors(project)` |
| Repeated workflow summaries and warnings | `pipeline::reporting` message builders, including both run-date validation messages |
| Scraper log input query | `pipeline::records::RecordColumns::input_query(table)` |
| Scrape-count report query | `pipeline::reporting::LogTable::scraper_log(table, predicate)` |
| Shared HTTP response and missing-element error text | `pipeline::scrape::{response_status_message, response_error_message, missing_element_message, missing_either_element_message}` |
| Shared parser and filename-token errors | `pipeline::parse::{selector_parse_error_message, parse_value_error_message, document_parse_error_message, module_dataset_parse_error_message}` and `pipeline::records::{file_element_as_string, parse_file_element, missing_file_element_message, invalid_file_element_message}` |
| Dataset marker filtering and bookkeeping classification | `pipeline::metadata::{values_with_name_marker, is_bookkeeping_dataset_name}` |
| CSS selector parsing | `netdata::HtmlTag` |

## Consumer adoption audit (October 24, 2026)

All three consumer `common` crates now use the shared `CutoffDateRecord` schema;
their cutoff calculation and selection policies remain application-owned.
Football re-exports the shared `CommonScraperKeys`, and football and basketball
delegate scraper-log query construction to `RecordColumns::input_query` and
date-range checks to `TableStore::has_rows` with `TimeWindow`. Basketball also
uses the shared project-root, bucket, archive, file-name, Parquet-extension and
insert-time constants. Stock's export and cutoff query use the shared
`INSERT_TIME_FIELD`. All three expose the shared `netdata::HtmlTag`.

This follow-up passed a workspace check in each consumer and Clippy across all
targets and features of each `common` crate. The broader method-by-method phase G
review remains open; this audit records only the shared component adoption in
these common-layer areas.

The next consumer pass moved backup and row-count adapters, filename-token
conversion, parse-and-finalize ordering, and archive compression dispatch onto
shared APIs in all three crates. Stock now uses the shared file, AWS and
compression service factories. Basketball also delegates bookkeeping markers,
bookkeeping classification, S3 data-path construction and run-summary text.
Source-specific parser decoding, error context and memory thresholds remain
application policies. The broader phase G removal audit remains open.

The source-level scan also found a cursor transform-and-upsert loop in both
football `odds_portal` and basketball `oddsportal`. It applies each project's
season mapping to its local fixture record, so it remains domain logic. The
football and basketball visitor-challenge implementations also remain local as
required by the provider-hook boundary above.

The existing foundational APIs remain available. `run_parse_with` is additive:
its lending async closure can borrow a mutable backend across `await`, while
`run_parse_steps` retains its existing callback contract and delegates to it.
No provider definitions, concrete project identities, schedules or domain schemas
were added to the library.

## Contract and behavior notes

- Stock and football place backups below their project data directory; basketball
  places backups directly below the SSD data root. `BackupRoot` represents the
  distinction explicitly. Module prefixing and legacy names are caller values.
- Active discovery differs: stock already filters inactive modules; the sports
  repositories filter in selection. Shared filtering is stable and can accept
  an already-filtered list. An explicitly selected inactive module is still
  allowed; absent resume targets retain the active export scope.
- Row-window checks and exports have differing historical end inclusions.
  `TimeWindow` and `EndBound` keep those independent. No inclusive boundary is
  inferred from another operation's window.
- Football replaces the selected stale staging file; stock and basketball do not
  automatically adopt that policy. `DatasetExportSpec::replace_staging` is explicit.
- Stock/football borrowing versus basketball clear-on-success is represented by
  separate borrowed and owned operations, with one implementation of clearing.
  Partial insertion is not transactional, and a failed owned flush retains the
  whole buffer and allocation. No insertion retry is added.
- The parser engine decodes buffered input in declaration order, then flushes
  outputs in declaration order. Earlier successful outputs may already have
  been cleared when a later output fails. Final data is flushed below thresholds.
  `decode_append` owns the per-document decode loop, with atomic-batch and
  per-document commit settings for the existing differing failure-buffer contracts.
- Ordinary saved records use `InputRecordFactory`: the local hook produces only
  an input value. The shared implementation attaches payload and modification
  time and constructs the record. Exceptional formats may implement the full
  saved-record factory contract directly.
- Scraper retry-header and provider-replay preparation are distinct reasons.
  Replay budget consumption and preparation are explicit policy values. The old
  stock loop's nested retry placement must be checked when migrating its header
  hook; this library change does not change that application or certify its trace.
  Pagination continuation also has a separate, explicit preparation setting.
- `TransportSession::header_context` provides disjoint mutable request/proxy/domain
  borrows and selected-client access across async hooks. Provider implementations
  can combine this session with a local visitor session; no global mutable state
  or self-referential owning service object is required.
- Clearance Redis persistence has disabled, best-effort and required policies.
  It is actually connected when enabled. Best-effort failures warn and retain
  in-memory operation; required failures return an error.
- Archive traversal skips symlinks and rejects non-Unicode directory tokens.
  Archive member names must stay relative. These are explicit new API contracts;
  legacy application walkers remain unchanged until their adapters are reviewed.
- `StorageExport` uploads before copying and removing the staging file. There is
  no rollback or failure cleanup. Failed copies retain the source; stale-file
  deletion is restricted to the supplied staging filename.
- Factories and engines return errors; only the selected external reporting
  boundary sends a notification. Internal low-level clients retain their existing
  logging behavior. Production services are never initialized by the fake tests.
- Async workflows use async file operations and subprocess waiting; archive
  compression runs on the blocking pool. Existing synchronous ClickHouse export
  entry points remain available and share their command construction and error
  handling with the new async export method. Their SQL arguments are fixture-tested.

## Method inventory

`method-ownership.tsv` inventories 5,000+ declarations from the current common
crates and source-specific scraper/parser/runner modules. Each row records the
repository, source path, line, declaration context, method and ownership target.
This is a baseline routing inventory, including private helpers and source-domain
methods to expose copied workflow overrides. It is not a claim that application
migration or a semantic removal audit has happened. Generated declarations are
represented by their generator and record contracts.

Before migrating a source, compare its operation trace and serialization fixtures
against this inventory. In particular, inspect `parse_source_dataset`,
`ParserForScraped`, Barchart retry-header overrides, visitor-session hooks,
Betting Pro/Euro Basketball output declarations, and handwritten field parsers.
An application hook may decode a payload or issue one query/request; it must not
contain another cursor, retry, import, archive or mode-dispatch loop.

## Validation fixtures

- Inline pipeline tests cover selection, ordering, all export errors, batch and
  parser failures, typed multi-output thresholds, repeated/final flushes, scraper
  success/retry/exhaustion/rejection/replay/fatal/persistence paths, runner modes,
  removal exclusions and a single notification boundary.
- `tests/framework_contracts.rs` is compiled as an external crate. It invokes the
  macro through a renamed import without bringing derive helpers into scope,
  declares stock and sports families, checks field/column order and JSON/BSON,
  exercises associated source types, and compiles async mutable session borrows.
- `tests/workflow_failures.rs` checks module-level cursor/row-count preparation,
  bookkeeping exclusions, every archive failure stage, and saved-file metadata
  attachment/import failure ordering with temporary filesystem fixtures.
- Project tests inject root and secret readers. Archive tests create and read a
  local compressed tar and verify copy-before-remove and failed-copy retention.

Live scraping and production uploads are not validation steps performed here.
The consumer workspace checks below compile the updated repositories without
calling production services.

## Current validation (October 23, 2026)

Passed 57 distinct focused tests covering the pipeline, project, batching,
external-consumer and workflow fixtures and ClickHouse export-command
compatibility. Eight doctests passed. `cargo check`,
`cargo clippy --all-targets --all-features`, `cargo fmt --check` and
`git diff --check` passed. No live services were called by these tests.

The shared cutoff-date row schema was also covered by the consumer contract test:

- `cargo test --test framework_contracts` (6 tests).
- `cargo fmt --check` and `git diff --check`.

## Consumer migration validation (October 24, 2026)

Passed:

- `cargo check --workspace --locked` in `isa_stock_rust`,
  `poisson_football_rust` and `poisson_basketball_rust`.
- `cargo clippy --locked --all-targets --all-features` for each repository's
  `common` crate.
- `cargo fmt` for each `common` crate and `git diff --check` in all three repos.

## Follow-up consumer validation (October 24, 2026)

Passed `cargo check --manifest-path common/Cargo.toml` in all three consumer
repositories. The shared filename-token, parse-finalizer and async archive
helpers passed focused unit tests; formatting and `git diff --check` passed in
the utilities repository. No production services were called.

The pre-existing `Cargo.toml` compression-feature and build-profile edits are
preserved; this framework implementation adds no dependencies.
