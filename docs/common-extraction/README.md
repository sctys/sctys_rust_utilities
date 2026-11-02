# Shared framework APIs

This guide describes the reusable mechanisms implemented in this library.
Callers supply domain definitions and policies through traits, metadata, and
configured adapters.

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

## Shared conventions

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

## Contracts and failure behavior

- `BackupRoot` chooses whether backup paths are relative to a project directory
  or a storage root. Project identities, prefixes, and legacy names are inputs.
- Active filtering preserves input order. Selection and export scopes remain
  explicit, including resume behavior and explicitly selected inactive modules.
- `TimeWindow` and `EndBound` specify row-check and export boundaries separately.
- `DatasetExportSpec::replace_staging` controls replacement of the selected
  staging file; unrelated files are never selected implicitly.
- Borrowed inserts leave the caller's buffer intact. Owned flushes clear rows
  only after success. Partial insertion is not transactional and adds no retry.
- Parser outputs flush in declaration order, including final rows below the
  threshold. Earlier outputs may have committed when a later output fails.
- `decode_append` supports atomic-batch and per-document commit policies.
  `decode_parallel_append` preserves declaration order and atomic output commit.
- `InputRecordFactory` combines caller-defined input decoding with shared payload
  and modification-time attachment. Exceptional formats can implement
  `SavedRecordFactory` directly.
- Retry-header preparation, challenge replay, and pagination continuation have
  separate reasons and explicit preparation/budget policies.
- `TransportSession::header_context` exposes disjoint mutable request, proxy,
  and domain borrows across async hooks. Session state remains local to a run.
- Clearance persistence supports disabled, best-effort, and required policies.
  Best-effort failures warn and retain in-memory operation; required failures
  return an error.
- Archive traversal skips symlinks, rejects non-Unicode directory tokens, and
  requires relative archive member paths.
- Storage export uploads before copying and removing staging files. Failed copies
  retain the source; there is no rollback or implicit failure cleanup.
- Factories and engines return errors. An explicitly selected reporting boundary
  can send a notification and apply the caller's termination policy.
- Async workflows use async file operations and subprocess waiting. Compression
  runs on the blocking pool. Synchronous and async export entry points share
  command construction and error handling.

## Validation

The integration fixtures compile this library through its public interface:

- `tests/framework_contracts.rs` exercises generated record families, renamed
  macro imports, serialization/column order, adapter traits, and async borrows.
- `tests/workflow_failures.rs` checks preparation errors, bookkeeping filtering,
  archive failures, and saved-file metadata with temporary filesystem fixtures.
- Inline pipeline tests exercise ordering, retry/replay behavior, batching,
  final flushes, partial failures, and reporting boundaries.
- Project tests inject root and secret readers rather than initialize services.

Run the focused suites and static checks locally:

```sh
cargo test --lib pipeline::
cargo test --lib project::
cargo test --test framework_contracts --test workflow_failures
cargo test --doc
cargo clippy --all-targets --all-features
cargo fmt --check
git diff --check
```

Live service tests require their own environment configuration and credentials;
they are separate from the pure framework contract tests.
