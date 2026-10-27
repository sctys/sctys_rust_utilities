# Shared scraping and parsing framework: sctys_rust_utilities

Revision 2 — October 23, 2026. This replaces the earlier helper-only extraction scope. Implementation and consumer-adoption status are recorded below.

Implementation note — October 23, 2026: the framework implementation request was
initially scoped to this repository. Shared project helpers, metadata, records,
storage/exports, scraper/session and parser engines, archive/import, runner
dispatch and reporting are now implemented here. On October 24, the three
consumer `common` crates adopted the shared cutoff row, record keys, query and
time-window helpers, and project/file conventions where duplicated. A follow-up
also moved shared storage backup/count adapters, filename-token conversion,
parse finalization ordering and async archive compression into the library;
basketball now uses shared bookkeeping markers, S3 path construction and report
builders, and stock uses the shared service factories. Their common-crate checks
passed. This does not complete the broader phase G removal audit. See [implementation ownership,
compatibility notes and validation fixtures](docs/common-extraction/README.md)
and the per-method inventory beside it.

## Outcome and current state

The goal is to change common behavior once in this library and have stock, football, and basketball inherit it. Shipping isolated helpers while leaving execution loops in three `common` crates does not meet this goal.

Already implemented and to be retained:

- `pipeline::ModuleSelection::new` and `pipeline::{ParseStep, run_parse_steps}`.
- `misc::batching::{BatchLimits, BatchRanges, insert_batches}`.
- `pipeline::export::{DatasetExport, export_dataset, ExportOutcome, ExportFailure, ExportStage}`. The adapter uses an associated error type and borrowed dataset arguments; outcomes distinguish completed/missing/empty, and failures preserve their source.

These were inspected for this revision. They are foundations, not evidence that the framework is complete. The old plan reports consumer migration pending; confirm each consumer's actual state before implementation. Preserve existing Rust and Cargo changes. Do not discard or recreate these APIs simply to expand scope.

## Coordinated plans

- [Stock](../isa_stock_rust/COMMON_EXTRACTION_PLAN.md)
- [Football](../poisson_football_rust/COMMON_EXTRACTION_PLAN.md)
- [Basketball](../poisson_basketball_rust/COMMON_EXTRACTION_PLAN.md)

All phases below are required. Pilot consumers and staged delivery control risk; they are not reasons to defer the main engines or declare completion early.

## Ownership and architecture

Library owns implementation; applications supply data and focused policies. Thin wrappers may preserve imports, but must delegate. A callback containing the old retry loop or parser loop is not an extraction.

| Existing application area | Shared implementation | Local definition or policy |
|---|---|---|
| `ProjectUtilities` | Service construction, secret resolution, initialization/error handling | Project ID, secret selectors, log settings, connection options, channel mapping |
| `ProjectPath` / `ProjectFile` | Root resolution, path/file construction, validation, date/extension helpers | Roots, project/source names, layout/naming rules, legacy exceptions |
| `ProjectMongoDB` / `ProjectClickHouse` | Common CRUD, row loading, insertion, backup/export/finalization operations | Database/collection/table identity, columns, keys, date column, specialized queries |
| `Modules` / `SourceDataset` contracts | Reusable metadata interfaces and common default algorithms | Concrete enums, mappings, schemas, active lists and schedules |
| Scraper records / `FileMetaInfo` | Record contracts, generation, conversions, file loading and metadata parsing | Fields, filename-to-field mapping, exceptional formats |
| `ModuleScraper` | Input filtering, client/session lifecycle, attempt loop, persistence, result/log handling | URLs, response interpretation, headers, challenge protocol, retry/session settings |
| `ModuleParser` / `SourceDatasetParser` / `ParserForScraped` | Cursor iteration, accumulation, accounting, flushing, persistence and reporting | Payload decoding, transformations, output definitions |
| `RunTemplate` / `ScraperTemplate` / `ParserTemplate` | Dispatch, sequencing, import/archive/export/cleanup/report workflows | CLI values, schedules, cutoff/retention/window decisions |

Use a shared `pipeline` framework composed with existing `io`, `netdata`, logging, messenger, and secret utilities. Proposed additional module groups: `project` (config, paths, files, services), `pipeline::metadata`, `pipeline::records`, `pipeline::storage`, `pipeline::scrape`, `pipeline::parse`, `pipeline::archive`, and `pipeline::runner`. Final names may change; ownership must not.

Do not import application crates or encode their enum variants in utilities. Eightbo/Chuqi hosts, endpoints, cookie names, challenge markers, verification protocol, and provider-specific parameters stay in local `visitor_challenge.rs`. The shared scraper calls a provider hook at the proper stage; it contains no Eightbo branch. Generic provider hook orchestration is required, extracting the provider protocol is not.

## Phase A: compatibility inventory and contracts

1. Inventory every public method/default implementation in the three common crates and representative source-specific overrides. Assign each to shared implementation, local configuration, local domain hook, or documented exceptional algorithm. Include copied loops in source crates, not only `common` files.
2. Record behavioral differences and classify them as intentional policy or accidental divergence. Do not silently pick the newest repo as canonical. Preserve defaults first; any reconciliation that changes observable behavior must be named and tested separately.
3. Define focused shared metadata/configuration contracts. Replace concrete local `Source` return types with associated source/descriptor types. Keep local enums implementing shared interfaces; do not create one giant trait spanning CLI, parsing, transport, and storage.
4. Make policies declarative when possible: ordered module lists, dataset mappings, naming/layout specs, windows, limits, skip-removal flags, staging replacement, browser/session choices. The library performs filtering, stable deduplication, path construction and execution from those definitions.
5. Decide async hook borrowing and error contracts with compileable stock and sports examples before scaling. Hooks must work with per-run mutable sessions and existing async traits; avoid global mutable state and self-referential service containers.

Deliverable: method ownership matrix and contract tests/fixtures for paths, names, serialized records, execution traces and failure behavior. No method is left duplicated merely because it references a local enum today.

## Phase B: configured project helpers and storage adapters

1. Implement reusable project config, path/file builders and service factories. Local wrappers supply config only. Generic naming and layout mechanics belong here; application configuration selects them, with a narrow hook for truly exceptional formats.
2. Centralize initialization and error context. Existing clients borrow loggers/secrets: use staged factories and an explicitly borrowed service context, or justify an ownership redesign. Do not force all services to initialize for modes that do not need them.
3. Lift common `ProjectMongoDB`/`ProjectClickHouse` behavior into configured adapters over existing low-level clients. Metadata provides database/table/collection/columns/keys/window identities; shared code performs the operations. Keep domain query construction local where it expresses business logic.
4. Share backup/export adapters as well as lifecycle ordering. Reimplementing upload, path selection mechanics, and copy/remove sequences independently in each local `DatasetExport` impl is not the final design.
5. Preserve storage formats, naming, date boundaries and query semantics. This is not a schema redesign or query optimization exercise.

Gate: all three projects can configure the same helpers; equivalent inputs produce identical paths, filenames and operation arguments. Tests use temporary files and fake factories/backends, not real secrets. A common initialization/path-construction fix has one implementation site.

## Phase C: records and saved-file parsing

1. Move reusable `FileMetaInfo`, record/input/log contracts and column metadata behavior to the library, resolving dependencies through shared metadata instead of local `Modules`.
2. Generalize stock's `scraper_family!` or equivalent shared generation machinery. Application declarations own domain fields and parsers; library code generates repeated types/conversions/ordering/serialization integration. Use hygienic paths and external-consumer compile tests.
3. Centralize file read, modification-time attachment, filename/folder token handling, record construction orchestration and upload. Local hooks map tokens to domain fields or decode exceptional formats; they do not repeat file I/O or upload loops.
4. Preserve column/field order, BSON/JSON field names/types, query keys, ordering/equality/hash consistency, and legacy filename interpretation. Support exceptional handwritten domain records through the same shared contracts.

Gate: representative stock and sports families work before bulk migration; every ordinary repeated family in all three repos uses shared generation or shared implementations. Custom field parsers remain local, repeated record plumbing does not.

## Phase D: shared scraper engine

1. Extract `ModuleScraper` execution into a single shared engine: input discovery orchestration, scraped/attempted filtering, client setup, attempts/retries, session lifecycle, persistence and scrape-log recording.
2. Define hooks for URL/input generation, response checking, pre-request/header setup, retry-header refresh, successful response transformation, and provider challenge response/replay. Specify call order, mutable session access, and whether a replay consumes retry budget; preserve each current policy through explicit configuration.
3. Support current transport choices using existing `SourceScraper`/Cloudflare/proxy components. Session TTL/width, browser choice and domain refresh policy are settings. The engine executes refresh/retry/session mechanisms; providers supply their values and exceptional behavior.
4. Retain terminal-error behavior and prevent duplicate persistence/logging when hooks replay requests. Centralize progress/error events; local routing controls where reports go.

Gate: fake-transport traces cover success, retry, exhaustion, response rejection, header refresh, provider challenge replay, transport failure and persistence failure. Stock's retry hook and sports visitor sessions run through the same engine without copied loops.

## Phase E: shared parser engines

1. Move `ModuleParser`, `SourceDatasetParser` and `ParserForScraped` workflow implementations into shared code. Applications implement payload parsing and output mapping, not cursor iteration or flushing.
2. Own cursor/stream iteration, error context, memory accounting, threshold checks, batch insertion, final flush, row-count reporting and capacity reuse in the shared engine. Reuse existing batching APIs.
3. Support multiple differently typed output buffers through a small shared buffer/sink abstraction and local typed output declarations or generated bundles. A local callback that copies the multi-output flush loop is insufficient.
4. Provide both borrowed insertion and owned-buffer flush operations. Centralize clear-after-success behavior as a shared operation; configuration/wrappers choose it. Preserve full-buffer retention on failure and acknowledge partial database insertion. Add no implicit retry.
5. Preserve parser-specific errors, output ordering, bookkeeping exclusions, and final below-threshold flushes. Handle returned errors inside engines; notification/exit policy is selected at the runner boundary.

Gate: typed multi-output tests, empty input, parse/cursor/insertion failures, memory saturation, repeated flushes and final flushes exercise shared code. Each repo deletes its common parser loop, including copies in source-specific multi-output parsers.

## Phase F: shared runners, archive/import and reporting

1. Consolidate `RunTemplate`, `ScraperTemplate` and `ParserTemplate` implementations into shared runner/workflow services using local configuration and hooks from B–E.
2. Reuse existing selection/sequence/export primitives. Add common active filtering, stable dataset deduplication, mode dispatch, setup, scrape/parse/export/backup/remove/import/report orchestration.
3. Centralize archive creation, compression, upload, local archival, import traversal and cleanup. Layout descriptors define folder depths/legacy mappings; shared traversal executes them. Preserve source-specific import decoding through focused hooks.
4. Shared runner applies supplied window/cutoff/schedule/retention policies, skip-removal flags, staging replacement settings, and notifications. Local CLI parses values into shared run options; it must not reproduce the dispatcher.
5. Eliminate panic-based internal adapters by the end, using structured errors and one configured notification/termination boundary. Compatibility entry points may retain their external behavior while delegating to fallible shared workflows.

Gate: operation traces and filesystem tests verify each run mode, archive/import layout, cleanup ordering, resume/export scope, missing/empty tables, stage failures, staging preservation and no double notifications.

## Phase G: consumer rollout and removal audit

Roll out B through F vertically: shared API plus at least one real consumer, then all three consumers, before calling that phase complete. Pilot stock and sports examples during contract design to avoid interfaces tailored to one repo. Keep existing exports additive during migration; wrappers are temporary compatibility or permanent one-call configuration facades, not copied algorithms.

In every repo, replace methods with delegates/re-exports/configuration and remove superseded code after tests pass. Do not stop after a pilot. Review remaining similar function bodies and justify each intentional domain exception by file/method. Do not use line-count reduction as the only success metric.

## Acceptance: prove changes have one implementation site

| Example future change | Required implementation owner |
|---|---|
| General retry bookkeeping/header-hook sequencing | Shared scraper engine |
| Parser memory/flush/final-output handling | Shared parser engine |
| File loading or input/document/log conversion fix | Shared record/file framework |
| Common path/date-suffix construction bug | Shared configured path/file helpers |
| Service initialization or generic secret error handling | Shared service factories |
| Generic insert/backup/export operation fix | Shared storage/workflow adapters |
| Run-mode ordering, archive copy-before-remove, reporting mechanics | Shared runner/archive services |
| Eightbo protocol, stock symbol decoding, football fixture schema | Corresponding repo-local definition/hook |

Completion requires all three consumers using every applicable shared implementation above, deletion of duplicate engines/ordinary record plumbing, and an ownership audit showing the example common fixes need no application implementation edits. API changes can still require coordinated updates; source-specific policy changes can require local changes. Do not promise zero consumer changes forever.

## Validation and historical results

For future Rust implementation: read applicable Rust/domain skills when editing those areas; run targeted pure/fake tests, library checks/lints/doctests, then each consumer's common and representative source tests plus workspace/all-target checks. Follow repository-required broader tests and formatting rules. Record service-dependent checks not run; do not run production scrapes/uploads to validate orchestration. Include external-consumer compile fixtures for shared traits/macros and config/serialization compatibility tests.

The following results are retained from the previous plan as historical records, not rerun or certification of the expanded framework:

## Library validation (October 23, 2026)

Passed:

- `cargo test --lib pipeline::` plus the added owned/borrowed-handle test:
  12 pipeline tests total.
- `cargo test --lib misc::batching`: 7 tests.
- `cargo test --lib misc::time_operation`: 5 existing tests.
- `cargo test --doc`: 6 doctests.
- `cargo check`.
- `cargo clippy --all-targets --all-features`.
- `cargo fmt --check` and `git diff --check`.

Service-, credential-, and environment-dependent library tests were not run.
Consumer buffer-clearing, staging/archive filesystem tests, workspace checks,
and binary checks remain part of the companion migrations.
