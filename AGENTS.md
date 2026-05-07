# Repository Guidelines

## Project Structure & Module Organization

This repository is a Rust 2021 library crate named `sctys_rust_utilities`. Public exports are declared in `src/lib.rs`. Core utilities are grouped by domain:

- `src/io/`: file I/O, compression, AWS S3, MongoDB, ClickHouse, DuckDB, and Redis helpers.
- `src/netdata/`: scraping, proxy, Playwright, CapSolver, request clients, and related data structs.
- `src/logging/`, `src/messenger/`, `src/secret/`, and `src/misc/`: shared utility modules.
- `src/netdata/python/`, `src/netdata/js/`, and `src/netdata/requirements.txt`: helper assets used by scraping integrations.

Keep new functionality in the closest existing module and expose it from `src/lib.rs` only when it is public API.

## Build, Test, and Development Commands

- `cargo check`: fast compile check.
- `cargo build`: compile the library and all dependencies.
- `cargo fmt`: apply standard Rust formatting.
- `cargo clippy --all-targets --all-features`: run Rust lints across library and tests.
- `cargo test`: run inline tests. Some require local services, network access, AWS credentials, or environment paths.
- `cargo test file_io`: run a narrower test filter while developing one module.

Python scraping helpers use `src/netdata/requirements.txt`; install them in a virtual environment before editing those scripts.

## Coding Style & Naming Conventions

Use rustfmt defaults: four-space indentation, standard import ordering, and idiomatic line wrapping. Follow Rust naming conventions: modules and functions in `snake_case`, types and traits in `PascalCase`, constants in `SCREAMING_SNAKE_CASE`. Prefer explicit error handling over unchecked `unwrap()` in production code; tests may unwrap setup values.

## Testing Guidelines

Tests live inline in `mod tests` blocks beside the implementation. Name tests with `test_<behavior>` and use `#[tokio::test]` for async code. Many integration-style tests read `SCTYS_DATA` and `SCTYS_SSD_DATA` or contact external systems. Isolate filesystem output under a test-specific folder and document required services or credentials in setup comments.

## Commit & Pull Request Guidelines

Recent history uses short imperative summaries, sometimes with a `feat:` prefix, for example `feat: Integrate sea-query...` or `clickhouse func`. Prefer concise commit messages that name the module and behavior changed. Pull requests should include a brief description, affected modules, required environment variables or services, and validation commands run. Link issues when applicable.

## Security & Configuration Tips

Do not commit credentials, tokens, local data paths, generated archives, or service-specific secrets. Keep runtime configuration in environment variables or external secret stores, and avoid hard-coding bucket names, webhook URLs, or proxy credentials.
