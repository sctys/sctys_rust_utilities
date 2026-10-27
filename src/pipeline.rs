//! Configured scraping, parsing, record, storage, and runner workflows.
//!
//! Applications own active-module discovery, dataset mapping, storage paths,
//! windows, notifications, and failure policy. These executors stop at the first
//! error and do not catch panics or roll back completed operations. Scraping
//! retries and provider replays follow explicit per-run policies; persistence
//! and export never gain implicit retries.

pub mod archive;
pub mod export;
pub mod metadata;
pub mod parse;
pub mod records;
pub mod reporting;
pub mod runner;
pub mod scrape;
pub mod selection;
pub mod sequence;
pub mod storage;
pub mod transport;

pub use selection::ModuleSelection;
pub use sequence::{run_parse_steps, run_parse_with, ParseStep};
