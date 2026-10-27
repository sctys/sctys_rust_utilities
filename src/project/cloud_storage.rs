//! Shared object-storage bucket and archive-prefix names.
use std::path::{Path, PathBuf};

pub const DATA_BUCKET: &str = "sctys";
pub const ARCHIVE_BUCKET: &str = "sctys-backup";

pub const RAW_ARCHIVE_FOLDER: &str = "raw_archive";
pub const DUCKDB_ARCHIVE_FOLDER: &str = "duckdb_archive";
pub const CLICKHOUSE_ARCHIVE_FOLDER: &str = "clickhouse_archive";

pub fn s3_data_folder(project: &str) -> PathBuf {
    Path::new(super::paths::DATA_FOLDER).join(project)
}
