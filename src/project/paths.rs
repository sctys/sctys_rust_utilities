//! Root resolution and path assembly driven by an explicit layout.
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

pub const PROJECT_ROOT_ENV: &str = "SCTYS_PROJECT";
pub const DATA_ROOT_ENV: &str = "SCTYS_DATA";
pub const SSD_PROJECT_ROOT_ENV: &str = "SCTYS_SSD_PROJECT";
pub const SSD_DATA_ROOT_ENV: &str = "SCTYS_SSD_DATA";

pub const DATA_FOLDER: &str = "data";
pub const BACKUP_FOLDER: &str = "backup";
pub const LOG_FOLDER: &str = "Log";
pub const LOG_PREFIX: &str = "log_";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackupRoot {
    DataRoot,
    ProjectData,
}

/// Project identity and backup-root behavior stay caller supplied. Use `standard`
/// for the shared folder and file-prefix conventions, or fill the fields directly.
#[derive(Clone, Copy, Debug)]
pub struct Layout<'a> {
    pub project: &'a str,
    pub backup_folder: &'a str,
    pub backup_root: BackupRoot,
    pub log_folder: &'a str,
    pub log_prefix: &'a str,
}

impl<'a> Layout<'a> {
    /// Builds the shared project layout used by the consumer applications.
    pub const fn standard(project: &'a str, backup_root: BackupRoot) -> Self {
        Self {
            project,
            backup_folder: BACKUP_FOLDER,
            backup_root,
            log_folder: LOG_FOLDER,
            log_prefix: LOG_PREFIX,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Unable to resolve root from environment variable {key}: {source}")]
pub struct RootError {
    pub key: String,
    #[source]
    pub source: std::env::VarError,
}

/// Resolves one root at a time, so modes do not require unused roots.
pub fn environment_root(key: &str) -> Result<PathBuf, RootError> {
    root_with(key, |name| std::env::var_os(name))
}

/// Injectable resolution for compatibility tests without mutating global environment.
pub fn root_with(
    key: &str,
    resolve: impl FnOnce(&str) -> Option<OsString>,
) -> Result<PathBuf, RootError> {
    let value = resolve(key).ok_or_else(|| RootError {
        key: key.into(),
        source: std::env::VarError::NotPresent,
    })?;
    // Legacy helpers used env::var, and rejected non-Unicode roots.
    let value = value.into_string().map_err(|value| RootError {
        key: key.into(),
        source: std::env::VarError::NotUnicode(value),
    })?;
    Ok(PathBuf::from(value))
}

impl Layout<'_> {
    pub fn data(&self, root: &Path) -> PathBuf {
        root.join(self.project)
    }
    pub fn backup(&self, root: &Path) -> PathBuf {
        match self.backup_root {
            BackupRoot::DataRoot => root.join(self.backup_folder),
            BackupRoot::ProjectData => self.data(root).join(self.backup_folder),
        }
    }
    pub fn source(&self, root: &Path, source: &str) -> PathBuf {
        self.data(root).join(source)
    }
    pub fn module(&self, root: &Path, prefix: &str, legacy_name: &str) -> PathBuf {
        self.data(root).join(format!("{prefix}{legacy_name}"))
    }
    pub fn logger(&self, root: &Path) -> PathBuf {
        root.join(self.log_folder)
            .join(format!("{}{}", self.log_prefix, self.project))
    }
    pub fn archive(&self, source: &str, category: &str) -> PathBuf {
        let path = Path::new(self.project).join(source);
        if category.is_empty() {
            path
        } else {
            path.join(category)
        }
    }
}

pub fn layers(root: &Path, components: &[&str]) -> PathBuf {
    components
        .iter()
        .fold(root.to_path_buf(), |path, component| path.join(component))
}

pub fn date_folder(root: &Path, date: &chrono::DateTime<chrono::Utc>) -> PathBuf {
    root.join(date.format("%Y%m%d").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn layout(backup_root: BackupRoot) -> Layout<'static> {
        Layout::standard("fixture_project", backup_root)
    }
    #[test]
    fn test_configured_paths_and_backup_difference() {
        let root = Path::new("/fixture");
        let project = layout(BackupRoot::ProjectData);
        assert_eq!(project.data(root), Path::new("/fixture/fixture_project"));
        assert_eq!(
            project.backup(root),
            Path::new("/fixture/fixture_project/backup")
        );
        assert_eq!(
            layout(BackupRoot::DataRoot).backup(root),
            Path::new("/fixture/backup")
        );
        assert_eq!(
            project.module(root, "src_", "legacy"),
            Path::new("/fixture/fixture_project/src_legacy")
        );
        assert_eq!(
            project.source(root, "source"),
            Path::new("/fixture/fixture_project/source")
        );
        assert_eq!(
            project.logger(root),
            Path::new("/fixture/Log/log_fixture_project")
        );
        assert_eq!(
            project.archive("source", "raw"),
            Path::new("fixture_project/source/raw")
        );
        assert_eq!(
            layers(Path::new("legacy"), &["2026", "10"]),
            Path::new("legacy/2026/10")
        );
    }
    #[test]
    fn test_root_resolution_does_not_read_other_roots() {
        assert_eq!(
            root_with("DATA", |key| {
                assert_eq!(key, "DATA");
                Some("/fixture".into())
            })
            .unwrap(),
            Path::new("/fixture")
        );
        let failure = root_with("MISSING", |_| None).unwrap_err();
        assert_eq!(failure.key, "MISSING");
        assert!(matches!(failure.source, std::env::VarError::NotPresent));
    }
}
