//! Explicit archive/import mechanics. Selection, retention and layout are data
//! supplied by the application; failures never remove staging or advance steps.
use async_trait::async_trait;
use std::{
    fs, io,
    path::{Path, PathBuf},
};

pub fn remove_stale_staging(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

/// Copy must complete before source removal. Destination creation is a caller
/// decision, so a failed copy retains the source exactly as legacy adapters do.
pub async fn copy_then_remove(source: &Path, destination: &Path) -> io::Result<()> {
    if source == destination {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "archive source and destination must differ",
        ));
    }
    tokio::fs::copy(source, destination).await?;
    tokio::fs::remove_file(source).await
}

#[derive(Clone, Debug)]
pub struct ArchiveEntry {
    pub source: PathBuf,
    pub stored_path: PathBuf,
    pub directory: bool,
}
#[derive(Clone, Copy, Debug)]
pub enum Compression {
    Bzip2,
    Gzip,
    ParallelBzip2 { workers: usize },
}

/// Creates and fully closes both tar and compressor, returning output errors.
/// Entry order and archive-relative names are supplied explicitly.
pub fn create_archive(
    output: &Path,
    entries: &[ArchiveEntry],
    compression: Compression,
) -> io::Result<()> {
    fn append<W: io::Write>(
        builder: &mut tar::Builder<W>,
        entries: &[ArchiveEntry],
    ) -> io::Result<()> {
        for entry in entries {
            if entry.stored_path.is_absolute()
                || entry
                    .stored_path
                    .components()
                    .any(|component| matches!(component, std::path::Component::ParentDir))
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "archive paths must stay relative",
                ));
            }
            if entry.directory {
                builder.append_dir_all(&entry.stored_path, &entry.source)?;
            } else {
                builder.append_path_with_name(&entry.source, &entry.stored_path)?;
            }
        }
        Ok(())
    }
    match compression {
        Compression::Bzip2 => {
            let mut builder = tar::Builder::new(bzip2::write::BzEncoder::new(
                fs::File::create(output)?,
                bzip2::Compression::best(),
            ));
            append(&mut builder, entries)?;
            builder.into_inner()?.finish()?;
        }
        Compression::Gzip => {
            let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
                fs::File::create(output)?,
                flate2::Compression::best(),
            ));
            append(&mut builder, entries)?;
            builder.into_inner()?.finish()?;
        }
        Compression::ParallelBzip2 { workers } => {
            let writer = crate::file_compress::ParallelBz2Writer::new(output, workers)?;
            let mut builder = tar::Builder::new(writer);
            append(&mut builder, entries)?;
            builder.into_inner()?.finish()?;
        }
    }
    Ok(())
}

/// Creates an archive on the blocking pool, keeping compressor work away from
/// the async executor while sharing the same archive format implementation.
pub async fn create_archive_async(
    output: &Path,
    entries: &[ArchiveEntry],
    compression: Compression,
) -> io::Result<()> {
    let output = output.to_path_buf();
    let entries = entries.to_vec();
    tokio::task::spawn_blocking(move || create_archive(&output, &entries, compression))
        .await
        .map_err(io::Error::other)?
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArchiveStage {
    Create,
    Upload,
    ArchiveLocal,
    Cleanup,
}
#[derive(Debug, thiserror::Error)]
#[error("Archive failed at {stage:?}: {source}")]
pub struct ArchiveFailure<E> {
    pub stage: ArchiveStage,
    #[source]
    pub source: E,
}

#[async_trait]
pub trait ArchiveOperations: Send {
    type Error: Send;
    async fn create(&mut self) -> Result<(), Self::Error>;
    async fn upload(&mut self) -> Result<(), Self::Error>;
    async fn archive_local(&mut self) -> Result<(), Self::Error>;
    async fn cleanup(&mut self) -> Result<(), Self::Error>;
}
pub async fn run_archive<B: ArchiveOperations>(
    backend: &mut B,
    cleanup: bool,
) -> Result<(), ArchiveFailure<B::Error>> {
    backend.create().await.map_err(|source| ArchiveFailure {
        stage: ArchiveStage::Create,
        source,
    })?;
    backend.upload().await.map_err(|source| ArchiveFailure {
        stage: ArchiveStage::Upload,
        source,
    })?;
    backend
        .archive_local()
        .await
        .map_err(|source| ArchiveFailure {
            stage: ArchiveStage::ArchiveLocal,
            source,
        })?;
    if cleanup {
        backend.cleanup().await.map_err(|source| ArchiveFailure {
            stage: ArchiveStage::Cleanup,
            source,
        })?;
    }
    Ok(())
}

/// Selected directory depth and token interpretations remain local definitions.
/// Symlinks are skipped so traversal cannot leave the configured tree.
#[derive(Clone, Debug)]
pub struct ImportFile {
    pub path: PathBuf,
    pub folder_tokens: Vec<String>,
}

pub fn discover_import_files(
    root: &Path,
    depth: usize,
    accept_folder: impl Fn(&[String]) -> bool,
    accept_file: impl Fn(&Path) -> bool,
) -> io::Result<Vec<ImportFile>> {
    fn visit(
        root: &Path,
        depth: usize,
        tokens: &mut Vec<String>,
        accept_folder: &impl Fn(&[String]) -> bool,
        accept_file: &impl Fn(&Path) -> bool,
        files: &mut Vec<ImportFile>,
    ) -> io::Result<()> {
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if depth == 0 && kind.is_file() && accept_file(&entry.path()) {
                files.push(ImportFile {
                    path: entry.path(),
                    folder_tokens: tokens.clone(),
                });
            } else if depth > 0 && kind.is_dir() {
                let name = entry.file_name().into_string().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "non-Unicode import folder")
                })?;
                tokens.push(name);
                if accept_folder(tokens) {
                    visit(
                        &entry.path(),
                        depth - 1,
                        tokens,
                        accept_folder,
                        accept_file,
                        files,
                    )?;
                }
                tokens.pop();
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    visit(
        root,
        depth,
        &mut Vec::new(),
        &accept_folder,
        &accept_file,
        &mut files,
    )?;
    Ok(files)
}

/// Configured concrete archive adapter, including the shared copy/remove path.
pub struct ArchivePlan {
    pub output: PathBuf,
    pub entries: Vec<ArchiveEntry>,
    pub compression: Compression,
    pub bucket: String,
    pub remote_folder: PathBuf,
    pub remote_name: String,
    pub local_archive: PathBuf,
    pub cleanup_files: Vec<PathBuf>,
    pub cleanup_folders: Vec<PathBuf>,
}
pub struct StorageArchive<'a, 'logger> {
    pub plan: &'a ArchivePlan,
    pub aws: &'a crate::aws_s3::AWSFileIO<'logger>,
}
#[async_trait]
impl ArchiveOperations for StorageArchive<'_, '_> {
    type Error = super::storage::StorageError;
    async fn create(&mut self) -> Result<(), Self::Error> {
        create_archive_async(&self.plan.output, &self.plan.entries, self.plan.compression).await?;
        Ok(())
    }
    async fn upload(&mut self) -> Result<(), Self::Error> {
        let folder = self.plan.output.parent().unwrap_or_else(|| Path::new(""));
        let name = self
            .plan
            .output
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "archive output requires a Unicode filename",
                )
            })?;
        self.aws
            .upload_file(
                &self.plan.bucket,
                &self.plan.remote_folder,
                &self.plan.remote_name,
                folder,
                name,
            )
            .await
            .map_err(|error| super::storage::StorageError::Upload(Box::new(error)))
    }
    async fn archive_local(&mut self) -> Result<(), Self::Error> {
        Ok(copy_then_remove(&self.plan.output, &self.plan.local_archive).await?)
    }
    async fn cleanup(&mut self) -> Result<(), Self::Error> {
        cleanup_paths(&self.plan.cleanup_files, &self.plan.cleanup_folders).await?;
        Ok(())
    }
}

/// A configured transfer is independent of compression and database services.
pub struct FileTransfer {
    pub source: PathBuf,
    pub local_archive: PathBuf,
    pub bucket: String,
    pub remote_folder: PathBuf,
    pub remote_name: String,
}
impl FileTransfer {
    pub async fn upload(
        &self,
        aws: &crate::aws_s3::AWSFileIO<'_>,
    ) -> Result<(), super::storage::StorageError> {
        let folder = self.source.parent().unwrap_or_else(|| Path::new(""));
        let name = self
            .source
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "transfer requires a Unicode filename",
                )
            })?;
        aws.upload_file(
            &self.bucket,
            &self.remote_folder,
            &self.remote_name,
            folder,
            name,
        )
        .await
        .map_err(|error| super::storage::StorageError::Upload(Box::new(error)))
    }
    pub async fn archive_local(&self) -> io::Result<()> {
        copy_then_remove(&self.source, &self.local_archive).await
    }
}
pub async fn upload_files(
    files: &[FileTransfer],
    aws: &crate::aws_s3::AWSFileIO<'_>,
) -> Result<(), super::storage::StorageError> {
    for file in files {
        file.upload(aws).await?;
    }
    Ok(())
}
pub async fn missing_files(
    files: &[FileTransfer],
    aws: &crate::aws_s3::AWSFileIO<'_>,
) -> Vec<String> {
    let mut missing = Vec::new();
    for file in files {
        if !aws
            .check_file_exist(&file.bucket, &file.remote_folder, &file.remote_name)
            .await
        {
            missing.push(file.remote_name.clone());
        }
    }
    missing
}
/// Shared owned cleanup; callers decide which paths are eligible for removal.
pub async fn cleanup_paths(files: &[PathBuf], folders: &[PathBuf]) -> io::Result<()> {
    for file in files {
        tokio::fs::remove_file(file).await?;
    }
    for folder in folders {
        tokio::fs::remove_dir_all(folder).await?;
    }
    Ok(())
}

/// Executes fixed or variable-depth layout declarations with stable directory
/// ordering. File enumeration within each directory retains backend order.
pub fn discover_import_layout(
    root: &Path,
    depths: &[usize],
    accept_file: impl Fn(&Path) -> bool,
) -> io::Result<Vec<ImportFile>> {
    let mut files = Vec::new();
    for depth in depths {
        files.extend(discover_import_files(root, *depth, |_| true, &accept_file)?);
    }
    files.sort_by(|a, b| a.folder_tokens.cmp(&b.folder_tokens));
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "shared-archive-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            fs::create_dir_all(&root).unwrap();
            Self(root)
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    #[tokio::test]
    async fn test_copy_failure_preserves_staging_and_success_removes_only_selected_file() {
        let temp = Temp::new();
        let source = temp.0.join("source");
        let other = temp.0.join("other");
        fs::write(&source, "new").unwrap();
        fs::write(&other, "other").unwrap();
        assert!(copy_then_remove(&source, &source).await.is_err());
        assert!(copy_then_remove(&source, &temp.0.join("absent/dest"))
            .await
            .is_err());
        assert_eq!(fs::read_to_string(&source).unwrap(), "new");
        let dest = temp.0.join("dest");
        fs::write(&dest, "old").unwrap();
        copy_then_remove(&source, &dest).await.unwrap();
        assert!(!source.exists());
        assert!(other.exists());
        assert_eq!(fs::read_to_string(dest).unwrap(), "new");
        remove_stale_staging(&source).unwrap();
    }
    #[tokio::test]
    async fn test_layout_traversal_and_archive_member_names() {
        let temp = Temp::new();
        let folder = temp.0.join("20261023");
        fs::create_dir(&folder).unwrap();
        let source = folder.join("fixture.json");
        fs::write(&source, "payload").unwrap();
        fs::write(folder.join("fixture.tmp"), "partial").unwrap();
        let files = discover_import_files(
            &temp.0,
            1,
            |tokens| tokens[0] == "20261023",
            |path| path.extension().is_some_and(|ext| ext == "json"),
        )
        .unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].folder_tokens, ["20261023"]);
        let output = temp.0.join("archive.tar.bz2");
        create_archive_async(
            &output,
            &[ArchiveEntry {
                source,
                stored_path: "legacy/20261023/fixture.json".into(),
                directory: false,
            }],
            Compression::Bzip2,
        )
        .await
        .unwrap();
        let mut archive =
            tar::Archive::new(bzip2::read::BzDecoder::new(fs::File::open(output).unwrap()));
        let paths: Vec<_> = archive
            .entries()
            .unwrap()
            .map(|entry| entry.unwrap().path().unwrap().into_owned())
            .collect();
        assert_eq!(paths, [PathBuf::from("legacy/20261023/fixture.json")]);
    }
}
