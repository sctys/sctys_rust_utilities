//! Dataset export lifecycle independent of databases, file formats, and storage.

use async_trait::async_trait;
use std::fmt;

/// A fallible stage of dataset export.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExportStage {
    CheckExistence,
    Finalize,
    CheckRows,
    Export,
    Upload,
    ArchiveLocal,
}

impl fmt::Display for ExportStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::CheckExistence => "check dataset existence",
            Self::Finalize => "finalize dataset",
            Self::CheckRows => "check export window",
            Self::Export => "export dataset",
            Self::Upload => "upload dataset",
            Self::ArchiveLocal => "archive dataset locally",
        })
    }
}

/// Successful completion or a skip requiring no export, upload, or archival.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExportOutcome {
    Completed,
    SkippedMissing,
    SkippedEmpty,
}

/// The original dataset handle, failed stage, and unmodified backend error.
#[derive(Debug, thiserror::Error)]
#[error("Failed to {stage} for dataset {dataset:?}: {source}")]
pub struct ExportFailure<D, E> {
    pub dataset: D,
    pub stage: ExportStage,
    #[source]
    pub source: E,
}

/// Application operations used by [`export_dataset`].
///
/// Implement with `#[async_trait::async_trait]`. Dataset handles may be enums or
/// borrowed handles. The adapter chooses finalization, row windows, paths,
/// staging behavior, and an error type that can represent its different backends
/// (for example a local enum with database, filesystem, and upload variants).
/// No database-specific error is required by the core.
/// Operations borrow the dataset, so owned handles do not need `Copy` or `Clone`.
#[async_trait]
pub trait DatasetExport<D: Send + Sync>: Sync {
    type Error;

    fn needs_finalization(&self, dataset: &D) -> bool;
    async fn table_exists(&self, dataset: &D) -> Result<bool, Self::Error>;
    async fn finalize(&self, dataset: &D) -> Result<(), Self::Error>;
    async fn has_rows(&self, dataset: &D) -> Result<bool, Self::Error>;
    async fn export(&self, dataset: &D) -> Result<(), Self::Error>;
    async fn upload(&self, dataset: &D) -> Result<(), Self::Error>;
    async fn archive_local(&self, dataset: &D) -> Result<(), Self::Error>;
}

/// Checks existence, optionally finalizes, checks rows, exports, uploads, archives.
///
/// Missing datasets skip before finalization; empty windows skip after optional
/// finalization. A failure stops immediately and retains its underlying error.
/// There are no retries, rollback, failure cleanup, or parallel operations.
/// Callers may log skips using the returned outcome.
pub async fn export_dataset<D, B>(
    dataset: D,
    backend: &B,
) -> Result<ExportOutcome, ExportFailure<D, B::Error>>
where
    D: Send + Sync,
    B: DatasetExport<D> + ?Sized,
{
    let mut stage = ExportStage::CheckExistence;
    let result = async {
        if !backend.table_exists(&dataset).await? {
            return Ok(ExportOutcome::SkippedMissing);
        }
        if backend.needs_finalization(&dataset) {
            stage = ExportStage::Finalize;
            backend.finalize(&dataset).await?;
        }
        stage = ExportStage::CheckRows;
        if !backend.has_rows(&dataset).await? {
            return Ok(ExportOutcome::SkippedEmpty);
        }
        stage = ExportStage::Export;
        backend.export(&dataset).await?;
        stage = ExportStage::Upload;
        backend.upload(&dataset).await?;
        stage = ExportStage::ArchiveLocal;
        backend.archive_local(&dataset).await?;
        Ok(ExportOutcome::Completed)
    }
    .await;
    result.map_err(|source| ExportFailure {
        dataset,
        stage,
        source,
    })
}

/// Exports a declared scope sequentially and observes skips/completion without
/// duplicating lifecycle or failure ordering in consumers.
pub async fn export_datasets<D: Copy + Send + Sync, B: DatasetExport<D>>(
    datasets: &[D],
    backend: &B,
    mut observe: impl FnMut(D, ExportOutcome),
) -> Result<(), ExportFailure<D, B::Error>> {
    for dataset in datasets {
        let outcome = export_dataset(*dataset, backend).await?;
        observe(*dataset, outcome);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{error::Error, sync::Mutex};

    const STAGES: [ExportStage; 6] = [
        ExportStage::CheckExistence,
        ExportStage::Finalize,
        ExportStage::CheckRows,
        ExportStage::Export,
        ExportStage::Upload,
        ExportStage::ArchiveLocal,
    ];

    struct FakeExport {
        exists: bool,
        rows: bool,
        finalize: bool,
        fail_at: Option<ExportStage>,
        operations: Mutex<Vec<ExportStage>>,
    }

    impl Default for FakeExport {
        fn default() -> Self {
            Self {
                exists: true,
                rows: true,
                finalize: true,
                fail_at: None,
                operations: Mutex::new(Vec::new()),
            }
        }
    }

    impl FakeExport {
        fn record(&self, dataset: &str, stage: ExportStage) -> std::io::Result<()> {
            assert_eq!(dataset, "test_dataset");
            self.operations.lock().unwrap().push(stage);
            if self.fail_at == Some(stage) {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "injected failure",
                ))
            } else {
                Ok(())
            }
        }
    }

    #[async_trait]
    impl<D: AsRef<str> + Send + Sync> DatasetExport<D> for FakeExport {
        type Error = std::io::Error;

        fn needs_finalization(&self, _: &D) -> bool {
            self.finalize
        }
        async fn table_exists(&self, dataset: &D) -> Result<bool, Self::Error> {
            self.record(dataset.as_ref(), ExportStage::CheckExistence)?;
            Ok(self.exists)
        }
        async fn finalize(&self, dataset: &D) -> Result<(), Self::Error> {
            self.record(dataset.as_ref(), ExportStage::Finalize)
        }
        async fn has_rows(&self, dataset: &D) -> Result<bool, Self::Error> {
            self.record(dataset.as_ref(), ExportStage::CheckRows)?;
            Ok(self.rows)
        }
        async fn export(&self, dataset: &D) -> Result<(), Self::Error> {
            self.record(dataset.as_ref(), ExportStage::Export)
        }
        async fn upload(&self, dataset: &D) -> Result<(), Self::Error> {
            self.record(dataset.as_ref(), ExportStage::Upload)
        }
        async fn archive_local(&self, dataset: &D) -> Result<(), Self::Error> {
            self.record(dataset.as_ref(), ExportStage::ArchiveLocal)
        }
    }

    #[tokio::test]
    async fn test_complete_export_order() {
        let backend = FakeExport::default();
        // Also exercises the async operation boundary through a trait object.
        let io: &dyn DatasetExport<&str, Error = std::io::Error> = &backend;
        assert_eq!(
            export_dataset("test_dataset", io).await.unwrap(),
            ExportOutcome::Completed
        );
        assert_eq!(*backend.operations.lock().unwrap(), STAGES);
    }

    #[tokio::test]
    async fn test_skip_missing_and_empty() {
        for (exists, rows, expected, operations) in [
            (false, true, ExportOutcome::SkippedMissing, &STAGES[..1]),
            (true, false, ExportOutcome::SkippedEmpty, &STAGES[..3]),
        ] {
            let backend = FakeExport {
                exists,
                rows,
                ..FakeExport::default()
            };
            assert_eq!(
                export_dataset("test_dataset", &backend).await.unwrap(),
                expected
            );
            assert_eq!(*backend.operations.lock().unwrap(), operations);
        }
    }

    #[tokio::test]
    async fn test_finalization_bypass() {
        for rows in [false, true] {
            let backend = FakeExport {
                finalize: false,
                rows,
                ..FakeExport::default()
            };
            let outcome = export_dataset("test_dataset", &backend).await.unwrap();
            assert_eq!(
                outcome,
                if rows {
                    ExportOutcome::Completed
                } else {
                    ExportOutcome::SkippedEmpty
                }
            );
            let expected: Vec<_> = STAGES
                .iter()
                .copied()
                .filter(|stage| *stage != ExportStage::Finalize)
                .take(if rows { 5 } else { 2 })
                .collect();
            assert_eq!(*backend.operations.lock().unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn test_each_failure_preserves_stage_dataset_and_source() {
        for (index, stage) in STAGES.iter().copied().enumerate() {
            let backend = FakeExport {
                fail_at: Some(stage),
                ..FakeExport::default()
            };
            let failure = export_dataset("test_dataset", &backend).await.unwrap_err();
            assert_eq!(failure.dataset, "test_dataset");
            assert_eq!(failure.stage, stage);
            assert_eq!(failure.source.kind(), std::io::ErrorKind::PermissionDenied);
            assert_eq!(failure.source().unwrap().to_string(), "injected failure");
            let message = failure.to_string();
            assert!(message.contains("test_dataset"));
            assert!(message.contains(&stage.to_string()));
            assert!(message.contains("injected failure"));
            assert_eq!(*backend.operations.lock().unwrap(), STAGES[..=index]);
        }
    }

    #[tokio::test]
    async fn test_owned_and_borrowed_handles() {
        let dataset = String::from("test_dataset");
        let backend = FakeExport {
            fail_at: Some(ExportStage::Upload),
            ..FakeExport::default()
        };
        let borrowed = export_dataset(dataset.as_str(), &backend)
            .await
            .unwrap_err();
        assert_eq!(borrowed.dataset, dataset);
        let original_allocation = dataset.as_ptr();
        let owned = export_dataset(dataset, &backend).await.unwrap_err();
        assert_eq!(owned.dataset.as_ptr(), original_allocation);
        assert_eq!(owned.dataset, "test_dataset");
        assert_eq!(owned.stage, ExportStage::Upload);
    }
}
