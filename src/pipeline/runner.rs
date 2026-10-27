//! Run-mode dispatch and one reporting boundary over focused application hooks.
use super::{
    export::{export_dataset, DatasetExport, ExportOutcome, ExportStage},
    metadata::ordered_datasets,
    ModuleSelection,
};
use async_trait::async_trait;
use std::hash::Hash;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunMode {
    Full,
    Parse,
    Export,
    Backup,
    Remove,
    Insert,
    Scrape,
    Report,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ServiceNeeds {
    pub database: bool,
    pub files: bool,
    pub upload: bool,
    pub transport: bool,
}
impl RunMode {
    pub fn services(self) -> ServiceNeeds {
        ServiceNeeds {
            database: true,
            files: matches!(
                self,
                Self::Full | Self::Export | Self::Backup | Self::Remove | Self::Insert
            ),
            upload: matches!(self, Self::Full | Self::Export | Self::Backup),
            transport: matches!(self, Self::Scrape),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunStage {
    Setup,
    Parse,
    Backup,
    Remove,
    Insert,
    Scrape,
    Report,
    Export(ExportStage),
}
#[derive(Debug, thiserror::Error)]
#[error("Run failed at {stage:?}, module {module:?}, dataset {dataset:?}: {source}")]
pub struct RunFailure<M, D, E> {
    pub stage: RunStage,
    pub module: Option<M>,
    pub dataset: Option<D>,
    #[source]
    pub source: E,
}
#[derive(Debug)]
pub struct RunReport<D> {
    pub exports: Vec<(D, ExportOutcome)>,
}

/// Concrete hooks are one module/domain operation; none implements a run-mode
/// dispatcher. Export ordering is enforced through the shared DatasetExport API.
#[async_trait]
pub trait RunOperations<M: Send, D: Copy + Send + Sync>: Send {
    type Error: Send;
    type Export: DatasetExport<D, Error = Self::Error>;
    fn export_backend(&self) -> &Self::Export;
    /// Captures export windows and validates the scope once, after processing,
    /// including when that scope contains no datasets.
    async fn before_export(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn setup(&mut self, services: ServiceNeeds) -> Result<(), Self::Error>;
    async fn parse(&mut self, module: M) -> Result<(), Self::Error>;
    async fn backup(&mut self, module: M) -> Result<(), Self::Error>;
    async fn remove(&mut self, module: M) -> Result<(), Self::Error>;
    async fn insert(&mut self, module: M) -> Result<(), Self::Error>;
    async fn scrape(&mut self, module: M) -> Result<(), Self::Error>;
    async fn report(&mut self, module: M) -> Result<(), Self::Error>;
}

/// Selection and dataset mappings are consumed as declarations. Removal exclusions
/// are applied after backup. Resume never narrows the active export scope.
pub async fn run<M, D, B>(
    selection: &ModuleSelection<M>,
    mode: RunMode,
    datasets: impl Fn(&M) -> Vec<D>,
    skip_remove: impl Fn(M) -> bool,
    backend: &mut B,
) -> Result<RunReport<D>, RunFailure<M, D, B::Error>>
where
    M: Copy + Send,
    D: Copy + Eq + Hash + Send + Sync,
    B: RunOperations<M, D>,
{
    backend
        .setup(mode.services())
        .await
        .map_err(|source| RunFailure {
            stage: RunStage::Setup,
            module: None,
            dataset: None,
            source,
        })?;
    if matches!(mode, RunMode::Full | RunMode::Parse) {
        let mut report = RunReport {
            exports: Vec::new(),
        };
        for step in
            super::sequence::parse_steps(&selection.processing_modules, mode == RunMode::Full)
        {
            use super::ParseStep;
            let (module, stage, result) = match step {
                ParseStep::Parse(module) => (module, RunStage::Parse, backend.parse(module).await),
                ParseStep::Backup(module) => {
                    (module, RunStage::Backup, backend.backup(module).await)
                }
                ParseStep::Remove(module) => (
                    module,
                    RunStage::Remove,
                    if skip_remove(module) {
                        Ok(())
                    } else {
                        backend.remove(module).await
                    },
                ),
                ParseStep::Export => {
                    export_scope(selection, &datasets, backend, &mut report).await?;
                    continue;
                }
            };
            result.map_err(|source| RunFailure {
                stage,
                module: Some(module),
                dataset: None,
                source,
            })?;
        }
        return Ok(report);
    }
    for &module in &selection.processing_modules {
        let mut stage = RunStage::Parse;
        let result = async {
            match mode {
                RunMode::Backup => {
                    stage = RunStage::Backup;
                    backend.backup(module).await?;
                }
                RunMode::Remove => {
                    if !skip_remove(module) {
                        stage = RunStage::Remove;
                        backend.remove(module).await?;
                    }
                }
                RunMode::Insert => {
                    stage = RunStage::Insert;
                    backend.insert(module).await?;
                }
                RunMode::Scrape => {
                    stage = RunStage::Scrape;
                    backend.scrape(module).await?;
                }
                RunMode::Report => {
                    stage = RunStage::Report;
                    backend.report(module).await?;
                }
                RunMode::Export | RunMode::Full | RunMode::Parse => (),
            }
            Ok(())
        }
        .await;
        result.map_err(|source| RunFailure {
            stage,
            module: Some(module),
            dataset: None,
            source,
        })?;
    }
    let mut report = RunReport {
        exports: Vec::new(),
    };
    if mode == RunMode::Export {
        export_scope(selection, &datasets, backend, &mut report).await?;
    }
    Ok(report)
}

async fn export_scope<M, D, B>(
    selection: &ModuleSelection<M>,
    datasets: &impl Fn(&M) -> Vec<D>,
    backend: &mut B,
    report: &mut RunReport<D>,
) -> Result<(), RunFailure<M, D, B::Error>>
where
    M: Send,
    D: Copy + Eq + Hash + Send + Sync,
    B: RunOperations<M, D>,
{
    backend.before_export().await.map_err(|source| RunFailure {
        stage: RunStage::Export(ExportStage::CheckExistence),
        module: None,
        dataset: None,
        source,
    })?;
    for dataset in ordered_datasets(&selection.export_modules, datasets) {
        let outcome = export_dataset(dataset, backend.export_backend())
            .await
            .map_err(|failure| RunFailure {
                stage: RunStage::Export(failure.stage),
                module: None,
                dataset: Some(dataset),
                source: failure.source,
            })?;
        report.exports.push((dataset, outcome));
    }
    Ok(())
}

/// A single external boundary observes returned failures. Engines never notify,
/// so nested workflows cannot emit the same failure more than once.
#[async_trait]
pub trait FailureReporter<E: Sync>: Send {
    async fn failure(&mut self, error: &E);
}
pub async fn report_result<T, E: Sync>(
    result: Result<T, E>,
    reporter: &mut impl FailureReporter<E>,
) -> Result<T, E> {
    if let Err(error) = &result {
        reporter.failure(error).await;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    type Trace = Arc<Mutex<Vec<String>>>;
    struct Backend {
        trace: Trace,
        fail: Option<&'static str>,
    }
    impl Backend {
        fn step(&self, event: String) -> Result<(), &'static str> {
            self.trace.lock().unwrap().push(event.clone());
            if self.fail.is_some_and(|failure| event.starts_with(failure)) {
                Err("injected")
            } else {
                Ok(())
            }
        }
    }
    #[async_trait]
    impl DatasetExport<u8> for Backend {
        type Error = &'static str;
        fn needs_finalization(&self, _: &u8) -> bool {
            true
        }
        async fn table_exists(&self, d: &u8) -> Result<bool, Self::Error> {
            self.step(format!("exists:{d}"))?;
            Ok(true)
        }
        async fn finalize(&self, d: &u8) -> Result<(), Self::Error> {
            self.step(format!("finalize:{d}"))
        }
        async fn has_rows(&self, d: &u8) -> Result<bool, Self::Error> {
            self.step(format!("rows:{d}"))?;
            Ok(true)
        }
        async fn export(&self, d: &u8) -> Result<(), Self::Error> {
            self.step(format!("export:{d}"))
        }
        async fn upload(&self, d: &u8) -> Result<(), Self::Error> {
            self.step(format!("upload:{d}"))
        }
        async fn archive_local(&self, d: &u8) -> Result<(), Self::Error> {
            self.step(format!("archive:{d}"))
        }
    }
    #[async_trait]
    impl RunOperations<char, u8> for Backend {
        type Error = &'static str;
        type Export = Self;
        fn export_backend(&self) -> &Self {
            self
        }
        async fn before_export(&mut self) -> Result<(), Self::Error> {
            if self.fail == Some("window") {
                self.step("window".into())
            } else {
                Ok(())
            }
        }
        async fn setup(&mut self, _: ServiceNeeds) -> Result<(), Self::Error> {
            self.step("setup".into())
        }
        async fn parse(&mut self, m: char) -> Result<(), Self::Error> {
            self.step(format!("parse:{m}"))
        }
        async fn backup(&mut self, m: char) -> Result<(), Self::Error> {
            self.step(format!("backup:{m}"))
        }
        async fn remove(&mut self, m: char) -> Result<(), Self::Error> {
            self.step(format!("remove:{m}"))
        }
        async fn insert(&mut self, m: char) -> Result<(), Self::Error> {
            self.step(format!("insert:{m}"))
        }
        async fn scrape(&mut self, m: char) -> Result<(), Self::Error> {
            self.step(format!("scrape:{m}"))
        }
        async fn report(&mut self, m: char) -> Result<(), Self::Error> {
            self.step(format!("report:{m}"))
        }
    }
    fn backend(fail: Option<&'static str>) -> Backend {
        Backend {
            trace: Trace::default(),
            fail,
        }
    }
    fn datasets(m: &char) -> Vec<u8> {
        if *m == 'A' {
            vec![1, 2]
        } else {
            vec![2, 3]
        }
    }
    #[tokio::test]
    async fn test_full_resume_keeps_source_wide_export_and_skip_remove_policy() {
        let mut b = backend(None);
        let selected = ModuleSelection::new(vec!['A', 'B'], Some('B'), true);
        let report = run(&selected, RunMode::Full, datasets, |_| true, &mut b)
            .await
            .unwrap();
        assert_eq!(
            report.exports,
            [
                (1, ExportOutcome::Completed),
                (2, ExportOutcome::Completed),
                (3, ExportOutcome::Completed)
            ]
        );
        let trace = b.trace.lock().unwrap();
        assert_eq!(&trace[..3], ["setup", "parse:B", "backup:B"]);
        assert_eq!(
            trace
                .iter()
                .filter(|step| step.starts_with("exists:"))
                .cloned()
                .collect::<Vec<_>>(),
            ["exists:1", "exists:2", "exists:3"]
        );
        assert!(!trace.iter().any(|step| step.starts_with("remove:")));
    }
    #[tokio::test]
    async fn test_each_run_mode_has_one_shared_dispatcher() {
        let selection = ModuleSelection::new(vec!['A'], None, false);
        for (mode, event) in [
            (RunMode::Parse, "parse:A"),
            (RunMode::Backup, "backup:A"),
            (RunMode::Remove, "remove:A"),
            (RunMode::Insert, "insert:A"),
            (RunMode::Scrape, "scrape:A"),
            (RunMode::Report, "report:A"),
        ] {
            let mut b = backend(None);
            run(&selection, mode, |_| vec![1u8], |_| false, &mut b)
                .await
                .unwrap();
            assert_eq!(*b.trace.lock().unwrap(), ["setup", event]);
        }
        assert!(!RunMode::Parse.services().upload);
        assert!(RunMode::Scrape.services().transport);
        assert!(RunMode::Report.services().database);
    }
    #[tokio::test]
    async fn test_empty_processing_still_exports_and_errors_stop_all_later_work() {
        let selected = ModuleSelection::new(vec!['A'], Some('Z'), true);
        let mut b = backend(None);
        run(&selected, RunMode::Full, |_| vec![1u8], |_| false, &mut b)
            .await
            .unwrap();
        assert_eq!(
            *b.trace.lock().unwrap(),
            [
                "setup",
                "exists:1",
                "finalize:1",
                "rows:1",
                "export:1",
                "upload:1",
                "archive:1"
            ]
        );
        let selection = ModuleSelection::new(vec!['A', 'B'], None, false);
        let full = [
            "setup",
            "parse:A",
            "backup:A",
            "remove:A",
            "parse:B",
            "backup:B",
            "remove:B",
            "exists:1",
            "finalize:1",
            "rows:1",
            "export:1",
            "upload:1",
            "archive:1",
        ];
        for (index, failed) in full.iter().enumerate() {
            let mut b = backend(Some(failed));
            let failure = run(&selection, RunMode::Full, |_| vec![1u8], |_| false, &mut b)
                .await
                .unwrap_err();
            assert_eq!(failure.source, "injected");
            assert_eq!(*b.trace.lock().unwrap(), full[..=index]);
        }
    }
    #[tokio::test]
    async fn test_notification_boundary_reports_once_and_returns_original_error() {
        struct Reporter(usize);
        #[async_trait]
        impl FailureReporter<&'static str> for Reporter {
            async fn failure(&mut self, _: &&'static str) {
                self.0 += 1;
            }
        }
        let mut reporter = Reporter(0);
        assert_eq!(
            report_result(Err::<(), _>("failed"), &mut reporter).await,
            Err("failed")
        );
        assert_eq!(
            report_result(Ok::<_, &'static str>(()), &mut reporter).await,
            Ok(())
        );
        assert_eq!(reporter.0, 1);
    }
    #[tokio::test]
    async fn test_export_scope_validation_follows_processing_even_without_datasets() {
        let trace = Trace::default();
        let mut backend = Backend {
            trace: trace.clone(),
            fail: Some("window"),
        };
        let selection = ModuleSelection::new(vec!['A'], None, false);
        assert!(matches!(
            run(
                &selection,
                RunMode::Full,
                |_| Vec::<u8>::new(),
                |_| false,
                &mut backend
            )
            .await,
            Err(RunFailure {
                stage: RunStage::Export(ExportStage::CheckExistence),
                dataset: None,
                ..
            })
        ));
        assert_eq!(
            *trace.lock().unwrap(),
            ["setup", "parse:A", "backup:A", "remove:A", "window"]
        );
    }
}
