use async_trait::async_trait;
use sctys_rust_utilities::misc::batching::BatchLimits;
use sctys_rust_utilities::pipeline::{archive::*, parse::*, records::*};
use std::sync::{Arc, Mutex};

type Trace = Arc<Mutex<Vec<&'static str>>>;
struct Writer {
    trace: Trace,
    fail: bool,
}
#[async_trait]
impl RowSink<usize> for Writer {
    type Error = &'static str;
    async fn insert(&mut self, _: &[usize]) -> Result<(), Self::Error> {
        self.trace.lock().unwrap().push("insert");
        if self.fail {
            Err("insert")
        } else {
            Ok(())
        }
    }
}
type Decode = fn(&[usize], &mut Vec<usize>) -> Result<usize, &'static str>;
type Output = TypedOutput<usize, Decode, Writer, fn(&usize) -> usize>;
struct Parser {
    output: Output,
    trace: Trace,
    fail: Option<&'static str>,
    counts: usize,
    documents: Vec<Result<usize, &'static str>>,
}
#[async_trait]
impl ModuleParseOperations for Parser {
    type Dataset = u8;
    type Document = usize;
    type Error = &'static str;
    type Cursor = futures::stream::Iter<std::vec::IntoIter<Result<usize, &'static str>>>;
    type Outputs = Output;
    fn datasets(&self) -> Vec<(u8, bool)> {
        vec![(1, false), (2, true)]
    }
    fn outputs(&mut self) -> &mut Output {
        &mut self.output
    }
    fn estimate(_: &usize) -> usize {
        1
    }
    fn exceeds(_: usize) -> bool {
        false
    }
    async fn prepare(&mut self, dataset: u8) -> Result<(), Self::Error> {
        assert_eq!(dataset, 1);
        self.trace.lock().unwrap().push("prepare");
        if self.fail == Some("prepare") {
            Err("prepare")
        } else {
            Ok(())
        }
    }
    async fn row_count(&mut self, dataset: u8) -> Result<usize, Self::Error> {
        assert_eq!(dataset, 1);
        let stage = if self.counts == 0 { "before" } else { "after" };
        self.counts += 1;
        self.trace.lock().unwrap().push(stage);
        if self.fail == Some(stage) {
            Err(stage)
        } else {
            Ok(10 + self.counts)
        }
    }
    async fn open_cursor(&mut self) -> Result<Self::Cursor, Self::Error> {
        self.trace.lock().unwrap().push("cursor");
        if self.fail == Some("cursor") {
            Err("cursor")
        } else {
            Ok(futures::stream::iter(self.documents.clone()))
        }
    }
}
fn parser(fail: Option<&'static str>) -> Parser {
    let trace = Trace::default();
    let decode: Decode = if fail == Some("decode") {
        |_, _| Err("decode")
    } else {
        |docs, rows| append_rows(rows, Ok(docs.to_vec()), |_| 1)
    };
    Parser {
        output: TypedOutput::new(
            decode,
            Writer {
                trace: trace.clone(),
                fail: fail == Some("insert"),
            },
            (|_: &usize| 1) as fn(&usize) -> usize,
            BatchLimits::new(10, 10).unwrap(),
        ),
        trace,
        fail,
        counts: 0,
        documents: vec![Ok(1), Ok(2)],
    }
}
#[tokio::test]
async fn module_parser_owns_setup_counts_bookkeeping_exclusions_and_final_flush() {
    let mut parser = parser(None);
    let report = parse_module(&mut parser).await.unwrap();
    assert_eq!(report.documents, 2);
    assert_eq!(report.rows.len(), 1);
    assert_eq!((report.rows[0].before, report.rows[0].after), (11, 12));
    assert_eq!(
        *parser.trace.lock().unwrap(),
        ["prepare", "before", "cursor", "insert", "after"]
    );
    assert!(parser.output.rows.is_empty());
}
#[tokio::test]
async fn parser_failures_keep_source_and_stop_later_stages() {
    for (failed, stage, expected) in [
        ("prepare", ParseStage::Prepare, &["prepare"][..]),
        (
            "before",
            ParseStage::CountBefore,
            &["prepare", "before"][..],
        ),
        (
            "cursor",
            ParseStage::OpenCursor,
            &["prepare", "before", "cursor"][..],
        ),
        (
            "decode",
            ParseStage::Parse,
            &["prepare", "before", "cursor"][..],
        ),
        (
            "insert",
            ParseStage::Parse,
            &["prepare", "before", "cursor", "insert"][..],
        ),
        (
            "after",
            ParseStage::CountAfter,
            &["prepare", "before", "cursor", "insert", "after"][..],
        ),
    ] {
        let mut parser = parser(Some(failed));
        let failure = parse_module(&mut parser).await.unwrap_err();
        assert_eq!(failure.stage, stage);
        assert_eq!(failure.source, failed);
        assert_eq!(*parser.trace.lock().unwrap(), expected);
        if failed == "insert" {
            assert_eq!(parser.output.rows, [1, 2]);
        }
    }
    let mut parser = parser(None);
    parser.documents = vec![Ok(1), Err("stream"), Ok(3)];
    assert_eq!(
        parse_module(&mut parser).await.unwrap_err().source,
        "stream"
    );
    assert_eq!(
        *parser.trace.lock().unwrap(),
        ["prepare", "before", "cursor"]
    );
}

struct Archive {
    events: Vec<ArchiveStage>,
    fail_at: Option<ArchiveStage>,
}
impl Archive {
    fn step(&mut self, stage: ArchiveStage) -> Result<(), &'static str> {
        self.events.push(stage);
        if self.fail_at == Some(stage) {
            Err("injected")
        } else {
            Ok(())
        }
    }
}
#[async_trait]
impl ArchiveOperations for Archive {
    type Error = &'static str;
    async fn create(&mut self) -> Result<(), Self::Error> {
        self.step(ArchiveStage::Create)
    }
    async fn upload(&mut self) -> Result<(), Self::Error> {
        self.step(ArchiveStage::Upload)
    }
    async fn archive_local(&mut self) -> Result<(), Self::Error> {
        self.step(ArchiveStage::ArchiveLocal)
    }
    async fn cleanup(&mut self) -> Result<(), Self::Error> {
        self.step(ArchiveStage::Cleanup)
    }
}
#[tokio::test]
async fn archive_stops_after_every_failure_and_honors_skip_cleanup() {
    let stages = [
        ArchiveStage::Create,
        ArchiveStage::Upload,
        ArchiveStage::ArchiveLocal,
        ArchiveStage::Cleanup,
    ];
    for (index, stage) in stages.iter().copied().enumerate() {
        let mut archive = Archive {
            events: Vec::new(),
            fail_at: Some(stage),
        };
        let failure = run_archive(&mut archive, true).await.unwrap_err();
        assert_eq!(failure.stage, stage);
        assert_eq!(failure.source, "injected");
        assert_eq!(archive.events, stages[..=index]);
    }
    let mut archive = Archive {
        events: Vec::new(),
        fail_at: None,
    };
    run_archive(&mut archive, false).await.unwrap();
    assert_eq!(archive.events, stages[..3]);
}

struct Temp(std::path::PathBuf);
impl Temp {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "record-contracts-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        Self(root)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[derive(Debug)]
struct Saved {
    data: String,
    date: i32,
    modified: std::time::SystemTime,
}
struct Factory;
impl SavedRecordFactory<Saved> for Factory {
    type Error = RecordError;
    fn decode(&self, metadata: &FileMetaInfo<'_>, data: String) -> Result<Saved, RecordError> {
        let mut tokens = FileMetaInfo::extract_elements_from_file_name(metadata.file_name);
        let date = parse_token(tokens.next(), 0)?;
        assert_eq!(metadata.file_date, date);
        Ok(Saved {
            data,
            date,
            modified: metadata.modified,
        })
    }
}
struct Upload {
    records: Mutex<Vec<i32>>,
    fail_at: Option<usize>,
}
#[async_trait]
impl RecordUploader<Saved> for Upload {
    type Error = RecordError;
    async fn upload(&self, record: &Saved) -> Result<(), RecordError> {
        assert_eq!(record.data, "payload");
        let mut records = self.records.lock().unwrap();
        records.push(record.date);
        if self.fail_at == Some(records.len()) {
            Err(RecordError::MissingToken(99))
        } else {
            Ok(())
        }
    }
}
#[tokio::test]
async fn saved_file_loading_attaches_real_modification_time_and_stops_failed_imports() {
    let temp = Temp::new();
    let mut files = Vec::new();
    for date in 1..=3 {
        let path = temp.0.join(format!("{date}.json"));
        std::fs::write(&path, "payload").unwrap();
        files.push(ImportFile {
            path,
            folder_tokens: Vec::new(),
        });
    }
    let record = load_file(&files[0].path, 1, 100, &Factory).unwrap();
    assert_eq!(
        record.modified,
        std::fs::metadata(&files[0].path)
            .unwrap()
            .modified()
            .unwrap()
    );
    let uploader = Upload {
        records: Mutex::new(Vec::new()),
        fail_at: Some(2),
    };
    let result = import_records(
        &files,
        |file| {
            Ok((
                file.path
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
                100,
            ))
        },
        &Factory,
        &uploader,
    )
    .await;
    assert!(matches!(result, Err(RecordError::MissingToken(99))));
    assert_eq!(*uploader.records.lock().unwrap(), [1, 2]);
    assert!(matches!(
        load_file::<Saved, _>(&temp.0.join("missing.json"), 0, 0, &Factory),
        Err(RecordError::File { .. })
    ));
}

#[test]
fn export_storage_failure_retains_typed_upload_and_io_source() {
    use sctys_rust_utilities::{aws_s3::AWSWriteFileError, pipeline::storage::StorageError};
    use std::error::Error;
    let error = StorageError::Upload(Box::new(AWSWriteFileError::IOError(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "staging denied",
    ))));
    let source = error
        .source()
        .unwrap()
        .downcast_ref::<Box<AWSWriteFileError>>()
        .unwrap();
    assert!(
        matches!(source.as_ref(), AWSWriteFileError::IOError(error) if error.kind() == std::io::ErrorKind::PermissionDenied)
    );
    assert!(error.to_string().contains("staging denied"));
}

#[tokio::test]
async fn scheduled_reports_skip_empty_scopes_and_stop_at_failed_query() {
    use sctys_rust_utilities::pipeline::reporting::*;
    struct Counts {
        calls: Mutex<Vec<(u8, bool)>>,
        fail: bool,
    }
    #[async_trait]
    impl LogCounts<u8> for Counts {
        type Error = &'static str;
        async fn count(&self, dataset: &u8, _: i64, success: bool) -> Result<usize, Self::Error> {
            self.calls.lock().unwrap().push((*dataset, success));
            if self.fail {
                Err("query")
            } else {
                Ok(if success { 1 } else { 2 })
            }
        }
    }
    let time = chrono::DateTime::from_timestamp(0, 0).unwrap();
    let counts = Counts {
        calls: Mutex::new(Vec::new()),
        fail: false,
    };
    assert_eq!(
        report_scrapes(&[] as &[ReportTarget<u8>], time, time, &counts)
            .await
            .unwrap(),
        None
    );
    assert!(counts.calls.lock().unwrap().is_empty());
    let targets = [
        ReportTarget {
            module_name: "first".into(),
            dataset: 1,
        },
        ReportTarget {
            module_name: "second".into(),
            dataset: 2,
        },
    ];
    let message = report_scrapes(&targets, time, time, &counts)
        .await
        .unwrap()
        .unwrap();
    assert!(message.contains("first: 1/2 (50.00%)\nsecond: 1/2 (50.00%)"));
    assert_eq!(
        *counts.calls.lock().unwrap(),
        [(1, true), (1, false), (2, true), (2, false)]
    );
    let failed = Counts {
        calls: Mutex::new(Vec::new()),
        fail: true,
    };
    let failure = report_scrapes(&targets, time, time, &failed)
        .await
        .unwrap_err();
    assert_eq!(failure.source, "query");
    assert_eq!(failure.target_index, 0);
    assert_eq!(*failed.calls.lock().unwrap(), [(1, true)]);
}

#[test]
fn report_query_fixtures_preserve_legacy_boolean_predicates() {
    use sctys_rust_utilities::pipeline::reporting::*;
    let mut table = LogTable {
        table: "fixture_log".into(),
        insert_column: "insert_time".into(),
        scraped_column: "scraped".into(),
        predicate: ScrapedPredicate::Truthy,
    };
    assert_eq!(
        table.count_query(10, false),
        "SELECT COUNT(*) FROM `fixture_log` WHERE `insert_time` >= 10"
    );
    assert_eq!(
        table.count_query(10, true),
        "SELECT COUNT(*) FROM `fixture_log` WHERE `insert_time` >= 10 AND `scraped`"
    );
    table.predicate = ScrapedPredicate::EqualsTrue;
    assert_eq!(
        table.count_query(10, true),
        "SELECT COUNT(*) FROM `fixture_log` WHERE `insert_time` >= 10 AND `scraped` = TRUE"
    );
}
