//! External-crate fixtures: domain enums, macros, serialization and async hooks.
use framework::pipeline::records::{RecordColumns, RecordInput, ScraperRecord};
use sctys_rust_utilities as framework;

framework::scraper_family! {
    pub mod stock {
        keys: SymbolDateKeys, log: SymbolDateLog, document: SymbolDate, input: SymbolDateInput,
        fields: { Symbol => symbol: String, Date => date: i32 }
    }
}

trait ColumnAdapter {
    fn get_column_type(self) -> framework::click_house::ClickHouseType;
    // Match the consumers' by-value adapter contract for Copy column enums.
    #[allow(clippy::wrong_self_convention)]
    fn is_hash_key(self) -> bool;
}

trait ScraperLogAdapter {
    type T;
    fn scraped(&mut self, scraped: bool);
}

trait InputAdapter {
    type S;
    type P;
    fn form_scraper_log(&self) -> Self::S;
    fn form_scraper_struct(&self) -> Self::P;
}

trait ScraperRecordAdapter {
    fn convert_modified_time_to_bson() -> mongodb::bson::DateTime;
}

framework::scraper_family_with_adapters! {
    pub mod adapted {
        keys: AdaptedKeys, log: AdaptedLog, document: AdaptedRecord, input: AdaptedInput,
        fields: { Date => date: i32 },
        adapters: {
            column: ColumnAdapter,
            log: ScraperLogAdapter,
            input: InputAdapter,
            record: ScraperRecordAdapter
        }
    }
}

impl ScraperRecordAdapter for adapted::AdaptedRecord {
    fn convert_modified_time_to_bson() -> mongodb::bson::DateTime {
        mongodb::bson::DateTime::from_millis(123)
    }
}
framework::scraper_family! {
    pub mod sports {
        keys: FixtureKeys, log: FixtureLog, document: Fixture, input: FixtureInput,
        fields: { Competition => competition: i32, Season => season: i32, Fixture => fixture: i64 }
    }
}

#[test]
fn generated_families_preserve_order_types_keys_and_conversions() {
    let input = stock::SymbolDateInput {
        symbol: "ABC".into(),
        date: 20261023,
    };
    let mut record = input.record_at(mongodb::bson::DateTime::from_millis(123));
    record.update_data("payload".into());
    let document = record.document().unwrap();
    assert_eq!(
        document.keys().map(String::as_str).collect::<Vec<_>>(),
        ["modified", "data", "symbol", "date"]
    );
    assert_eq!(
        document
            .get_datetime("modified")
            .unwrap()
            .timestamp_millis(),
        123
    );
    assert_eq!(
        record.query().unwrap(),
        mongodb::bson::doc! { "symbol": "ABC", "date": 20261023 }
    );
    assert_eq!(
        stock::SymbolDate::index_keys(),
        ["symbol", "date", "modified"]
    );
    assert_eq!(
        stock::SymbolDateInput::projection(),
        mongodb::bson::doc! { "symbol": 1, "date": 1 }
    );
    assert_eq!(
        serde_json::to_string(&input).unwrap(),
        r#"{"symbol":"ABC","date":20261023}"#
    );
    let log = input.log_at(100);
    assert_eq!(
        <stock::SymbolDateLog as clickhouse::Row>::COLUMN_NAMES,
        ["symbol", "date", "scraped", "insert_time"]
    );
    assert_eq!(
        serde_json::to_value(log).unwrap(),
        serde_json::json!({"symbol":"ABC","date":20261023,"scraped":false,"insert_time":100})
    );
    let first = sports::FixtureInput {
        competition: 1,
        season: 2025,
        fixture: 10,
    };
    let later = sports::FixtureInput {
        competition: 1,
        season: 2026,
        fixture: 1,
    };
    assert!(first < later);
    let encoded = serde_json::to_string(&first).unwrap();
    assert_eq!(encoded, r#"{"competition":1,"season":2025,"fixture":10}"#);
    assert_eq!(
        serde_json::from_str::<sports::FixtureInput>(&encoded).unwrap(),
        first
    );
    let columns = sports::FixtureKeys::columns();
    assert_eq!(
        columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["competition", "season", "fixture"]
    );
    assert!(columns.iter().all(|column| column.is_hash_key));
}

#[test]
fn shared_cutoff_date_row_preserves_the_common_storage_schema() {
    use framework::pipeline::records::{CutoffDateRecord, CUTOFF_DATE_FIELD, INSERT_TIME_FIELD};

    let cutoff = CutoffDateRecord {
        cutoff_date: 20261023,
        insert_time: 1_792_742_400,
    };
    assert_eq!(CUTOFF_DATE_FIELD, "cutoff_date");
    assert_eq!(
        <CutoffDateRecord as clickhouse::Row>::COLUMN_NAMES,
        [CUTOFF_DATE_FIELD, INSERT_TIME_FIELD]
    );
    assert_eq!(
        serde_json::to_value(cutoff).unwrap(),
        serde_json::json!({"cutoff_date": 20261023, "insert_time": 1_792_742_400})
    );
}

#[test]
fn generated_family_adapters_use_the_shared_record_contracts() {
    let input = adapted::AdaptedInput { date: 20261023 };
    let mut log = InputAdapter::form_scraper_log(&input);
    ScraperLogAdapter::scraped(&mut log, true);
    assert!(log.scraped);
    assert_eq!(log.date, 20261023);

    let record = InputAdapter::form_scraper_struct(&input);
    assert_eq!(record.modified.timestamp_millis(), 123);
    assert_eq!(record.date, 20261023);
    assert!(ColumnAdapter::is_hash_key(adapted::AdaptedKeys::Date));
    assert!(matches!(
        ColumnAdapter::get_column_type(adapted::AdaptedKeys::Date),
        framework::click_house::ClickHouseType::Int32(false)
    ));
}

#[test]
fn associated_sources_keep_domain_definitions_external() {
    use framework::pipeline::metadata::{DatasetMetadata, ModuleMetadata, SourceIdentity};
    struct Source;
    impl SourceIdentity for Source {
        fn source_name(&self) -> String {
            "fixture_source".into()
        }
        fn prefix(&self) -> String {
            "fx_".into()
        }
    }
    struct Dataset;
    impl DatasetMetadata for Dataset {
        type Source = Source;
        fn source(&self) -> Source {
            Source
        }
        fn dataset_name(&self) -> String {
            "fixture".into()
        }
        fn is_bookkeeping(&self) -> bool {
            false
        }
    }
    struct Module;
    impl ModuleMetadata for Module {
        type Source = Source;
        type Dataset = Dataset;
        fn source(&self) -> Source {
            Source
        }
        fn module_name(&self) -> String {
            "module".into()
        }
        fn datasets(&self) -> Vec<Dataset> {
            vec![Dataset]
        }
    }
    assert_eq!(Module.source().prefix(), "fx_");
    assert_eq!(
        Module.datasets()[0].source().source_name(),
        "fixture_source"
    );
}

#[test]
fn transport_sessions_and_async_header_borrows_are_send() {
    use framework::pipeline::transport::{HeaderContext, TransportSession};
    fn send<T: Send>() {}
    send::<TransportSession<'static>>();
    send::<HeaderContext<'static, 'static>>();
    // Compiles the borrowing pattern used by per-run stock/sports provider hooks.
    async fn header_hook(session: &mut TransportSession<'_>) {
        let context = session.header_context();
        tokio::task::yield_now().await;
        context.request_options.headers = None;
        *context.domain_update = None;
    }
    fn future_is_send<T: std::future::Future + Send>(_: T) {}
    fn check(session: &mut TransportSession<'_>) {
        future_is_send(header_hook(session));
    }
    let _ = check;
}

#[test]
fn ordinary_saved_families_only_decode_inputs_and_share_record_construction() {
    use framework::pipeline::records::{
        parse_token, FileMetaInfo, InputRecordFactory, RecordError, SavedRecordFactory,
    };
    let factory = InputRecordFactory::new(|metadata: &FileMetaInfo<'_>| {
        let mut tokens = FileMetaInfo::extract_elements_from_file_name(metadata.file_name);
        Ok::<_, RecordError>(stock::SymbolDateInput {
            symbol: parse_token(tokens.next(), 0)?,
            date: parse_token(tokens.next(), 1)?,
        })
    });
    let metadata = FileMetaInfo {
        folder_path: std::path::Path::new("/fixture"),
        file_name: "ABC_20261023.json",
        file_date: 20261023,
        download_time: 0,
        modified: std::time::UNIX_EPOCH + std::time::Duration::from_millis(123),
    };
    let record = factory.decode(&metadata, "payload".into()).unwrap();
    assert_eq!(record.symbol, "ABC");
    assert_eq!(record.date, 20261023);
    assert_eq!(record.modified.timestamp_millis(), 123);
    assert_eq!(record.data, "payload");
    assert_eq!(
        <stock::SymbolDateKeys as strum::VariantArray>::VARIANTS.len(),
        2
    );
}
