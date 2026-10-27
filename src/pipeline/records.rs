//! Record contracts, saved-file loading and hygienic family generation.
use crate::click_house::{ClickHouseColumn, ClickHouseType};
use async_trait::async_trait;
use mongodb::bson::{self, Document};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt,
    path::{Path, PathBuf},
    time::SystemTime,
};

/// BSON field names used by the shared record contract.
pub const MODIFIED_FIELD: &str = "modified";
pub const DATA_FIELD: &str = "data";
pub const SCRAPED_FIELD: &str = "scraped";
pub const INSERT_TIME_FIELD: &str = "insert_time";
pub const CUTOFF_DATE_FIELD: &str = "cutoff_date";

/// Standard cutoff-date row stored in scraper bookkeeping datasets.
///
/// The date policy that produces `cutoff_date` belongs to each application;
/// this type keeps the shared ClickHouse and serialized row schema in one place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize, clickhouse::Row)]
pub struct CutoffDateRecord {
    pub cutoff_date: i32,
    pub insert_time: i64,
}

/// Shared BSON fields written by scraper records and scrape logs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommonScraperKeys {
    Modified,
    Data,
    Scraped,
}

impl fmt::Display for CommonScraperKeys {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Modified => MODIFIED_FIELD,
            Self::Data => DATA_FIELD,
            Self::Scraped => SCRAPED_FIELD,
        })
    }
}

pub fn missing_file_element_message(element: Option<&str>) -> String {
    format!("Unable to obtain the value from {element:?}")
}

pub fn invalid_file_element_message(
    element: &str,
    target_type: &str,
    error: impl fmt::Display,
) -> String {
    format!("Unable to convert the value {element:?} to {target_type}. {error}")
}

pub fn file_element_as_string(element: Option<&str>) -> Result<&str, String> {
    element.ok_or_else(|| missing_file_element_message(element))
}

pub fn parse_file_element<T>(element: Option<&str>, target_type: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: fmt::Display,
{
    let value = file_element_as_string(element)?;
    value
        .parse()
        .map_err(|error| invalid_file_element_message(value, target_type, error))
}

#[derive(Debug)]
pub struct FileMetaInfo<'a> {
    pub folder_path: &'a Path,
    pub file_name: &'a str,
    pub file_date: i32,
    pub download_time: i64,
    pub modified: SystemTime,
}
impl FileMetaInfo<'_> {
    /// Retains the legacy underscore/extension tokenization, including empty tokens.
    pub fn extract_elements_from_file_name(name: &str) -> impl Iterator<Item = &str> {
        name.split('_').filter_map(|part| part.split('.').next())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error("Unable to read {path}: {source}")]
    File {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Missing filename token {0}")]
    MissingToken(usize),
    #[error("Invalid filename token {index}: {value}")]
    InvalidToken { index: usize, value: String },
    #[error(transparent)]
    Bson(#[from] bson::ser::Error),
}
pub fn parse_token<T: std::str::FromStr>(
    value: Option<&str>,
    index: usize,
) -> Result<T, RecordError> {
    let value = value.ok_or(RecordError::MissingToken(index))?;
    value.parse().map_err(|_| RecordError::InvalidToken {
        index,
        value: value.into(),
    })
}

pub trait RecordColumns {
    fn columns() -> Vec<ClickHouseColumn>;
    fn input_query(table: &str) -> String {
        use sea_query::{Alias, MysqlQueryBuilder, Query};
        Query::select()
            .columns(Self::query_keys().into_iter().map(Alias::new))
            .from(Alias::new(table))
            .to_string(MysqlQueryBuilder)
    }
    fn query_keys() -> Vec<String> {
        Self::columns()
            .into_iter()
            .map(|column| column.name)
            .collect()
    }
    fn index_keys() -> Vec<String> {
        let mut keys = Self::query_keys();
        keys.push(MODIFIED_FIELD.into());
        keys
    }
}

pub trait ScraperRecord: Serialize + DeserializeOwned + Send + Sync + RecordColumns {
    fn update_data(&mut self, data: String);
    fn estimate_memory(&self) -> usize;
    fn document(&self) -> Result<Document, RecordError> {
        Ok(bson::to_document(self)?)
    }
    fn query(&self) -> Result<Document, RecordError> {
        let mut document = self.document()?;
        document.remove(MODIFIED_FIELD);
        document.remove(DATA_FIELD);
        Ok(document)
    }
}

pub trait ScrapeLogRecord: Serialize + Send {
    fn set_scraped(&mut self, scraped: bool);
}

pub trait RecordInput:
    Serialize + DeserializeOwned + Eq + Ord + std::hash::Hash + Send + Sync
{
    type Record: ScraperRecord;
    type Log: ScrapeLogRecord;
    fn record_at(&self, modified: bson::DateTime) -> Self::Record;
    fn log_at(&self, insert_time: i64) -> Self::Log;
    fn projection() -> Document {
        Self::Record::query_keys()
            .into_iter()
            .map(|key| (key, bson::Bson::Int32(1)))
            .collect()
    }
}

pub trait FieldType {
    fn column_type() -> ClickHouseType;
}
impl FieldType for i32 {
    fn column_type() -> ClickHouseType {
        ClickHouseType::Int32(false)
    }
}
impl FieldType for i64 {
    fn column_type() -> ClickHouseType {
        ClickHouseType::Int64(false)
    }
}
impl FieldType for String {
    fn column_type() -> ClickHouseType {
        ClickHouseType::String(false)
    }
}
impl FieldType for bool {
    fn column_type() -> ClickHouseType {
        ClickHouseType::Boolean(false)
    }
}

/// Local hooks interpret filenames or exceptional payloads; loading stays shared.
pub trait SavedRecordFactory<R> {
    type Error: From<RecordError>;
    fn decode(&self, metadata: &FileMetaInfo<'_>, data: String) -> Result<R, Self::Error>;
}

/// Ordinary file families declare only filename/folder-to-input decoding. The
/// shared adapter attaches modification time and payload to the generated record.
pub struct InputRecordFactory<I, F> {
    parse_input: F,
    input: std::marker::PhantomData<fn() -> I>,
}
impl<I, F> InputRecordFactory<I, F> {
    pub fn new(parse_input: F) -> Self {
        Self {
            parse_input,
            input: std::marker::PhantomData,
        }
    }
}
impl<I, F, E> SavedRecordFactory<I::Record> for InputRecordFactory<I, F>
where
    I: RecordInput,
    F: Fn(&FileMetaInfo<'_>) -> Result<I, E>,
    E: From<RecordError>,
{
    type Error = E;
    fn decode(&self, metadata: &FileMetaInfo<'_>, data: String) -> Result<I::Record, Self::Error> {
        let input = (self.parse_input)(metadata)?;
        let mut record = input.record_at(bson::DateTime::from_system_time(metadata.modified));
        record.update_data(data);
        Ok(record)
    }
}

pub fn load_saved_record<R, F: SavedRecordFactory<R>>(
    metadata: &FileMetaInfo<'_>,
    factory: &F,
) -> Result<R, F::Error> {
    let path = metadata.folder_path.join(metadata.file_name);
    let data =
        std::fs::read_to_string(&path).map_err(|source| RecordError::File { path, source })?;
    factory.decode(metadata, data)
}

/// Reads modification time and payload once; local code supplies only the date
/// interpretation and record factory.
pub fn load_file<R, F: SavedRecordFactory<R>>(
    path: &Path,
    file_date: i32,
    download_time: i64,
    factory: &F,
) -> Result<R, F::Error> {
    let file_error = |source| RecordError::File {
        path: path.to_path_buf(),
        source,
    };
    let modified = std::fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .map_err(file_error)?;
    load_saved_record(
        &file_metadata(path, file_date, download_time, modified)?,
        factory,
    )
}

fn file_metadata(
    path: &Path,
    file_date: i32,
    download_time: i64,
    modified: SystemTime,
) -> Result<FileMetaInfo<'_>, RecordError> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| RecordError::File {
            path: path.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "missing Unicode filename",
            ),
        })?;
    let folder = path.parent().unwrap_or_else(|| Path::new(""));
    Ok(FileMetaInfo {
        folder_path: folder,
        file_name: name,
        file_date,
        download_time,
        modified,
    })
}

pub async fn load_saved_record_async<R, F: SavedRecordFactory<R>>(
    metadata: &FileMetaInfo<'_>,
    factory: &F,
) -> Result<R, F::Error> {
    let path = metadata.folder_path.join(metadata.file_name);
    let data = tokio::fs::read_to_string(&path)
        .await
        .map_err(|source| RecordError::File { path, source })?;
    factory.decode(metadata, data)
}

pub async fn load_file_async<R, F: SavedRecordFactory<R>>(
    path: &Path,
    file_date: i32,
    download_time: i64,
    factory: &F,
) -> Result<R, F::Error> {
    let modified = tokio::fs::metadata(path)
        .await
        .and_then(|metadata| metadata.modified())
        .map_err(|source| RecordError::File {
            path: path.to_path_buf(),
            source,
        })?;
    load_saved_record_async(
        &file_metadata(path, file_date, download_time, modified)?,
        factory,
    )
    .await
}

#[async_trait]
pub trait RecordUploader<R: Sync>: Sync {
    type Error: Send;
    async fn upload(&self, record: &R) -> Result<(), Self::Error>;
}

/// Shared saved-file construction and persistence. Errors are never notified here.
pub async fn load_and_upload<R: Sync, F, U>(
    metadata: &FileMetaInfo<'_>,
    factory: &F,
    uploader: &U,
) -> Result<(), U::Error>
where
    F: SavedRecordFactory<R, Error = U::Error>,
    U: RecordUploader<R>,
{
    let record = load_saved_record_async(metadata, factory).await?;
    uploader.upload(&record).await
}

/// Shared import traversal execution. The date hook interprets folder/filename
/// tokens without loading files or repeating upload orchestration.
pub async fn import_records<R: Sync, F, U>(
    files: &[super::archive::ImportFile],
    mut dates: impl FnMut(&super::archive::ImportFile) -> Result<(i32, i64), U::Error>,
    factory: &F,
    uploader: &U,
) -> Result<usize, U::Error>
where
    F: SavedRecordFactory<R, Error = U::Error>,
    U: RecordUploader<R>,
{
    let mut count = 0usize;
    for file in files {
        let (date, download_time) = dates(file)?;
        let record = load_file_async(&file.path, date, download_time, factory).await?;
        uploader.upload(&record).await?;
        count = count.saturating_add(1);
    }
    Ok(count)
}

/// Declares the domain fields once. The module creates an isolated namespace for
/// derive helpers, so callers need no imports (and may rename this dependency).
/// Filename decoding is supplied separately through [`SavedRecordFactory`].
///
/// ```
/// sctys_rust_utilities::scraper_family! {
///     pub mod fixture {
///         keys: Keys, log: Log, document: Record, input: Input,
///         fields: { Symbol => symbol: String, Date => date: i32 }
///     }
/// }
/// let input = fixture::Input { symbol: "ABC".into(), date: 20261023 };
/// assert_eq!(input.symbol, "ABC");
/// ```
#[macro_export]
macro_rules! scraper_family {
    ($visibility:vis mod $family:ident {
        keys: $keys:ident, log: $log:ident, document: $record:ident, input: $input:ident,
        fields: { $( $variant:ident => $field:ident : $ty:ty ),+ $(,)? }
    }) => {
        $visibility mod $family {
            use $crate::__record_dependencies::{serde as __serde, clickhouse};
            use $crate::pipeline::records::{RecordColumns, ScraperRecord};
            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            pub enum $keys { $( $variant ),+ }
            impl $crate::__record_dependencies::strum::VariantArray for $keys {
                const VARIANTS: &'static [Self] = &[$( Self::$variant ),+];
            }
            impl ::std::fmt::Display for $keys {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    f.write_str(match self { $( Self::$variant => stringify!($field), )+ })
                }
            }
            impl RecordColumns for $keys {
                fn columns() -> ::std::vec::Vec<$crate::click_house::ClickHouseColumn> {
                    ::std::vec![$( $crate::click_house::ClickHouseColumn {
                        name: stringify!($field).into(), column_type: <$ty as $crate::pipeline::records::FieldType>::column_type(), is_hash_key: true,
                    }, )+]
                }
            }
            #[derive(Debug, Clone, __serde::Serialize, __serde::Deserialize, clickhouse::Row)]
            #[serde(crate = "__serde")]
            pub struct $log { $( pub $field: $ty, )+ pub scraped: bool, pub insert_time: i64 }
            impl $crate::pipeline::records::ScrapeLogRecord for $log {
                fn set_scraped(&mut self, scraped: bool) { self.scraped = scraped; }
            }
            #[derive(Debug, Clone, __serde::Serialize, __serde::Deserialize)]
            #[serde(crate = "__serde")]
            pub struct $record {
                pub modified: $crate::__record_dependencies::mongodb::bson::DateTime,
                pub data: ::std::string::String,
                $( pub $field: $ty, )+
            }
            impl RecordColumns for $record {
                fn columns() -> ::std::vec::Vec<$crate::click_house::ClickHouseColumn> { <$keys as RecordColumns>::columns() }
            }
            impl ScraperRecord for $record {
                fn update_data(&mut self, data: ::std::string::String) { self.data = data; }
                fn estimate_memory(&self) -> usize { self.data.capacity() }
            }
            #[derive(Debug, __serde::Serialize, __serde::Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, clickhouse::Row)]
            #[serde(crate = "__serde")]
            pub struct $input { $( pub $field: $ty, )+ }
            impl $crate::pipeline::records::RecordInput for $input {
                type Record = $record;
                type Log = $log;
                fn record_at(&self, modified: $crate::__record_dependencies::mongodb::bson::DateTime) -> Self::Record {
                    $record { modified, data: ::std::string::String::new(), $( $field: self.$field.clone(), )+ }
                }
                fn log_at(&self, insert_time: i64) -> Self::Log {
                    $log { $( $field: self.$field.clone(), )+ scraped: false, insert_time }
                }
            }
        }
    };
}

/// Generates a record family and the standard adapters used by stock and
/// sports common crates. Domain-specific filename decoders remain caller-owned.
///
/// The adapter traits must provide the same minimal contract as
/// `SourceDatasetColumn`, `ScraperLog`, `InputStruct`, and `ScraperStruct` in
/// the application crates. Paths are supplied by the caller, so this macro
/// does not depend on any application crate.
#[macro_export]
macro_rules! scraper_family_with_adapters {
    (
        $visibility:vis mod $family:ident {
            keys: $keys:ident, log: $log:ident, document: $record:ident, input: $input:ident,
            fields: { $( $variant:ident => $field:ident : $ty:ty ),+ $(,)? },
            adapters: {
                column: $column_trait:path,
                log: $scraper_log_trait:path,
                input: $input_trait:path,
                record: $scraper_record_trait:path $(,)?
            }
        }
    ) => {
        $crate::scraper_family! {
            $visibility mod $family {
                keys: $keys, log: $log, document: $record, input: $input,
                fields: { $( $variant => $field : $ty ),+ }
            }
        }
        $visibility use $family::{$keys, $log, $record, $input};

        impl $column_trait for $keys {
            fn get_column_type(self) -> $crate::click_house::ClickHouseType {
                match self {
                    $( Self::$variant => <$ty as $crate::pipeline::records::FieldType>::column_type(), )+
                }
            }

            fn is_hash_key(self) -> bool {
                true
            }
        }

        impl $scraper_log_trait for $log {
            type T = $keys;

            fn scraped(&mut self, scraped: bool) {
                $crate::pipeline::records::ScrapeLogRecord::set_scraped(self, scraped);
            }
        }

        impl $input_trait for $input {
            type S = $log;
            type P = $record;

            fn form_scraper_log(&self) -> Self::S {
                $crate::pipeline::records::RecordInput::log_at(
                    self,
                    $crate::__record_dependencies::chrono::Utc::now().timestamp(),
                )
            }

            fn form_scraper_struct(&self) -> Self::P {
                $crate::pipeline::records::RecordInput::record_at(
                    self,
                    <$record as $scraper_record_trait>::convert_modified_time_to_bson(),
                )
            }
        }
    };
}

/// Multiple records from one provider payload use the same persistence loop as imports.
pub async fn upload_records<R: Sync, U: RecordUploader<R>>(
    records: &[R],
    uploader: &U,
) -> Result<(), U::Error> {
    for record in records {
        uploader.upload(record).await?;
    }
    Ok(())
}

/// Legacy folder components after a configured module directory. Non-Unicode
/// components are ignored, preserving saved-file decoder behavior.
pub fn folder_components(folder: &Path, module_folder: String) -> impl Iterator<Item = &str> {
    let mut after_module = false;
    folder.iter().filter_map(move |component| {
        if after_module {
            component.to_str()
        } else {
            if component.to_str() == Some(module_folder.as_str()) {
                after_module = true;
            }
            None
        }
    })
}

/// A family whose input layout and sort order differ from its stored columns.
/// Field order and payload mutation policy are explicit compatibility settings.
#[macro_export]
macro_rules! scraper_family_layout {
    ($vis:vis mod $family:ident {
        keys: $keys:ident, log: $log:ident, document: $record:ident, input: $input:ident,
        fields: { $( $variant:ident => $field:ident : $ty:ty ),+ $(,)? },
        input_fields: { $( $input_field:ident : $input_ty:ty ),+ $(,)? },
        order: [ $( $sort:ident ),+ $(,)? ],
        refresh_modified: $refresh:expr,
        adapters: { column: $column:path, log: $log_trait:path, input: $input_trait:path, record: $record_trait:path }
    }) => {
        $vis mod $family {
        use $crate::__record_dependencies::{serde as __serde, clickhouse};
        $crate::scraper_family! {
            pub mod generated {
                keys: $keys, log: $log, document: $record, input: GeneratedInput,
                fields: { $( $variant => $field : $ty ),+ }
            }
        }
        pub use generated::{$keys, $log, $record};
        #[derive(Debug, Clone, __serde::Serialize, __serde::Deserialize, PartialEq, Eq, Hash, clickhouse::Row)]
        #[serde(crate = "__serde")]
        pub struct $input { $( pub $input_field: $input_ty, )+ }
        impl Ord for $input {
            fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
                ::std::cmp::Ordering::Equal$(.then_with(|| self.$sort.cmp(&other.$sort)))+
            }
        }
        impl PartialOrd for $input {
            fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> { Some(self.cmp(other)) }
        }
        impl $column for $keys {
            fn get_column_type(self) -> $crate::click_house::ClickHouseType {
                match self { $( Self::$variant => <$ty as $crate::pipeline::records::FieldType>::column_type(), )+ }
            }
            fn is_hash_key(self) -> bool { true }
        }
        impl $log_trait for $log {
            type T = $keys;
            fn scraped(&mut self, scraped: bool) { self.scraped = scraped; }
        }
        impl $crate::pipeline::records::RecordInput for $input {
            type Record = $record;
            type Log = $log;
            fn record_at(&self, modified: $crate::__record_dependencies::mongodb::bson::DateTime) -> Self::Record {
                $record { modified, data: String::new(), $( $field: self.$field.clone(), )+ }
            }
            fn log_at(&self, insert_time: i64) -> Self::Log {
                $log { $( $field: self.$field.clone(), )+ scraped: false, insert_time }
            }
        }
        impl $input_trait for $input {
            type S = $log;
            type P = $record;
            fn form_scraper_log(&self) -> Self::S {
                $crate::pipeline::records::RecordInput::log_at(self, $crate::__record_dependencies::chrono::Utc::now().timestamp())
            }
            fn form_scraper_struct(&self) -> Self::P {
                $crate::pipeline::records::RecordInput::record_at(self, <$record as $record_trait>::convert_modified_time_to_bson())
            }
        }
        impl $record_trait for $record {
            type T = $keys;
            fn update_data(&mut self, data: String) {
                $crate::pipeline::records::ScraperRecord::update_data(self, data);
                if $refresh { self.modified = <Self as $record_trait>::convert_modified_time_to_bson(); }
            }
            fn estimate_data_memory(&self) -> usize {
                $crate::pipeline::records::ScraperRecord::estimate_memory(self)
            }
        }
        }
        $vis use $family::{$keys, $log, $record, $input};
    };
}

/// Stages a provider-transformed record for persistence by the scraper engine.
/// The mutation hook preserves consumer-specific timestamp policies.
pub fn stage_record<R: Clone>(
    record: &mut R,
    data: String,
    update: impl FnOnce(&mut R, String),
    pending: &mut Vec<R>,
) {
    update(record, data);
    pending.push(record.clone());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_element_parsing_uses_shared_errors() {
        assert_eq!(file_element_as_string(Some("ABC")), Ok("ABC"));
        assert_eq!(
            file_element_as_string(None),
            Err("Unable to obtain the value from None".into())
        );
        assert_eq!(parse_file_element::<i32>(Some("42"), "i32"), Ok(42));
        assert_eq!(
            parse_file_element::<i32>(Some("bad"), "i32"),
            Err("Unable to convert the value \"bad\" to i32. invalid digit found in string".into())
        );
    }
}
