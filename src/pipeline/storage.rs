//! Configured persistence adapters over the existing utility clients.
use super::parse::RowSink;
use crate::click_house::ClickHouse;
use async_trait::async_trait;
use clickhouse::Row;
use serde::Serialize;

/// A borrowed service context avoids owning a logger and a client borrowing it.
pub struct ClickHouseSink<'a, 'logger> {
    pub click_house: &'a ClickHouse<'logger>,
    pub client: &'a clickhouse::Client,
    pub table: String,
}

#[async_trait]
impl<R> RowSink<R> for ClickHouseSink<'_, '_>
where
    R: Serialize + Row + std::fmt::Debug + Send + Sync,
{
    type Error = clickhouse::error::Error;
    async fn insert(&mut self, rows: &[R]) -> Result<(), Self::Error> {
        self.click_house
            .insert_table_from_row(self.client, &self.table, rows)
            .await
    }
}

use super::export::DatasetExport;
use super::records::MODIFIED_FIELD;
use crate::{
    aws_s3::{AWSFileIO, AWSWriteFileError},
    mongo_db::MongoDB,
};
use chrono::{DateTime, Utc};
use mongodb::{
    bson::{doc, Document},
    Collection, Cursor,
};
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};

/// Bounds are explicit because existing row checks and exports use different
/// end-point inclusion rules. The adapter never changes a supplied date window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndBound {
    Exclusive,
    Inclusive,
}
#[derive(Clone, Copy, Debug)]
pub struct TimeWindow {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub end: EndBound,
}
impl TimeWindow {
    pub fn filter(&self, column: &str) -> String {
        format!(
            "{column} >= {} AND {column} {} {}",
            self.from.timestamp(),
            if self.end == EndBound::Inclusive {
                "<="
            } else {
                "<"
            },
            self.to.timestamp()
        )
    }
}

pub struct CollectionStore<'a, 'logger> {
    pub mongo: &'a MongoDB<'logger>,
    pub client: &'a mongodb::Client,
    pub database: &'a str,
    pub collection: &'a str,
    pub modified_key: &'a str,
}

impl<'a, 'logger> CollectionStore<'a, 'logger> {
    pub fn new(
        mongo: &'a MongoDB<'logger>,
        client: &'a mongodb::Client,
        database: &'a str,
        collection: &'a str,
    ) -> Self {
        Self {
            mongo,
            client,
            database,
            collection,
            modified_key: MODIFIED_FIELD,
        }
    }

    pub fn with_modified_key(mut self, modified_key: &'a str) -> Self {
        self.modified_key = modified_key;
        self
    }
}

impl CollectionStore<'_, '_> {
    pub fn collection<R: Send + Sync>(&self) -> Collection<R> {
        let db = self.mongo.obtain_database(self.client, self.database);
        self.mongo.obtain_collection(&db, self.collection)
    }
    pub async fn find<R: Send + Sync>(
        &self,
        query: Document,
        projection: Option<Document>,
        sort: Option<Document>,
    ) -> mongodb::error::Result<Cursor<R>> {
        self.mongo
            .find_documents(&self.collection(), query, projection, sort)
            .await
    }
    pub async fn load<R: Send + Sync>(&self) -> mongodb::error::Result<Cursor<R>> {
        self.find(doc! {}, None, Some(doc! {self.modified_key: 1}))
            .await
    }
    pub async fn upload<R: Send + Sync + Serialize>(
        &self,
        record: &R,
        query: Document,
    ) -> mongodb::error::Result<()> {
        let collection = self.collection();
        let mut keys: Vec<_> = query.keys().cloned().collect();
        keys.push(self.modified_key.into());
        self.mongo.create_indexes(&collection, &keys).await?;
        self.mongo
            .replace_document(&collection, record, query)
            .await
    }
    pub fn backup(
        &self,
        from: &DateTime<Utc>,
        to: &DateTime<Utc>,
        folder: &Path,
    ) -> mongodb::error::Result<()> {
        backup_collection_by_modified_date(
            self.mongo,
            self.database,
            self.collection,
            from,
            to,
            folder,
        )
    }
    pub async fn remove(
        &self,
        from: &DateTime<Utc>,
        to: &DateTime<Utc>,
    ) -> mongodb::error::Result<u64> {
        self.mongo
            .delete_documents_by_modified_date(&self.collection::<Document>(), from, to)
            .await
    }
}

/// Backs up a collection using the shared modified-date window semantics.
/// Collection and project identities remain application-owned values.
pub fn backup_collection_by_modified_date(
    mongo: &MongoDB<'_>,
    database: &str,
    collection: &str,
    from: &DateTime<Utc>,
    to: &DateTime<Utc>,
    folder: &Path,
) -> mongodb::error::Result<()> {
    mongo.backup_collection_by_modified_date(database, collection, from, to, folder)
}

/// A configured table; schema and business queries remain caller declarations.
pub struct TableStore<'a, 'logger> {
    pub click_house: &'a ClickHouse<'logger>,
    pub client: &'a clickhouse::Client,
    pub database: &'a str,
    pub table: &'a str,
}
impl TableStore<'_, '_> {
    pub async fn exists(&self) -> clickhouse::error::Result<bool> {
        table_exists(self.client, self.table).await
    }
    pub async fn has_rows(
        &self,
        column: &str,
        window: TimeWindow,
    ) -> clickhouse::error::Result<bool> {
        table_has_rows(self.client, self.table, column, window).await
    }
    pub async fn create_raw(
        &self,
        columns: &[crate::click_house::ClickHouseColumn],
    ) -> clickhouse::error::Result<()> {
        self.click_house
            .create_replacing_merge_tree_table(self.client, self.table, columns)
            .await
    }
    pub async fn create_standardized(
        &self,
        columns: &[crate::click_house::ClickHouseColumn],
    ) -> clickhouse::error::Result<()> {
        self.click_house
            .create_aggregating_merge_tree_table(self.client, self.table, columns)
            .await
    }
    pub async fn insert<R: Serialize + Row + std::fmt::Debug>(
        &self,
        rows: &[R],
    ) -> clickhouse::error::Result<()> {
        self.click_house
            .insert_table_from_row(self.client, self.table, rows)
            .await
    }
    pub async fn load<R: DeserializeOwned + Row + std::fmt::Debug>(
        &self,
    ) -> clickhouse::error::Result<Vec<R>> {
        self.click_house.load_table(self.client, self.table).await
    }
    pub async fn count(&self) -> clickhouse::error::Result<usize> {
        let count = self
            .client
            .query(&format!(
                "SELECT count() FROM `{}`",
                self.table.replace('`', "``")
            ))
            .fetch_one::<u64>()
            .await?;
        usize::try_from(count).map_err(|error| clickhouse::error::Error::Custom(error.to_string()))
    }
    pub async fn query<R: DeserializeOwned + Row + std::fmt::Debug>(
        &self,
        query: &str,
    ) -> clickhouse::error::Result<Vec<R>> {
        self.click_house.query_table(self.client, query).await
    }
    pub fn query_rows<R: DeserializeOwned + Row + std::fmt::Debug>(
        &self,
        query: &str,
    ) -> clickhouse::error::Result<clickhouse::query::RowCursor<R>> {
        self.click_house.query_rows_from_table(self.client, query)
    }
    pub async fn execute(&self, query: &str) -> clickhouse::error::Result<()> {
        self.client.query(query).execute().await
    }
    pub async fn create_dictionary(
        &self,
        name: &str,
        keys: &[crate::click_house::ClickHouseColumn],
        values: &[crate::click_house::ClickHouseColumn],
        query: &str,
    ) -> clickhouse::error::Result<()> {
        self.click_house
            .create_dictionary_if_not_exists(
                self.client,
                self.database,
                self.table,
                name,
                keys,
                values,
                query,
            )
            .await
    }
    pub fn rows<R: DeserializeOwned + Row + std::fmt::Debug>(
        &self,
    ) -> clickhouse::error::Result<clickhouse::query::RowCursor<R>> {
        self.click_house
            .load_rows_from_table(self.client, self.table)
    }
    pub async fn finalize(&self) -> clickhouse::error::Result<()> {
        self.click_house
            .deduplication_on_table(self.client, self.table)
            .await
    }
    pub fn import_parquet(&self, folder: &Path, name: &str) -> clickhouse::error::Result<()> {
        if folder.join(name).is_file() {
            self.click_house
                .insert_table_from_parquet(self.database, self.table, folder, name)
        } else {
            Ok(())
        }
    }
    pub fn export(
        &self,
        column: &str,
        window: TimeWindow,
        folder: &Path,
        name: &str,
    ) -> clickhouse::error::Result<()> {
        self.click_house.export_table_to_parquet_with_filter(
            self.database,
            self.table,
            &window.filter(column),
            folder,
            name,
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error(transparent)]
    Database(#[from] clickhouse::error::Error),
    #[error(transparent)]
    File(#[from] std::io::Error),
    #[error(transparent)]
    Task(#[from] tokio::task::JoinError),
    #[error("Upload failed: {0}")]
    Upload(#[source] Box<AWSWriteFileError>),
}

/// Complete operation arguments supplied from local metadata and layout config.
#[derive(Debug)]
pub struct DatasetExportSpec {
    pub database: String,
    pub table: String,
    pub date_column: String,
    pub row_window: TimeWindow,
    pub export_window: TimeWindow,
    pub finalize: bool,
    pub replace_staging: bool,
    pub staging_folder: PathBuf,
    pub archive_folder: PathBuf,
    pub file_name: String,
    pub bucket: String,
    pub remote_folder: PathBuf,
}

pub struct StorageExport<'a, 'logger> {
    pub click_house: &'a ClickHouse<'logger>,
    pub client: &'a clickhouse::Client,
    pub aws: &'a AWSFileIO<'logger>,
}
impl StorageExport<'_, '_> {
    fn table<'a>(&'a self, spec: &'a DatasetExportSpec) -> TableStore<'a, 'a> {
        TableStore {
            click_house: self.click_house,
            client: self.client,
            database: &spec.database,
            table: &spec.table,
        }
    }
}
#[async_trait]
impl DatasetExport<DatasetExportSpec> for StorageExport<'_, '_> {
    type Error = StorageError;
    fn needs_finalization(&self, spec: &DatasetExportSpec) -> bool {
        spec.finalize
    }
    async fn table_exists(&self, spec: &DatasetExportSpec) -> Result<bool, Self::Error> {
        Ok(self.table(spec).exists().await?)
    }
    async fn finalize(&self, spec: &DatasetExportSpec) -> Result<(), Self::Error> {
        Ok(self.table(spec).finalize().await?)
    }
    async fn has_rows(&self, spec: &DatasetExportSpec) -> Result<bool, Self::Error> {
        Ok(self
            .table(spec)
            .has_rows(&spec.date_column, spec.row_window)
            .await?)
    }
    async fn export(&self, spec: &DatasetExportSpec) -> Result<(), Self::Error> {
        if spec.replace_staging {
            super::archive::remove_stale_staging(&spec.staging_folder.join(&spec.file_name))?;
        }
        Ok(self
            .click_house
            .export_table_to_parquet_with_filter_async(
                &spec.database,
                &spec.table,
                &spec.export_window.filter(&spec.date_column),
                &spec.staging_folder,
                &spec.file_name,
            )
            .await?)
    }
    async fn upload(&self, spec: &DatasetExportSpec) -> Result<(), Self::Error> {
        self.aws
            .upload_file(
                &spec.bucket,
                &spec.remote_folder,
                &spec.file_name,
                &spec.staging_folder,
                &spec.file_name,
            )
            .await
            .map_err(|error| StorageError::Upload(Box::new(error)))
    }
    async fn archive_local(&self, spec: &DatasetExportSpec) -> Result<(), Self::Error> {
        Ok(super::archive::copy_then_remove(
            &spec.staging_folder.join(&spec.file_name),
            &spec.archive_folder.join(&spec.file_name),
        )
        .await?)
    }
}

#[async_trait]
impl<R: super::records::ScraperRecord> super::records::RecordUploader<R>
    for CollectionStore<'_, '_>
{
    type Error = mongodb::error::Error;
    async fn upload(&self, record: &R) -> Result<(), Self::Error> {
        let query = record.query().map_err(mongodb::error::Error::custom)?;
        CollectionStore::upload(self, record, query).await
    }
}

/// Read-only checks also support compatibility callers that already own a client.
pub async fn table_exists(
    client: &clickhouse::Client,
    table: &str,
) -> clickhouse::error::Result<bool> {
    client
        .query(&format!("EXISTS TABLE `{}`", table.replace('`', "``")))
        .fetch_one::<u8>()
        .await
        .map(|value| value == 1)
}
pub async fn table_has_rows(
    client: &clickhouse::Client,
    table: &str,
    column: &str,
    window: TimeWindow,
) -> clickhouse::error::Result<bool> {
    use sea_query::{Alias, Expr, MysqlQueryBuilder, Query};
    let mut query = Query::select();
    query
        .expr(Expr::val(1))
        .from(Alias::new(table))
        .and_where(Expr::col(Alias::new(column)).gte(window.from.timestamp()));
    let end = Expr::col(Alias::new(column));
    query.and_where(match window.end {
        EndBound::Exclusive => end.lt(window.to.timestamp()),
        EndBound::Inclusive => end.lte(window.to.timestamp()),
    });
    client
        .query(&query.limit(1).to_string(MysqlQueryBuilder))
        .fetch_optional::<u8>()
        .await
        .map(|row| row.is_some())
}
impl TableStore<'_, '_> {
    pub async fn import_prepared(
        &self,
        columns: &[crate::click_house::ClickHouseColumn],
        folder: &Path,
        name: &str,
    ) -> clickhouse::error::Result<()> {
        self.create_raw(columns).await?;
        self.import_parquet(folder, name)
    }
}

/// Fully configured table import; application identity/schema/layout remain data.
pub struct TableImportSpec {
    pub database: String,
    pub table: String,
    pub columns: Vec<crate::click_house::ClickHouseColumn>,
    pub folder: PathBuf,
    pub name: String,
}
pub async fn import_tables(
    click_house: &ClickHouse<'_>,
    client: &clickhouse::Client,
    tables: &[TableImportSpec],
) -> clickhouse::error::Result<()> {
    for spec in tables {
        let table = TableStore {
            click_house,
            client,
            database: &spec.database,
            table: &spec.table,
        };
        table
            .import_prepared(&spec.columns, &spec.folder, &spec.name)
            .await?;
        table.finalize().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_explicit_window_boundaries() {
        let mut window = TimeWindow {
            from: DateTime::from_timestamp(10, 0).unwrap(),
            to: DateTime::from_timestamp(20, 0).unwrap(),
            end: EndBound::Exclusive,
        };
        assert_eq!(
            window.filter("insert_time"),
            "insert_time >= 10 AND insert_time < 20"
        );
        window.end = EndBound::Inclusive;
        assert_eq!(window.filter("date"), "date >= 10 AND date <= 20");
    }
}
