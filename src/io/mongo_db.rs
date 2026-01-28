use std::{path::Path, process::Command};

use chrono::{DateTime, Utc};
use mongodb::{
    bson::{doc, DateTime as BsonDateTime, Document},
    error::Result,
    Client, Collection, Cursor, Database, IndexModel,
};
use serde::Serialize;

use crate::logger::ProjectLogger;

pub struct MongoDB<'a> {
    project_logger: &'a ProjectLogger,
}

impl<'a> MongoDB<'a> {
    const DB_URL: &'static str = "mongodb://localhost:27017/";
    const ID: &'static str = "_id";
    const MODIFIED: &'static str = "modified";

    pub fn new(project_logger: &'a ProjectLogger) -> Self {
        Self { project_logger }
    }
    
    pub fn get_logger(&self) -> &'a ProjectLogger {
        self.project_logger
    }

    pub async fn create_connection(&self) -> Result<Client> {
        Client::with_uri_str(Self::DB_URL).await.map_or_else(
            |e| {
                let error_str = format!("Fail to connect to MongoDB. {e}");
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |client| {
                let debug_str = "Connected to MongoDB";
                self.project_logger.log_debug(debug_str);
                Ok(client)
            },
        )
    }

    pub fn obtain_database(&self, client: &Client, database_name: &str) -> Database {
        let database = client.database(database_name);
        let debug_str = format!("Connected to database {}", database_name);
        self.project_logger.log_debug(&debug_str);
        database
    }

    pub async fn list_collection(&self, database: &Database) -> Result<Vec<String>> {
        database.list_collection_names().await.map_err(|e| {
            let error_str = format!("Fail to list collection. {e}");
            self.project_logger.log_error(&error_str);
            e
        })
    }

    pub async fn check_collection_exist(
        &self,
        database: &Database,
        collection_name: &str,
    ) -> Result<bool> {
        let collection_list = self.list_collection(database).await?;
        Ok(collection_list.contains(&collection_name.to_string()))
    }

    pub fn obtain_collection<T: Send + Sync>(
        &self,
        database: &Database,
        collection_name: &str,
    ) -> Collection<T> {
        let collection = database.collection(collection_name);
        let debug_str = format!("Connected to collection {}", collection_name);
        self.project_logger.log_debug(&debug_str);
        collection
    }

    fn parse_index_name(index_name: &str) -> &str {
        index_name.rsplitn(2, '_').last().unwrap_or(index_name)
    }

    pub async fn list_index<T: Send + Sync>(
        &self,
        collection: &Collection<T>,
    ) -> Result<Vec<String>> {
        collection.list_index_names().await.map_or_else(
            |e| {
                let error_str = format!("Fail to list index. {e}");
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |index_list| {
                Ok(index_list
                    .iter()
                    .map(|index| Self::parse_index_name(index).to_string())
                    .collect())
            },
        )
    }

    pub async fn create_indexes<T: Send + Sync>(
        &self,
        collection: &Collection<T>,
        indexes: &[String],
    ) -> Result<()> {
        let existing_index = self.list_index(collection).await.unwrap_or(vec![]);
        for index in indexes {
            if !existing_index.contains(index) {
                let index_model = IndexModel::builder().keys(doc! {index: 1}).build();
                match collection.create_index(index_model).await {
                    Ok(_) => {
                        let debug_str = format!(
                            "Created index {} for collection {}",
                            index,
                            collection.name()
                        );
                        self.project_logger.log_debug(&debug_str);
                    }
                    Err(e) => {
                        let error_str = format!(
                            "Fail to create index {} for collection {}. {e}",
                            index,
                            collection.name()
                        );
                        self.project_logger.log_error(&error_str);
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn replace_document<T: Send + Sync + Serialize>(
        &self,
        collection: &Collection<T>,
        document: &T,
        query: Document,
    ) -> Result<()> {
        let query_str = &query.to_string();
        collection
            .replace_one(query, document)
            .upsert(true)
            .await
            .map_or_else(
                |e| {
                    let error_str = format!("Unable to replace document {query_str}. {e}");
                    self.project_logger.log_error(&error_str);
                    Err(e)
                },
                |_| {
                    let debug_str = format!("Document {query_str} replaced.");
                    self.project_logger.log_debug(&debug_str);
                    Ok(())
                },
            )
    }

    fn exclude_id_from_projection(mut projection: Option<Document>) -> Document {
        match projection {
            Some(ref mut projection) => {
                projection.insert(Self::ID, 0);
            }
            None => {
                projection = Some(doc! {Self::ID: 0});
            }
        };
        projection.unwrap_or_else(|| panic!("Projection should at least include {}", Self::ID))
    }

    pub async fn find_documents<T: Send + Sync>(
        &self,
        collection: &Collection<T>,
        query: Document,
        projection: Option<Document>,
        sort: Option<Document>,
    ) -> Result<Cursor<T>> {
        let query_str = &query.to_string();
        let mut query_builder = collection.find(query);
        let full_projection = Self::exclude_id_from_projection(projection);
        let projection_str = &full_projection.to_string();
        query_builder = query_builder.projection(full_projection);
        if let Some(sort) = sort {
            query_builder = query_builder.sort(sort);
        }
        query_builder.await.map_err(|e| {
            let error_str = format!(
                "Unable to find documents with query {query_str} and project {projection_str}. {e}"
            );
            self.project_logger.log_error(&error_str);
            e
        })
    }

    pub async fn find_aggregate<T: Send + Sync>(
        &self,
        collection: &Collection<T>,
        mut pipeline: Vec<Document>,
        projection: Option<Document>,
    ) -> Result<Cursor<T>> {
        let full_projection = Self::exclude_id_from_projection(projection);
        pipeline.push(doc! {"$project": full_projection});
        let pipeline_str = pipeline
            .iter()
            .map(|doc| doc.to_string())
            .collect::<Vec<String>>()
            .join(", ");
        collection
            .aggregate(pipeline)
            .with_type()
            .await
            .map_err(|e| {
                let error_str =
                    format!("Unable to aggregate documents with pipeline {pipeline_str}. {e}");
                self.project_logger.log_error(&error_str);
                e
            })
    }

    pub async fn delete_documents<T: Send + Sync>(
        &self,
        collection: &Collection<T>,
        query: Document,
    ) -> Result<u64> {
        let query_str = &query.to_string();
        collection.delete_many(query).await.map_or_else(
            |e| {
                let error_str = format!("Unable to delete documents with query {query_str}. {e}");
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |delete_result| {
                let debug_str = format!("Documents with query {query_str} deleted.");
                self.project_logger.log_debug(&debug_str);
                Ok(delete_result.deleted_count)
            },
        )
    }

    pub async fn delete_documents_by_modified_date<T: Send + Sync>(
        &self,
        collection: &Collection<T>,
        start_date: &DateTime<Utc>,
        end_date: &DateTime<Utc>,
    ) -> Result<u64> {
        let start_date_doc = BsonDateTime::from_millis(start_date.timestamp_millis());
        let end_date_doc = BsonDateTime::from_millis(end_date.timestamp_millis());
        let query = doc! {Self::MODIFIED: {"$gte": start_date_doc, "$lt": end_date_doc}};
        self.delete_documents(collection, query).await
    }

    pub async fn delete_documents_by_int_date<T: Send + Sync>(
        &self,
        collection: &Collection<T>,
        start_date: i32,
        end_date: i32,
        date_key: &str,
    ) -> Result<u64> {
        let query = doc! {date_key: {"$gte": start_date, "$lt": end_date}};
        self.delete_documents(collection, query).await
    }

    pub fn backup_collection(
        &self,
        database_name: &str,
        collection_name: &str,
        query_str: &str,
        output_folder: &Path,
    ) -> Result<()> {
        let output = Command::new("mongodump")
            .arg("--uri")
            .arg(Self::DB_URL)
            .arg("--db")
            .arg(database_name)
            .arg("--collection")
            .arg(collection_name)
            .arg("--query")
            .arg(query_str)
            .arg("--out")
            .arg(output_folder.as_os_str())
            .output();
        match output {
            Ok(output) => {
                if output.status.success() {
                    let debug_str = format!(
                        "Collection {} with query {} in database {} backed up in folder {}",
                        collection_name,
                        query_str,
                        database_name,
                        output_folder.display()
                    );
                    self.project_logger.log_debug(&debug_str);
                    Ok(())
                } else {
                    let error_str = format!(
                        "Unable to backup collection {} with query {} in database {}. {}",
                        collection_name,
                        query_str,
                        database_name,
                        String::from_utf8_lossy(&output.stderr)
                    );
                    self.project_logger.log_error(&error_str);
                    Err(mongodb::error::Error::custom(error_str))
                }
            }
            Err(e) => {
                let error_str = format!(
                    "Unable to backup collection {} with query {} in database {}. {}",
                    collection_name, query_str, database_name, e
                );
                self.project_logger.log_error(&error_str);
                Err(mongodb::error::Error::custom(error_str))
            }
        }
    }

    pub fn backup_collection_by_modified_date(
        &self,
        database_name: &str,
        collection_name: &str,
        start_date: &DateTime<Utc>,
        end_date: &DateTime<Utc>,
        output_folder: &Path,
    ) -> Result<()> {
        let start_date_str = start_date.to_rfc3339();
        let end_date_str = end_date.to_rfc3339();
        let query_str = format!(
            r#"{{ "{}": {{ "$gte": {{ "$date": "{}" }}, "$lt": {{ "$date": "{}" }} }} }}"#,
            Self::MODIFIED,
            start_date_str,
            end_date_str
        );
        self.backup_collection(database_name, collection_name, &query_str, output_folder)
    }

    pub fn backup_collection_by_int_date(
        &self,
        database_name: &str,
        collection_name: &str,
        start_date: i32,
        end_date: i32,
        date_key: &str,
        output_folder: &Path,
    ) -> Result<()> {
        let query_str = format!(
            r#"{{ "{}": {{ "$gte": {}, "$lt": {} }} }}"#,
            date_key, start_date, end_date
        );
        self.backup_collection(database_name, collection_name, &query_str, output_folder)
    }
}

#[cfg(test)]
mod tests {

    use std::env;

    use futures::StreamExt;
    use log::LevelFilter;
    use mongodb::bson::DateTime as BsonDateTime;
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestDocument {
        pub date: i32,
        pub modified: BsonDateTime,
        pub test: i32,
        pub data: String,
    }

    fn set_logger() -> ProjectLogger {
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let logger_name = "test_mongo_db";
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        project_logger
    }

    #[tokio::test]
    async fn test_replace_document() {
        let project_logger = set_logger();
        let mongo_db = MongoDB::new(&project_logger);
        let client = mongo_db.create_connection().await.unwrap();
        let database = mongo_db.obtain_database(&client, "test_io");
        let collection: Collection<TestDocument> =
            mongo_db.obtain_collection(&database, "test_collection");
        let document = TestDocument {
            date: 20250101,
            modified: BsonDateTime::now(),
            test: 123,
            data: "test_data".to_string(),
        };
        let indexes = vec![
            "date".to_string(),
            "modified".to_string(),
            "test".to_string(),
        ];
        mongo_db
            .create_indexes(&collection, &indexes)
            .await
            .unwrap();
        dbg!(mongo_db.list_index(&collection).await.unwrap());
        let query = doc! {"date": 20250101, "test": 123};
        mongo_db
            .replace_document(&collection, &document, query.clone())
            .await
            .unwrap();
        let mut documents = mongo_db
            .find_documents(&collection, query, None, None)
            .await
            .unwrap();
        while let Some(doc) = documents.next().await {
            dbg!(doc.unwrap());
        }
    }

    #[tokio::test]
    async fn test_backup_collection() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let project_logger = set_logger();
        let mongo_db = MongoDB::new(&project_logger);
        let database_name = "test_io";
        let collection_name = "test_collection";
        let query_str = r#"{ "modified": { "$gte": { "$date": "2025-01-01T00:00:00.000Z" }, "$lt": { "$date": "2026-01-01T00:00:00.000Z" } } }"#;
        mongo_db
            .backup_collection(database_name, collection_name, query_str, &folder_path)
            .unwrap()
    }

    #[tokio::test]
    async fn test_delete_documents() {
        let project_logger = set_logger();
        let mongo_db = MongoDB::new(&project_logger);
        let client = mongo_db.create_connection().await.unwrap();
        let database = mongo_db.obtain_database(&client, "test_io");
        let collection: Collection<TestDocument> =
            mongo_db.obtain_collection(&database, "test_collection");
        let query = doc! {"date": 20250101, "test": 123};
        mongo_db
            .delete_documents(&collection, query.clone())
            .await
            .unwrap();
    }
}
