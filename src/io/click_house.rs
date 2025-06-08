use std::{env, fs, path::Path, process::Command};

use clickhouse::{error::Result, query::RowCursor, Client, Row};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::logger::ProjectLogger;

pub struct ClickHouse<'a> {
    project_logger: &'a ProjectLogger,
    password: String,
}

impl<'a> ClickHouse<'a> {
    const DB_URL: &'static str = "http://localhost:8123";
    const CLICKHOUSE_LOCAL: &'static str = "clickhouse-local";
    const LOCAL_HOST_PORT: &'static str = "localhost:9000";
    const USER_NAME: &'static str = "default";
    const INSERT_TIME: &'static str = "insert_time";

    pub fn new(project_logger: &'a ProjectLogger) -> Self {
        let password = Password::load_password().password;
        Self {
            project_logger,
            password,
        }
    }

    pub fn create_database_client(&self, database: &str) -> Client {
        let client = Client::default()
            .with_url(Self::DB_URL)
            .with_user(Self::USER_NAME)
            .with_password(&self.password)
            .with_database(database);
        let debug_str = format!("Connected to Clickhouse database {database}");
        self.project_logger.log_debug(&debug_str);
        client
    }

    async fn sql_execution(&self, client: &Client, query_str: &str) -> Result<()> {
        client.query(query_str).execute().await.map_or_else(
            |e| {
                let error_str = format!("Unable to query {query_str}. {e}");
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |()| {
                let debug_str = format!("Query {query_str} executed.");
                self.project_logger.log_debug(&debug_str);
                Ok(())
            },
        )
    }

    pub async fn create_table(
        &self,
        client: &Client,
        table_name: &str,
        columns: &[ClickHouseColumn],
    ) -> Result<()> {
        let mut hash_key_columns = String::new();
        let mut query = format!("CREATE TABLE IF NOT EXISTS {table_name} (");
        for column in columns {
            query.push_str(&format!(
                "{} {}, ",
                column.name,
                column.column_type.get_type()
            ));
            if column.is_hash_key {
                hash_key_columns.push_str(&format!("{}, ", column.name));
            }
        }
        query.push_str(&format!(
            "{} Int64 DEFAULT toUnixTimestamp(now())",
            Self::INSERT_TIME
        ));
        query.push_str(&format!(
            ") ENGINE = ReplacingMergeTree({}) ",
            Self::INSERT_TIME
        ));
        if !hash_key_columns.is_empty() {
            hash_key_columns = hash_key_columns.trim_end_matches(", ").to_string();
        };
        query.push_str(&format!(
            "ORDER BY ({hash_key_columns}, cityHash64({hash_key_columns}))"
        ));
        self.sql_execution(client, query.as_str()).await
    }

    pub fn insert_table_from_parquet(
        &self,
        database: &str,
        table_name: &str,
        folder_path: &Path,
        file_name: &str,
    ) -> Result<()> {
        let status = Command::new(Self::CLICKHOUSE_LOCAL)
            .arg("--query")
            .arg(format!("INSERT INTO FUNCTION remote('{}', '{database}.{table_name}', '{}', '{}') SELECT *, toUnixTimestamp(now()) AS insert_time FROM file('{}/{file_name}', Parquet)",
                Self::LOCAL_HOST_PORT,
                Self::USER_NAME,
                &self.password,
                folder_path.display(),
            ))
            .status()
            .map_err(|e| {
                let error_str = format!("Unable to execute command. {e}");
                let error_str_redacted = error_str.replace(&self.password, "***");
                self.project_logger.log_error(&error_str_redacted);
                e
            })?;
        if status.success() {
            let debug_str = format!(
                "Inserted file {}/{file_name} to table {table_name} in database {database}",
                folder_path.display()
            );
            self.project_logger.log_debug(&debug_str);
            Ok(())
        } else {
            let error_str = format!(
                "Unable to insert file {}/{file_name} to table {table_name} in database {database}",
                folder_path.display()
            );
            self.project_logger.log_error(&error_str);
            Err(std::io::Error::new(std::io::ErrorKind::Other, error_str).into())
        }
    }

    pub async fn insert_table_from_row<T: Serialize + Row + std::fmt::Debug>(
        &self,
        client: &Client,
        table_name: &str,
        rows: &[T],
    ) -> Result<()> {
        let mut insert = client.insert(table_name).map_err(|e| {
            let error_str = format!("Unable to create insert for table {table_name}. {e}");
            self.project_logger.log_error(&error_str);
            e
        })?;
        for row in rows {
            insert.write(row).await.map_err(|e| {
                let error_str = format!(
                    "Unable to write to insert for table {table_name} for row {row:?}. {e}"
                );
                self.project_logger.log_error(&error_str);
                e
            })?;
        }
        insert.end().await.map_err(|e| {
            let error_str = format!("Unable to end insert for table {table_name}. {e}");
            self.project_logger.log_error(&error_str);
            e
        })
    }

    pub async fn deduplication_on_table(&self, client: &Client, table_name: &str) -> Result<()> {
        let query_str = format!("OPTIMIZE TABLE {table_name} FINAL");
        self.sql_execution(client, query_str.as_str()).await
    }

    pub async fn load_table<T: DeserializeOwned + Row + std::fmt::Debug>(
        &self,
        client: &Client,
        table_name: &str,
    ) -> Result<Vec<T>> {
        let query_str = format!("SELECT * FROM {table_name}");
        self.query_table(client, query_str.as_str()).await
    }

    pub async fn query_table<T: DeserializeOwned + Row + std::fmt::Debug>(
        &self,
        client: &Client,
        query_str: &str,
    ) -> Result<Vec<T>> {
        client.query(query_str).fetch_all().await.map_err(|e| {
            let error_str = format!("Unable to load table {query_str}. {e}");
            self.project_logger.log_error(&error_str);
            e
        })
    }

    pub fn load_rows_from_table<T: DeserializeOwned + Row + std::fmt::Debug>(
        &self,
        client: &Client,
        table_name: &str,
    ) -> Result<RowCursor<T>> {
        let query_str = format!("SELECT * FROM {table_name}");
        self.query_rows_from_table(client, query_str.as_str())
    }

    pub fn query_rows_from_table<T: DeserializeOwned + Row + std::fmt::Debug>(
        &self,
        client: &Client,
        query_str: &str,
    ) -> Result<RowCursor<T>> {
        client.query(query_str).fetch().map_err(|e| {
            let error_str = format!("Unable to query {query_str}. {e}");
            self.project_logger.log_error(&error_str);
            e
        })
    }

    pub fn export_table_to_parquet(
        &self,
        database: &str,
        table_name: &str,
        folder_path: &Path,
        file_name: &str,
    ) -> Result<()> {
        let status = Command::new(Self::CLICKHOUSE_LOCAL)
            .arg("--query")
            .arg(format!("SELECT * FROM remote('{}', '{database}.{table_name}', '{}', '{}') INTO OUTFILE '{}/{file_name}' FORMAT Parquet",
                Self::LOCAL_HOST_PORT,
                Self::USER_NAME,
                &self.password,
                folder_path.display(),
            ))
            .status()
            .map_err(|e| {
                let error_str = format!("Unable to execute command. {e}");
                let error_str_redacted = error_str.replace(&self.password, "***");
                self.project_logger.log_error(&error_str_redacted);
                e
            })?;
        if status.success() {
            let debug_str = format!(
                "Exported table {table_name} in database {database} to {}/{file_name}",
                folder_path.display()
            );
            self.project_logger.log_debug(&debug_str);
            Ok(())
        } else {
            let error_str = format!(
                "Unable to export table {table_name} in database {database} to {}/{file_name}",
                folder_path.display()
            );
            self.project_logger.log_error(&error_str);
            Err(std::io::Error::new(std::io::ErrorKind::Other, error_str).into())
        }
    }

    pub async fn exporty_query_to_parquet(
        &self,
        client: &Client,
        query_str: &str,
        folder_path: &Path,
        file_name: &str,
    ) -> Result<()> {
        let main_query_str = format!(
            "{query_str} INTO OUTFILE '{}/{file_name}' FORMAT Parquet",
            folder_path.display()
        );
        self.sql_execution(client, main_query_str.as_str()).await
    }

    pub async fn count_row_in_table(
        &self,
        client: &Client,
        table_name: &str,
        distinct_columns: Option<&[&str]>,
    ) -> usize {
        let query_str = match distinct_columns {
            Some(columns) => {
                let columns_str = columns.join(", ");
                format!("SELECT COUNT(DISTINCT ({columns_str})) FROM {table_name}")
            }
            None => {
                format!("SELECT DISTINCT COUNT(*) FROM {table_name}")
            }
        };
        let count: usize = client.query(&query_str).fetch_one().await.unwrap_or(0);
        count
    }
}

#[derive(Debug)]
pub struct ClickHouseColumn {
    pub name: String,
    pub column_type: ClickHouseType,
    pub is_hash_key: bool,
}

#[derive(Debug)]
pub enum ClickHouseType {
    Boolean(bool),
    Int32(bool),
    Int64(bool),
    Float64(bool),
    String(bool),
}

impl ClickHouseType {
    fn get_type(&self) -> &str {
        match self {
            Self::Boolean(nullable) => {
                if *nullable {
                    "Nullable(UInt8)"
                } else {
                    "UInt8"
                }
            }
            Self::Int32(nullable) => {
                if *nullable {
                    "Nullable(Int32)"
                } else {
                    "Int32"
                }
            }
            Self::Int64(nullable) => {
                if *nullable {
                    "Nullable(Int64)"
                } else {
                    "Int64"
                }
            }
            Self::Float64(nullable) => {
                if *nullable {
                    "Nullable(Float64)"
                } else {
                    "Float64"
                }
            }
            Self::String(nullable) => {
                if *nullable {
                    "Nullable(String)"
                } else {
                    "String"
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct Password {
    password: String,
}

impl Password {
    const PROJECT_KEY: &str = "SCTYS_PROJECT";
    const PASSWORD_PATH: &str = "Secret/secret_sctys_rust_utilities";
    const PASSWORD_FILE: &str = "clickhouse_password.toml";

    fn load_password() -> Self {
        let full_api_path =
            Path::new(&env::var(Self::PROJECT_KEY).expect("Unable to find project path"))
                .join(Self::PASSWORD_PATH)
                .join(Self::PASSWORD_FILE);
        let password_str = fs::read_to_string(full_api_path)
            .unwrap_or_else(|e| panic!("Unable to load the api file. {e}"));
        toml::from_str(&password_str)
            .unwrap_or_else(|e| panic!("Unable to parse the api file. {e}"))
    }
}

#[cfg(test)]
mod tests {
    use log::LevelFilter;
    use strum::VariantArray;
    use strum_macros::VariantArray;

    use super::*;

    #[derive(Debug, Serialize, Deserialize, Row)]
    #[serde(rename_all = "PascalCase")]
    struct TestData {
        venue: String,
        surface_i_d: i32,
        course_i_d: String,
        home_straight: Option<i32>,
        width: f64,
    }

    #[derive(Debug, strum_macros::Display, VariantArray)]
    pub enum TestDataCol {
        Venue,
        SurfaceID,
        CourseID,
        HomeStraight,
        Width,
    }

    impl TestDataCol {
        fn get_name(&self) -> String {
            self.to_string()
        }

        fn get_colume_type(&self) -> ClickHouseType {
            match self {
                Self::Venue => ClickHouseType::String(false),
                Self::SurfaceID => ClickHouseType::Int32(false),
                Self::CourseID => ClickHouseType::String(false),
                Self::HomeStraight => ClickHouseType::Int32(true),
                Self::Width => ClickHouseType::Float64(false),
            }
        }

        fn is_hash_key(&self) -> bool {
            matches!(self, Self::Venue | Self::CourseID)
        }

        fn form_columns() -> Vec<ClickHouseColumn> {
            Self::VARIANTS
                .iter()
                .map(|variant| ClickHouseColumn {
                    name: variant.get_name(),
                    column_type: variant.get_colume_type(),
                    is_hash_key: variant.is_hash_key(),
                })
                .collect()
        }
    }

    #[tokio::test]
    async fn test_create_table_and_load_parquet() {
        let logger_name = "test_clickhouse";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let clickhouse = ClickHouse::new(&project_logger);
        let database = "test";
        let clickhouse_client = clickhouse.create_database_client(database);
        let test_table = "test_table";
        let columns = TestDataCol::form_columns();
        clickhouse
            .create_table(&clickhouse_client, test_table, &columns)
            .await
            .unwrap();
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let data_file = "test.parquet";
        clickhouse
            .insert_table_from_parquet(database, test_table, &folder_path, data_file)
            .unwrap();
    }

    #[test]
    fn test_export_table_parquet() {
        let logger_name = "test_clickhouse";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let clickhouse = ClickHouse::new(&project_logger);
        let database = "test";
        let test_table = "test_table";
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let data_file = "test_clickhouse_out.parquet";
        clickhouse
            .export_table_to_parquet(database, test_table, &folder_path, data_file)
            .unwrap();
    }

    #[tokio::test]
    async fn test_deduplication_table() {
        let logger_name = "test_clickhouse";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let clickhouse = ClickHouse::new(&project_logger);
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let data_file = "test.parquet";
        let database = "test";
        let clickhouse_client = clickhouse.create_database_client(database);
        let test_table = "test_table";
        clickhouse
            .insert_table_from_parquet(database, test_table, &folder_path, data_file)
            .unwrap();
        let row_count = clickhouse
            .count_row_in_table(&clickhouse_client, test_table, None)
            .await;
        dbg!(row_count);
        clickhouse
            .deduplication_on_table(&clickhouse_client, test_table)
            .await
            .unwrap();
        let new_row_count = clickhouse
            .count_row_in_table(&clickhouse_client, test_table, None)
            .await;
        dbg!(new_row_count);
    }
}
