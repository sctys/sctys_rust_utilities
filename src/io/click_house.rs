use std::{path::Path, process::Command};

use clickhouse::{error::Result, inserter::Inserter, query::RowCursor, Client, Row};
use sea_query::{
    Alias, Asterisk, ColumnDef, ColumnRef, Condition, Expr, ExprTrait, Func, Iden, IntoIden,
    MysqlQueryBuilder, Query, QueryBuilder, SimpleExpr, Table, TableCreateStatement,
};
use serde::{de::DeserializeOwned, de::Error as _, Deserialize, Serialize};

use crate::{logger::ProjectLogger, secret::aws_secret::Secret, PROJECT};

const DATABASE_ENGINE_ERROR: &str =
    "ClickHouse operations requiring atomic exchange need an Atomic database";
const ORDER_BY_REQUIRED_ERROR: &str = "MergeTree tables require an ORDER BY key";
const MERGE_TREE_ENGINE: &str = "MergeTree";
const REPLACING_MERGE_TREE_ENGINE: &str = "ReplacingMergeTree";
const AGGREGATING_MERGE_TREE_ENGINE: &str = "AggregatingMergeTree()";
const SIMPLE_AGGREGATE_FUNCTION: &str = "SimpleAggregateFunction";
const MIN_AGGREGATE_FUNCTION: &str = "min";
const IDENTIFIER_QUOTE: char = '`';
const SYSTEM_DATABASE: &str = "system";
const DATABASES_TABLE: &str = "databases";
const DATABASE_ENGINE_COLUMN: &str = "engine";
const DATABASE_NAME_COLUMN: &str = "name";
const CURRENT_DATABASE_FUNCTION: &str = "currentDatabase";
const ATOMIC_ENGINE: &str = "Atomic";
const INVALID_IDENTIFIER_ERROR: &str = "invalid ClickHouse identifier";
const LIMIT_BY_COLUMNS_REQUIRED_ERROR: &str = "ClickHouse LIMIT BY requires at least one column";
const LIMIT_BY_PREFIX: &str = " LIMIT ";
const LIMIT_BY_COLUMNS_SEPARATOR: &str = ", ";
const LIMIT_BY_KEYWORD_SEPARATOR: &str = " BY ";
const UNIX_TIMESTAMP_FUNCTION: &str = "toUnixTimestamp";
const NOW_FUNCTION: &str = "now";
const CITY_HASH_FUNCTION: &str = "cityHash64";
const NULLABLE_FLOAT32_TYPE: &str = "Nullable(Float32)";
const NULLABLE_FLOAT64_TYPE: &str = "Nullable(Float64)";
const NULLABLE_INT8_TYPE: &str = "Nullable(Int8)";
const NULLABLE_INT32_TYPE: &str = "Nullable(Int32)";
const NULLABLE_INT64_TYPE: &str = "Nullable(Int64)";
const NULLABLE_STRING_TYPE: &str = "Nullable(String)";
const NULLABLE_UINT8_TYPE: &str = "Nullable(UInt8)";
const NULLABLE_UUID_TYPE: &str = "Nullable(UUID)";
const FLOAT32_TYPE: &str = "Float32";
const FLOAT64_TYPE: &str = "Float64";
const INT8_TYPE: &str = "Int8";
const INT32_TYPE: &str = "Int32";
const INT64_TYPE: &str = "Int64";
const STRING_TYPE: &str = "String";
const UINT8_TYPE: &str = "UInt8";
const UUID_TYPE: &str = "UUID";

enum MergeTreeEngine {
    MergeTree,
    Replacing { version_column: String },
    Aggregating,
}

fn render_merge_tree_with_order_by(
    mut create: TableCreateStatement,
    engine: MergeTreeEngine,
    order_by: &[String],
) -> Result<String> {
    if order_by.is_empty() {
        return Err(clickhouse::error::Error::custom(ORDER_BY_REQUIRED_ERROR));
    }

    let engine = match engine {
        MergeTreeEngine::MergeTree => MERGE_TREE_ENGINE.to_owned(),
        MergeTreeEngine::Replacing { version_column } => format!(
            "{REPLACING_MERGE_TREE_ENGINE}({})",
            quote_identifier(&version_column)
        ),
        MergeTreeEngine::Aggregating => AGGREGATING_MERGE_TREE_ENGINE.to_owned(),
    };

    // SeaQuery's table builder supports MySQL engine options; ClickHouse's sorting key is
    // appended as an extension because it is not part of SeaQuery's CREATE TABLE model.
    create.engine(engine);
    create.extra(format!("ORDER BY ({})", order_by.join(", ")));
    Ok(create.to_string(MysqlQueryBuilder))
}

fn current_database_engine_statement() -> String {
    Query::select()
        .column(Alias::new(DATABASE_ENGINE_COLUMN))
        .from((Alias::new(SYSTEM_DATABASE), Alias::new(DATABASES_TABLE)))
        .and_where(
            Expr::col(Alias::new(DATABASE_NAME_COLUMN))
                .eq(Func::cust(Alias::new(CURRENT_DATABASE_FUNCTION)).args([])),
        )
        .to_string(MysqlQueryBuilder)
}

fn identifier_name<I: Iden>(identifier: &I) -> Result<String> {
    let name = identifier.to_string();
    let mut bytes = name.bytes();
    let valid_start = bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_');
    let valid_rest = bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    if valid_start && valid_rest {
        Ok(name)
    } else {
        Err(clickhouse::error::Error::custom(INVALID_IDENTIFIER_ERROR))
    }
}

fn quoted_identifiers<I: Iden>(identifiers: &[I]) -> Result<Vec<String>> {
    identifiers
        .iter()
        .map(identifier_name)
        .map(|name| name.map(|name| quote_identifier(&name)))
        .collect()
}

fn quote_identifier(identifier: &str) -> String {
    format!("{IDENTIFIER_QUOTE}{identifier}{IDENTIFIER_QUOTE}")
}

#[derive(Debug, Row, Deserialize)]
struct DatabaseEngineRow {
    engine: String,
}

pub struct ClickHouse<'a> {
    project_logger: &'a ProjectLogger,
    password: String,
}

impl<'a> ClickHouse<'a> {
    const DB_URL: &'static str = "http://localhost:8123";
    const LOCAL_EXECUTABLE: &'static str = "clickhouse-local";
    const LOCAL_HOST_PORT: &'static str = "localhost:9000";
    const USER_NAME: &'static str = "default";
    pub const INSERT_TIME: &'static str = "insert_time";
    pub const TIMESTAMP: &'static str = "timestamp";

    pub async fn new(project_logger: &'a ProjectLogger, secret: &Secret<'a>) -> Result<Self> {
        const CATEGORY: &str = "clickhouse";
        const NAME: &str = "password";
        let password = secret
            .get_secret_value(PROJECT, CATEGORY, NAME)
            .await
            .map_err(|e| {
                let error_str = format!("Unable to get clickhouse password. {e}");
                project_logger.log_error(&error_str);
                clickhouse::error::Error::Custom(error_str)
            })?;
        Ok(Self {
            project_logger,
            password,
        })
    }

    pub fn get_logger(&self) -> &'a ProjectLogger {
        self.project_logger
    }

    pub fn create_database_client(&self, database: &str) -> Client {
        let client = Client::default()
            .with_url(Self::DB_URL)
            .with_user(Self::USER_NAME)
            .with_password(&self.password)
            .with_database(database)
            .with_option("network_compression_method", "LZ4");
        let debug_str = format!("Connected to Clickhouse database {database}");
        self.project_logger.log_debug(&debug_str);
        client
    }

    pub async fn sql_execution(&self, client: &Client, query_str: &str) -> Result<()> {
        client.query(query_str).execute().await.map_or_else(
            |e| {
                let error_str = format!("Unable to query {query_str}. {e}");
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |()| {
                let redacted_query = query_str.replace(&self.password, "********");
                let debug_str = format!("Query {redacted_query} executed.");
                self.project_logger.log_debug(&debug_str);
                Ok(())
            },
        )
    }

    /// Creates a SeaQuery column definition with a native ClickHouse type.
    pub fn type_column(name: impl IntoIden, column_type: ClickHouseType) -> ColumnDef {
        let mut column = ColumnDef::new(name);
        column.custom(column_type);
        column
    }

    fn new_table_schema(table_name: &str) -> Result<TableCreateStatement> {
        let table = Alias::new(table_name);
        identifier_name(&table)?;

        let mut create = Table::create();
        create.table(table).if_not_exists();
        Ok(create)
    }

    fn table_schema(
        table_name: &str,
        columns: &[ClickHouseColumn],
    ) -> Result<TableCreateStatement> {
        let mut create = Self::new_table_schema(table_name)?;
        for column in columns {
            let name = Alias::new(column.name.clone());
            identifier_name(&name)?;
            create.col(Self::type_column(name, column.column_type));
        }
        Ok(create)
    }

    fn min_aggregate_column(name: impl IntoIden, column_type: ClickHouseType) -> ColumnDef {
        let mut column = ColumnDef::new(name);
        column.custom(Alias::new(format!(
            "{SIMPLE_AGGREGATE_FUNCTION}({MIN_AGGREGATE_FUNCTION}, {})",
            column_type.get_type()
        )));
        column
    }

    fn hash_key_columns(columns: &[ClickHouseColumn]) -> Result<Vec<String>> {
        let key_columns = columns
            .iter()
            .filter(|column| column.is_hash_key)
            .map(|column| {
                let name = Alias::new(column.name.clone());
                identifier_name(&name).map(|name| quote_identifier(&name))
            })
            .collect::<Result<Vec<_>>>()?;
        if key_columns.is_empty() {
            return Err(clickhouse::error::Error::custom(ORDER_BY_REQUIRED_ERROR));
        }
        Ok(key_columns)
    }

    /// Renders a table schema with the MergeTree engine and sorting key.
    pub fn render_merge_tree_ddl<I: Iden>(
        create: TableCreateStatement,
        order_by: &[I],
    ) -> Result<String> {
        let order_by = quoted_identifiers(order_by)?;
        render_merge_tree_with_order_by(create, MergeTreeEngine::MergeTree, &order_by)
    }

    /// Renders a table schema with a versioned ReplacingMergeTree engine.
    pub fn render_replacing_merge_tree_ddl<V: Iden, I: Iden>(
        create: TableCreateStatement,
        version_column: V,
        order_by: &[I],
    ) -> Result<String> {
        let version_column = identifier_name(&version_column)?;
        let order_by = quoted_identifiers(order_by)?;
        render_merge_tree_with_order_by(
            create,
            MergeTreeEngine::Replacing { version_column },
            &order_by,
        )
    }

    /// Appends ClickHouse's LIMIT BY extension using SeaQuery-rendered column references.
    pub fn render_limit_by(
        mut statement: String,
        limit: u64,
        columns: &[ColumnRef],
    ) -> Result<String> {
        if columns.is_empty() {
            return Err(clickhouse::error::Error::custom(
                LIMIT_BY_COLUMNS_REQUIRED_ERROR,
            ));
        }

        let mut rendered_columns = Vec::with_capacity(columns.len());
        for column in columns {
            let mut rendered_column = String::new();
            MysqlQueryBuilder.prepare_column_ref(column, &mut rendered_column);
            rendered_columns.push(rendered_column);
        }
        statement.push_str(LIMIT_BY_PREFIX);
        statement.push_str(&limit.to_string());
        statement.push_str(LIMIT_BY_KEYWORD_SEPARATOR);
        statement.push_str(&rendered_columns.join(LIMIT_BY_COLUMNS_SEPARATOR));
        Ok(statement)
    }

    /// Creates a MergeTree table, using columns marked `is_hash_key` as its sorting key.
    pub async fn create_merge_tree_table(
        &self,
        client: &Client,
        table_name: &str,
        columns: &[ClickHouseColumn],
    ) -> Result<()> {
        let create = Self::table_schema(table_name, columns)?;
        let order_by = Self::hash_key_columns(columns)?;
        let statement =
            render_merge_tree_with_order_by(create, MergeTreeEngine::MergeTree, &order_by)?;
        self.sql_execution(client, &statement).await
    }

    /// Verifies that the current ClickHouse database supports atomic table exchange.
    pub async fn ensure_current_database_atomic(&self, client: &Client) -> Result<()> {
        let database_engine = client
            .query(&current_database_engine_statement())
            .fetch_one::<DatabaseEngineRow>()
            .await?;
        if database_engine.engine != ATOMIC_ENGINE {
            return Err(clickhouse::error::Error::custom(DATABASE_ENGINE_ERROR));
        }
        Ok(())
    }

    /// Adds ClickHouse's FINAL modifier to selected tables in a SeaQuery SELECT statement.
    pub fn add_final_to_from_tables<I: Iden>(
        mut statement: String,
        tables: &[I],
    ) -> Result<String> {
        for table in tables {
            let table = identifier_name(table)?;
            let from_clause = format!("FROM {}", quote_identifier(&table));
            let final_from_clause = format!("{from_clause} FINAL");
            if !statement.contains(&final_from_clause) {
                statement = statement.replace(&from_clause, &final_from_clause);
            }
        }
        Ok(statement)
    }

    /// Renders ClickHouse's EXCHANGE TABLES statement.
    pub fn exchange_tables_statement<L: Iden, R: Iden>(left: L, right: R) -> Result<String> {
        let left = identifier_name(&left)?;
        let right = identifier_name(&right)?;
        Ok(format!(
            "EXCHANGE TABLES {} AND {}",
            quote_identifier(&left),
            quote_identifier(&right)
        ))
    }

    /// Atomically exchanges two ClickHouse tables.
    pub async fn exchange_tables<L: Iden, R: Iden>(
        &self,
        client: &Client,
        left: L,
        right: R,
    ) -> Result<()> {
        let statement = Self::exchange_tables_statement(left, right)?;
        self.sql_execution(client, &statement).await
    }

    /// Inserts serializable rows in bounded batches using an estimated row size.
    pub async fn insert_rows_in_batches<T, I, F>(
        &self,
        client: &Client,
        table: I,
        rows: &[T],
        limits: crate::misc::batching::BatchLimits,
        estimate: F,
    ) -> Result<()>
    where
        T: Serialize + Row + std::fmt::Debug,
        I: Iden,
        F: FnMut(&T) -> usize,
    {
        let table_name = identifier_name(&table)?;
        crate::misc::batching::insert_batches(
            crate::misc::batching::BatchRanges::new(rows, limits, estimate),
            |range| {
                let table_name = table_name.clone();
                async move {
                    self.insert_table_from_row(client, &table_name, &rows[range])
                        .await
                }
            },
        )
        .await
    }

    // Use for the case where we want to keep only the latest insert_time of the data
    pub async fn create_replacing_merge_tree_table(
        &self,
        client: &Client,
        table_name: &str,
        columns: &[ClickHouseColumn],
    ) -> Result<()> {
        let mut create = Self::table_schema(table_name, columns)?;

        let mut insert_time =
            Self::type_column(Alias::new(Self::INSERT_TIME), ClickHouseType::Int64(false));
        insert_time.default(
            Func::cust(Alias::new(UNIX_TIMESTAMP_FUNCTION))
                .arg(Func::cust(Alias::new(NOW_FUNCTION)).args([])),
        );
        create.col(insert_time);

        let key_columns = Self::hash_key_columns(columns)?;
        let hash_key = format!("{CITY_HASH_FUNCTION}({})", key_columns.join(", "));
        let mut order_by = key_columns;
        order_by.push(hash_key);
        let statement = render_merge_tree_with_order_by(
            create,
            MergeTreeEngine::Replacing {
                version_column: Self::INSERT_TIME.to_owned(),
            },
            &order_by,
        )?;
        self.sql_execution(client, &statement).await
    }

    // Use for the case where we want to keep only the oldest timestamp of the data
    pub async fn create_aggregating_merge_tree_table(
        &self,
        client: &Client,
        table_name: &str,
        columns: &[ClickHouseColumn],
    ) -> Result<()> {
        let mut create = Self::new_table_schema(table_name)?;
        for column in columns {
            let name = Alias::new(column.name.clone());
            identifier_name(&name)?;
            if column.is_hash_key {
                create.col(Self::type_column(name, column.column_type));
            } else {
                create.col(Self::min_aggregate_column(name, column.column_type));
            }
        }
        create.col(Self::min_aggregate_column(
            Alias::new(Self::TIMESTAMP),
            ClickHouseType::Int64(false),
        ));

        let order_by = Self::hash_key_columns(columns)?;
        let statement =
            render_merge_tree_with_order_by(create, MergeTreeEngine::Aggregating, &order_by)?;
        self.sql_execution(client, &statement).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_dictionary_if_not_exists(
        &self,
        client: &Client,
        database: &str,
        table_name: &str,
        dict_name: &str,
        key_columns: &[ClickHouseColumn],
        value_columns: &[ClickHouseColumn],
        query: &str,
    ) -> Result<()> {
        let create_query = Self::build_create_dictionary_query(
            database,
            table_name,
            &self.password,
            dict_name,
            key_columns,
            value_columns,
            query,
        )?;
        self.sql_execution(client, create_query.as_str()).await
    }

    fn build_create_dictionary_query(
        database: &str,
        table_name: &str,
        password: &str,
        dict_name: &str,
        key_columns: &[ClickHouseColumn],
        value_columns: &[ClickHouseColumn],
        query: &str,
    ) -> Result<String> {
        let mut host_port_parts = Self::LOCAL_HOST_PORT.split(':');
        if let (Some(host), Some(port)) = (host_port_parts.next(), host_port_parts.next()) {
            if key_columns.is_empty() {
                let error_str = format!(
                    "Unable to create dictionary {dict_name}. No key columns were provided."
                );
                return Err(clickhouse::error::Error::Custom(error_str));
            }

            let mut column_defs = String::new();
            let mut key_column_names = String::new();

            for column in key_columns {
                column_defs.push_str(&format!(
                    "{} {}, ",
                    column.name,
                    column.column_type.get_type()
                ));
                key_column_names.push_str(&format!("{}, ", column.name));
            }

            for column in value_columns {
                column_defs.push_str(&format!(
                    "{} {}, ",
                    column.name,
                    column.column_type.get_type()
                ));
            }

            column_defs = column_defs.trim_end_matches(", ").to_string();
            key_column_names = key_column_names.trim_end_matches(", ").to_string();
            let qualifying_query = Self::qualifying_database_for_table(query, database, table_name);
            let escaped_query = Self::escape_clickhouse_string_literal(qualifying_query.as_str());

            Ok(format!(
                "CREATE DICTIONARY IF NOT EXISTS {dict_name} ({column_defs}) \
                PRIMARY KEY ({key_column_names}) \
                SOURCE(CLICKHOUSE(\
                    HOST '{host}' \
                    PORT {port} \
                    USER '{}' \
                    PASSWORD '{password}' \
                    DB '{database}' \
                    QUERY '{escaped_query}'\
                )) \
                LIFETIME(MIN 300 MAX 600) \
                LAYOUT(COMPLEX_KEY_HASHED())",
                Self::USER_NAME
            ))
        } else {
            let error_str = format!(
                "Unable to split the host and port {}.",
                Self::LOCAL_HOST_PORT
            );
            Err(clickhouse::error::Error::Custom(error_str))
        }
    }

    fn escape_clickhouse_string_literal(value: &str) -> String {
        value.replace('\\', "\\\\").replace('\'', "\\'")
    }

    fn qualifying_database_for_table(query: &str, database: &str, table_name: &str) -> String {
        let qualified_table_name = format!("`{database}`.`{table_name}`");
        query.replace(&format!("`{table_name}`"), qualified_table_name.as_str())
    }

    pub fn insert_table_from_parquet(
        &self,
        database: &str,
        table_name: &str,
        folder_path: &Path,
        file_name: &str,
    ) -> Result<()> {
        let status = Command::new(Self::LOCAL_EXECUTABLE)
            .arg("--query")
            .arg(format!("
                    SET input_format_skip_unknown_fields = 0;
                    SET input_format_with_names_use_header = 1;
                    SET input_format_defaults_for_omitted_fields = 0;
                    SET input_format_null_as_default = 0;
                    INSERT INTO FUNCTION remote('{}', '{database}.{table_name}', '{}', '{}') 
                    SELECT *, toUnixTimestamp(now()) AS insert_time FROM file('{}/{file_name}', Parquet)",
                Self::LOCAL_HOST_PORT,
                Self::USER_NAME,
                self.password,
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
            Err(clickhouse::error::Error::Custom(error_str))
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

    pub fn get_inserter_for_table<T: Serialize + Row + std::fmt::Debug>(
        &self,
        client: &Client,
        table_name: &str,
    ) -> Result<Inserter<T>> {
        client.inserter(table_name).map_err(|e| {
            let error_str = format!("Unable to create inserter for table {table_name}. {e}");
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
        let mut query = Query::select();
        query.column(Asterisk).from(Alias::new(table_name));
        let query_str = query.to_string(MysqlQueryBuilder);
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
        let mut query = Query::select();
        query.column(Asterisk).from(Alias::new(table_name));
        let query_str = query.to_string(MysqlQueryBuilder);
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

    fn parquet_export_command(
        &self,
        database: &str,
        table_name: &str,
        filter: Option<&str>,
        folder_path: &Path,
        file_name: &str,
    ) -> Command {
        let filter = filter.map_or_else(String::new, |filter| format!(" WHERE {filter}"));
        let mut command = Command::new(Self::LOCAL_EXECUTABLE);
        command.arg("--query").arg(format!("SELECT * FROM remote('{}', '{database}.{table_name}', '{}', '{}'){filter} INTO OUTFILE '{}/{file_name}' FORMAT Parquet", Self::LOCAL_HOST_PORT, Self::USER_NAME, self.password, folder_path.display()));
        command
    }

    fn check_parquet_export(
        &self,
        status: std::io::Result<std::process::ExitStatus>,
        database: &str,
        table_name: &str,
        folder_path: &Path,
        file_name: &str,
    ) -> Result<()> {
        let status = status.map_err(|error| {
            self.project_logger.log_error(
                &format!("Unable to execute command. {error}").replace(&self.password, "***"),
            );
            error
        })?;
        if status.success() {
            self.project_logger.log_debug(&format!(
                "Exported table {table_name} in database {database} to {}/{file_name}",
                folder_path.display()
            ));
            Ok(())
        } else {
            let message = format!(
                "Unable to export table {table_name} in database {database} to {}/{file_name}",
                folder_path.display()
            );
            self.project_logger.log_error(&message);
            Err(clickhouse::error::Error::Custom(message))
        }
    }

    pub fn export_table_to_parquet(
        &self,
        database: &str,
        table_name: &str,
        folder_path: &Path,
        file_name: &str,
    ) -> Result<()> {
        let status = self
            .parquet_export_command(database, table_name, None, folder_path, file_name)
            .status();
        self.check_parquet_export(status, database, table_name, folder_path, file_name)
    }

    pub async fn export_table_to_parquet_async(
        &self,
        database: &str,
        table_name: &str,
        folder_path: &Path,
        file_name: &str,
    ) -> Result<()> {
        let command =
            self.parquet_export_command(database, table_name, None, folder_path, file_name);
        let status = tokio::process::Command::from(command).status().await;
        self.check_parquet_export(status, database, table_name, folder_path, file_name)
    }

    pub fn export_table_to_parquet_with_filter(
        &self,
        database: &str,
        table_name: &str,
        filter_str: &str,
        folder_path: &Path,
        file_name: &str,
    ) -> Result<()> {
        let status = self
            .parquet_export_command(
                database,
                table_name,
                Some(filter_str),
                folder_path,
                file_name,
            )
            .status();
        self.check_parquet_export(status, database, table_name, folder_path, file_name)
    }

    /// Same command and error handling as the synchronous export, without blocking
    /// an async worker while clickhouse-local exports the dataset.
    pub async fn export_table_to_parquet_with_filter_async(
        &self,
        database: &str,
        table_name: &str,
        filter_str: &str,
        folder_path: &Path,
        file_name: &str,
    ) -> Result<()> {
        let command = self.parquet_export_command(
            database,
            table_name,
            Some(filter_str),
            folder_path,
            file_name,
        );
        let status = tokio::process::Command::from(command).status().await;
        self.check_parquet_export(status, database, table_name, folder_path, file_name)
    }

    pub async fn count_row_in_table(
        &self,
        client: &Client,
        table_name: &str,
        distinct_columns: Option<&[&str]>,
    ) -> usize {
        let mut query = Query::select();
        query.from(Alias::new(table_name));

        match distinct_columns {
            Some(columns) => {
                let columns_exprs = columns.iter().map(|c| Expr::col(Alias::new(*c)).into());
                query.expr(Func::count_distinct(Expr::tuple(columns_exprs)));
            }
            None => {
                query.expr(Func::count(Expr::col(Asterisk))).distinct();
            }
        }

        let query_str = query.to_string(MysqlQueryBuilder);
        let count: usize = client.query(&query_str).fetch_one().await.unwrap_or(0);
        count
    }

    pub async fn is_filter_empty(
        &self,
        client: &Client,
        table_name: &str,
        filter_str: &str,
    ) -> bool {
        let mut query = Query::select();
        query.from(Alias::new(table_name));
        query
            .expr(Expr::value(1))
            .and_where(Expr::cust(filter_str))
            .limit(1);

        let query_str = query.to_string(MysqlQueryBuilder);
        !client
            .query(&query_str)
            .fetch_one::<u8>()
            .await
            .is_ok_and(|value| value == 1)
    }
}

#[derive(Debug)]
pub struct ClickHouseColumn {
    pub name: String,
    pub column_type: ClickHouseType,
    pub is_hash_key: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum ClickHouseType {
    Boolean(bool),
    Int8(bool),
    Int32(bool),
    Int64(bool),
    UInt8(bool),
    Float32(bool),
    Float64(bool),
    String(bool),
    Uuid(bool),
}

impl ClickHouseType {
    fn get_type(&self) -> &str {
        match self {
            Self::Boolean(nullable) => {
                if *nullable {
                    NULLABLE_UINT8_TYPE
                } else {
                    UINT8_TYPE
                }
            }
            Self::Int8(nullable) => {
                if *nullable {
                    NULLABLE_INT8_TYPE
                } else {
                    INT8_TYPE
                }
            }
            Self::Int32(nullable) => {
                if *nullable {
                    NULLABLE_INT32_TYPE
                } else {
                    INT32_TYPE
                }
            }
            Self::Int64(nullable) => {
                if *nullable {
                    NULLABLE_INT64_TYPE
                } else {
                    INT64_TYPE
                }
            }
            Self::UInt8(nullable) => {
                if *nullable {
                    NULLABLE_UINT8_TYPE
                } else {
                    UINT8_TYPE
                }
            }
            Self::Float32(nullable) => {
                if *nullable {
                    NULLABLE_FLOAT32_TYPE
                } else {
                    FLOAT32_TYPE
                }
            }
            Self::Float64(nullable) => {
                if *nullable {
                    NULLABLE_FLOAT64_TYPE
                } else {
                    FLOAT64_TYPE
                }
            }
            Self::String(nullable) => {
                if *nullable {
                    NULLABLE_STRING_TYPE
                } else {
                    STRING_TYPE
                }
            }
            Self::Uuid(nullable) => {
                if *nullable {
                    NULLABLE_UUID_TYPE
                } else {
                    UUID_TYPE
                }
            }
        }
    }
}

impl Iden for ClickHouseType {
    fn unquoted(&self, writer: &mut dyn std::fmt::Write) {
        let _ = writer.write_str(self.get_type());
    }
}

pub struct ClickHouseFunc;

impl ClickHouseFunc {
    const MATCH_FUNCTION: &'static str = "match";
    const ARG_MAX_FUNCTION: &'static str = "argMax";
    const ARG_MAX_IF_FUNCTION: &'static str = "argMaxIf";
    const UNIQ_EXACT_FUNCTION: &'static str = "uniqExact";
    const UNIQ_EXACT_IF_FUNCTION: &'static str = "uniqExactIf";
    const MIN_IF_FUNCTION: &'static str = "minIf";
    const NULL_IF_FUNCTION: &'static str = "nullIf";
    const COUNT_IF_FUNCTION: &'static str = "countIf";
    const TUPLE_ELEMENT_FUNCTION: &'static str = "tupleElement";
    const LOWER_UTF8_FUNCTION: &'static str = "lowerUTF8";
    const REPLACE_REGEXP_ALL_FUNCTION: &'static str = "replaceRegexpAll";
    const LAG_IN_FRAME_FUNCTION: &'static str = "lagInFrame";
    const ROW_NUMBER_FUNCTION: &'static str = "row_number";
    const TO_UUID_FUNCTION: &'static str = "toUUID";
    const TO_INT8_EXPR: &'static str = "toInt8(?)";
    const TO_INT32_EXPR: &'static str = "toInt32(?)";
    const TO_INT32_OR_NULL_EXPR: &'static str = "toInt32OrNull(?)";
    const TO_INT64_EXPR: &'static str = "toInt64(?)";
    const TO_INT64_OR_NULL_EXPR: &'static str = "toInt64OrNull(?)";
    const TO_FLOAT32_EXPR: &'static str = "toFloat32(?)";
    const TO_FLOAT32_OR_NULL_EXPR: &'static str = "toFloat32OrNull(?)";
    const UNION_CLAUSE: &'static str = " UNION ";
    const UNION_DISTINCT_CLAUSE: &'static str = " UNION DISTINCT ";
    const ABS_EXPR: &'static str = "abs(?)";
    const IS_NAN_EXPR: &'static str = "isNaN(?)";
    const IF_EXPR: &'static str = "if(?, ?, ?)";
    const INT_DIV_EXPR: &'static str = "intDiv(?, ?)";
    const MODULO_EXPR: &'static str = "modulo(?, ?)";
    const TO_DATE_TIME_EXPR: &'static str = "toDateTime(?)";
    const FORMAT_DATE_TIME_EXPR: &'static str = "toYYYYMMDD(?)";
    const PARSE_DATE_TIME_BEST_EFFORT_OR_NULL_EXPR: &'static str =
        "parseDateTimeBestEffortOrNull(?)";
    const TRIM_BOTH_EXPR: &'static str = "trimBoth(?)";
    const TRIM_RIGHT_EXPR: &'static str = "trimRight(?, ?)";
    const SPLIT_BY_STRING_EXPR: &'static str = "splitByString(?, ?)";
    const SPLIT_BY_CHAR_EXPR: &'static str = "splitByChar(?, ?)";
    const ARRAY_ELEMENT_EXPR: &'static str = "arrayElement(?, ?)";
    const LENGTH_EXPR: &'static str = "length(?)";
    const STARTS_WITH_EXPR: &'static str = "startsWith(?, ?)";
    const ENDS_WITH_EXPR: &'static str = "endsWith(?, ?)";
    const CONTAINS_SUBSTR_EXPR: &'static str = "position(?, ?) > 0";
    const REPLACE_ALL_EXPR: &'static str = "replaceAll(?, ?, ?)";
    const DIGITS_ONLY_EXPR: &'static str = "replaceRegexpAll(?, '[^0-9]', '')";
    const CONCAT_EXPR: &'static str = "concat(?, ?)";
    const SUBSTRING_EXPR: &'static str = "substring(?, ?, ?)";
    const DICT_GET_EXPR: &'static str = "dictGet(?, ?, ?)";
    const DICT_HAS_EXPR: &'static str = "dictHas(?, ?)";

    pub fn insert_time_col() -> Alias {
        Alias::new(ClickHouse::INSERT_TIME)
    }

    fn function(
        function_name: &'static str,
        arguments: impl IntoIterator<Item = SimpleExpr>,
    ) -> SimpleExpr {
        Func::cust(Alias::new(function_name)).args(arguments).into()
    }

    pub fn match_pattern<E1, E2>(value: E1, pattern: E2) -> SimpleExpr
    where
        E1: Into<SimpleExpr>,
        E2: Into<SimpleExpr>,
    {
        Self::function(Self::MATCH_FUNCTION, [value.into(), pattern.into()])
    }

    pub fn arg_max<E1, E2>(value: E1, weight: E2) -> SimpleExpr
    where
        E1: Into<SimpleExpr>,
        E2: Into<SimpleExpr>,
    {
        Self::function(Self::ARG_MAX_FUNCTION, [value.into(), weight.into()])
    }

    pub fn arg_max_if<E1, E2, E3>(value: E1, weight: E2, condition: E3) -> SimpleExpr
    where
        E1: Into<SimpleExpr>,
        E2: Into<SimpleExpr>,
        E3: Into<SimpleExpr>,
    {
        Self::function(
            Self::ARG_MAX_IF_FUNCTION,
            [value.into(), weight.into(), condition.into()],
        )
    }

    pub fn uniq_exact<E: Into<SimpleExpr>>(value: E) -> SimpleExpr {
        Self::function(Self::UNIQ_EXACT_FUNCTION, [value.into()])
    }

    pub fn uniq_exact_if<E1, E2>(value: E1, condition: E2) -> SimpleExpr
    where
        E1: Into<SimpleExpr>,
        E2: Into<SimpleExpr>,
    {
        Self::function(
            Self::UNIQ_EXACT_IF_FUNCTION,
            [value.into(), condition.into()],
        )
    }

    pub fn min_if<E1, E2>(value: E1, condition: E2) -> SimpleExpr
    where
        E1: Into<SimpleExpr>,
        E2: Into<SimpleExpr>,
    {
        Self::function(Self::MIN_IF_FUNCTION, [value.into(), condition.into()])
    }

    pub fn null_if<E1, E2>(value: E1, sentinel: E2) -> SimpleExpr
    where
        E1: Into<SimpleExpr>,
        E2: Into<SimpleExpr>,
    {
        Self::function(Self::NULL_IF_FUNCTION, [value.into(), sentinel.into()])
    }

    pub fn count_if<E: Into<SimpleExpr>>(condition: E) -> SimpleExpr {
        Self::function(Self::COUNT_IF_FUNCTION, [condition.into()])
    }

    pub fn tuple_element<E1, E2>(tuple: E1, index: E2) -> SimpleExpr
    where
        E1: Into<SimpleExpr>,
        E2: Into<SimpleExpr>,
    {
        Self::function(Self::TUPLE_ELEMENT_FUNCTION, [tuple.into(), index.into()])
    }

    pub fn lower_utf8<E: Into<SimpleExpr>>(value: E) -> SimpleExpr {
        Self::function(Self::LOWER_UTF8_FUNCTION, [value.into()])
    }

    pub fn replace_regexp_all<E1, E2, E3>(value: E1, pattern: E2, replacement: E3) -> SimpleExpr
    where
        E1: Into<SimpleExpr>,
        E2: Into<SimpleExpr>,
        E3: Into<SimpleExpr>,
    {
        Self::function(
            Self::REPLACE_REGEXP_ALL_FUNCTION,
            [value.into(), pattern.into(), replacement.into()],
        )
    }

    pub fn lag_in_frame<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        Func::cust(Alias::new(Self::LAG_IN_FRAME_FUNCTION))
            .arg(expr)
            .into()
    }

    pub fn row_number() -> SimpleExpr {
        Func::cust(Alias::new(Self::ROW_NUMBER_FUNCTION)).into()
    }

    pub fn array_last_element<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        let array_expr = expr.into();
        Self::array_element(array_expr.clone(), Self::length(array_expr))
    }

    pub fn null_nan_safe_equality(
        column_type: ClickHouseType,
        current_expr: SimpleExpr,
        previous_expr: SimpleExpr,
    ) -> SimpleExpr {
        let (is_float, is_nullable) = match column_type {
            ClickHouseType::Float32(nullable) | ClickHouseType::Float64(nullable) => {
                (true, nullable)
            }
            ClickHouseType::Boolean(nullable)
            | ClickHouseType::Int8(nullable)
            | ClickHouseType::Int32(nullable)
            | ClickHouseType::Int64(nullable)
            | ClickHouseType::UInt8(nullable)
            | ClickHouseType::String(nullable)
            | ClickHouseType::Uuid(nullable) => (false, nullable),
        };

        let value_equality = if is_float {
            Condition::any()
                .add(current_expr.clone().eq(previous_expr.clone()))
                .add(Self::is_nan(current_expr.clone()).and(Self::is_nan(previous_expr.clone())))
        } else {
            Condition::any().add(current_expr.clone().eq(previous_expr.clone()))
        };

        if is_nullable {
            Condition::any()
                .add(
                    current_expr
                        .clone()
                        .is_null()
                        .and(previous_expr.clone().is_null()),
                )
                .add(
                    current_expr
                        .is_not_null()
                        .and(previous_expr.is_not_null())
                        .and(value_equality.into()),
                )
                .into()
        } else {
            value_equality.into()
        }
    }

    pub fn to_uuid<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        Self::function(Self::TO_UUID_FUNCTION, [expr.into()])
    }

    pub fn to_int8<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        Expr::cust_with_exprs(Self::TO_INT8_EXPR, [expr.into()])
    }

    pub fn to_int32<E>(expr: E) -> SimpleExpr
    where
        E: Into<SimpleExpr>,
    {
        Expr::cust_with_exprs(Self::TO_INT32_EXPR, [expr.into()])
    }

    pub fn to_int32_or_null<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        Expr::cust_with_exprs(Self::TO_INT32_OR_NULL_EXPR, [expr.into()])
    }

    pub fn to_int64<E>(expr: E) -> SimpleExpr
    where
        E: Into<SimpleExpr>,
    {
        Expr::cust_with_exprs(Self::TO_INT64_EXPR, [expr.into()])
    }

    pub fn to_int64_or_null<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        Expr::cust_with_exprs(Self::TO_INT64_OR_NULL_EXPR, [expr.into()])
    }

    pub fn to_float32<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        Expr::cust_with_exprs(Self::TO_FLOAT32_EXPR, [expr.into()])
    }

    pub fn to_float32_or_null<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        Expr::cust_with_exprs(Self::TO_FLOAT32_OR_NULL_EXPR, [expr.into()])
    }

    pub fn replace_union_with_union_distinct(query_str: String) -> String {
        query_str.replace(Self::UNION_CLAUSE, Self::UNION_DISTINCT_CLAUSE)
    }

    pub fn abs<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        Expr::cust_with_exprs(Self::ABS_EXPR, [expr.into()])
    }

    pub fn is_nan<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        Expr::cust_with_exprs(Self::IS_NAN_EXPR, [expr.into()])
    }

    pub fn if_expr<C, T, E>(condition: C, then_expr: T, else_expr: E) -> SimpleExpr
    where
        C: Into<SimpleExpr>,
        T: Into<SimpleExpr>,
        E: Into<SimpleExpr>,
    {
        Expr::cust_with_exprs(
            Self::IF_EXPR,
            [condition.into(), then_expr.into(), else_expr.into()],
        )
    }

    pub fn int_div<E: Into<SimpleExpr>>(expr: E, divisor: u64) -> SimpleExpr {
        let expr = expr.into();
        Expr::cust_with_exprs(Self::INT_DIV_EXPR, [expr, divisor.into()])
    }

    pub fn modulo<E: Into<SimpleExpr>>(expr: E, divisor: u64) -> SimpleExpr {
        let expr = expr.into();
        Expr::cust_with_exprs(Self::MODULO_EXPR, [expr, divisor.into()])
    }

    pub fn to_date_time<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        Expr::cust_with_exprs(Self::TO_DATE_TIME_EXPR, [expr.into()])
    }

    pub fn format_date_time<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        Expr::cust_with_exprs(Self::FORMAT_DATE_TIME_EXPR, [expr.into()])
    }

    pub fn parse_date_time_best_effort_or_null<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        Expr::cust_with_exprs(
            Self::PARSE_DATE_TIME_BEST_EFFORT_OR_NULL_EXPR,
            vec![expr.into()],
        )
    }

    pub fn trim_both<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        Expr::cust_with_exprs(Self::TRIM_BOTH_EXPR, [expr.into()])
    }

    pub fn trim_right<E: Into<SimpleExpr>>(expr: E, trim_chars: &str) -> SimpleExpr {
        Expr::cust_with_exprs(
            Self::TRIM_RIGHT_EXPR,
            [expr.into(), Expr::val(trim_chars).into()],
        )
    }

    pub fn split_by_string<E: Into<SimpleExpr>>(expr: E, delimiter: &str) -> SimpleExpr {
        Expr::cust_with_exprs(
            Self::SPLIT_BY_STRING_EXPR,
            [Expr::val(delimiter).into(), expr.into()],
        )
    }

    pub fn split_by_char<E: Into<SimpleExpr>>(expr: E, delimiter: char) -> SimpleExpr {
        Expr::cust_with_exprs(
            Self::SPLIT_BY_CHAR_EXPR,
            [Expr::val(delimiter).into(), expr.into()],
        )
    }

    pub fn array_element<E: Into<SimpleExpr>>(expr: E, index: impl Into<SimpleExpr>) -> SimpleExpr {
        Expr::cust_with_exprs(Self::ARRAY_ELEMENT_EXPR, [expr.into(), index.into()])
    }

    pub fn length<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        Expr::cust_with_exprs(Self::LENGTH_EXPR, [expr.into()])
    }

    pub fn starts_with<E: Into<SimpleExpr>>(expr: E, prefix: &str) -> SimpleExpr {
        Expr::cust_with_exprs(
            Self::STARTS_WITH_EXPR,
            [expr.into(), Expr::val(prefix).into()],
        )
    }

    pub fn ends_with<E: Into<SimpleExpr>>(expr: E, suffix: &str) -> SimpleExpr {
        Expr::cust_with_exprs(
            Self::ENDS_WITH_EXPR,
            [expr.into(), Expr::val(suffix).into()],
        )
    }

    pub fn contains_substr<E: Into<SimpleExpr>>(expr: E, substr: &str) -> SimpleExpr {
        Expr::cust_with_exprs(
            Self::CONTAINS_SUBSTR_EXPR,
            [expr.into(), Expr::val(substr).into()],
        )
    }

    pub fn replace_all<E: Into<SimpleExpr>>(expr: E, from: &str, to: &str) -> SimpleExpr {
        Expr::cust_with_exprs(
            Self::REPLACE_ALL_EXPR,
            [expr.into(), Expr::val(from).into(), Expr::val(to).into()],
        )
    }

    pub fn digits_only<E: Into<SimpleExpr>>(expr: E) -> SimpleExpr {
        Expr::cust_with_exprs(Self::DIGITS_ONLY_EXPR, [expr.into()])
    }

    pub fn concat<E1: Into<SimpleExpr>, E2: Into<SimpleExpr>>(left: E1, right: E2) -> SimpleExpr {
        Expr::cust_with_exprs(Self::CONCAT_EXPR, [left.into(), right.into()])
    }

    pub fn substring<E1: Into<SimpleExpr>, E2: Into<SimpleExpr>, E3: Into<SimpleExpr>>(
        expr: E1,
        start: E2,
        len: E3,
    ) -> SimpleExpr {
        Expr::cust_with_exprs(
            Self::SUBSTRING_EXPR,
            [expr.into(), start.into(), len.into()],
        )
    }

    pub fn dict_get<E: Into<SimpleExpr>>(dict_name: &str, value: &str, keys: E) -> SimpleExpr {
        Expr::cust_with_exprs(
            Self::DICT_GET_EXPR,
            [
                Expr::val(dict_name).into(),
                Expr::val(value).into(),
                keys.into(),
            ],
        )
    }

    pub fn dict_has<E: Into<SimpleExpr>>(dict_name: &str, keys: E) -> SimpleExpr {
        Expr::cust_with_exprs(
            Self::DICT_HAS_EXPR,
            [Expr::val(dict_name).into(), keys.into()],
        )
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use log::LevelFilter;
    use sea_query::IntoColumnRef;
    use strum::VariantArray;
    use strum_macros::{Display, VariantArray};

    use super::*;

    const TEST_REGISTRY_TABLE: &str = "fixture_registry_test";
    const TEST_SOURCE_TABLE: &str = "fixture_registry_test_source";
    const TEST_FIXTURE_ID_COLUMN: &str = "fixture_id";
    const TEST_GENERATION_COLUMN: &str = "generation";
    const TEST_MERGE_TREE_ENGINE: &str = "ENGINE=MergeTree";
    const TEST_REPLACING_MERGE_TREE_ENGINE: &str = "ReplacingMergeTree";
    const TEST_UUID_TYPE: &str = "UUID";
    const TEST_NULLABLE_UUID_TYPE: &str = "Nullable(UUID)";
    const TEST_FINAL_MODIFIER: &str = "FINAL";
    const TEST_EXCHANGE_STATEMENT: &str = "EXCHANGE TABLES";
    const TEST_INVALID_IDENTIFIER: &str = "invalid ClickHouse identifier";
    const TEST_INVALID_IDENTIFIER_VALUE: &str = "fixture.registry";
    const TEST_DATABASES_FROM_CLAUSE: &str = "FROM `system`.`databases`";
    const TEST_LIMIT_BY_SUFFIX: &str = " LIMIT 1 BY `fixture_id`";
    const TEST_LIMIT_BY_LIMIT: u64 = 1;
    const TEST_LIMIT_BY_ERROR: &str = "ClickHouse LIMIT BY requires at least one column";
    const TEST_TYPED_RENDERING_EXPECT: &str = "typed ClickHouse identifiers render safely";
    const TEST_FUNCTION_VALUE: &str = "value";
    const TEST_FUNCTION_PATTERN: &str = "pattern";
    const TEST_FUNCTION_WEIGHT: &str = "weight";
    const TEST_FUNCTION_REPLACEMENT: &str = "replacement";
    const TEST_TUPLE_VALUE: &str = "tuple";
    const TEST_TUPLE_INDEX: i32 = 1;
    const TEST_FUNCTION_CONDITION: bool = true;
    const TEST_CUSTOM_FUNCTION_NAMES: [&str; 10] = [
        "match",
        "argMax",
        "argMaxIf",
        "uniqExact",
        "uniqExactIf",
        "minIf",
        "countIf",
        "tupleElement",
        "lowerUTF8",
        "replaceRegexpAll",
    ];

    #[derive(Clone, Copy)]
    struct FixtureIdColumn;

    impl Iden for FixtureIdColumn {
        fn unquoted(&self, writer: &mut dyn std::fmt::Write) {
            let _ = writer.write_str(TEST_FIXTURE_ID_COLUMN);
        }
    }

    #[test]
    fn test_clickhouse_types_and_merge_tree_ddl_are_shared() {
        let schema = Table::create()
            .table(Alias::new(TEST_REGISTRY_TABLE))
            .if_not_exists()
            .col(ClickHouse::type_column(
                FixtureIdColumn,
                ClickHouseType::Uuid(false),
            ))
            .col(ClickHouse::type_column(
                Alias::new(TEST_GENERATION_COLUMN),
                ClickHouseType::Uuid(true),
            ))
            .to_owned();
        let ddl = ClickHouse::render_merge_tree_ddl(schema, &[FixtureIdColumn])
            .expect(TEST_TYPED_RENDERING_EXPECT);

        assert!(ddl.contains(TEST_MERGE_TREE_ENGINE));
        assert!(ddl.contains(TEST_UUID_TYPE));
        assert!(ddl.contains(TEST_NULLABLE_UUID_TYPE));
        assert!(ddl.contains(TEST_FIXTURE_ID_COLUMN));
    }

    #[test]
    fn test_shared_clickhouse_extensions_render_only_native_syntax() {
        let schema = Table::create()
            .table(Alias::new(TEST_REGISTRY_TABLE))
            .if_not_exists()
            .col(ColumnDef::new(Alias::new(TEST_FIXTURE_ID_COLUMN)).string())
            .to_owned();
        let replacing_ddl = ClickHouse::render_replacing_merge_tree_ddl(
            schema,
            Alias::new(TEST_GENERATION_COLUMN),
            &[FixtureIdColumn],
        )
        .expect(TEST_TYPED_RENDERING_EXPECT);
        let select = Query::select()
            .column(FixtureIdColumn)
            .from(Alias::new(TEST_SOURCE_TABLE))
            .to_string(MysqlQueryBuilder);
        let final_select =
            ClickHouse::add_final_to_from_tables(select, &[Alias::new(TEST_SOURCE_TABLE)])
                .expect(TEST_TYPED_RENDERING_EXPECT);
        let exchange = ClickHouse::exchange_tables_statement(
            Alias::new(TEST_REGISTRY_TABLE),
            Alias::new(TEST_SOURCE_TABLE),
        )
        .expect(TEST_TYPED_RENDERING_EXPECT);

        assert!(replacing_ddl.contains(TEST_REPLACING_MERGE_TREE_ENGINE));
        assert!(final_select.contains(TEST_FINAL_MODIFIER));
        assert!(exchange.contains(TEST_EXCHANGE_STATEMENT));
    }

    #[test]
    fn test_limit_by_renders_typed_seaquery_columns_and_rejects_empty_keys() {
        let select = Query::select()
            .column(Alias::new(TEST_FIXTURE_ID_COLUMN))
            .from(Alias::new(TEST_SOURCE_TABLE))
            .to_string(MysqlQueryBuilder);
        let columns = [Alias::new(TEST_FIXTURE_ID_COLUMN).into_column_ref()];
        let rendered = ClickHouse::render_limit_by(select, TEST_LIMIT_BY_LIMIT, &columns)
            .expect(TEST_TYPED_RENDERING_EXPECT);
        let empty_keys = ClickHouse::render_limit_by(String::new(), TEST_LIMIT_BY_LIMIT, &[])
            .expect_err(TEST_LIMIT_BY_ERROR);

        assert!(rendered.ends_with(TEST_LIMIT_BY_SUFFIX));
        assert!(empty_keys.to_string().contains(TEST_LIMIT_BY_ERROR));
    }

    #[test]
    fn test_atomic_database_query_uses_seaquery_and_native_renderers_validate_ids() {
        let database_query = current_database_engine_statement();
        let exchange_error = ClickHouse::exchange_tables_statement(
            Alias::new(TEST_INVALID_IDENTIFIER_VALUE),
            Alias::new(TEST_SOURCE_TABLE),
        )
        .expect_err(TEST_INVALID_IDENTIFIER);

        assert!(database_query.contains(TEST_DATABASES_FROM_CLAUSE));
        assert!(exchange_error.to_string().contains(TEST_INVALID_IDENTIFIER));
    }

    #[test]
    fn test_clickhouse_function_helpers_render_shared_function_names() {
        let query = Query::select()
            .expr(ClickHouseFunc::match_pattern(
                Expr::val(TEST_FUNCTION_VALUE),
                Expr::val(TEST_FUNCTION_PATTERN),
            ))
            .expr(ClickHouseFunc::arg_max(
                Expr::val(TEST_FUNCTION_VALUE),
                Expr::val(TEST_FUNCTION_WEIGHT),
            ))
            .expr(ClickHouseFunc::arg_max_if(
                Expr::val(TEST_FUNCTION_VALUE),
                Expr::val(TEST_FUNCTION_WEIGHT),
                Expr::val(TEST_FUNCTION_CONDITION),
            ))
            .expr(ClickHouseFunc::uniq_exact(Expr::val(TEST_FUNCTION_VALUE)))
            .expr(ClickHouseFunc::uniq_exact_if(
                Expr::val(TEST_FUNCTION_VALUE),
                Expr::val(TEST_FUNCTION_CONDITION),
            ))
            .expr(ClickHouseFunc::min_if(
                Expr::val(TEST_FUNCTION_VALUE),
                Expr::val(TEST_FUNCTION_CONDITION),
            ))
            .expr(ClickHouseFunc::count_if(Expr::val(TEST_FUNCTION_CONDITION)))
            .expr(ClickHouseFunc::tuple_element(
                Expr::val(TEST_TUPLE_VALUE),
                Expr::val(TEST_TUPLE_INDEX),
            ))
            .expr(ClickHouseFunc::lower_utf8(Expr::val(TEST_FUNCTION_VALUE)))
            .expr(ClickHouseFunc::replace_regexp_all(
                Expr::val(TEST_FUNCTION_VALUE),
                Expr::val(TEST_FUNCTION_PATTERN),
                Expr::val(TEST_FUNCTION_REPLACEMENT),
            ))
            .to_string(MysqlQueryBuilder);

        assert!(TEST_CUSTOM_FUNCTION_NAMES
            .iter()
            .all(|function| query.contains(function)));
    }

    #[test]
    fn test_parquet_export_command_preserves_existing_arguments() {
        let logger = ProjectLogger::new_logger(Path::new("/tmp"), "export-command-fixture");
        let clickhouse = ClickHouse {
            project_logger: &logger,
            password: "fixture-password".into(),
        };
        for (filter, condition) in [
            (None, ""),
            (
                Some("insert_time >= 10 AND insert_time < 20"),
                " WHERE insert_time >= 10 AND insert_time < 20",
            ),
        ] {
            let command = clickhouse.parquet_export_command(
                "fixture_db",
                "fixture_table",
                filter,
                Path::new("/staging"),
                "fixture.parquet",
            );
            assert_eq!(command.get_program(), "clickhouse-local");
            let args: Vec<_> = command
                .get_args()
                .map(|arg| arg.to_str().unwrap())
                .collect();
            assert_eq!(args, ["--query", &format!("SELECT * FROM remote('localhost:9000', 'fixture_db.fixture_table', 'default', 'fixture-password'){condition} INTO OUTFILE '/staging/fixture.parquet' FORMAT Parquet")]);
        }
    }

    #[derive(Debug, Display, VariantArray)]
    pub enum TestDataCol {
        Venue,
        SurfaceID,
        CourseID,
        HomeStraight,
        Width,
    }

    #[tokio::test]
    async fn test_build_create_dictionary_query() {
        let logger_name = "test_clickhouse";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let secret = Secret::new(&project_logger).await;
        let clickhouse = ClickHouse::new(&project_logger, &secret).await.unwrap();
        let database = "test";
        let table_name = "test_table";
        let clickhouse_client = clickhouse.create_database_client(database);
        let dict_name = "test_dict";
        let key_columns = vec![
            ClickHouseColumn {
                name: "Venue".to_string(),
                column_type: ClickHouseType::String(false),
                is_hash_key: true,
            },
            ClickHouseColumn {
                name: "CourseID".to_string(),
                column_type: ClickHouseType::String(false),
                is_hash_key: true,
            },
        ];
        let value_columns = vec![ClickHouseColumn {
            name: "Width".to_string(),
            column_type: ClickHouseType::Float64(false),
            is_hash_key: false,
        }];
        let query = Query::select()
            .columns(vec!["Venue", "CourseID", "Width"])
            .from("test_table")
            .to_string(MysqlQueryBuilder);
        clickhouse
            .create_dictionary_if_not_exists(
                &clickhouse_client,
                database,
                table_name,
                dict_name,
                &key_columns,
                &value_columns,
                &query,
            )
            .await
            .unwrap();
        let value = "Width";
        let keys = Expr::tuple([
            Expr::col((Alias::new("t"), "Venue")).into(),
            Expr::col((Alias::new("t"), "CourseID")).into(),
        ]);
        let query = Query::select()
            .expr(ClickHouseFunc::dict_get(dict_name, value, keys))
            .from_as(table_name, Alias::new("t"))
            .to_string(MysqlQueryBuilder);
        clickhouse
            .sql_execution(&clickhouse_client, &query)
            .await
            .unwrap();
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
        let secret = Secret::new(&project_logger).await;
        let clickhouse = ClickHouse::new(&project_logger, &secret).await.unwrap();
        let database = "test";
        let clickhouse_client = clickhouse.create_database_client(database);
        let test_table = "test_table";
        let columns = TestDataCol::form_columns();
        clickhouse
            .create_replacing_merge_tree_table(&clickhouse_client, test_table, &columns)
            .await
            .unwrap();
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let data_file = "test.parquet";
        clickhouse
            .insert_table_from_parquet(database, test_table, &folder_path, data_file)
            .unwrap();
    }

    #[tokio::test]
    async fn test_export_table_parquet() {
        let logger_name = "test_clickhouse";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let secret = Secret::new(&project_logger).await;
        let clickhouse = ClickHouse::new(&project_logger, &secret).await.unwrap();
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
        let secret = Secret::new(&project_logger).await;
        let clickhouse = ClickHouse::new(&project_logger, &secret).await.unwrap();
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

    #[tokio::test]
    async fn test_is_filter_empty() {
        let logger_name = "test_clickhouse";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let secret = Secret::new(&project_logger).await;
        let clickhouse = ClickHouse::new(&project_logger, &secret).await.unwrap();
        let client = clickhouse.create_database_client("test");
        let table_name = "test_table";
        let filter_str = "insert_time >= 1747030000";
        let is_empty = clickhouse
            .is_filter_empty(&client, table_name, filter_str)
            .await;
        dbg!(is_empty);
    }

    #[tokio::test]
    async fn test_count_row_in_table_integration() {
        let logger_name = "test_clickhouse_count";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let secret = Secret::new(&project_logger).await;
        let clickhouse = ClickHouse::new(&project_logger, &secret).await.unwrap();
        let database = "test";
        let clickhouse_client = clickhouse.create_database_client(database);
        let table_name = "test_table";

        // Test total count
        let total_count = clickhouse
            .count_row_in_table(&clickhouse_client, table_name, None)
            .await;
        println!("Total row count: {}", total_count);

        // Test distinct count with single column
        let distinct_venue = clickhouse
            .count_row_in_table(&clickhouse_client, table_name, Some(&["Venue"]))
            .await;
        println!("Distinct Venues: {}", distinct_venue);

        // Test distinct count with multiple columns (ClickHouse tuple syntax)
        let multi_distinct = clickhouse
            .count_row_in_table(&clickhouse_client, table_name, Some(&["Venue", "CourseID"]))
            .await;
        println!("Distinct Venue-CourseID pairs: {}", multi_distinct);
    }
    #[derive(Debug, clickhouse::Row, serde::Deserialize, serde::Serialize)]
    struct TestDataRow {
        #[serde(rename = "Venue")]
        venue: String,
        #[serde(rename = "SurfaceID")]
        surface_id: i32,
        #[serde(rename = "CourseID")]
        course_id: String,
        #[serde(rename = "HomeStraight")]
        home_straight: Option<i32>,
        #[serde(rename = "Width")]
        width: f64,
        insert_time: i64,
    }

    #[tokio::test]
    async fn test_load_table() {
        let logger_name = "test_clickhouse_load";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let secret = Secret::new(&project_logger).await;
        let clickhouse = ClickHouse::new(&project_logger, &secret).await.unwrap();
        let database = "test";
        let client = clickhouse.create_database_client(database);
        let table_name = "test_table";

        let rows: Vec<TestDataRow> = clickhouse.load_table(&client, table_name).await.unwrap();
        println!("Loaded {} rows", rows.len());
        if let Some(row) = rows.first() {
            println!("First row: {:?}", row);
        }
    }
}
