use std::path::Path;

use duckdb::{AppenderParams, Connection, Result, AccessMode, Config};

use crate::logger::ProjectLogger;

pub struct DuckDB<'a> {
    project_logger: &'a ProjectLogger,
}

impl<'a> DuckDB<'a> {
    pub fn new(project_logger: &'a ProjectLogger) -> Self {
        Self { project_logger }
    }

    pub fn create_connection(&self, folder_path: &Path, file_name: &str) -> Result<Connection> {
        let full_path = Path::new(folder_path).join(file_name);
        Connection::open(full_path).map_or_else(
            |e| {
                let error_str = format!(
                    "Unable to open connection to DuckDB at {}/{file_name}. {e}",
                    &folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |conn| {
                let debug_str = format!(
                    "DuckDB at {}/{file_name} connected.",
                    &folder_path.display()
                );
                self.project_logger.log_debug(&debug_str);
                Ok(conn)
            },
        )
    }

    pub fn create_read_only_connection(&self, folder_path: &Path, file_name: &str) -> Result<Connection> {
        let full_path = Path::new(folder_path).join(file_name);
        let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap_or_else(|e| {
            panic!("Unable to set read-only access mode to DuckDB at {}/{file_name}. {e}",
                &folder_path.display()
            );
        });
        Connection::open_with_flags(full_path, config).map_or_else(
            |e| {
                let error_str = format!(
                    "Unable to open read-only connection to DuckDB at {}/{file_name}. {e}",
                    &folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |conn| {
                let debug_str = format!(
                    "DuckDB at {}/{file_name} connected at read-only mode.",
                    &folder_path.display()
                );
                self.project_logger.log_debug(&debug_str);
                Ok(conn)
            },
        )
    }

    fn sql_execution(&self, conn: &Connection, query_str: &str) -> Result<()> {
        conn.execute_batch(query_str).map_or_else(
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

    pub fn create_table_from_parquet(
        &self,
        conn: &Connection,
        table_name: &str,
        folder_path: &Path,
        file_name: &str,
    ) -> Result<()> {
        let query_str = format!("CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_parquet('{}/{file_name}');", folder_path.display());
        self.sql_execution(conn, &query_str)
    }

    pub fn replace_table_from_parquet(
        &self,
        conn: &Connection,
        table_name: &str,
        folder_path: &Path,
        file_name: &str,
    ) -> Result<()> {
        let query_str = format!(
            "CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{}/{file_name}');",
            folder_path.display()
        );
        self.sql_execution(conn, &query_str)
    }

    pub fn insert_table_from_parquet(
        &self,
        conn: &Connection,
        table_name: &str,
        folder_path: &Path,
        file_name: &str,
    ) -> Result<()> {
        let query_str = format!(
            "COPY {table_name} FROM '{}/{file_name}' (FORMAT PARQUET);",
            folder_path.display()
        );
        self.sql_execution(conn, &query_str)
    }

    pub fn insert_table_from_appender<P, I>(
        &self,
        conn: &Connection,
        table_name: &str,
        rows: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = P>,
        P: AppenderParams,
    {
        let mut app = conn.appender(table_name)?;
        app.append_rows(rows)?;
        Ok(())
    }

    pub fn delete_record_from_table(
        &self,
        conn: &Connection,
        table_name: &str,
        where_clause: &str,
    ) -> Result<()> {
        let query_str = format!("DELETE FROM {table_name} WHERE {where_clause};");
        self.sql_execution(conn, &query_str)
    }

    pub fn export_table_to_parquet(
        &self,
        conn: &Connection,
        table_name: &str,
        folder_path: &Path,
        file_name: &str,
    ) -> Result<()> {
        let query_str = format!(
            "COPY {table_name} TO '{}/{file_name}' (FORMAT PARQUET);",
            folder_path.display()
        );
        self.sql_execution(conn, &query_str)
    }

    pub fn export_query_to_parquet(
        &self,
        conn: &Connection,
        query_str: &str,
        folder_path: &Path,
        file_name: &str,
    ) -> Result<()> {
        let main_query_str = format!(
            "COPY ({query_str}) TO '{}/{file_name}' (FORMAT PARQUET);",
            folder_path.display()
        );
        self.sql_execution(conn, &main_query_str)
    }

    pub fn count_row_in_table(
        &self,
        conn: &Connection,
        table_name: &str,
        distinct_columns: Option<&[&str]>,
    ) -> usize {
        let query_str = match distinct_columns {
            Some(columns) => {
                let columns_str = columns.join(", ");
                format!("SELECT COUNT(DISTINCT ({columns_str})) FROM {table_name};")
            }
            None => {
                format!("SELECT DISTINCT COUNT(*) FROM {table_name};")
            }
        };
        let stmt = conn.prepare(&query_str);
        stmt.ok().map_or(0, |mut stmt| {
            let row = stmt.query_row([], |row| row.get(0));
            row.ok().map_or(0, |row| row)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use log::LevelFilter;

    use super::*;

    #[test]
    fn test_create_table_from_parquet() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let db_file = "test.duckdb";
        let logger_name = "test_duck_db";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let duckdb = DuckDB::new(&project_logger);
        let conn = duckdb.create_connection(&folder_path, db_file).unwrap();
        let data_file = "test.parquet";
        let table_name = "test";
        duckdb
            .create_table_from_parquet(&conn, table_name, &folder_path, data_file)
            .unwrap();
    }

    #[test]
    fn test_insert_table_from_parquet() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let db_file = "test.duckdb";
        let logger_name = "test_duck_db";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let duckdb = DuckDB::new(&project_logger);
        let conn = duckdb.create_connection(&folder_path, db_file).unwrap();
        let data_file = "test.parquet";
        let table_name = "test";
        duckdb
            .insert_table_from_parquet(&conn, table_name, &folder_path, data_file)
            .unwrap();
    }

    #[test]
    fn test_delete_record_from_parquet() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let db_file = "test.duckdb";
        let logger_name = "test_duck_db";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let duckdb = DuckDB::new(&project_logger);
        let conn = duckdb.create_connection(&folder_path, db_file).unwrap();
        let table_name = "test";
        let where_clause = "Venue = 'ST'";
        duckdb
            .delete_record_from_table(&conn, table_name, where_clause)
            .unwrap();
    }

    #[test]
    fn test_export_table_parquet() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let db_file = "test.duckdb";
        let logger_name = "test_duck_db";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let duckdb = DuckDB::new(&project_logger);
        let conn = duckdb.create_connection(&folder_path, db_file).unwrap();
        let data_file = "test_duckdb_out.parquet";
        let table_name = "test";
        duckdb
            .export_table_to_parquet(&conn, table_name, &folder_path, data_file)
            .unwrap();
    }

    #[test]
    fn test_count_row_in_table() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let db_file = "test.duckdb";
        let logger_name = "test_duck_db";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let duckdb = DuckDB::new(&project_logger);
        let conn = duckdb.create_connection(&folder_path, db_file).unwrap();
        let table_name = "test";
        let distinct_columns = None;
        let row_count = duckdb.count_row_in_table(&conn, table_name, distinct_columns);
        dbg!(row_count);
    }
}
