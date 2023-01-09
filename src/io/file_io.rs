use crate::logging::logger::ProjectLogger;
use crate::time_operation;
use chrono::{DateTime, TimeZone};
use polars::frame::DataFrame;
use polars::prelude::{CsvReader, CsvWriter, ParquetReader, ParquetWriter};
use polars_io::{SerReader, SerWriter};
use std::fs;
use std::fs::{DirEntry, File, ReadDir};
use std::io::Result;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

pub struct FileIO<'a> {
    project_logger: &'a ProjectLogger,
}

impl<'a> FileIO<'a> {
    pub fn new(project_logger: &'a ProjectLogger) -> Self {
        Self { project_logger }
    }

    pub fn check_folder_exist(folder_path: &PathBuf) -> bool {
        folder_path.is_dir()
    }

    pub fn check_file_exist(folder_path: &PathBuf, file: &String) -> bool {
        let full_path_file = Path::new(folder_path).join(file);
        full_path_file.is_file()
    }

    pub fn create_directory_if_not_exists(&self, folder_path: &PathBuf) {
        if !folder_path.is_dir() {
            match fs::create_dir_all(folder_path) {
                Ok(()) => {
                    let debug_str = format!("Folder {} created", folder_path.display());
                    self.project_logger.log_debug(&debug_str);
                }
                Err(e) => {
                    let error_str =
                        format!("Unable to create folder {}. {e}", folder_path.display());
                    self.project_logger.log_error(&error_str);
                }
            }
        }
    }

    fn get_last_modification_time(&self, full_path: &PathBuf) -> SystemTime {
        match fs::metadata(full_path) {
            Ok(m_d) => match m_d.modified() {
                Ok(m_t) => m_t,
                Err(e) => {
                    let error_str = format!(
                        "Unable to get the last modification time for {}, {e}",
                        full_path.display()
                    );
                    self.project_logger.log_error(&error_str);
                    panic!("{}", &error_str);
                }
            },
            Err(e) => {
                let error_str = format!(
                    "Unable to get the last modification time for {}, {e}",
                    full_path.display()
                );
                self.project_logger.log_error(&error_str);
                panic!("{error_str}");
            }
        }
    }

    pub fn get_elements_in_folder(&self, folder_path: &PathBuf) -> ReadDir {
        let elements = match fs::read_dir(folder_path) {
            Ok(r_d) => r_d,
            Err(e) => {
                let error_str = format!(
                    "Unable to get the elements in folder {}, {e}",
                    folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                panic!("{}", &error_str)
            }
        };
        elements
    }

    pub fn filter_element_after<T: TimeZone>(
        &self,
        element: &Result<DirEntry>,
        cutoff_date_time: DateTime<T>,
    ) -> bool {
        let dir_entry = match element {
            Ok(d_e) => d_e,
            Err(e) => panic!("Unable to identify the element. {e}"),
        };
        let full_path = dir_entry.path();
        let modified_time = self.get_last_modification_time(&full_path);
        time_operation::diff_system_time_date_time_sec(modified_time, cutoff_date_time) > 0
    }

    pub fn filter_element_between<T: TimeZone>(
        &self,
        element: &Result<DirEntry>,
        cutoff_date_time_early: DateTime<T>,
        cutoff_date_time_late: DateTime<T>,
    ) -> bool {
        let dir_entry = match element {
            Ok(d_e) => d_e,
            Err(e) => panic!("Unable to identify the element. {e}"),
        };
        let full_path = dir_entry.path();
        let modified_time = self.get_last_modification_time(&full_path);
        (time_operation::diff_system_time_date_time_sec(modified_time, cutoff_date_time_early) >= 0)
            && (time_operation::diff_system_time_date_time_sec(
                modified_time,
                cutoff_date_time_late,
            ) < 0)
    }

    pub fn load_file_as_string(&self, folder_path: &PathBuf, file: &String) -> String {
        let full_path = folder_path.join(file);
        match fs::read_to_string(&full_path) {
            Ok(s) => {
                let debug_str = format!("File {} loaded.", &full_path.display());
                self.project_logger.log_debug(&debug_str);
                s
            }
            Err(e) => {
                let error_str = format!(
                    "Unable to load file {} as string. {e}",
                    &full_path.display()
                );
                self.project_logger.log_error(&error_str);
                panic!("{}", &error_str)
            }
        }
    }

    pub fn write_string_to_file(&self, folder_path: &PathBuf, file: &String, content: &String) {
        let full_path = folder_path.join(file);
        match fs::write(&full_path, content) {
            Ok(()) => {
                let debug_str = format!("File {} saved.", &full_path.display());
                self.project_logger.log_debug(&debug_str);
            }
            Err(e) => {
                let error_str = format!(
                    "Unable to save string to file {}. {e}",
                    &full_path.display()
                );
                self.project_logger.log_error(&error_str);
                panic!("{}", &error_str);
            }
        };
    }

    // allow for more complicated loading options from the reader
    pub fn get_csv_reader(&self, folder_path: &PathBuf, file: &String) -> CsvReader<File> {
        let full_path = folder_path.join(file);
        match CsvReader::from_path(&full_path) {
            Ok(c_r) => {
                let debug_str = format!("File {} loaded.", &full_path.display());
                self.project_logger.log_debug(&debug_str);
                c_r
            }
            Err(e) => {
                let error_str = format!("Unable to load file {} as csv. {e}", &full_path.display());
                self.project_logger.log_error(&error_str);
                panic!("{}", &error_str)
            }
        }
    }

    // directly loading the csv file with default options
    pub fn load_csv_file(&self, folder_path: &PathBuf, file: &String) -> DataFrame {
        let csv_reader = self.get_csv_reader(folder_path, file);
        match csv_reader.has_header(true).finish() {
            Ok(df) => df,
            Err(e) => panic!(
                "Unable to convert csv file {}/{file} into data frame. {e}",
                folder_path.display()
            ),
        }
    }

    // allow for more complicated writing options for the writer
    pub fn get_file_writer(&self, folder_path: &PathBuf, file: &String) -> File {
        let full_path = folder_path.join(file);
        match File::create(&full_path) {
            Ok(c_f) => {
                let debug_str = format!("File {} created.", &full_path.display());
                self.project_logger.log_debug(&debug_str);
                c_f
            }
            Err(e) => {
                let error_str = format!("Unable to create file {}. {}", &full_path.display(), e);
                self.project_logger.log_error(&error_str);
                panic!("{}", &error_str)
            }
        }
    }

    // directly writing the csv file with default options
    pub fn write_csv_file(&self, folder_path: &PathBuf, file: &String, data: &mut DataFrame) {
        let csv_writer = CsvWriter::new(self.get_file_writer(folder_path, file));
        if let Err(e) = csv_writer
            .has_header(true)
            .with_delimiter(b',')
            .finish(data)
        {
            let error_str = format!(
                "Unable to write csv file {}/{file}. {e}",
                folder_path.display()
            );
            self.project_logger.log_error(&error_str);
            panic!("{}", &error_str)
        }
    }

    // allow for more complicated loading options from the reader
    pub fn get_parquet_reader(&self, folder_path: &PathBuf, file: &String) -> ParquetReader<File> {
        let full_path = folder_path.join(file);
        let file_reader = match File::open(&full_path) {
            Ok(p_f) => {
                let debug_str = format!("File {} loaded.", &full_path.display());
                self.project_logger.log_debug(&debug_str);
                p_f
            }
            Err(e) => {
                let error_str = format!("Unable to load file {}. {e}", &full_path.display());
                self.project_logger.log_error(&error_str);
                panic!("{}", &error_str)
            }
        };
        ParquetReader::new(file_reader)
    }

    // directly reading the parquet file with default options
    pub fn load_parquet_file(&self, folder_path: &PathBuf, file: &String) -> DataFrame {
        let parquet_reader: ParquetReader<File> = self.get_parquet_reader(folder_path, file);
        match parquet_reader.finish() {
            Ok(df) => df,
            Err(e) => panic!(
                "Unable to convert parquet file {}/{file} into data frame. {e}",
                folder_path.display()
            ),
        }
    }

    // directly writing the parquet file with default options
    pub fn write_parquet_file(&self, folder_path: &PathBuf, file: &String, data: &mut DataFrame) {
        let parquet_writer = ParquetWriter::new(self.get_file_writer(folder_path, file));
        if let Err(e) = parquet_writer.finish(data) {
            let error_str = format!(
                "Unable to write parquet file {}/{file}. {e}",
                folder_path.display()
            );
            self.project_logger.log_error(&error_str);
            panic!("{}", &error_str)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::env;

    #[test]
    fn test_folder_exist() {
        let folder_path =
            Path::new(&env::var("SCTYS_PROJECT").unwrap()).join("sctys_rust_utilities");
        assert!(FileIO::check_folder_exist(&folder_path));
    }

    #[test]
    fn test_file_exist() {
        let folder_path =
            Path::new(&env::var("SCTYS_PROJECT").unwrap()).join("sctys_rust_utilities");
        let file = "Cargo.toml".to_owned();
        assert!(FileIO::check_file_exist(&folder_path, &file));
    }

    #[test]
    fn test_create_directory_if_not_exist() {
        let folder_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("sctys_rust_utilities")
            .join("abc");
        assert!(!FileIO::check_folder_exist(&folder_path));
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger();
        let file_io = FileIO::new(&project_logger);
        file_io.create_directory_if_not_exists(&folder_path);
        assert!(FileIO::check_folder_exist(&folder_path));
        fs::remove_dir(&folder_path).unwrap();
        assert!(!FileIO::check_folder_exist(&folder_path));
    }

    #[test]
    fn test_filter_file_modified_after() {
        let folder_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_notify");
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger();
        let file_io = FileIO::new(&project_logger);
        let elements = file_io.get_elements_in_folder(&folder_path);
        let cutoff_date_time = time_operation::utc_date_time(2023, 1, 1, 0, 0, 0);
        let file_list = elements.filter(|x| file_io.filter_element_after(x, cutoff_date_time));
        assert_eq!(file_list.count(), 1);
    }

    #[test]
    fn test_filter_file_modified_between() {
        let folder_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_notify");
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger();
        let file_io = FileIO::new(&project_logger);
        let elements = file_io.get_elements_in_folder(&folder_path);
        let cutoff_date_time_early = time_operation::utc_date_time(2021, 10, 1, 0, 0, 0);
        let cutoff_date_time_late = time_operation::utc_date_time(2021, 10, 31, 0, 0, 0);
        let file_list = elements.filter(|x| {
            file_io.filter_element_between(x, cutoff_date_time_early, cutoff_date_time_late)
        });
        assert_eq!(file_list.count(), 2);
    }

    #[test]
    fn test_html() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).to_path_buf();
        let file = "test.html".to_owned();
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger();
        let file_io = FileIO::new(&project_logger);
        let html_content = file_io.load_file_as_string(&folder_path, &file);
        let new_file = "test_new.html".to_owned();
        file_io.write_string_to_file(&folder_path, &new_file, &html_content);
        assert!(true);
    }

    #[test]
    fn test_json() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).to_path_buf();
        let file = "test.json".to_owned();
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger();
        let file_io = FileIO::new(&project_logger);
        let json_content = file_io.load_file_as_string(&folder_path, &file);
        let new_file = "test_new.json".to_owned();
        file_io.write_string_to_file(&folder_path, &new_file, &json_content);
        assert!(true);
    }

    #[test]
    fn test_csv() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).to_path_buf();
        let file = "test.csv".to_owned();
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger();
        let file_io = FileIO::new(&project_logger);
        let mut data = file_io.load_csv_file(&folder_path, &file);
        let new_file = "test_new.csv".to_owned();
        file_io.write_csv_file(&folder_path, &new_file, &mut data);
        assert!(true);
    }

    #[test]
    fn test_parquet() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).to_path_buf();
        let file = "test.parquet".to_owned();
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger();
        let file_io = FileIO::new(&project_logger);
        let mut data = file_io.load_parquet_file(&folder_path, &file);
        let new_file = "test_new.parquet".to_owned();
        file_io.write_parquet_file(&folder_path, &new_file, &mut data);
        assert!(true);
    }
}
