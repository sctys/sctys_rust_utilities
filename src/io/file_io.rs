use crate::logger::ProjectLogger;
use crate::time_operation;
use chrono::{DateTime, TimeZone, Utc};
use polars::frame::DataFrame;
use polars::io::{SerReader, SerWriter};
use polars::lazy::frame::{LazyCsvReader, LazyFrame, ScanArgsParquet};
use polars::prelude::*;
use serde::Serialize;
use std::fs::{self, DirEntry};
use std::fs::{File, ReadDir};
use std::io::{Cursor, Error, ErrorKind, Result};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::SystemTime;
use walkdir::WalkDir;


#[derive(Debug)]
pub struct FileIO<'a> {
    project_logger: &'a ProjectLogger,
}

impl<'a> FileIO<'a> {
    pub fn new(project_logger: &'a ProjectLogger) -> Self {
        Self { project_logger }
    }

    pub fn get_logger(&self) -> &'a ProjectLogger {
        self.project_logger
    }

    pub fn check_folder_exist(folder_path: &Path) -> bool {
        folder_path.is_dir()
    }

    pub fn check_file_exist(folder_path: &Path, file: &str) -> bool {
        let full_path_file = Path::new(folder_path).join(file);
        full_path_file.is_file()
    }

    pub fn create_directory_if_not_exists(&self, folder_path: &Path) -> Result<()> {
        if !folder_path.is_dir() {
            fs::create_dir_all(folder_path).map_or_else(
                |e| {
                    let error_str =
                        format!("Unable to create folder {}. {e}", folder_path.display());
                    self.project_logger.log_error(&error_str);
                    Err(e)
                },
                |()| {
                    let debug_str = format!("Folder {} created", folder_path.display());
                    self.project_logger.log_debug(&debug_str);
                    Ok(())
                },
            )
        } else {
            let error_str = format!("Folder {} already exist.", folder_path.display());
            Err(Error::new(ErrorKind::AlreadyExists, error_str))
        }
    }

    pub fn copy_file(
        &self,
        source_path: &Path,
        target_path: &Path,
        original_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let original_full_path = Path::new(source_path).join(original_name);
        let new_full_path = Path::new(target_path).join(new_name);
        fs::copy(original_full_path, new_full_path).map_or_else(
            |e| {
                let error_str = format!(
                    "Unable to copy file from {original_name} in {} to {new_name} in {}. {e}",
                    source_path.display(),
                    target_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |_| {
                let debug_str = format!(
                    "File {original_name} in {} copied to {new_name} in {}",
                    source_path.display(),
                    target_path.display()
                );
                self.project_logger.log_debug(&debug_str);
                Ok(())
            },
        )
    }

    pub fn remove_file(&self, folder_path: &Path, file: &str) -> Result<()> {
        let full_path_file = Path::new(folder_path).join(file);
        fs::remove_file(&full_path_file).map_or_else(
            |e| {
                let error_str = format!("Unable to remove file {}. {e}", full_path_file.display());
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |()| {
                let debug_str = format!("File {} removed", full_path_file.display());
                self.project_logger.log_debug(&debug_str);
                Ok(())
            },
        )
    }

    pub fn move_file(
        &self,
        original_folder_path: &Path,
        new_folder_path: &Path,
        file_name: &str,
    ) -> Result<()> {
        let original_full_path = Path::new(original_folder_path).join(file_name);
        let new_full_path = Path::new(new_folder_path).join(file_name);
        let output = Command::new("mv")
            .arg(original_full_path)
            .arg(new_full_path)
            .output();

        match output {
            Ok(output) => {
                if output.status.success() {
                    let debug_str = format!(
                        "File {file_name} moved from {} to {}",
                        original_folder_path.display(),
                        new_folder_path.display()
                    );
                    self.project_logger.log_debug(&debug_str);
                    Ok(())
                } else {
                    let error_str = format!(
                        "Unable to move file {file_name} from {} to {}. {}",
                        original_folder_path.display(),
                        new_folder_path.display(),
                        String::from_utf8_lossy(&output.stderr)
                    );
                    self.project_logger.log_error(&error_str);
                    Err(Error::other(error_str))
                }
            }
            Err(e) => {
                let error_str = format!(
                    "Unable to move file {file_name} from {} to {}. {e}",
                    original_folder_path.display(),
                    new_folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(e)
            }
        }
    }

    pub fn rename_file(
        &self,
        folder_path: &Path,
        original_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let original_full_path = Path::new(folder_path).join(original_name);
        let new_full_path = Path::new(folder_path).join(new_name);
        fs::rename(original_full_path, new_full_path).map_or_else(
            |e| {
                let error_str = format!(
                    "Unable to rename file from {original_name} to {new_name} in {}. {e}",
                    folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |()| {
                let debug_str = format!(
                    "File {original_name} renamed to {new_name} in {}",
                    folder_path.display()
                );
                self.project_logger.log_debug(&debug_str);
                Ok(())
            },
        )
    }

    pub fn copy_folder(&self, source_path: &Path, destination_path: &Path) -> Result<()> {
        let output = Command::new("cp")
            .arg("-a")
            .arg("-v")
            .arg(source_path.join("").as_os_str())
            .arg(destination_path.as_os_str())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .output();

        match output {
            Ok(output) => {
                if output.status.success() {
                    let debug_str = format!(
                        "Copied folder from {} to {}",
                        source_path.display(),
                        destination_path.display()
                    );
                    self.project_logger.log_debug(&debug_str);
                    Ok(())
                } else {
                    let error_str = format!(
                        "Unable to copy folder from {} to {}",
                        source_path.display(),
                        destination_path.display(),
                    );
                    self.project_logger.log_error(&error_str);
                    Err(Error::other(error_str))
                }
            }
            Err(e) => {
                let error_str = format!(
                    "Error in copy folder from {} to {}, {e}",
                    source_path.display(),
                    destination_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(e)
            }
        }
    }

    pub fn copy_folder_with_tar(&self, source_path: &Path, destination_path: &Path) -> Result<()> {
        match destination_path.file_name().and_then(|module_name| {
            module_name
                .to_str()
                .map(|module_name| format!("{module_name}.tar"))
        }) {
            Some(tar_file_name) => {
                let source_path_parent = source_path
                    .parent()
                    .unwrap_or_else(|| panic!("Unable to get parent of {}", source_path.display()));
                let tar_file_full_path = source_path_parent.join(&tar_file_name);
                let source_path_folder = source_path.file_name().unwrap_or_else(|| {
                    panic!("Unable to get folder name of {}", source_path.display())
                });
                let tar_status = Command::new("tar")
                    .arg("-vcf")
                    .arg(tar_file_full_path.as_os_str())
                    .arg("-C")
                    .arg(source_path_parent)
                    .arg(source_path_folder)
                    .stdout(Stdio::inherit())
                    .stderr(Stdio::inherit())
                    .status();

                match tar_status {
                    Ok(tar_status) => {
                        if tar_status.success() {
                            let debug_str = format!(
                                "Tar file {} created from folder {}",
                                tar_file_full_path.display(),
                                source_path.display()
                            );
                            self.project_logger.log_debug(&debug_str);
                            self.copy_file(
                                source_path,
                                destination_path,
                                &tar_file_name,
                                &tar_file_name,
                            )?;
                            let destination_path_parent =
                                destination_path.parent().unwrap_or_else(|| {
                                    panic!("Unable to get parent of {}", source_path.display())
                                });
                            let tar_destination_full_path =
                                destination_path_parent.join(&tar_file_name);
                            let extract_status = Command::new("tar")
                                .arg("-xf")
                                .arg(tar_destination_full_path.as_os_str())
                                .arg("-C")
                                .arg(destination_path)
                                .stdout(Stdio::inherit())
                                .stderr(Stdio::inherit())
                                .status();

                            match extract_status {
                                Ok(extract_status) => {
                                    if extract_status.success() {
                                        let debug_str = format!(
                                            "Tar file {} extracted to folder {}",
                                            tar_destination_full_path.display(),
                                            destination_path.display()
                                        );
                                        self.project_logger.log_debug(&debug_str);
                                        self.remove_file(&tar_file_full_path, &tar_file_name)?;
                                        self.remove_file(
                                            &tar_destination_full_path,
                                            &tar_file_name,
                                        )?;
                                        Ok(())
                                    } else {
                                        let error_str = format!(
                                            "Unable to extract tar file {} to folder {}",
                                            tar_destination_full_path.display(),
                                            destination_path.display()
                                        );
                                        self.project_logger.log_error(&error_str);
                                        Err(Error::other(error_str))
                                    }
                                }
                                Err(e) => {
                                    let error_str = format!(
                                        "Unable to extract tar file {} to folder {}, {e}",
                                        tar_destination_full_path.display(),
                                        destination_path.display()
                                    );
                                    self.project_logger.log_error(&error_str);
                                    Err(e)
                                }
                            }
                        } else {
                            let error_str = format!(
                                "Unable to create tar file {} from folder {}",
                                tar_file_full_path.display(),
                                source_path.display()
                            );
                            self.project_logger.log_error(&error_str);
                            Err(Error::other(error_str))
                        }
                    }
                    Err(e) => {
                        let error_str = format!(
                            "Unable to create tar file {} from folder {}, {e}",
                            tar_file_full_path.display(),
                            source_path.display()
                        );
                        self.project_logger.log_error(&error_str);
                        Err(e)
                    }
                }
            }
            None => {
                let error_str = format!(
                    "Unable to create tar file from folder {}",
                    source_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(Error::new(ErrorKind::InvalidInput, error_str))
            }
        }
    }

    pub fn safe_remove_folder(&self, folder_path: &Path) -> Result<()> {
        if folder_path.as_os_str() == ""
            || folder_path.as_os_str() == "/"
            || folder_path.as_os_str() == "."
        {
            let error_str = format!("Folder path is invalid: {}", folder_path.display());
            self.project_logger.log_error(&error_str);
            Err(Error::new(ErrorKind::InvalidInput, error_str))
        } else if folder_path.exists() {
            let output = Command::new("rm")
                .arg("-rf")
                .arg(folder_path.as_os_str())
                .output();

            match output {
                Ok(output) => {
                    if output.status.success() {
                        let debug_str = format!("Folder {} removed", folder_path.display());
                        self.project_logger.log_debug(&debug_str);
                        Ok(())
                    } else {
                        let error_str = format!(
                            "Unable to remove folder {}, {}",
                            folder_path.display(),
                            String::from_utf8_lossy(&output.stderr)
                        );
                        self.project_logger.log_error(&error_str);
                        Err(Error::other(error_str))
                    }
                }
                Err(e) => {
                    let error_str =
                        format!("Unable to remove folder {}, {e}", folder_path.display());
                    self.project_logger.log_error(&error_str);
                    Err(e)
                }
            }
        } else {
            let error_str = format!("Folder {} does not exist", folder_path.display());
            self.project_logger.log_error(&error_str);
            Err(Error::new(ErrorKind::NotFound, error_str))
        }
    }

    pub fn get_last_modification_time(&self, full_path: &Path) -> Result<SystemTime> {
        fs::metadata(full_path).map_or_else(
            |e| {
                let error_str = format!(
                    "Unable to get the last modification time for {}, {e}",
                    full_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |metadata| {
                metadata.modified().map_err(|e| {
                    let error_str = format!(
                        "Unable to get the last modification time for {}, {e}",
                        full_path.display()
                    );
                    self.project_logger.log_error(&error_str);
                    e
                })
            },
        )
    }

    pub fn get_elements_in_folder(&self, folder_path: &Path) -> Result<ReadDir> {
        fs::read_dir(folder_path).map_err(|e| {
            let error_str = format!(
                "Unable to get the elements in folder {}, {e}",
                folder_path.display()
            );
            self.project_logger.log_error(&error_str);
            e
        })
    }

    pub fn filter_element_after<T: TimeZone>(
        &self,
        element: &Result<DirEntry>,
        cutoff_date_time: &DateTime<T>,
    ) -> bool {
        let dir_entry = match element {
            Ok(d_e) => d_e,
            Err(e) => panic!("Unable to identify the element. {e}"),
        };
        let full_path = dir_entry.path();
        self.get_last_modification_time(&full_path)
            .is_ok_and(|modified_time| {
                time_operation::diff_system_time_date_time_sec(&modified_time, cutoff_date_time) > 0
            })
    }

    pub fn count_files_modified_between<T: TimeZone>(
        folder_path: &Path,
        cutoff_date_time_early: &DateTime<T>,
        cutoff_date_time_late: &DateTime<T>,
    ) -> usize {
        WalkDir::new(folder_path)
            .into_iter()
            .filter_map(|dir_entry| {
                dir_entry.ok().and_then(|dir_entry| {
                    dir_entry.metadata().ok().and_then(|metadata| {
                        metadata.modified().ok().and_then(|modified| {
                            (metadata.is_file()
                                && (time_operation::diff_system_time_date_time_sec(
                                    &modified,
                                    cutoff_date_time_early,
                                ) >= 0)
                                && (time_operation::diff_system_time_date_time_sec(
                                    &modified,
                                    cutoff_date_time_late,
                                ) < 0))
                                .then_some(1)
                        })
                    })
                })
            })
            .sum()
    }

    pub fn count_files_modified_after<T: TimeZone>(
        folder_path: &Path,
        cutoff_date_time: &DateTime<T>,
    ) -> usize {
        WalkDir::new(folder_path)
            .into_iter()
            .filter_map(|dir_entry| {
                dir_entry.ok().and_then(|dir_entry| {
                    dir_entry.metadata().ok().and_then(|metadata| {
                        metadata.modified().ok().and_then(|modified| {
                            (metadata.is_file()
                                && (time_operation::diff_system_time_date_time_sec(
                                    &modified,
                                    cutoff_date_time,
                                ) >= 0))
                                .then_some(1)
                        })
                    })
                })
            })
            .sum()
    }

    pub fn filter_element_between<T: TimeZone>(
        &self,
        element: &Result<DirEntry>,
        cutoff_date_time_early: &DateTime<T>,
        cutoff_date_time_late: &DateTime<T>,
    ) -> bool {
        let dir_entry = match element {
            Ok(d_e) => d_e,
            Err(e) => panic!("Unable to identify the element. {e}"),
        };
        let full_path = dir_entry.path();
        self.get_last_modification_time(&full_path)
            .is_ok_and(|modified_time| {
                (time_operation::diff_system_time_date_time_sec(
                    &modified_time,
                    cutoff_date_time_early,
                ) >= 0)
                    && (time_operation::diff_system_time_date_time_sec(
                        &modified_time,
                        cutoff_date_time_late,
                    ) < 0)
            })
    }

    pub fn obtain_folder_between_dates(
        &self,
        folder_path: &Path,
        cutoff_date_time_early: &DateTime<Utc>,
        cutoff_date_time_late: &DateTime<Utc>,
    ) -> Result<impl Iterator<Item = DateTime<Utc>>> {
        let start_time_int = cutoff_date_time_early
            .format("%Y%m%d")
            .to_string()
            .parse::<i32>()
            .unwrap_or_else(|e| {
                panic!("Unable to parse start time {cutoff_date_time_early} into i32. {e}")
            });
        let end_time_int = cutoff_date_time_late
            .format("%Y%m%d")
            .to_string()
            .parse::<i32>()
            .unwrap_or_else(|e| {
                panic!("Unable to parse end time {cutoff_date_time_late} into i32. {e}")
            });
        let elements = self.get_elements_in_folder(folder_path)?;
        Ok(elements.filter_map(move |dir| {
            dir.ok().and_then(|element| {
                element.file_name().to_str().and_then(|file_name| {
                    let file_name_date = if file_name.len() < 8 {
                        format!("{file_name}01")
                    } else {
                        file_name.to_string()
                    };
                    file_name_date.parse::<i32>().ok().and_then(|folder_date| {
                        ((folder_date >= start_time_int) && (folder_date < end_time_int))
                            .then_some(time_operation::parse_int_to_utc_date_time(folder_date))
                    })
                })
            })
        }))
    }

    pub fn load_file_as_string(&self, folder_path: &Path, file: &str) -> Result<String> {
        let full_path = folder_path.join(file);
        fs::read_to_string(&full_path).map_or_else(
            |e| {
                let error_str = format!(
                    "Unable to load file {} as string. {e}",
                    &full_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |string| {
                let debug_str = format!("File {} loaded.", &full_path.display());
                self.project_logger.log_debug(&debug_str);
                Ok(string)
            },
        )
    }

    pub fn write_string_to_file(
        &self,
        folder_path: &Path,
        file: &str,
        content: &str,
    ) -> Result<()> {
        let full_path = folder_path.join(file);
        fs::write(&full_path, content).map_or_else(
            |e| {
                let error_str = format!(
                    "Unable to save string to file {}. {e}",
                    &full_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |()| {
                let debug_str = format!("File {} saved.", &full_path.display());
                self.project_logger.log_debug(&debug_str);
                Ok(())
            },
        )
    }

    pub async fn async_write_string_to_file(
        &self,
        folder_path: &Path,
        file: &str,
        content: &str,
    ) -> Result<()> {
        let full_path = folder_path.join(file);
        tokio::fs::write(&full_path, content).await.map_or_else(
            |e| {
                let error_str = format!(
                    "Unable to save string to file {}. {e}",
                    &full_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |()| {
                let debug_str = format!("File {} saved.", &full_path.display());
                self.project_logger.log_debug(&debug_str);
                Ok(())
            },
        )
    }
    
    pub fn convert_structs_vec_to_data_frame<T: Serialize>(&self, data: &[T]) -> serde_json::Result<DataFrame> {
        let json_data = serde_json::to_string(data)?;
        let cursor = Cursor::new(json_data);
        JsonReader::new(cursor).finish().map_err(|e| {
            let error_str = format!("Unable to convert struct vec to data frame. {e}");
            self.project_logger.log_error(&error_str);
            serde::de::Error::custom(error_str)
        })
    }

    // allow for more complicated loading options from the reader
    pub fn get_csv_reader(&self, folder_path: &Path, file: &str) -> PolarsResult<CsvReader<File>> {
        let full_path = folder_path.join(file);

        match File::open(&full_path) {
            Err(e) => {
                let error_str = format!("Unable to load file {} as csv. {e}", &full_path.display());
                self.project_logger.log_error(&error_str);
                Err(PolarsError::IO {
                    error: e.into(),
                    msg: None,
                })
            }
            Ok(file) => {
                let debug_str = format!("File {} loaded.", &full_path.display());
                self.project_logger.log_debug(&debug_str);

                // Use CsvReader::new with the file.
                Ok(CsvReader::new(file))
            }
        }
    }

    // directly loading the csv file with default options
    pub fn load_csv_file(&self, folder_path: &Path, file: &str) -> PolarsResult<DataFrame> {
        let full_path = folder_path.join(file);
        CsvReadOptions::default()
            .with_has_header(true)
            .try_into_reader_with_file_path(Some(full_path))
            .map_err(|e| {
                let error_str = format!(
                    "Unable to convert csv file {}/{file} into data frame. {e}",
                    folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                e
            })
            .and_then(|reader| reader.finish())
    }

    // allow for more complicated writing options for the writer
    pub fn get_file_writer(&self, folder_path: &Path, file: &str) -> Result<File> {
        let full_path = folder_path.join(file);
        File::create(&full_path).map_or_else(
            |e| {
                let error_str = format!("Unable to create file {}. {}", &full_path.display(), e);
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |file_writer| {
                let debug_str = format!("File {} created.", &full_path.display());
                self.project_logger.log_debug(&debug_str);
                Ok(file_writer)
            },
        )
    }

    // directly writing the csv file with default options
    pub fn write_csv_file(
        &self,
        folder_path: &Path,
        file: &str,
        data: &mut DataFrame,
    ) -> PolarsResult<()> {
        let csv_writer = CsvWriter::new(self.get_file_writer(folder_path, file)?);
        csv_writer
            .include_header(true)
            .with_separator(b',')
            .finish(data)
            .map_err(|e| {
                let error_str = format!(
                    "Unable to write csv file {}/{file}. {e}",
                    folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                e
            })
    }

    pub fn scan_csv_file(&self, folder_path: &Path, file: &str) -> PolarsResult<LazyFrame> {
        let full_path = folder_path.join(file);
        LazyCsvReader::new(&full_path).finish().map_or_else(
            |e| {
                let error_str = format!(
                    "Unable to scan csv file {}/{file}. {e}",
                    folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |lazy_frame| {
                let debug_str = format!("File {} scanned.", &full_path.display());
                self.project_logger.log_debug(&debug_str);
                Ok(lazy_frame)
            },
        )
    }

    // allow for more complicated loading options from the reader
    pub fn get_parquet_reader(
        &self,
        folder_path: &Path,
        file: &str,
    ) -> Result<ParquetReader<File>> {
        let full_path = folder_path.join(file);
        File::open(&full_path).map_or_else(
            |e| {
                let error_str = format!("Unable to load file {}. {e}", &full_path.display());
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |parquet_reader| {
                let debug_str = format!("File {} loaded.", &full_path.display());
                self.project_logger.log_debug(&debug_str);
                Ok(ParquetReader::new(parquet_reader))
            },
        )
    }

    // directly reading the parquet file with default options
    pub fn load_parquet_file(&self, folder_path: &Path, file: &str) -> PolarsResult<DataFrame> {
        let parquet_reader: ParquetReader<File> = self.get_parquet_reader(folder_path, file)?;
        parquet_reader.finish().map_err(|e| {
            let error_str = format!(
                "Unable to convert parquet file {}/{file} into data frame. {e}",
                folder_path.display()
            );
            self.project_logger.log_error(&error_str);
            e
        })
    }

    // directly writing the parquet file with default options
    pub fn write_parquet_file(
        &self,
        folder_path: &Path,
        file: &str,
        data: &mut DataFrame,
    ) -> PolarsResult<()> {
        let parquet_writer = ParquetWriter::new(self.get_file_writer(folder_path, file)?);
        parquet_writer.finish(data).map_or_else(
            |e| {
                let error_str = format!(
                    "Unable to write parquet file {}/{file}. {e}",
                    folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |_| Ok(()),
        )
    }

    pub fn scan_parquet_file(&self, folder_path: &Path, file: &str) -> PolarsResult<LazyFrame> {
        let full_path = folder_path.join(file);
        let args = ScanArgsParquet::default();
        LazyFrame::scan_parquet(&full_path, args).map_or_else(
            |e| {
                let error_str = format!(
                    "Unable to scan parquet file {}/{file} into lazy frame. {e}.",
                    folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |lazy_frame| {
                let debug_str = format!("File {} scanned.", &full_path.display());
                self.project_logger.log_debug(&debug_str);
                Ok(lazy_frame)
            },
        )
    }

    pub fn sink_parquet_file(
        &self,
        folder_path: &Path,
        file: &str,
        data: LazyFrame,
    ) -> PolarsResult<()> {
        let full_path = folder_path.join(file);
        let options = ParquetWriteOptions::default();
        data.sink_parquet(&full_path, options, None).map_or_else(
            |e| {
                let error_str = format!(
                    "Unable to sink parquet file {}/{file} from lazy frame. {e}.",
                    folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |()| {
                let debug_str = format!("File {}/{file} sinked.", &folder_path.display());
                self.project_logger.log_debug(&debug_str);
                Ok(())
            },
        )
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use log::LevelFilter;
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
        let file = "Cargo.toml";
        assert!(FileIO::check_file_exist(&folder_path, file));
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
        project_logger.set_logger(LevelFilter::Debug);
        let file_io = FileIO::new(&project_logger);
        file_io
            .create_directory_if_not_exists(&folder_path)
            .unwrap();
        assert!(FileIO::check_folder_exist(&folder_path));
        fs::remove_dir(&folder_path).unwrap();
        assert!(!FileIO::check_folder_exist(&folder_path));
    }

    #[test]
    fn test_copy_folder() {
        let source_folder = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let target_folder = Path::new(&env::var("SCTYS_SSD_DATA").unwrap()).join("test_io");
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let file_io = FileIO::new(&project_logger);
        file_io.copy_folder(&source_folder, &target_folder).unwrap();
        assert!(FileIO::check_folder_exist(&target_folder));
    }

    #[test]
    fn test_safe_remove_folder() {
        let target_folder = Path::new(&env::var("SCTYS_SSD_DATA").unwrap()).join("test_io");
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let file_io = FileIO::new(&project_logger);
        file_io.safe_remove_folder(&target_folder).unwrap();
        assert!(!FileIO::check_folder_exist(&target_folder));
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
        project_logger.set_logger(LevelFilter::Debug);
        let file_io = FileIO::new(&project_logger);
        let elements = file_io.get_elements_in_folder(&folder_path).unwrap();
        let cutoff_date_time = time_operation::utc_date_time(2023, 1, 1, 0, 0, 0);
        let file_list = elements.filter(|x| file_io.filter_element_after(x, &cutoff_date_time));
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
        project_logger.set_logger(LevelFilter::Debug);
        let file_io = FileIO::new(&project_logger);
        let elements = file_io.get_elements_in_folder(&folder_path).unwrap();
        let cutoff_date_time_early = time_operation::utc_date_time(2021, 10, 1, 0, 0, 0);
        let cutoff_date_time_late = time_operation::utc_date_time(2021, 10, 31, 0, 0, 0);
        let file_list = elements.filter(|x| {
            file_io.filter_element_between(x, &cutoff_date_time_early, &cutoff_date_time_late)
        });
        assert_eq!(file_list.count(), 2);
    }

    #[test]
    fn test_walk_directory() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        for entry in WalkDir::new(folder_path) {
            let entry = entry.unwrap();
            let meta = entry.metadata().unwrap();
            if meta.is_file() {
                println!(
                    "{:?}, {}, {:?}",
                    entry.path().parent().unwrap(),
                    entry.file_name().to_string_lossy(),
                    meta.modified().unwrap()
                )
            }
        }
    }

    #[test]
    fn test_count_file_modified_in_between() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let cutoff_date_time_early = time_operation::utc_date_time(2023, 2, 1, 0, 0, 0);
        let cutoff_date_time_late = time_operation::utc_date_time(2023, 2, 28, 0, 0, 0);
        let file_count = FileIO::count_files_modified_between(
            &folder_path,
            &cutoff_date_time_early,
            &cutoff_date_time_late,
        );
        dbg!(file_count);
    }

    #[test]
    fn test_html() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let file = "test.html";
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let file_io = FileIO::new(&project_logger);
        let html_content = file_io.load_file_as_string(&folder_path, file).unwrap();
        let new_file = "test_new.html";
        file_io
            .write_string_to_file(&folder_path, new_file, &html_content)
            .unwrap();
    }

    #[test]
    fn test_json() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let file = "test.json";
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let file_io = FileIO::new(&project_logger);
        let json_content = file_io.load_file_as_string(&folder_path, file).unwrap();
        let new_file = "test_new.json";
        file_io
            .write_string_to_file(&folder_path, new_file, &json_content)
            .unwrap();
    }

    #[test]
    fn test_csv() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let file = "test.csv";
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let file_io = FileIO::new(&project_logger);
        let mut data = file_io.load_csv_file(&folder_path, file).unwrap();
        let new_file = "test_new.csv";
        file_io
            .write_csv_file(&folder_path, new_file, &mut data)
            .unwrap();
    }

    #[test]
    fn test_scan_csv() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let file = "test.csv";
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let file_io = FileIO::new(&project_logger);
        let data = file_io.scan_csv_file(&folder_path, file).unwrap();
        dbg!(data.collect().unwrap());
    }

    #[test]
    fn test_parquet() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let file = "test.parquet";
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let file_io = FileIO::new(&project_logger);
        let mut data = file_io.load_parquet_file(&folder_path, file).unwrap();
        let new_file = "test_new.parquet";
        file_io
            .write_parquet_file(&folder_path, new_file, &mut data)
            .unwrap();
    }

    #[test]
    fn test_scan_parquet() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let file = "test.parquet";
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let file_io = FileIO::new(&project_logger);
        let data = file_io.scan_parquet_file(&folder_path, file).unwrap();
        dbg!(data.collect().unwrap());
    }

    #[test]
    fn test_sink_parquet() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let file = "test.parquet";
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let file_io = FileIO::new(&project_logger);
        let data = file_io.scan_parquet_file(&folder_path, file).unwrap();
        let new_file = "test.parquet";
        file_io
            .sink_parquet_file(&folder_path, new_file, data)
            .unwrap();
    }
    
    #[derive(Debug, Serialize)]
    struct TestStruct {
        a: i32,
        b: String,
    }
    
    #[test]
    fn test_convert_structs_vec_to_data_frame() {
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let file_io = FileIO::new(&project_logger);
        let data = vec![TestStruct { a: 1, b: "test".to_string() }, TestStruct { a: 2, b: "test2".to_string() }];
        let data_frame = file_io.convert_structs_vec_to_data_frame(&data).unwrap();
        dbg!(data_frame);
    }
}
