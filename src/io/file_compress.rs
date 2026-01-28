use crate::logger::ProjectLogger;
use bzip2::write::BzEncoder;
use flate2::write::GzEncoder;
use std::io::Result;
use std::path::Path;
use std::{fs::File, io::Write};
use tar::Builder;

pub struct FileCompress<'a> {
    project_logger: &'a ProjectLogger,
}

impl<'a> FileCompress<'a> {
    pub fn new(project_logger: &'a ProjectLogger) -> Self {
        Self { project_logger }
    }

    pub fn get_logger(&self) -> &'a ProjectLogger {
        self.project_logger
    }

    pub fn get_gz_compressor(
        &self,
        folder_path: &Path,
        compressed_file_name: &str,
    ) -> Builder<GzEncoder<File>> {
        let full_path = folder_path.join(compressed_file_name);
        match File::create(&full_path) {
            Ok(compressed_file) => {
                let encoder = GzEncoder::new(compressed_file, flate2::Compression::best());
                tar::Builder::new(encoder)
            }
            Err(e) => {
                let error_str = format!(
                    "Unable to create the compressed file {}. {e}",
                    &full_path.display()
                );
                self.project_logger.log_error(&error_str);
                panic!("{error_str}");
            }
        }
    }

    pub fn get_bz2_compressor(
        &self,
        folder_path: &Path,
        compressed_file_name: &str,
    ) -> Builder<BzEncoder<File>> {
        let full_path = folder_path.join(compressed_file_name);
        match File::create(&full_path) {
            Ok(compressed_file) => {
                let encoder = BzEncoder::new(compressed_file, bzip2::Compression::best());
                tar::Builder::new(encoder)
            }
            Err(e) => {
                let error_str = format!(
                    "Unable to create the compressed file {}. {e}",
                    &full_path.display()
                );
                self.project_logger.log_error(&error_str);
                panic!("{error_str}");
            }
        }
    }

    pub fn tar_additional_file<W: Write>(
        &self,
        folder_path: &Path,
        archive_path: &Path,
        file_name: &str,
        builder: &mut Builder<W>,
    ) -> Result<()> {
        let full_path = folder_path.join(file_name);
        let full_archive_path = archive_path.join(file_name);
        File::open(&full_path).map_or_else(
            |e| {
                let error_str = format!("Unable to open the file {}. {e}", &full_path.display());
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |mut file| {
                builder
                    .append_file(&full_archive_path, &mut file)
                    .map_or_else(
                        |e| {
                            let error_str = format!(
                                "Unable to append file {} to tar file. {e}",
                                &full_path.display()
                            );
                            self.project_logger.log_error(&error_str);
                            Err(e)
                        },
                        |_| {
                            let debug_str = format!(
                                "File {} has been append to tar file",
                                &full_path.display()
                            );
                            self.project_logger.log_debug(&debug_str);
                            Ok(())
                        },
                    )
            },
        )
    }

    pub fn tar_additional_folder<W: Write>(
        &self,
        folder_path: &Path,
        archive_path: &Path,
        builder: &mut Builder<W>,
    ) -> Result<()> {
        builder
            .append_dir_all(archive_path, folder_path)
            .map_or_else(
                |e| {
                    let error_str = format!(
                        "Unable to append folder {} to tar gz. {e}",
                        &folder_path.display()
                    );
                    self.project_logger.log_error(&error_str);
                    Err(e)
                },
                |_| {
                    let debug_str = format!(
                        "Folder {} has been append to tar gz",
                        &folder_path.display()
                    );
                    self.project_logger.log_debug(&debug_str);
                    Ok(())
                },
            )
    }

    pub fn run_compression<W: Write>(&self, builder: &mut Builder<W>) -> Result<()> {
        builder.finish().map_err(|e| {
            let error_str = format!("Unable to finish the tar gz compression. {e}");
            self.project_logger.log_error(&error_str);
            e
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use log::LevelFilter;
    use std::env;

    #[test]
    fn test_tar_files() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let archive_path = Path::new("test_io");
        let file_list = (0..5).map(|x| "test_scrape{ind}.html".replace("{ind}", &x.to_string()));
        let logger_name = "test_tar_file";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let file_compress = FileCompress::new(&project_logger);
        let compressed_file_name = "test_scrape.tar.bz2".to_string();
        let mut compressor = file_compress.get_bz2_compressor(&folder_path, &compressed_file_name);
        for file in file_list {
            file_compress
                .tar_additional_file(&folder_path, archive_path, &file, &mut compressor)
                .unwrap();
        }
        file_compress.run_compression(&mut compressor).unwrap();
    }

    #[test]
    fn test_tar_folder() {
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let archive_path = Path::new("test_io");
        let logger_name = "test_tar_folder";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let file_compress = FileCompress::new(&project_logger);
        let compressed_file_name = "test_browse_folder.tar.bz2".to_string();
        let mut compressor = file_compress.get_bz2_compressor(&folder_path, &compressed_file_name);
        file_compress
            .tar_additional_folder(
                &folder_path.join("test_folder"),
                archive_path,
                &mut compressor,
            )
            .unwrap();
        file_compress.run_compression(&mut compressor).unwrap();
    }
}
