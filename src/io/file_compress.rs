use crate::logger::ProjectLogger;
use bzip2::write::BzEncoder;
use flate2::write::GzEncoder;
use std::io::{Error, ErrorKind, Result};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::{fs::File, io::Write, path::PathBuf};
use tar::Builder;

/// A tar output writer backed by a parallel `pbzip2` process.
///
/// The process is started with bzip2 compression level 9. The writer must be
/// finished after the tar builder has emitted its end-of-archive blocks so the
/// child process can receive EOF and its exit status can be checked.
#[must_use = "the writer must be finished after the tar archive is complete"]
pub struct ParallelBz2Writer {
    child: Child,
    stdin: Option<ChildStdin>,
    output_path: PathBuf,
}

impl ParallelBz2Writer {
    fn new(output_path: &Path, worker_count: usize) -> Result<Self> {
        if worker_count == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "parallel bzip2 worker count must be greater than zero",
            ));
        }

        let output_file = File::create(output_path)?;
        let mut child = Command::new("pbzip2")
            .arg("-q")
            .arg("-c")
            .arg("-9")
            .arg(format!("-p{worker_count}"))
            .stdin(Stdio::piped())
            .stdout(Stdio::from(output_file))
            .spawn()?;

        let stdin = match child.stdin.take() {
            Some(stdin) => stdin,
            None => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(Error::other(
                    "pbzip2 did not provide a writable standard input",
                ));
            }
        };

        Ok(Self {
            child,
            stdin: Some(stdin),
            output_path: output_path.to_owned(),
        })
    }

    /// Closes the tar stream sent to `pbzip2` and waits for compression to
    /// finish successfully.
    pub fn finish(mut self) -> Result<()> {
        self.stdin.take();

        let status = self.child.wait()?;
        if status.success() {
            Ok(())
        } else {
            Err(Error::other(format!(
                "pbzip2 failed for {} with status {status}",
                self.output_path.display()
            )))
        }
    }
}

impl Write for ParallelBz2Writer {
    fn write(&mut self, buffer: &[u8]) -> Result<usize> {
        self.stdin
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "parallel bzip2 writer is closed"))?
            .write(buffer)
    }

    fn flush(&mut self) -> Result<()> {
        self.stdin
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "parallel bzip2 writer is closed"))?
            .flush()
    }
}

impl Drop for ParallelBz2Writer {
    fn drop(&mut self) {
        self.stdin.take();

        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.wait();
        }
    }
}

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
                    full_path.display()
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
                    full_path.display()
                );
                self.project_logger.log_error(&error_str);
                panic!("{error_str}");
            }
        }
    }

    /// Creates a level-9 bzip2 compressor backed by a parallel `pbzip2`
    /// process.
    ///
    /// `pbzip2` must be installed and available on `PATH`. The returned tar
    /// builder should be completed with [`Self::run_parallel_bz2_compression`]
    /// instead of [`Self::run_compression`].
    pub fn get_parallel_bz2_compressor(
        &self,
        folder_path: &Path,
        compressed_file_name: &str,
        worker_count: usize,
    ) -> Result<Builder<ParallelBz2Writer>> {
        let full_path = folder_path.join(compressed_file_name);
        ParallelBz2Writer::new(&full_path, worker_count).map_or_else(
            |e| {
                let error_str = format!(
                    "Unable to start parallel bzip2 compression for {}. {e}",
                    full_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(e)
            },
            |writer| Ok(Builder::new(writer)),
        )
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
                let error_str = format!("Unable to open the file {}. {e}", full_path.display());
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
                                full_path.display()
                            );
                            self.project_logger.log_error(&error_str);
                            Err(e)
                        },
                        |_| {
                            let debug_str =
                                format!("File {} has been append to tar file", full_path.display());
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
                        folder_path.display()
                    );
                    self.project_logger.log_error(&error_str);
                    Err(e)
                },
                |_| {
                    let debug_str =
                        format!("Folder {} has been append to tar gz", folder_path.display());
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

    /// Finishes a tar archive created by [`Self::get_parallel_bz2_compressor`]
    /// and waits for `pbzip2` to complete.
    pub fn run_parallel_bz2_compression(&self, builder: Builder<ParallelBz2Writer>) -> Result<()> {
        let writer = builder.into_inner().map_err(|e| {
            let error_str = format!("Unable to finish the parallel bzip2 tar archive. {e}");
            self.project_logger.log_error(&error_str);
            e
        })?;

        writer.finish().map_err(|e| {
            let error_str = format!("Unable to finish parallel bzip2 compression. {e}");
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

    #[test]
    fn parallel_bz2_rejects_zero_workers() {
        let result = ParallelBz2Writer::new(Path::new("unused.tar.bz2"), 0);

        assert!(matches!(
            result,
            Err(error) if error.kind() == ErrorKind::InvalidInput
        ));
    }
}
