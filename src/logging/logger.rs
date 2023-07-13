extern crate byte_unit;

use std::path::{Path, PathBuf};

use log::LevelFilter;
use log::{debug, error, info, trace, warn};

use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::append::rolling_file::policy::compound::{
    roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger,
};
use log4rs::append::rolling_file::RollingFileAppender;

use log4rs::encode::pattern::PatternEncoder;

use log4rs::config::{Appender, Logger, Root};
use log4rs::{Config, Handle};

const DEFAULT_MAX_FILE_SIZE_MB: u128 = 10;
const DEFAULT_ROLLER_COUNT: u32 = 10;

#[derive(Debug)]
pub struct ProjectLogger {
    logger_name: String,
    error_logger_name: String,
    full_logger_path_file: PathBuf,
    full_error_logger_path_file: PathBuf,
    archive_logger_file_name: String,
    max_file_size_mb: u128,
    roller_count: u32,
}

impl ProjectLogger {
    pub fn new_logger(logger_path: &Path, logger_name: &str) -> Self {
        let error_logger_name = format!("{logger_name}_error");
        let standard_logger_file_name = format!("{logger_name}.log");
        let error_logger_file_name = format!("{}.log", &error_logger_name);
        let full_logger_path_file = logger_path.join(standard_logger_file_name);
        let full_error_logger_path_file = logger_path.join(error_logger_file_name);
        let archive_logger_file_name = full_logger_path_file
            .to_str()
            .unwrap_or_else(|| panic!("Unable to convert full logger path file to str."))
            .replace(".log", "_log_{}.gz");
        Self {
            logger_name: logger_name.to_owned(),
            error_logger_name,
            full_logger_path_file,
            full_error_logger_path_file,
            archive_logger_file_name,
            max_file_size_mb: DEFAULT_MAX_FILE_SIZE_MB,
            roller_count: DEFAULT_ROLLER_COUNT,
        }
    }

    pub fn set_logger(&self, logger_level: LevelFilter) -> Handle {
        let log_line_pattern = "{d(%Y-%m-%d %H:%M:%S)} | {h({l}):5.5} | {t} - {m}{n}";

        let trigger_size = byte_unit::n_mb_bytes!(self.max_file_size_mb) as u64;
        let trigger = Box::new(SizeTrigger::new(trigger_size));

        let roller = Box::new(
            FixedWindowRoller::builder()
                .build(&self.archive_logger_file_name, self.roller_count)
                .unwrap_or_else(|_| {
                    panic!(
                        "Error in building fixed window roller for {}",
                        self.logger_name
                    )
                }),
        );

        let compound_policy = Box::new(CompoundPolicy::new(trigger, roller));

        let std_file_ap = RollingFileAppender::builder()
            .encoder(Box::new(PatternEncoder::new(log_line_pattern)))
            .build(&self.full_logger_path_file, compound_policy)
            .unwrap_or_else(|_| {
                panic!(
                    "Error in building standard rolling file appender for {}",
                    self.logger_name
                )
            });

        let err_file_ap = FileAppender::builder()
            .encoder(Box::new(PatternEncoder::new(log_line_pattern)))
            .build(&self.full_error_logger_path_file)
            .unwrap_or_else(|_| {
                panic!(
                    "Error in building error file appender for {}",
                    self.error_logger_name
                )
            });

        let stdout_ap = ConsoleAppender::builder().build();

        let config = Config::builder()
            .appender(Appender::builder().build("stdout_ap", Box::new(stdout_ap)))
            .appender(Appender::builder().build("std_file_ap", Box::new(std_file_ap)))
            .appender(Appender::builder().build("err_file_ap", Box::new(err_file_ap)))
            .logger(
                Logger::builder()
                    .appender("std_file_ap")
                    .build(&self.logger_name, LevelFilter::Debug),
            )
            .logger(
                Logger::builder()
                    .appender("err_file_ap")
                    .build(&self.error_logger_name, LevelFilter::Error),
            )
            .build(Root::builder().appender("stdout_ap").build(logger_level))
            .unwrap_or_else(|_| {
                panic!("Error in configuration of logger for {}", self.logger_name)
            });

        log4rs::init_config(config)
            .unwrap_or_else(|_| panic!("Error in init_config for {}", self.logger_name))
    }

    pub fn log_trace(&self, message: &str) {
        trace!(target: &self.logger_name, "{message}");
    }

    pub fn log_debug(&self, message: &str) {
        debug!(target: &self.logger_name, "{message}");
    }

    pub fn log_info(&self, message: &str) {
        info!(target: &self.logger_name, "{message}");
    }

    pub fn log_warn(&self, message: &str) {
        warn!(target: &self.logger_name, "{message}");
    }

    pub fn log_error(&self, message: &str) {
        error!(target: &self.logger_name, "{message}");
        error!(target: &self.error_logger_name, "{message}");
    }

    pub fn get_logger_name(&self) -> &str {
        &self.logger_name
    }

    pub fn get_error_logger_name(&self) -> &str {
        &self.error_logger_name
    }

    pub fn set_max_file_size_mb(&mut self, max_file_size_mb: u128) {
        self.max_file_size_mb = max_file_size_mb
    }

    pub fn set_roller_count(&mut self, roller_count: u32) {
        self.roller_count = roller_count
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::env;
    use std::path::Path;

    #[test]
    fn test_logger() {
        let logger_name = "test";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_rust_utilities");
        let logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = logger.set_logger(LevelFilter::Debug);
        logger.log_trace(&format!("This is trace from {}", logger.get_logger_name()));
        logger.log_debug(&format!("This is debug from {}", logger.get_logger_name()));
        logger.log_info(&format!("This is info from {}", logger.get_logger_name()));
        logger.log_warn(&format!("This is warn from {}", logger.get_logger_name()));
        logger.log_error(&format!("This is error from {}", logger.get_logger_name()));
    }
}
