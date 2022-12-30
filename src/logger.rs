extern crate byte_unit;

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


pub struct ProjectLogger {
    logger_name: String,
    error_logger_name: String,
    full_logger_path_file: String,
    full_error_logger_path_file: String,
    archive_logger_file_name: String,
    max_file_size_mb: u128,
    roller_count: u32,
}

impl ProjectLogger {

    pub fn new_logger(logger_path: String, logger_name: String, max_file_size_mb: u128, roller_count: u32) -> Self {
        let error_logger_name = format!("{}_error", logger_name);
        let standard_logger_file_name = format!("{}.log", logger_name);
        let error_logger_file_name = format!("{}.log", &error_logger_name);
        let full_logger_path_file = format!("{}/{}", logger_path, &standard_logger_file_name);
        let full_error_logger_path_file = format!("{}/{}", logger_path, &error_logger_file_name);
        let archive_logger_file_name = standard_logger_file_name.replace(".log", "_log_{}.gz");
        Self {
            logger_name, error_logger_name, full_logger_path_file, full_error_logger_path_file, 
            archive_logger_file_name, max_file_size_mb, roller_count
        }
    }

    pub fn set_logger(&self) -> Handle {
        let log_line_pattern = "{d(%Y-%m-%d %H:%M:%S)} | {({l}):5.5} | {f}:{L} — {m}{n}";

        let trigger_size = byte_unit::n_mb_bytes!(self.max_file_size_mb) as u64;
        let trigger = Box::new(SizeTrigger::new(trigger_size));

        let roller = Box::new(
            FixedWindowRoller::builder()
                .build(&self.archive_logger_file_name, self.roller_count)
                .unwrap_or_else(|_| panic!("Error in building fixed window roller for {}", self.logger_name))
        );

        let compound_policy = Box::new(CompoundPolicy::new(trigger, roller));

        let std_file_ap = RollingFileAppender::builder()
            .encoder(Box::new(PatternEncoder::new(log_line_pattern)))
            .build(&self.full_logger_path_file, compound_policy)
            .unwrap_or_else(|_| panic!("Error in building standard rolling file appender for {}", self.logger_name));
        
        let err_file_ap = FileAppender::builder()
            .encoder(Box::new(PatternEncoder::new(log_line_pattern)))
            .build(&self.full_error_logger_path_file)
            .unwrap_or_else(|_| panic!("Error in building error file appender for {}", self.error_logger_name));
        
        let stdout_ap = ConsoleAppender::builder().build();

        let config = Config::builder()
            .appender(Appender::builder().build("stdout_ap", Box::new(stdout_ap)))
            .appender(Appender::builder().build("std_file_ap", Box::new(std_file_ap)))
            .appender(Appender::builder().build("err_file_ap", Box::new(err_file_ap)))
            .logger(Logger::builder().appender("std_file_ap").build(&self.logger_name, LevelFilter::Debug))
            .logger(Logger::builder().appender("err_file_ap").build(&self.error_logger_name, LevelFilter::Error))
            .build(Root::builder().appender("stdout_ap").build(LevelFilter::Debug))
            .unwrap_or_else(|_| panic!("Error in configuration of logger for {}", self.logger_name));
        
        log4rs::init_config(config).unwrap_or_else(|_| panic!("Error in init_config for {}", self.logger_name))
    }

    pub fn log_trace(&self, message: &String) {
        trace!(target: &self.logger_name, "{}", message);
    }

    pub fn log_debug(&self, message: &String) {
        debug!(target: &self.logger_name, "{}", message);
    }

    pub fn log_info(&self, message: &String) {
        info!(target: &self.logger_name, "{}", message);
    }

    pub fn log_warn(&self, message: &String) {
        warn!(target: &self.logger_name, "{}", message);
    }

    pub fn log_error(&self, message: &String) {
        error!(target: &self.logger_name, "{}", message);
        error!(target: &self.error_logger_name, "{}", message);
    }

    pub fn get_logger_name(&self) -> &String {
        &self.logger_name
    }

    pub fn get_error_logger_name(&self) -> &String {
        &self.error_logger_name
    }
}