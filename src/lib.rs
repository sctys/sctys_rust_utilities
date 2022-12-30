mod logger;


#[cfg(test)]
mod tests {

    use std::env;
    use super::logger::ProjectLogger;

    #[test]
    fn test_logger() {
        let logger_name = "test".to_owned();
        let logger_path = format!("{}{}", env::var("SCTYS_PROJECT").unwrap(), "/Log/log_sctys_rust_utilities");
        let max_file_size_mb = 10;
        let roller_count = 10;
        let logger = ProjectLogger::new_logger(logger_path, logger_name, max_file_size_mb, roller_count);
        let _handle = logger.set_logger();
        logger.log_trace(&format!("This is trace from {}", logger.get_logger_name()));
        logger.log_debug(&format!("This is debug from {}", logger.get_logger_name()));
        logger.log_info(&format!("This is info from {}", logger.get_logger_name()));
        logger.log_warn(&format!("This is warn from {}", logger.get_logger_name()));
        logger.log_error(&format!("This is error from {}", logger.get_logger_name()));
        assert!(true)
    }
}
