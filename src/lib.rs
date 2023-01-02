pub mod logger;
pub mod utilities_function;


#[cfg(test)]
mod tests {

    use std::env;
    use super::logger::ProjectLogger;
    use super::utilities_function;

    #[test]
    fn test_logger() {
        let logger_name = "test";
        let logger_path = format!("{}{}", env::var("SCTYS_PROJECT").unwrap(), "/Log/log_sctys_rust_utilities");
        let logger = ProjectLogger::new_logger(logger_path, logger_name);
        let _handle = logger.set_logger();
        logger.log_trace(&format!("This is trace from {}", logger.get_logger_name()));
        logger.log_debug(&format!("This is debug from {}", logger.get_logger_name()));
        logger.log_info(&format!("This is info from {}", logger.get_logger_name()));
        logger.log_warn(&format!("This is warn from {}", logger.get_logger_name()));
        logger.log_error(&format!("This is error from {}", logger.get_logger_name()));
        assert!(true)
    }

    #[test]
    fn test_get_function_name() {
        let func_name = utilities_function::get_function_name(test_logger);
        assert_eq!(func_name, "sctys_rust_utilities::tests::test_logger")
    }

    #[test]
    fn test_timeit() {
        fn looping_sum(count: u64) -> u64 {
            let mut total: u64 = 0;
            for i in 1..(count + 1) {
                total += i;
            }
            total
        }
        let num: u64 = 100000;
        let expected_total = num / 2 * (1 + num);
        let cal_total = utilities_function::timeit!(looping_sum(num));
        assert_eq!(expected_total, cal_total)
    }
}
