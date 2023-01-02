pub mod logging;
pub mod misc;
pub mod messenger;


#[cfg(test)]
mod tests {

    use std::env;
    use serde::Deserialize;
    use std::fs;
    use toml;
    use super::logging::logger::ProjectLogger;
    use super::misc::utilities_function;
    use super::messenger::slack_messenger::SlackMessenger;

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
    fn test_get_current_function_name() {
        let expected_func_name = "test_get_current_function_name";
        let func_name = utilities_function::function_name!();
        assert_eq!(expected_func_name, func_name)
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

    #[derive(Deserialize)]
    struct ChannelID {
        channel_id: String,
    }

    #[test]
    fn test_send_slack_message() {
        fn load_channel_id(channel_config_path: &String, channel_config_file: &str) -> String {
            let full_channel_path = format!("{}/{}", channel_config_path, channel_config_file);
            let channel_id_str = match fs::read_to_string(&full_channel_path) {
                Ok(c_s) => c_s,
                Err(e) => panic!("Unable to load the channel id file {}, {}", full_channel_path, e)
            };
            let channel_id_data: ChannelID = match toml::from_str(&channel_id_str) {
                Ok(c_d) => c_d,
                Err(e) => panic!("Unable to parse the channel_id file {}, {}", full_channel_path, e)
            };
            channel_id_data.channel_id
        }


        let logger_name = "test_slack_send_message";
        let logger_path = format!("{}/{}", env::var("SCTYS_PROJECT").unwrap(), "Log/log_sctys_notify");
        let project_logger = ProjectLogger::new_logger(logger_path, logger_name);
        let api_key_path = format!("{}/{}", env::var("SCTYS_PROJECT").unwrap(), "Config/config_sctys_rust_messenger");
        let api_key_file = "messenger_api.toml";
        let channel_config_path = format!("{}/{}", env::var("SCTYS_PROJECT").unwrap(), "Config/config_sctys_rust_messenger");
        let channel_config_file = "messenger_channel_id.toml";
        let channel_id = load_channel_id(&channel_config_path, channel_config_file);
        let log_channel_id = channel_id.clone();
        let slack_messenger = SlackMessenger::new(api_key_path, api_key_file, channel_id, log_channel_id, &project_logger);
        slack_messenger.retry_send_message(&"Test message from rust".to_owned(), false);
        assert!(true);
    }
}
