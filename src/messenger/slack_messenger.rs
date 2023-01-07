extern crate slack;

use serde::Deserialize;
use std::fs;
use toml;
use crate::logger::ProjectLogger;
use crate::time_operation;

const NUM_RETRY: u32 = 5;
const RETRY_SLEEP: u64 = 5;


pub struct SlackMessenger<'a> {
    api_token: String,
    main_channel_id: String,
    log_channel_id: String,
    logger: &'a ProjectLogger,
    num_retry: u32,
    retry_sleep: u64,
}

impl<'a> SlackMessenger<'a> {
    pub fn new(
        api_key_path: String, api_key_file: &str, main_channel_id: String, 
        log_channel_id: String, logger: &'a ProjectLogger
    ) -> Self {
        let api_token = load_apikey(&api_key_path, api_key_file);
        Self { api_token, main_channel_id, log_channel_id, 
            logger, num_retry: NUM_RETRY, retry_sleep: RETRY_SLEEP}
    }

    pub fn get_channel_id(&self, log_only: bool) -> &String {
        if log_only {
            &self.log_channel_id
        } else {
            &self.main_channel_id
        }
    }
    
    pub fn set_num_retry(&mut self, num_retry: u32) {
        self.num_retry = num_retry;
    }

    pub fn set_retry_sleep(&mut self, retry_sleep: u64) {
        self.retry_sleep = retry_sleep;
    }

    pub fn retry_send_message(&self, calling_func: &str, message: &String, log_only: bool) {
        let channel_id = self.get_channel_id(log_only);
        let client = match slack::api::requests::default_client() {
            Ok(c) => c,
            Err(e) => panic!("Unable to login for slack, {}", e)
        };
        let full_message = format!("{}: {}", calling_func, message);
        let request = slack::api::chat::PostMessageRequest {channel: channel_id, text: &full_message, ..Default::default()};
        let mut counter: u32 = 1;
        let mut message_sent = false;
        while (counter <= self.num_retry) & !message_sent {
            match slack::api::chat::post_message(&client, &self.api_token, &request) {
                Ok(_) => message_sent = true,
                Err(e) => {
                    self.logger.log_error(&format!("Error in sending message after trial {}, {}", counter, e));
                    counter += 1;
                    time_operation::sleep(self.retry_sleep)
                }
            }
        }
    }
}


#[derive(Deserialize)]
struct APIKey {
    api_token: String
}

fn load_apikey(api_key_path: &String, api_key_file: &str) -> String {
    let full_api_path = format!("{}/{}", api_key_path, api_key_file);
    let api_str = match fs::read_to_string(&full_api_path) {
        Ok(a_s) => a_s,
        Err(e) => panic!("Unable to load the api file {}, {}", full_api_path, e)
    };
    let api_key_data: APIKey = match toml::from_str(&api_str) {
        Ok(a_d) => a_d,
        Err(e) => panic!("Unable to parse the api file {}, {}", full_api_path, e)
    };
    api_key_data.api_token
}


#[cfg(test)]
mod tests {

    use std::env;
    use serde::Deserialize;
    use std::fs;
    use toml;
    use super::*;
    use crate::utilities_function;
    
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
        let calling_func = utilities_function::function_name!(true);
        slack_messenger.retry_send_message(calling_func, &"Test message from rust".to_owned(), false);
        assert!(true);
    }
}