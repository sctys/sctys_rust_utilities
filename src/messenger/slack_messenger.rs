use crate::logger::ProjectLogger;
use crate::time_operation;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::json;
use std::env;
use std::fs;
use std::path::Path;
use std::time::Duration;
use toml;

const NUM_RETRY: u32 = 5;
const RETRY_SLEEP: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct SlackMessenger<'a> {
    api_token: String,
    error_channel_id: String,
    report_channel_id: String,
    log_channel_id: String,
    logger: &'a ProjectLogger,
    num_retry: u32,
    retry_sleep: Duration,
}

impl<'a> SlackMessenger<'a> {
    const CHANNEL: &'static str = "channel";
    const TEXT: &'static str = "text";
    const SLACK_URL: &'static str = "https://slack.com/api/chat.postMessage";
    pub fn new(report_channel_id: String, error_channel_id: String, log_channel_id: String, logger: &'a ProjectLogger) -> Self {
        let api_token = APIKey::load_apikey();
        Self {
            api_token,
            error_channel_id,
            report_channel_id,
            log_channel_id,
            logger,
            num_retry: NUM_RETRY,
            retry_sleep: RETRY_SLEEP,
        }
    }

    pub fn get_channel_id(&self, channel: &Channel) -> &str {
        match channel {
            Channel::Report => &self.report_channel_id,
            Channel::Error => &self.error_channel_id,
            Channel::LogOnly => &self.log_channel_id,
        }
    }

    pub fn set_num_retry(&mut self, num_retry: u32) {
        self.num_retry = num_retry;
    }

    pub fn set_retry_sleep(&mut self, retry_sleep: Duration) {
        self.retry_sleep = retry_sleep;
    }

    pub fn retry_send_message(&self, caller: &str, message: &str, channel: &Channel) {
        let channel_id = self.get_channel_id(channel);
        let client = Client::new();
        let full_message = format!("Message sending from {caller}:\n\n{message}");
        let request = json!({
            Self::CHANNEL: channel_id,
            Self::TEXT: Some(full_message),
        });
        let mut counter: u32 = 1;
        let mut message_sent = false;
        while (counter <= self.num_retry) & !message_sent {
            match client
                .post(Self::SLACK_URL)
                .bearer_auth(&self.api_token)
                .json(&request)
                .send()
            {
                Ok(_) => message_sent = true,
                Err(e) => {
                    self.logger.log_error(&format!(
                        "Error in sending message after trial {counter}, {e}"
                    ));
                    counter += 1;
                    time_operation::sleep(self.retry_sleep)
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct APIKey {
    api_token: String,
}

impl APIKey {
    const PROJECT_KEY: &str = "SCTYS_PROJECT";
    const API_KEY_PATH: &str = "Secret/secret_sctys_rust_utilities";
    const API_KEY_FILE: &str = "messenger_api.toml";

    fn load_apikey() -> String {
        let full_api_path =
            Path::new(&env::var(Self::PROJECT_KEY).expect("Unable to find project path"))
                .join(Self::API_KEY_PATH)
                .join(Self::API_KEY_FILE);
        let api_str = match fs::read_to_string(full_api_path) {
            Ok(a_s) => a_s,
            Err(e) => panic!("Unable to load the api file. {e}"),
        };
        let api_key_data: APIKey = match toml::from_str(&api_str) {
            Ok(a_d) => a_d,
            Err(e) => panic!("Unable to parse the api file. {e}"),
        };
        api_key_data.api_token
    }
}

pub enum Channel {
    Report,
    Error,
    LogOnly
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::utilities_function;
    use log::LevelFilter;
    use serde::Deserialize;
    use std::env;
    use std::fs;
    use toml;

    #[derive(Deserialize)]
    struct ChannelID {
        channel_id: String,
    }

    #[test]
    fn test_send_slack_message() {
        fn load_channel_id(channel_config_path: &Path, channel_config_file: &str) -> String {
            let full_channel_path = channel_config_path.join(channel_config_file);
            let channel_id_str = match fs::read_to_string(&full_channel_path) {
                Ok(c_s) => c_s,
                Err(e) => panic!(
                    "Unable to load the channel id file {}, {e}",
                    full_channel_path.display()
                ),
            };
            let channel_id_data: ChannelID = match toml::from_str(&channel_id_str) {
                Ok(c_d) => c_d,
                Err(e) => panic!(
                    "Unable to parse the channel_id file {}, {e}",
                    full_channel_path.display()
                ),
            };
            channel_id_data.channel_id
        }

        let logger_name = "test_slack_send_message";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_notify");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let channel_config_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_utilities");
        let channel_config_file = "messenger_channel_id.toml";
        let report_channel_id = load_channel_id(&channel_config_path, channel_config_file);
        let log_channel_id = report_channel_id.clone();
        let error_channel_id = report_channel_id.clone();
        let slack_messenger = SlackMessenger::new(report_channel_id, error_channel_id, log_channel_id, &project_logger);
        let calling_func = utilities_function::function_name!(true);
        slack_messenger.retry_send_message(calling_func, "Test message from rust", &Channel::Report);
    }
}
