use crate::logger::ProjectLogger;
use crate::secret::aws_secret::Secret;
use crate::time_operation;
use crate::PROJECT;
use reqwest::Client;
use serde_json::json;
use std::time::Duration;

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

    pub async fn new(
        report_channel_id: String,
        error_channel_id: String,
        log_channel_id: String,
        logger: &'a ProjectLogger,
        secret: &Secret<'a>,
    ) -> serde_json::Result<Self> {
        const CATEGORY: &str = "slack";
        const NAME: &str = "token";

        let api_token = secret
            .get_secret_value(PROJECT, CATEGORY, NAME)
            .await
            .map_err(|e| {
                let error_str = format!("Unable to get slack token. {e}");
                logger.log_error(&error_str);
                serde::de::Error::custom(error_str)
            })?;
        Ok(Self {
            api_token,
            error_channel_id,
            report_channel_id,
            log_channel_id,
            logger,
            num_retry: NUM_RETRY,
            retry_sleep: RETRY_SLEEP,
        })
    }
    
    pub fn get_logger(&self) -> &'a ProjectLogger {
        self.logger
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

    pub async fn retry_send_message(&self, caller: &str, message: &str, channel: &Channel) {
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
                .await
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

pub enum Channel {
    Report,
    Error,
    LogOnly,
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::utilities_function;
    use log::LevelFilter;
    use std::{env, path::Path};

    #[tokio::test]
    async fn test_send_slack_message() {
        let logger_name = "test_slack_send_message";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_notify");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let secret = Secret::new(&project_logger).await;
        let report_channel_id = secret
            .get_secret_value(PROJECT, "slack", "channel_id")
            .await
            .unwrap();
        let log_channel_id = report_channel_id.clone();
        let error_channel_id = report_channel_id.clone();
        let slack_messenger = SlackMessenger::new(
            report_channel_id,
            error_channel_id,
            log_channel_id,
            &project_logger,
            &secret,
        )
        .await
        .unwrap();
        let calling_func = utilities_function::function_name!(true);
        slack_messenger
            .retry_send_message(calling_func, "Test message from rust", &Channel::Report)
            .await;
    }
}
