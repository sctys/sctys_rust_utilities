extern crate slack;

use serde::Deserialize;
use std::fs;
use std::{thread, time};
use toml;
use crate::logging::logger::ProjectLogger;

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

    pub fn retry_send_message(&self, message: &String, log_only: bool) {
        let channel_id = self.get_channel_id(log_only);
        let client = match slack::api::requests::default_client() {
            Ok(c) => c,
            Err(e) => panic!("Unable to login for slack, {}", e)
        };
        let request = slack::api::chat::PostMessageRequest {channel: channel_id, text: message, ..Default::default()};
        let mut counter: u32 = 1;
        let mut message_sent = false;
        while (counter <= self.num_retry) & !message_sent {
            match slack::api::chat::post_message(&client, &self.api_token, &request) {
                Ok(_) => message_sent = true,
                Err(e) => {
                    self.logger.log_error(&format!("Error in sending message after trial {}, {}", counter, e));
                    counter += 1;
                    thread::sleep(time::Duration::from_secs(self.retry_sleep))
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