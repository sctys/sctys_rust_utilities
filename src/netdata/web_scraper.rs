use reqwest::Client as AsyncClient;
use reqwest::blocking::{Client, RequestBuilder, Response};
use reqwest::{Result, Url};
use std::path::PathBuf;
use thirtyfour_sync::{ChromeCapabilities, WebDriver, WebDriverCommands};
use thirtyfour_sync::error::WebDriverResult;
use std::time::Duration;
use tqdm;

use crate::logger::ProjectLogger;
use crate::slack_messenger::SlackMessenger;
use crate::file_io::FileIO;
use crate::{utilities_function, time_operation};


#[derive(Debug)]
pub struct UrlFile {
    pub url: Url,
    pub file_name: String,
}

impl UrlFile {
    pub fn new(url: Url, file_name: String) -> Self {
        Self {url, file_name}
    }
}

pub enum ResponseCheckResult {
    Ok,
    ErrContinue(String),
    ErrTerminate(String),
}

#[derive(Debug)]
pub struct WebScraper<'a> {
    project_logger: &'a ProjectLogger,
    slack_messenger: &'a SlackMessenger<'a>,
    file_io: &'a FileIO<'a>,
    num_retry: u32,
    retry_sleep: Duration,
    consecutive_sleep: (Duration, Duration),
    timeout: Duration,
    web_driver_port: u32,
    fail_list: Vec<String>,
    client: Option<Client>,
    async_client: Option<AsyncClient>,
    web_driver: Option<WebDriver>,
    browser: Option<ChromeCapabilities>,
}

impl<'a> WebScraper<'a> {

    const NUM_RETRY: u32 = 3;
    const RETRY_SLEEP: Duration = Duration::from_secs(10);
    const CONSECUTIVE_SLEEP: (Duration, Duration) = (Duration::from_secs(0), Duration::from_secs(30));
    const TIMEOUT: Duration = Duration::from_secs(120);
    const GOOGLE_SHEET_REPLACE_TOKEN: (&str, &str) = ("edit#gid=", "/export?format=csv&gid=");
    const WEB_DRIVER_PORT: u32 = 4444;
    const WEB_DRIVER_PROG: &str = "http://localhost:";


    pub fn new(project_logger: &'a ProjectLogger, slack_messenger: &'a SlackMessenger, file_io: &'a FileIO) -> Self {
        Self {
            project_logger, slack_messenger, file_io, num_retry: Self::NUM_RETRY, retry_sleep: Self::RETRY_SLEEP, 
            consecutive_sleep: Self::CONSECUTIVE_SLEEP, timeout: Self::TIMEOUT, web_driver_port: Self::WEB_DRIVER_PORT,
            fail_list: Vec::<String>::new(), client: None, async_client: None, web_driver: None, browser: None,
        }
    }

    pub fn set_num_retry(&mut self, num_retry: u32) {
        self.num_retry = num_retry;
    }

    pub fn set_retry_sleep(&mut self, retry_sleep: Duration) {
        self.retry_sleep = retry_sleep;
    }

    pub fn set_consecutive_sleep(&mut self, consecutive_sleep: (Duration, Duration)) {
        self.consecutive_sleep = consecutive_sleep;
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    pub fn set_web_driver_port(&mut self, web_driver_port: u32) {
        self.web_driver_port = web_driver_port;
    }

    pub fn set_blocking_client(&mut self, client: Client) {
        self.client = Some(client);
    }

    pub fn set_async_client(&mut self, client: AsyncClient) {
        self.async_client = Some(client)
    }

    pub fn set_browser(&mut self, browser: ChromeCapabilities) {
        self.browser = Some(browser);
    }

    pub fn get_default_blocking_client(&mut self) -> Client {
        let mut counter = 0;
        while counter < self.num_retry {
            match Client::builder().timeout(self.timeout).build() {
                Ok(c) => {
                    self.client = Some(c.clone());
                    return c
                }
                Err(e) => {
                    counter += 1;
                    let warn_str = format!("Unable to build connection client after trial {counter}. {e}");
                    self.project_logger.log_warn(&warn_str);
                }
            };
        }
        let error_str = format!("Fail to build connection client");
        let calling_func = utilities_function::function_name!(true);
        self.project_logger.log_error(&error_str);
        self.slack_messenger.retry_send_message(calling_func, &error_str, false);
        panic!("{}", &error_str);
    }

    pub fn get_default_browser(&mut self) -> ChromeCapabilities {
        let mut browser = ChromeCapabilities::new();
        if let Err(e) = browser.set_headless() {
            let error_str = format!("Unable to set headless for the chrome browser, {e}");
            self.project_logger.log_error(&error_str);
            panic!("{}", &error_str);
        };
        for arg in ["--disable-dev-shm-usage", "--disable-gpu", "--window-size=1920,1080", "disable-blink-features=AutomationControlled"].iter() {
            if let Err(e) = browser.add_chrome_arg(arg) {
                let error_str = format!("Unable to set the argument {arg}, {e}");
                self.project_logger.log_error(&error_str);
                panic!("{}", &error_str);
            };
        };
        self.browser = Some(browser.clone());
        browser
    }

    fn web_driver_path(&self) -> String {
        format!("{}{}", &Self::WEB_DRIVER_PROG, &self.web_driver_port.to_string())
    }

    pub fn set_web_driver(&mut self) {
        let server_url = self.web_driver_path();
        if self.browser.is_none() {
            self.get_default_browser();
        }
        match WebDriver::new_with_timeout(&server_url, &self.browser, Some(self.timeout)) {
            Ok(w_d) => self.web_driver = Some(w_d),
            Err(e) => {
                let error_str = format!("Unable to set the web driver. {e}");
                self.project_logger.log_error(&error_str);
                panic!("{}", &error_str);
            }
        }
    }

    pub fn restart_web_driver(&mut self) {
        if let Some(w_d) = &self.web_driver {
            match w_d.close() {
                Ok(()) => self.set_web_driver(),
                Err(e) => {
                    let error_str = format!("Unable to quit web driver. Please check and clear the process. {e}");
                    self.project_logger.log_error(&error_str);
                    panic!{"{}", &error_str};
                }
            }
        }
    }

    pub fn close_web_driver(mut self) {
        if let Some(w_d) = self.web_driver {
            match w_d.quit() {
                Ok(()) => {
                    let debug_str = format!("Web driver quitted.");
                    self.project_logger.log_debug(&debug_str);
                    self.web_driver = None;
                },
                Err(e) => {
                    let error_str = format!("Unable to quit web driver. Please check and clear the process. {e}");
                    self.project_logger.log_error(&error_str);
                    panic!{"{}", &error_str};
                }
            }
        }
    }

    fn get_request_simple(&mut self, url: Url) -> Result<Response> {
        match &self.client {
            Some(c) => c.get(url).send(),
            None => {
                self.get_default_blocking_client();
                self.get_request_simple(url)
            }
        }
    }

    fn get_request_from_builder(&mut self, request_builder: &RequestBuilder, url: Url) -> Result<Response> {
        match request_builder.try_clone() {
            Some(r_b) => r_b.send(),
            None => {
                let warn_str = "Unable to clone the request_builder. Request by simple request builder".to_owned();
                self.project_logger.log_warn(&warn_str);
                self.get_request_simple(url)
            }
        }
    }

    pub fn null_check_func(_response: &Response) -> ResponseCheckResult {
        ResponseCheckResult::Ok
    }

    pub fn retry_request_simple(&mut self, url: &Url, check_func: fn(&Response) -> ResponseCheckResult) -> Option<String> {
        let mut counter = 0;
        while counter < self.num_retry {
            match self.get_request_simple(url.clone()) {
                Ok(r) => {
                    match check_func(&r) {
                        ResponseCheckResult::Ok => match r.text() {
                            Ok(s) => {
                                let debug_str = format!("Request {} loaded.", url.as_str());
                                self.project_logger.log_debug(&debug_str);
                                return Some(s)
                            },
                            Err(e) => {
                                counter += 1;
                                let warn_str = format!("Unable to decode the response text for {} after trial . {e}", url.as_str());
                                self.project_logger.log_warn(&warn_str);
                                time_operation::sleep(self.retry_sleep);
                            }
                        },
                        ResponseCheckResult::ErrContinue(e) => {
                            counter += 1;
                            let warn_str = format!("Checking of the response failed for {} after trial {counter}. {e}", url.as_str());
                            self.project_logger.log_warn(&warn_str);
                            time_operation::sleep(self.retry_sleep);
                        },
                        ResponseCheckResult::ErrTerminate(e) => {
                            let error_str = format!("Terminate to load the page {}. {e}", url.as_str());
                            self.project_logger.log_error(&error_str);
                            counter = self.num_retry;
                        }
                    }
                },
                Err(e) => {
                    counter += 1;
                    let warn_str = format!("Unable to load the page {} after trial {counter}. {e}", url.as_str());
                    self.project_logger.log_warn(&warn_str);
                    time_operation::sleep(self.retry_sleep);
                }
            }
        }
        let error_str = format!("Fail to load the page {}.", url.as_str());
        self.project_logger.log_error(&error_str);
        self.fail_list.push(url.as_str().to_owned());
        None
    }

    pub fn retry_request_from_builder(&mut self, request_builder: &RequestBuilder, url: &'a Url, check_func: fn(&Response) -> ResponseCheckResult) -> Option<String> {
        let mut counter = 0;
        while counter < self.num_retry {
            match self.get_request_from_builder(request_builder, url.clone()) {
                Ok(r) => {
                    match check_func(&r) {
                        ResponseCheckResult::Ok => {
                            match r.text() {
                                Ok(s) => {
                                    let debug_str = format!("Request {} loaded.", url.as_str());
                                    self.project_logger.log_debug(&debug_str);
                                    return Some(s)
                                },
                                Err(e) => {
                                    counter += 1;
                                    let warn_str = format!("Unable to decode the response text. {e}");
                                    self.project_logger.log_warn(&warn_str);
                                    time_operation::sleep(self.retry_sleep)
                                }
                            }
                        },
                        ResponseCheckResult::ErrContinue(e) => {
                            counter += 1;
                            let warn_str = format!("Checking of the response failed for {} after trial {counter}. {e}", url.as_str());
                            self.project_logger.log_warn(&warn_str);
                            time_operation::sleep(self.retry_sleep);
                        },
                        ResponseCheckResult::ErrTerminate(e) => {
                            let error_str = format!("Terminate to load the page {}. {e}", url.as_str());
                            self.project_logger.log_error(&error_str);
                            counter = self.num_retry;
                        }
                    }
                },
                Err(e) => {
                    counter += 1;
                    let warn_str = format!("Unable to load the page {} after trial {counter}. {e}", url.as_str());
                    self.project_logger.log_warn(&warn_str);
                    time_operation::sleep(self.retry_sleep)
                }
            }
        }
        let error_str = format!("Fail to load the page {}.", url.as_str());
        self.project_logger.log_error(&error_str);
        self.fail_list.push(url.as_str().to_owned());
        None
    }

    pub fn save_request_content(&self, folder_path: &PathBuf, file: &String, content: Option<String>) {
        if let Some(c) = content {
            self.file_io.write_string_to_file(folder_path, file, &c);
        }   
    }

    pub fn multiple_requests(&mut self, url_file_list: &'a Vec<UrlFile>, folder_path: &PathBuf, check_func: fn(&Response) -> ResponseCheckResult, calling_func: &str, log_only: bool) {
        self.fail_list.clear();
        for url_file in tqdm::tqdm(url_file_list.iter()) {
            let content = self.retry_request_simple(&url_file.url, check_func);
            self.save_request_content(folder_path, &url_file.file_name, content);
            time_operation::random_sleep(self.consecutive_sleep);
        };
        if !self.fail_list.is_empty() {
            let fail_url_list = format!("The following urls were not loaded successfully:\n\n {}", self.fail_list.join("\n"));
            self.project_logger.log_error(&fail_url_list);
            let fail_url_message = format!("The urls starting with {:?} has {} out of {} fail urls.", self.fail_list.first(), self.fail_list.len(), url_file_list.len());
            self.slack_messenger.retry_send_message(calling_func, &fail_url_message, log_only);
            self.fail_list.clear();
        }
    }

    fn url_from_google_sheet_link(google_sheet_link: &str) -> Url {
        let csv_link = google_sheet_link.replace(Self::GOOGLE_SHEET_REPLACE_TOKEN.0, Self::GOOGLE_SHEET_REPLACE_TOKEN.1);
        match Url::parse(&csv_link) {
            Ok(u) => u,
            Err(e) => panic!("Unable to parse the google sheet link {google_sheet_link}. {e}")
        }
    }

    pub fn retry_download_google_sheet(&mut self, google_sheet_link: &str) -> Option<String> {
        let google_sheet_url = Self::url_from_google_sheet_link(google_sheet_link);
        self.retry_request_simple(&google_sheet_url, Self::null_check_func)
    }

    pub fn browse_request(&mut self, url: &Url, browse_action: fn(&WebDriver) -> WebDriverResult<()>) -> WebDriverResult<String>{
        match &self.web_driver {
            Some(w_d) => {
                w_d.get(url.clone())?;
                browse_action(w_d)?;
                w_d.page_source()
            },
            None => {
                self.set_web_driver();
                self.browse_request(url, browse_action)
            }
        }
    }

    pub fn retry_browse_request(&mut self, url: &Url, browse_action: fn(&WebDriver) -> WebDriverResult<()>, check_func: fn(&String) -> ResponseCheckResult) -> Option<String>{
        let mut counter = 0;
        while counter < self.num_retry {
            match self.browse_request(url, browse_action) {
                Ok(r) => {
                    match check_func(&r) {
                        ResponseCheckResult::Ok => {
                            let debug_str = format!("Request {} browsed.", url.as_str());
                            self.project_logger.log_debug(&debug_str);
                            return Some(r)
                        },
                        ResponseCheckResult::ErrContinue(e) => {
                            counter += 1;
                            let warn_str = format!("Checking for the response failed for {} after trial {counter}. {e}", url.as_str());
                            self.project_logger.log_warn(&warn_str);
                            time_operation::sleep(self.retry_sleep);
                        },
                        ResponseCheckResult::ErrTerminate(e) => {
                            let error_str = format!("Terminate to load the page {}. {e}", url.as_str());
                            self.project_logger.log_error(&error_str);
                            counter = self.num_retry;
                        }
                    };
                },
                Err(e) => {
                    counter += 1;
                    let warn_str = format!("Unable to browse the page {} after trial {counter}. {e}", url.as_str());
                    self.project_logger.log_warn(&warn_str);
                    time_operation::sleep(self.retry_sleep);
                }
            }
        };
        let error_str = format!("Fail to browse the page {}.", url.as_str());
        self.project_logger.log_error(&error_str);
        self.fail_list.push(url.as_str().to_owned());
        None
    }

    pub fn multiple_browse_requests(&mut self, url_file_list: &'a Vec<UrlFile>, folder_path: &PathBuf, browse_action: fn(&WebDriver) -> WebDriverResult<()>, check_func: fn(&String) -> ResponseCheckResult, restart_web_driver: bool, calling_func: &str, log_only: bool) {
        self.fail_list.clear();
        for url_file in tqdm::tqdm(url_file_list.iter()) {
            let content = self.retry_browse_request(&url_file.url, browse_action, check_func);
            self.save_request_content(folder_path, &url_file.file_name, content);
            time_operation::random_sleep(self.consecutive_sleep);
            if restart_web_driver {
                self.restart_web_driver();
            }
        };
        if !self.fail_list.is_empty() {
            let fail_url_list = format!("The following urls were not browsed successfully:\n\n {}", self.fail_list.join("\n"));
            self.project_logger.log_error(&fail_url_list);
            let fail_url_message = format!("The urls starting with {:?} has {} out of {} fail urls.", self.fail_list.first(), self.fail_list.len(), url_file_list.len());
            self.slack_messenger.retry_send_message(calling_func, &fail_url_message, log_only);
            self.fail_list.clear();
        }
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use serde::Deserialize;
    use std::path::Path;
    use std::env;
    use std::fs;
    use toml;
    use crate::utilities_function;
    use thirtyfour_sync::By;
    use thirtyfour_sync::prelude::ElementWaitable;


    #[derive(Deserialize)]
    struct ChannelID {
        channel_id: String,
    }

    fn load_channel_id(channel_config_path: &PathBuf, channel_config_file: &str) -> String {
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

    #[test]
    fn test_simple_scraping() {
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger();
        let api_key_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_messenger");
        let api_key_file = "messenger_api.toml";
        let channel_config_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_messenger");
        let channel_config_file = "messenger_channel_id.toml";
        let channel_id = load_channel_id(&channel_config_path, channel_config_file);
        let log_channel_id = channel_id.clone();
        let slack_messenger = SlackMessenger::new(
            &api_key_path,
            api_key_file,
            channel_id,
            log_channel_id,
            &project_logger,
        );
        let file_io = FileIO::new(&project_logger);
        let mut web_scraper = WebScraper::new(&project_logger, &slack_messenger, &file_io);
        let url = Url::parse("https://tfl.gov.uk/travel-information/timetables/").unwrap();
        let content = web_scraper.retry_request_simple(&url, WebScraper::null_check_func);
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io").to_path_buf();
        let file = "test_scrape.html".to_owned();
        web_scraper.save_request_content(&folder_path, &file, content);
        assert!(true);

    }

    #[test]
    fn test_multiple_requests() {
        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger();
        let api_key_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_messenger");
        let api_key_file = "messenger_api.toml";
        let channel_config_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_messenger");
        let channel_config_file = "messenger_channel_id.toml";
        let channel_id = load_channel_id(&channel_config_path, channel_config_file);
        let log_channel_id = channel_id.clone();
        let slack_messenger = SlackMessenger::new(
            &api_key_path,
            api_key_file,
            channel_id,
            log_channel_id,
            &project_logger,
        );
        let file_io = FileIO::new(&project_logger);
        let mut web_scraper = WebScraper::new(&project_logger, &slack_messenger, &file_io);
        let url_suffix = ["bakerloo", "central", "circle", "district", "jubilee"];
        let url = Url::parse("https://tfl.gov.uk/travel-information/timetables/").unwrap();
        let file = "test_scrape{index}.html".to_owned();
        let url_file_list = Vec::from_iter(url_suffix.iter().enumerate().map(|(i, x)| UrlFile::new(url.join(&format!("{x}/")).unwrap(), file.replace("{index}", &i.to_string()))));
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io").to_path_buf();
        let calling_func = utilities_function::function_name!(true);
        web_scraper.multiple_requests(&url_file_list, &folder_path, WebScraper::null_check_func, calling_func, true);
        assert!(true);
    }

    #[test]
    fn test_simple_browsing() {
        fn browse_action_wait(web_driver: &WebDriver) -> WebDriverResult<()> {
            let element_css= "div#matchList.matchList";
            web_driver.set_implicit_wait_timeout(Duration::from_secs(5))?;
            web_driver.find_element(By::Css(element_css))?.wait_until().displayed()?;
            Ok(())
        }

        fn check_func_ok(_response: &String) -> ResponseCheckResult {
            ResponseCheckResult::Ok
        }

        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger();
        let api_key_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_messenger");
        let api_key_file = "messenger_api.toml";
        let channel_config_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_messenger");
        let channel_config_file = "messenger_channel_id.toml";
        let channel_id = load_channel_id(&channel_config_path, channel_config_file);
        let log_channel_id = channel_id.clone();
        let slack_messenger = SlackMessenger::new(
            &api_key_path,
            api_key_file,
            channel_id,
            log_channel_id,
            &project_logger,
        );
        let file_io = FileIO::new(&project_logger);
        let mut web_scraper = WebScraper::new(&project_logger, &slack_messenger, &file_io);
        let url = Url::parse("https://www.nowgoal.com/").unwrap();
        let content = web_scraper.retry_browse_request(&url, browse_action_wait, check_func_ok);
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io").to_path_buf();
        let file = "test_browse.html".to_owned();
        web_scraper.save_request_content(&folder_path, &file, content);
        web_scraper.close_web_driver();
        assert!(true);

    }

    #[test]
    fn test_multiple_browsing() {
        fn browse_action_wait(web_driver: &WebDriver) -> WebDriverResult<()> {
            let element_css= "div#matchList.matchList";
            web_driver.set_implicit_wait_timeout(Duration::from_secs(5))?;
            web_driver.find_element(By::Css(element_css))?.wait_until().displayed()?;
            Ok(())
        }

        fn check_func_ok(_response: &String) -> ResponseCheckResult {
            ResponseCheckResult::Ok
        }

        let logger_name = "test_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger();
        let api_key_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_messenger");
        let api_key_file = "messenger_api.toml";
        let channel_config_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_messenger");
        let channel_config_file = "messenger_channel_id.toml";
        let channel_id = load_channel_id(&channel_config_path, channel_config_file);
        let log_channel_id = channel_id.clone();
        let slack_messenger = SlackMessenger::new(
            &api_key_path,
            api_key_file,
            channel_id,
            log_channel_id,
            &project_logger,
        );
        let file_io = FileIO::new(&project_logger);
        let mut web_scraper = WebScraper::new(&project_logger, &slack_messenger, &file_io);
        let url_suffix = ["football/live", "football/results", "football/schedule"];
        let url = Url::parse("https://www.nowgoal.com/").unwrap();
        let file = "test_browse{index}.html".to_owned();
        let url_file_list = Vec::from_iter(url_suffix.iter().enumerate().map(|(i, x)| UrlFile::new(url.join(x).unwrap(), file.replace("{index}", &i.to_string()))));
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io").to_path_buf();
        let calling_func = utilities_function::function_name!(true);
        web_scraper.multiple_browse_requests(&url_file_list, &folder_path, browse_action_wait, check_func_ok,  true, calling_func, true);
        web_scraper.close_web_driver();
        assert!(true);

    }

}