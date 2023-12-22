use polars::io::SerReader;
use polars::prelude::{CsvReader, DataFrame};
use reqwest::blocking::{Client, RequestBuilder, Response};
use reqwest::{Result, Url};
use std::io::Cursor;
use std::path::Path;
use std::process::{Child, Command};
use std::time::Duration;
use thirtyfour_sync::error::WebDriverResult;
use thirtyfour_sync::{ChromeCapabilities, WebDriver, WebDriverCommands};
use tqdm;

use super::data_struct::{BrowseSetting, RequestSetting, ResponseCheckResult, UrlFile};
use crate::file_io::FileIO;
use crate::logger::ProjectLogger;
use crate::slack_messenger::SlackMessenger;
use crate::{function_name, time_operation, utilities_function};

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
    client: Option<Client>,
    web_driver: Option<WebDriver>,
    browser: Option<ChromeCapabilities>,
    chrome_process: Option<Child>,
}

impl<'a> WebScraper<'a> {
    const NUM_RETRY: u32 = 3;
    const RETRY_SLEEP: Duration = Duration::from_secs(10);
    const CONSECUTIVE_SLEEP: (Duration, Duration) =
        (Duration::from_secs(0), Duration::from_secs(30));
    const TIMEOUT: Duration = Duration::from_secs(120);
    const GOOGLE_SHEET_URL: &str = "https://docs.google.com/spreadsheets/d/";
    const GOOGLE_SHEET_REPLACE_TOKEN: (&str, &str) = ("edit#gid=", "export?format=csv&gid=");
    const WEB_DRIVER_PORT: u32 = 4444;
    const WEB_DRIVER_PROG: &str = "http://localhost:";
    const CHROME_PROCESS: &str = "chromedriver";

    pub fn new(
        project_logger: &'a ProjectLogger,
        slack_messenger: &'a SlackMessenger,
        file_io: &'a FileIO,
    ) -> Self {
        Self {
            project_logger,
            slack_messenger,
            file_io,
            num_retry: Self::NUM_RETRY,
            retry_sleep: Self::RETRY_SLEEP,
            consecutive_sleep: Self::CONSECUTIVE_SLEEP,
            timeout: Self::TIMEOUT,
            web_driver_port: Self::WEB_DRIVER_PORT,
            client: None,
            web_driver: None,
            browser: None,
            chrome_process: None,
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

    pub fn set_browser(&mut self, browser: ChromeCapabilities) {
        self.browser = Some(browser);
    }

    pub fn get_default_blocking_client(&mut self) -> Client {
        let mut counter = 0;
        while counter < self.num_retry {
            match Client::builder().timeout(self.timeout).build() {
                Ok(c) => {
                    self.client = Some(c.clone());
                    return c;
                }
                Err(e) => {
                    counter += 1;
                    let warn_str =
                        format!("Unable to build connection client after trial {counter}. {e}");
                    self.project_logger.log_warn(&warn_str);
                }
            };
        }
        let error_str = "Fail to build connection client".to_string();
        let calling_func = utilities_function::function_name!(true);
        self.project_logger.log_error(&error_str);
        self.slack_messenger
            .retry_send_message(calling_func, &error_str, false);
        panic!("{}", &error_str);
    }

    pub fn get_default_browser(&mut self) -> ChromeCapabilities {
        let mut browser = ChromeCapabilities::new();
        if let Err(e) = browser.set_headless() {
            let error_str = format!("Unable to set headless for the chrome browser, {e}");
            self.project_logger.log_error(&error_str);
            panic!("{}", &error_str);
        };
        for arg in [
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--window-size=1920,1080",
            "disable-blink-features=AutomationControlled",
        ]
        .iter()
        {
            if let Err(e) = browser.add_chrome_arg(arg) {
                let error_str = format!("Unable to set the argument {arg}, {e}");
                self.project_logger.log_error(&error_str);
                panic!("{}", &error_str);
            };
        }
        self.browser = Some(browser.clone());
        browser
    }

    pub fn turn_on_chrome_process(&mut self) {
        if self.chrome_process.is_none() {
            let web_driver_port = format!("--port={}", self.web_driver_port);
            match Command::new(Self::CHROME_PROCESS)
                .arg(web_driver_port)
                .spawn()
            {
                Ok(c) => {
                    self.chrome_process = Some(c);
                }
                Err(e) => {
                    let error_str = format!("Unable to start chromedriver. {e}");
                    self.project_logger.log_error(&error_str);
                    panic!("{}", &error_str);
                }
            }
        }
    }

    pub fn kill_chrome_process(&mut self) {
        let chrome_process = self.chrome_process.take();
        if let Some(mut c) = chrome_process {
            match c.kill() {
                Ok(()) => {
                    let debug_str = format!("Chromedriver at port {} killed", self.web_driver_port);
                    self.project_logger.log_debug(&debug_str);
                    self.chrome_process = None;
                }
                Err(e) => {
                    let error_str = format!(
                        "Unable to kill chromedriver at port {}. {e}",
                        self.web_driver_port
                    );
                    self.project_logger.log_error(&error_str);
                    panic!("{}", &error_str);
                }
            }
        }
    }

    fn web_driver_path(&self) -> String {
        format!("{}{}", &Self::WEB_DRIVER_PROG, self.web_driver_port)
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

    pub fn get_web_driver(&mut self) -> Option<&mut WebDriver> {
        self.web_driver.as_mut()
    }

    pub fn restart_web_driver(&mut self) {
        if let Some(w_d) = &self.web_driver {
            match w_d.close() {
                Ok(()) => self.set_web_driver(),
                Err(e) => {
                    let error_str = format!(
                        "Unable to quit web driver. Please check and clear the process. {e}"
                    );
                    self.project_logger.log_error(&error_str);
                    panic! {"{}", &error_str};
                }
            }
        }
    }

    pub fn close_web_driver(&mut self) {
        let web_driver = self.web_driver.take();
        if let Some(w_d) = web_driver {
            match w_d.quit() {
                Ok(()) => {
                    let debug_str = "Web driver quitted.".to_string();
                    self.project_logger.log_debug(&debug_str);
                }
                Err(e) => {
                    let error_str = format!(
                        "Unable to quit web driver. Please check and clear the process. {e}"
                    );
                    self.project_logger.log_error(&error_str);
                    panic! {"{}", &error_str};
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

    fn get_request_from_builder(
        &mut self,
        request_builder: &RequestBuilder,
        url: Url,
    ) -> Result<Response> {
        match request_builder.try_clone() {
            Some(r_b) => r_b.send(),
            None => {
                let warn_str =
                    "Unable to clone the request_builder. Request by simple request builder"
                        .to_owned();
                self.project_logger.log_warn(&warn_str);
                self.get_request_simple(url)
            }
        }
    }

    pub fn null_check_func(response: &str) -> ResponseCheckResult {
        ResponseCheckResult::Ok(response.to_string())
    }

    pub fn retry_request_simple(
        &mut self,
        url: &Url,
        check_func: fn(&str) -> ResponseCheckResult,
    ) -> ResponseCheckResult {
        let mut counter = 0;
        while counter < self.num_retry {
            match self.get_request_simple(url.clone()) {
                Ok(response) => {
                    if response.status().is_success() || response.status().is_redirection() {
                        match response.text() {
                            Ok(response_text) => match check_func(&response_text) {
                                ResponseCheckResult::Ok(response_text) => {
                                    let debug_str = format!("Request {} loaded.", url.as_str());
                                    self.project_logger.log_debug(&debug_str);
                                    return ResponseCheckResult::Ok(response_text);
                                }
                                ResponseCheckResult::ErrContinue(e) => {
                                    let warn_str = format!(
                                        "Checking of the response failed for {}. {e}",
                                        url.as_str()
                                    );
                                    self.project_logger.log_warn(&warn_str);
                                    counter += 1
                                }
                                ResponseCheckResult::ErrTerminate(e) => {
                                    let warn_str =
                                        format!("Terminate to load the page {}. {e}", url.as_str());
                                    self.project_logger.log_warn(&warn_str);
                                    return ResponseCheckResult::ErrTerminate(e);
                                }
                            },
                            Err(e) => {
                                let warn_str = format!("Unable to decode the response text. {e}");
                                self.project_logger.log_warn(&warn_str);
                                counter += 1
                            }
                        }
                    } else if response.status().is_server_error() {
                        let warn_str = format!(
                            "Fail in loading the page {}. Server return status code {}",
                            url.as_str(),
                            response.status().as_str()
                        );
                        self.project_logger.log_warn(&warn_str);
                        counter += 1
                    } else {
                        let warn_str = format!(
                            "Terminate to load the page {}. Server return status code {}",
                            url.as_str(),
                            response.status().as_str()
                        );
                        self.project_logger.log_warn(&warn_str);
                        counter += 1
                    }
                }
                Err(e) => {
                    let warn_str = format!("Unable to load the page {}. {e}", url.as_str());
                    self.project_logger.log_warn(&warn_str);
                    counter += 1
                }
            }
        }
        let error_str = format!("Fail to load the page {}.", url.as_str());
        self.project_logger.log_error(&error_str);
        ResponseCheckResult::ErrTerminate(error_str)
    }

    pub fn retry_request_from_builder(
        &mut self,
        request_builder: &RequestBuilder,
        url: &'a Url,
        check_func: fn(&str) -> ResponseCheckResult,
    ) -> ResponseCheckResult {
        let mut counter = 0;
        while counter < self.num_retry {
            match self.get_request_from_builder(request_builder, url.clone()) {
                Ok(response) => {
                    if response.status().is_success() || response.status().is_redirection() {
                        match response.text() {
                            Ok(response_text) => match check_func(&response_text) {
                                ResponseCheckResult::Ok(response_text) => {
                                    let debug_str = format!("Request {} loaded.", url.as_str());
                                    self.project_logger.log_debug(&debug_str);
                                    return ResponseCheckResult::Ok(response_text);
                                }
                                ResponseCheckResult::ErrContinue(e) => {
                                    let warn_str = format!(
                                        "Checking of the response failed for {}. {e}",
                                        url.as_str()
                                    );
                                    self.project_logger.log_warn(&warn_str);
                                    counter += 1;
                                    time_operation::sleep(self.retry_sleep);
                                }
                                ResponseCheckResult::ErrTerminate(e) => {
                                    let warn_str =
                                        format!("Terminate to load the page {}. {e}", url.as_str());
                                    self.project_logger.log_warn(&warn_str);
                                    return ResponseCheckResult::ErrTerminate(e);
                                }
                            },
                            Err(e) => {
                                let warn_str = format!("Unable to decode the response text. {e}");
                                self.project_logger.log_warn(&warn_str);
                                counter += 1
                            }
                        }
                    } else if response.status().is_server_error() {
                        let warn_str = format!(
                            "Fail in loading the page {}. Server return status code {}",
                            url.as_str(),
                            response.status().as_str()
                        );
                        self.project_logger.log_warn(&warn_str);
                        counter += 1;
                        time_operation::sleep(self.retry_sleep);
                    } else {
                        let warn_str = format!(
                            "Terminate to load the page {}. Server return status code {}",
                            url.as_str(),
                            response.status().as_str()
                        );
                        self.project_logger.log_warn(&warn_str);
                        counter += 1;
                        time_operation::sleep(self.retry_sleep);
                    }
                }
                Err(e) => {
                    let warn_str = format!("Unable to load the page {}. {e}", url.as_str());
                    self.project_logger.log_warn(&warn_str);
                    counter += 1;
                    time_operation::sleep(self.retry_sleep);
                }
            }
        }
        let error_str = format!("Fail to load the page {}.", url.as_str());
        self.project_logger.log_error(&error_str);
        ResponseCheckResult::ErrTerminate(error_str)
    }

    pub fn save_request_content(&self, folder_path: &Path, file: &str, content: &str) {
        self.file_io
            .write_string_to_file(folder_path, file, content)
            .unwrap_or_else(|e| {
                let function_name = function_name!(true);
                let error_msg = format!(
                    "Unable to save file {file} in {}. {e}",
                    folder_path.display()
                );
                self.slack_messenger
                    .retry_send_message(function_name, &error_msg, true);
                panic!("{error_msg}")
            });
    }

    pub fn multiple_requests(
        &mut self,
        url_file_list: &'a Vec<UrlFile>,
        folder_path: &Path,
        check_func: fn(&str) -> ResponseCheckResult,
        request_setting: RequestSetting,
    ) -> Vec<UrlFile> {
        let mut fail_list = Vec::new();
        for url_file in tqdm::tqdm(url_file_list.iter()) {
            if let ResponseCheckResult::Ok(content) =
                self.retry_request_simple(&url_file.url, check_func)
            {
                self.save_request_content(folder_path, &url_file.file_name, &content);
            } else {
                fail_list.push(url_file.clone())
            }
            time_operation::random_sleep(self.consecutive_sleep);
        }
        if !fail_list.is_empty() {
            let fail_url_list = format!(
                "The following urls were not loaded successfully:\n\n {}",
                fail_list
                    .iter()
                    .map(|x| x.url.as_str())
                    .collect::<Vec<&str>>()
                    .join("\n")
            );
            self.project_logger.log_error(&fail_url_list);
            let fail_url_message = format!(
                "The urls starting with {:?} has {} out of {} fail urls.",
                fail_list[0].url.as_str(),
                fail_list.len(),
                url_file_list.len()
            );
            self.slack_messenger.retry_send_message(
                request_setting.calling_func,
                &fail_url_message,
                request_setting.log_only,
            );
        }
        fail_list
    }

    pub fn multiple_requests_with_builder(
        &mut self,
        url_file_list: &'a Vec<UrlFile>,
        request_builder_list: &[RequestBuilder],
        folder_path: &Path,
        check_func: fn(&str) -> ResponseCheckResult,
        request_setting: RequestSetting,
    ) -> Vec<UrlFile> {
        let mut fail_list = Vec::new();
        for (url_file, request_builder) in
            tqdm::tqdm(url_file_list.iter().zip(request_builder_list.iter()))
        {
            if let ResponseCheckResult::Ok(content) =
                self.retry_request_from_builder(request_builder, &url_file.url, check_func)
            {
                self.save_request_content(folder_path, &url_file.file_name, &content);
            } else {
                fail_list.push(url_file.clone())
            }
            time_operation::random_sleep(self.consecutive_sleep);
        }
        if !fail_list.is_empty() {
            let fail_url_list = format!(
                "The following urls were not loaded successfully:\n\n {}",
                fail_list
                    .iter()
                    .map(|x| x.url.as_str())
                    .collect::<Vec<&str>>()
                    .join("\n")
            );
            self.project_logger.log_error(&fail_url_list);
            let fail_url_message = format!(
                "The urls starting with {:?} has {} out of {} fail urls.",
                fail_list.first(),
                fail_list.len(),
                url_file_list.len()
            );
            self.slack_messenger.retry_send_message(
                request_setting.calling_func,
                &fail_url_message,
                request_setting.log_only,
            );
        }
        fail_list
    }

    fn url_from_google_sheet_link(google_sheet_key: &str) -> Url {
        let csv_link = format!(
            "{}{}",
            Self::GOOGLE_SHEET_URL,
            google_sheet_key.replace(
                Self::GOOGLE_SHEET_REPLACE_TOKEN.0,
                Self::GOOGLE_SHEET_REPLACE_TOKEN.1,
            )
        );
        match Url::parse(&csv_link) {
            Ok(u) => u,
            Err(e) => panic!("Unable to parse the google sheet link {google_sheet_key}. {e}"),
        }
    }

    pub fn retry_download_google_sheet(&mut self, google_sheet_link: &str) -> ResponseCheckResult {
        let google_sheet_url = Self::url_from_google_sheet_link(google_sheet_link);
        self.retry_request_simple(&google_sheet_url, Self::null_check_func)
    }

    pub fn convert_google_sheet_string_to_data_frame(google_sheet_csv: &str) -> Option<DataFrame> {
        let cursor = Cursor::new(google_sheet_csv);
        CsvReader::new(cursor).has_header(true).finish().ok()
    }

    pub fn browse_page(&mut self, url: &Url) -> WebDriverResult<()> {
        match &mut self.web_driver {
            Some(w_d) => w_d.get(url.clone()),
            None => {
                self.set_web_driver();
                self.browse_page(url)
            }
        }
    }

    pub fn browse_request(
        &mut self,
        url: &Url,
        browse_action: fn(&mut WebDriver) -> WebDriverResult<()>,
    ) -> WebDriverResult<String> {
        match &mut self.web_driver {
            Some(w_d) => {
                w_d.get(url.clone())?;
                browse_action(w_d)?;
                w_d.page_source()
            }
            None => {
                self.set_web_driver();
                self.browse_request(url, browse_action)
            }
        }
    }

    pub fn retry_browse_request(
        &mut self,
        url: &Url,
        browse_action: fn(&mut WebDriver) -> WebDriverResult<()>,
        check_func: fn(&str) -> ResponseCheckResult,
    ) -> ResponseCheckResult {
        let mut counter = 0;
        while counter < self.num_retry {
            match self.browse_request(url, browse_action) {
                Ok(r) => {
                    match check_func(&r) {
                        ResponseCheckResult::Ok(r) => {
                            let debug_str = format!("Request {} browsed.", url.as_str());
                            self.project_logger.log_debug(&debug_str);
                            return ResponseCheckResult::Ok(r);
                        }
                        ResponseCheckResult::ErrContinue(e) => {
                            counter += 1;
                            let warn_str = format!("Checking for the response failed for {} after trial {counter}. {e}", url.as_str());
                            self.project_logger.log_warn(&warn_str);
                            time_operation::sleep(self.retry_sleep);
                        }
                        ResponseCheckResult::ErrTerminate(e) => {
                            let error_str =
                                format!("Terminate to load the page {}. {e}", url.as_str());
                            self.project_logger.log_error(&error_str);
                            return ResponseCheckResult::ErrTerminate(e);
                        }
                    };
                }
                Err(e) => {
                    counter += 1;
                    let warn_str = format!(
                        "Unable to browse the page {} after trial {counter}. {e}",
                        url.as_str()
                    );
                    self.project_logger.log_warn(&warn_str);
                    time_operation::sleep(self.retry_sleep);
                }
            }
        }
        let error_str = format!("Fail to browse the page {}.", url.as_str());
        self.project_logger.log_error(&error_str);
        ResponseCheckResult::ErrTerminate(error_str)
    }

    pub fn multiple_browse_requests(
        &mut self,
        url_file_list: &'a Vec<UrlFile>,
        folder_path: &Path,
        browse_action: fn(&mut WebDriver) -> WebDriverResult<()>,
        check_func: fn(&str) -> ResponseCheckResult,
        browse_setting: BrowseSetting,
    ) -> Vec<UrlFile> {
        let mut fail_list = Vec::new();
        for url_file in tqdm::tqdm(url_file_list.iter()) {
            if let ResponseCheckResult::Ok(content) =
                self.retry_browse_request(&url_file.url, browse_action, check_func)
            {
                self.save_request_content(folder_path, &url_file.file_name, &content);
            } else {
                fail_list.push(url_file.clone())
            }
            time_operation::random_sleep(self.consecutive_sleep);
            if browse_setting.restart_web_driver {
                self.restart_web_driver();
            }
        }
        if !fail_list.is_empty() {
            let fail_url_list = format!(
                "The following urls were not browsed successfully:\n\n {}",
                fail_list
                    .iter()
                    .map(|x| x.url.as_str())
                    .collect::<Vec<&str>>()
                    .join("\n")
            );
            self.project_logger.log_error(&fail_url_list);
            let fail_url_message = format!(
                "The urls starting with {:?} has {} out of {} fail urls.",
                fail_list.first(),
                fail_list.len(),
                url_file_list.len()
            );
            self.slack_messenger.retry_send_message(
                browse_setting.calling_func,
                &fail_url_message,
                browse_setting.log_only,
            );
        }
        fail_list
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::utilities_function;
    use log::LevelFilter;
    use serde::Deserialize;
    use std::env;
    use std::fs;
    use std::path::Path;
    use thirtyfour_sync::prelude::ElementWaitable;
    use thirtyfour_sync::By;
    use toml;

    #[derive(Deserialize)]
    struct ChannelID {
        channel_id: String,
    }

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

    #[test]
    fn test_simple_scraping() {
        let logger_name = "test_simple_scraping";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let channel_config_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_utilities");
        let channel_config_file = "messenger_channel_id.toml";
        let channel_id = load_channel_id(&channel_config_path, channel_config_file);
        let log_channel_id = channel_id.clone();
        let slack_messenger = SlackMessenger::new(&channel_id, &log_channel_id, &project_logger);
        let file_io = FileIO::new(&project_logger);
        let mut web_scraper = WebScraper::new(&project_logger, &slack_messenger, &file_io);
        let url = Url::parse("https://tfl.gov.uk/travel-information/timetables/").unwrap();
        let content = web_scraper.retry_request_simple(&url, WebScraper::null_check_func);
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let file = "test_scrape.html";
        web_scraper.save_request_content(&folder_path, file, &content.get_content().unwrap());
    }

    #[test]
    fn test_download_google_sheet() {
        let logger_name = "test_download_google_sheet";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let channel_config_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_utilities");
        let channel_config_file = "messenger_channel_id.toml";
        let channel_id = load_channel_id(&channel_config_path, channel_config_file);
        let log_channel_id = channel_id.clone();
        let slack_messenger = SlackMessenger::new(&channel_id, &log_channel_id, &project_logger);
        let file_io = FileIO::new(&project_logger);
        let mut web_scraper = WebScraper::new(&project_logger, &slack_messenger, &file_io);
        let url = "14Ep-CmoqWxrMU8HshxthRcdRW8IsXvh3n2-ZHVCzqzQ/edit#gid=1855920257";
        let content = web_scraper.retry_download_google_sheet(url);
        let mut data =
            WebScraper::convert_google_sheet_string_to_data_frame(&content.get_content().unwrap())
                .unwrap();
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let file = "test_google_sheet.parquet";
        web_scraper
            .file_io
            .write_parquet_file(&folder_path, file, &mut data)
            .unwrap();
    }

    #[test]
    fn test_multiple_requests() {
        let logger_name = "test_multiple_requests";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let channel_config_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_utilities");
        let channel_config_file = "messenger_channel_id.toml";
        let channel_id = load_channel_id(&channel_config_path, channel_config_file);
        let log_channel_id = channel_id.clone();
        let slack_messenger = SlackMessenger::new(&channel_id, &log_channel_id, &project_logger);
        let file_io = FileIO::new(&project_logger);
        let mut web_scraper = WebScraper::new(&project_logger, &slack_messenger, &file_io);
        let url_suffix = ["bakerloo", "central", "circle", "district", "jubilee"];
        let url = Url::parse("https://tfl.gov.uk/tube/timetable/").unwrap();
        let file = "test_scrape{index}.html".to_owned();
        let url_file_list = Vec::from_iter(url_suffix.iter().enumerate().map(|(i, x)| {
            UrlFile::new(
                url.join(&format!("{x}/")).unwrap(),
                file.replace("{index}", &i.to_string()),
            )
        }));
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let calling_func = utilities_function::function_name!(true);
        let request_setting = RequestSetting {
            calling_func,
            log_only: true,
            in_s3: false,
        };
        web_scraper.multiple_requests(
            &url_file_list,
            &folder_path,
            WebScraper::null_check_func,
            request_setting,
        );
    }

    const WAIT_TIME: Duration = Duration::from_secs(5);
    const ELEMENT_CSS: &str = "div#matchList.matchList";

    fn extra_action(web_driver: &mut WebDriver) -> WebDriverResult<()> {
        web_driver.set_implicit_wait_timeout(WAIT_TIME)?;
        web_driver
            .find_element(By::Css(ELEMENT_CSS))?
            .wait_until()
            .displayed()?;
        Ok(())
    }

    #[test]
    fn test_simple_browsing() {
        let logger_name = "test_simple_browsing";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let channel_config_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_utilities");
        let channel_config_file = "messenger_channel_id.toml";
        let channel_id = load_channel_id(&channel_config_path, channel_config_file);
        let log_channel_id = channel_id.clone();
        let slack_messenger = SlackMessenger::new(&channel_id, &log_channel_id, &project_logger);
        let file_io = FileIO::new(&project_logger);
        let mut web_scraper = WebScraper::new(&project_logger, &slack_messenger, &file_io);
        let browse_action = extra_action;
        let url = Url::parse("https://www.nowgoal.com/").unwrap();
        web_scraper.turn_on_chrome_process();
        let content =
            web_scraper.retry_browse_request(&url, browse_action, WebScraper::null_check_func);
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let file = "test_browse.html";
        web_scraper.save_request_content(&folder_path, file, &content.get_content().unwrap());
        web_scraper.close_web_driver();
        web_scraper.kill_chrome_process();
    }

    #[test]
    fn test_multiple_browsing() {
        let logger_name = "test_multiple_browsing";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let channel_config_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_utilities");
        let channel_config_file = "messenger_channel_id.toml";
        let channel_id = load_channel_id(&channel_config_path, channel_config_file);
        let log_channel_id = channel_id.clone();
        let slack_messenger = SlackMessenger::new(&channel_id, &log_channel_id, &project_logger);
        let file_io = FileIO::new(&project_logger);
        let browse_action = extra_action;
        let mut web_scraper = WebScraper::new(&project_logger, &slack_messenger, &file_io);
        let url_suffix = ["football/live", "football/results", "football/schedule"];
        let url = Url::parse("https://www.nowgoal.com/").unwrap();
        let file = "test_browse{index}.html";
        let url_file_list = Vec::from_iter(url_suffix.iter().enumerate().map(|(i, x)| {
            UrlFile::new(
                url.join(x).unwrap(),
                file.replace("{index}", &i.to_string()),
            )
        }));
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let calling_func = utilities_function::function_name!(true);
        let browse_setting = BrowseSetting {
            restart_web_driver: false,
            calling_func,
            log_only: true,
            in_s3: false,
        };
        web_scraper.turn_on_chrome_process();
        web_scraper.multiple_browse_requests(
            &url_file_list,
            &folder_path,
            browse_action,
            WebScraper::null_check_func,
            browse_setting,
        );
        web_scraper.close_web_driver();
        web_scraper.kill_chrome_process();
    }
}
