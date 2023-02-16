use futures::future;
use itertools::Itertools;
use reqwest::{Client, Proxy, RequestBuilder, Response, Url};
use sctys_proxy::ScraperProxy;
use std::future::Future;
use std::path::Path;
use std::process::{Child, Command};
use std::time::Duration;
use thirtyfour::error::WebDriverResult;
use thirtyfour::{ChromeCapabilities, WebDriver, CapabilitiesHelper};
use thirtyfour::Proxy as BrowserProxy;


use crate::file_io::FileIO;
use crate::logger::ProjectLogger;
use crate::slack_messenger::SlackMessenger;
use crate::utilities_function;
use super::data_struct::{UrlFile, RequestSetting, BrowseSetting, ResponseCheckResult};


#[derive(Debug)]
pub struct AsyncWebScraper<'a> {
    project_logger: &'a ProjectLogger,
    slack_messenger: &'a SlackMessenger<'a>,
    file_io: &'a FileIO<'a>,
    num_retry: u32,
    retry_sleep: Duration,
    consecutive_sleep: (Duration, Duration),
    timeout: Duration,
    web_driver_port: u32,
    chrome_process: Option<Child>,
}

impl<'a> AsyncWebScraper<'a> {
    const NUM_RETRY: u32 = 3;
    const RETRY_SLEEP: Duration = Duration::from_secs(10);
    const CONSECUTIVE_SLEEP: (Duration, Duration) =
        (Duration::from_secs(0), Duration::from_secs(30));
    const TIMEOUT: Duration = Duration::from_secs(120);
    const CHUNK_SIZE: usize = 100;
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

    pub fn get_default_client(&self, proxy: Proxy) -> Client {
        match Client::builder().proxy(proxy).timeout(self.timeout).build() {
            Ok(client) => client,
            Err(e) => {
                let error_str = format!("Fail to build connection client. {e}");
                let calling_func = utilities_function::function_name!(true);
                self.project_logger.log_error(&error_str);
                self.slack_messenger
                    .retry_send_message(calling_func, &error_str, false);
                panic!("{}", &error_str);
            }
        }
    }

    pub fn get_default_browser(&self) -> ChromeCapabilities {
        let mut browser = ChromeCapabilities::new();
        if let Err(e) = browser.set_headless() {
            let error_str = format!("Unable to set headless for the chrome browser, {e}");
            self.project_logger.log_error(&error_str);
            panic!("{}", &error_str);
        };
        if let Err(e) = browser.set_disable_dev_shm_usage() {
            let error_str = format!("Unable to set disable_dev_shm_usage for the chrome browser, {e}");
            self.project_logger.log_error(&error_str);
            panic!("{}", &error_str);
        };
        if let Err(e) = browser.set_disable_gpu() {
            let error_str = format!("Unable to set disable_gpu for the chrome browser, {e}");
            self.project_logger.log_error(&error_str);
            panic!("{}", &error_str);
        };
        for arg in [
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
        browser
    }

    pub fn set_browser_proxy(&self, browser: &ChromeCapabilities, browser_proxy: BrowserProxy) -> ChromeCapabilities {
        let mut browser_with_proxy = browser.clone();
        if let Err(e) = browser_with_proxy.set_proxy(browser_proxy) {
            let error_str = format!("Unable to set the proxy. {e}");
            self.project_logger.log_error(&error_str);
        }
        browser_with_proxy
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
        format!(
            "{}{}",
            &Self::WEB_DRIVER_PROG,
            &self.web_driver_port.to_string()
        )
    }

    pub async fn set_web_driver(&self, browser: ChromeCapabilities) -> WebDriver{
        let server_url = self.web_driver_path();
        match WebDriver::new(&server_url, browser).await {
            Ok(web_driver) => web_driver,
            Err(e) => {
                let error_str = format!("Unable to set the web driver. {e}");
                self.project_logger.log_error(&error_str);
                panic!("{}", &error_str);
            }
        }
    }

    pub async fn close_web_driver(&self, web_driver: WebDriver) {
        match web_driver.quit().await {
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

    pub fn null_check_func<T>(_response: &T) -> ResponseCheckResult {
        ResponseCheckResult::Ok
    }

    pub async fn request_with_proxy(
        &self,
        url: &Url,
        proxy: Proxy,
        request_builder_func: fn(Proxy, Url) -> RequestBuilder,
        check_func: fn(&Response) -> ResponseCheckResult,
    ) -> Option<String> {
        let request_builder = request_builder_func(proxy, url.clone());
        match request_builder.send().await {
            Ok(response) => match check_func(&response) {
                ResponseCheckResult::Ok => match response.text().await {
                    Ok(s) => {
                        let debug_str = format!("Request {} loaded.", url.as_str());
                        self.project_logger.log_debug(&debug_str);
                        Some(s)
                    }
                    Err(e) => {
                        let warn_str = format!("Unable to decode the response text. {e}");
                        self.project_logger.log_warn(&warn_str);
                        None
                    }
                },
                ResponseCheckResult::ErrContinue(e) => {
                    let warn_str = format!(
                        "Checking of the response failed for {}. {e}",
                        url.as_str()
                    );
                    self.project_logger.log_warn(&warn_str);
                    None
                }
                ResponseCheckResult::ErrTerminate(e) => {
                    let warn_str = format!("Terminate to load the page {}. {e}", url.as_str());
                    self.project_logger.log_warn(&warn_str);
                    None
                }
            },
            Err(e) => {
                let warn_str = format!(
                    "Unable to load the page {}. {e}",
                    url.as_str()
                );
                self.project_logger.log_warn(&warn_str);
                None
            }
        }
    }

    pub async fn save_request_content(&self, folder_path: &Path, file: &String, content: String) {
        self.file_io.async_write_string_to_file(folder_path, file, &content).await;
    }

    async fn request_and_save_content(
        &self, 
        url_file: &UrlFile,
        proxy: Proxy,
        request_builder_func: fn(Proxy, Url) -> RequestBuilder,
        folder_path: &Path,
        check_func: fn(&Response) -> ResponseCheckResult,
    ) -> Option<UrlFile> {
        if let Some(content) = self.request_with_proxy(&url_file.url, proxy, request_builder_func, check_func).await {
            self.save_request_content(folder_path, &url_file.file_name, content).await;
            None
        } else {
            Some(url_file.clone())
        }
    }

    pub async fn multiple_requests_with_proxy(
        &self,
        url_file_list: &Vec<UrlFile>,
        scraper_proxy: ScraperProxy,
        request_builder_func: fn(Proxy, Url) -> RequestBuilder,
        folder_path: &Path,
        check_func: fn(&Response) -> ResponseCheckResult,
        request_setting: RequestSetting<'a>,
    ) -> Vec<UrlFile> {
        let mut counter = 0;
        let mut pending_url_file_list = url_file_list.to_owned();
        while counter < self.num_retry && !pending_url_file_list.is_empty() {
            let mut proxy_list = scraper_proxy.generate_proxy().await;
            let mut fail_list = Vec::new();
            for chunk in pending_url_file_list.iter().chunks(Self::CHUNK_SIZE).into_iter() {
                let proxy_iter = ScraperProxy::sample_proxy(&mut proxy_list, Self::CHUNK_SIZE);
                let request_tasks = proxy_iter.zip(chunk).map(|(proxy_pair, url_file)| self.request_and_save_content(url_file, proxy_pair.proxy.clone(), request_builder_func, folder_path, check_func));
                let request_futures = future::join_all(request_tasks).await;
                fail_list.extend(request_futures.into_iter().flatten());
            };
            pending_url_file_list = fail_list;
            counter += 1;
        }
        if !pending_url_file_list.is_empty() {
            let fail_url_list = format!(
                "The following urls were not loaded successfully:\n\n {}",
                pending_url_file_list.iter().map(|x| x.url.as_str()).collect::<Vec<&str>>().join("\n")
            );
            self.project_logger.log_error(&fail_url_list);
            let fail_url_message = format!(
                "The urls starting with {:?} has {} out of {} fail urls.",
                pending_url_file_list.first(),
                pending_url_file_list.len(),
                url_file_list.len()
            );
            self.slack_messenger.retry_send_message(
                request_setting.calling_func,
                &fail_url_message,
                request_setting.log_only,
            );
        }
        pending_url_file_list
    } 

    pub async fn browse_page(web_driver: &mut WebDriver, url: &Url) -> WebDriverResult<()> {
        web_driver.goto(url.clone()).await
    } 
    

    pub async fn browse_request<F>(
        web_driver: &mut WebDriver,
        url: &Url,
        browse_action: &F,
    ) -> WebDriverResult<String> 
    where 
        F: for<'b> AsyncFn<&'b mut WebDriver, Output = WebDriverResult<()>>
    {
        Self::browse_page(web_driver, url).await?;
        browse_action(web_driver).await?;
        web_driver.source().await
    }

    pub async fn browse_request_with_proxy<F>(
        &self,
        url: &Url,
        proxy: BrowserProxy,
        browser: &ChromeCapabilities,
        browse_action: &F,
        check_func: fn(&String) -> ResponseCheckResult,
    ) -> Option<String> 
    where
        F: for<'b> AsyncFn<&'b mut WebDriver, Output = WebDriverResult<()>>
    {
        let browser_with_proxy = self.set_browser_proxy(browser, proxy);
        let mut web_driver = self.set_web_driver(browser_with_proxy).await;
        match Self::browse_request(&mut web_driver, url, browse_action).await {
            Ok(r) => {
                match check_func(&r) {
                    ResponseCheckResult::Ok => {
                        let debug_str = format!("Request {} browsed.", url.as_str());
                        self.project_logger.log_debug(&debug_str);
                        self.close_web_driver(web_driver).await;
                        Some(r)
                    }
                    ResponseCheckResult::ErrContinue(e) => {
                        let warn_str = format!("Checking for the response failed for {}. {e}", url.as_str());
                        self.project_logger.log_warn(&warn_str);
                        self.close_web_driver(web_driver).await;
                        None
                    }
                    ResponseCheckResult::ErrTerminate(e) => {
                        let error_str =
                            format!("Terminate to load the page {}. {e}", url.as_str());
                        self.project_logger.log_error(&error_str);
                        self.close_web_driver(web_driver).await;
                        None
                    }
                }
            }
            Err(e) => {
                let warn_str = format!(
                    "Unable to browse the page {}. {e}",
                    url.as_str()
                );
                self.project_logger.log_warn(&warn_str);
                self.close_web_driver(web_driver).await;
                None
            }
        }
    }

    async fn browse_and_save_content<F>(
        &self, 
        url_file: &UrlFile,
        proxy: BrowserProxy,
        browser: &ChromeCapabilities,
        folder_path: &Path,
        browse_action: &F,
        check_func: fn(&String) -> ResponseCheckResult,
    ) -> Option<UrlFile> 
    where
        F: for<'b> AsyncFn<&'b mut WebDriver, Output = WebDriverResult<()>>
    {
        if let Some(content) = self.browse_request_with_proxy(&url_file.url, proxy, browser, browse_action, check_func).await {
            self.save_request_content(folder_path, &url_file.file_name, content).await;
            None
        } else {
            Some(url_file.clone())
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn multiple_browse_requests<F>(
        &self,
        url_file_list: &Vec<UrlFile>,
        scraper_proxy: ScraperProxy,
        browser: &ChromeCapabilities,
        folder_path: &Path,
        browse_action: &F,
        check_func: fn(&String) -> ResponseCheckResult,
        browse_setting: BrowseSetting<'a>,
    ) -> Vec<UrlFile>
    where
        F: for<'b> AsyncFn<&'b mut WebDriver, Output = WebDriverResult<()>>
    {
        let mut counter = 0;
        let mut pending_url_file_list = url_file_list.to_owned();
        while counter < self.num_retry && !pending_url_file_list.is_empty() {
            let mut fail_list = Vec::new();
            let mut proxy_list = scraper_proxy.generate_proxy().await;
            for chunk in pending_url_file_list.iter().chunks(Self::CHUNK_SIZE).into_iter() {
                let proxy_iter = ScraperProxy::sample_proxy(&mut proxy_list, Self::CHUNK_SIZE);
                let request_tasks = proxy_iter.zip(chunk).map(|(proxy_pair, url_file)| self.browse_and_save_content(url_file, proxy_pair.browser_proxy.clone(), browser, folder_path, browse_action, check_func));
                let request_futures = future::join_all(request_tasks).await;
                fail_list.extend(request_futures.into_iter().flatten())
            };
            pending_url_file_list = fail_list;
            counter += 1;
        }
        if !pending_url_file_list.is_empty() {
            let fail_url_list = format!(
                "The following urls were not browsed successfully:\n\n {}",
                pending_url_file_list.iter().map(|x| x.url.as_str()).collect::<Vec<&str>>().join("\n")
            );
            self.project_logger.log_error(&fail_url_list);
            let fail_url_message = format!(
                "The urls starting with {:?} has {} out of {} fail urls.",
                pending_url_file_list.first(),
                pending_url_file_list.len(),
                url_file_list.len()
            );
            self.slack_messenger.retry_send_message(
                browse_setting.calling_func,
                &fail_url_message,
                browse_setting.log_only,
            );
        }
        pending_url_file_list
    }
}

pub trait AsyncFn<T>: Fn(T) -> <Self as AsyncFn<T>>::Fut {
    type Fut: Future<Output = <Self as AsyncFn<T>>::Output>;
    type Output;
}
impl<T, F, Fut> AsyncFn<T> for F where F: Fn(T) -> Fut, Fut: Future {
    type Fut = Fut;
    type Output = Fut::Output;
}


#[cfg(test)]
mod tests {

    use super::*;
    use log::LevelFilter;
    use sctys_proxy::ScraperProxy;
    use thirtyfour::By;
    use thirtyfour::prelude::ElementWaitable;
    use std::env;
    use std::fs;
    use serde::Deserialize;
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

    fn get_request_builder(proxy: Proxy, url: Url) -> RequestBuilder {
        Client::builder().proxy(proxy).timeout(Duration::from_secs(30)).build().unwrap().get(url)
    }

    #[tokio::test]
    async fn test_simple_scraping() {
        let logger_name = "test_simple_scraping";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Info);
        let channel_config_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_utilities");
        let channel_config_file = "messenger_channel_id.toml";
        let channel_id = load_channel_id(&channel_config_path, channel_config_file);
        let log_channel_id = channel_id.clone();
        let slack_messenger = SlackMessenger::new(&channel_id, &log_channel_id, &project_logger);
        let file_io = FileIO::new(&project_logger);
        let web_scraper = AsyncWebScraper::new(&project_logger, &slack_messenger, &file_io);
        let url = Url::parse("http://tfl.gov.uk/travel-information/timetables/").unwrap();
        let scraper_proxy = ScraperProxy::new(Duration::from_secs(10));
        let mut proxy_list = scraper_proxy.generate_proxy().await;
        let mut proxy_iter = ScraperProxy::sample_proxy(&mut proxy_list, 1);
        let request_builder_func = get_request_builder;
        let content = web_scraper.request_with_proxy(&url, proxy_iter.next().unwrap().proxy.clone(), request_builder_func ,AsyncWebScraper::null_check_func).await;
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let file = "test_scrape.html".to_owned();
        web_scraper.save_request_content(&folder_path, &file, content.unwrap()).await;
    }

    #[tokio::test]
    async fn test_multiple_requests() {
        let logger_name = "test_multiple_requests";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Info);
        let channel_config_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_utilities");
        let channel_config_file = "messenger_channel_id.toml";
        let channel_id = load_channel_id(&channel_config_path, channel_config_file);
        let log_channel_id = channel_id.clone();
        let slack_messenger = SlackMessenger::new(&channel_id, &log_channel_id, &project_logger);
        let file_io = FileIO::new(&project_logger);
        let web_scraper = AsyncWebScraper::new(&project_logger, &slack_messenger, &file_io);
        let url_suffix = ["bakerloo", "central", "circle", "district", "jubilee"];
        let url = Url::parse("http://tfl.gov.uk/tube/timetable/").unwrap();
        let file = "test_scrape{index}.html".to_owned();
        let url_file_list = Vec::from_iter(url_suffix.iter().enumerate().map(|(i, x)| {
            UrlFile::new(
                url.join(&format!("{x}/")).unwrap(),
                file.replace("{index}", &i.to_string()),
            )
        }));
        let scraper_proxy = ScraperProxy::new(Duration::from_secs(10));
        let request_builder_func = get_request_builder;
        // let mut proxy_list = scraper_proxy.generate_proxy().await;
        // let proxy_iter = ScraperProxy::sample_proxy(&mut proxy_list, url_suffix.len());
        // let request_builder_list = Vec::from_iter(proxy_iter.zip(url_file_list.iter()).map(|(proxy, url_file)| web_scraper.get_default_client(proxy.proxy.clone()).get(url_file.url.clone())));
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let calling_func = utilities_function::function_name!(true);
        let request_setting = RequestSetting {
            calling_func,
            log_only: true,
        };
        web_scraper.multiple_requests_with_proxy(
            &url_file_list,
            scraper_proxy,
            request_builder_func,
            &folder_path,
            AsyncWebScraper::null_check_func,
            request_setting,
        ).await;
    }

    const WAIT_TIME: Duration = Duration::from_secs(5);
    const ELEMENT_CSS: &str = "div#matchList.matchList";

    async fn extra_action(web_driver: &mut WebDriver) -> WebDriverResult<()> {
        web_driver.set_implicit_wait_timeout(WAIT_TIME).await?;
        web_driver
            .find(By::Css(ELEMENT_CSS)).await?
            .wait_until()
            .displayed().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_simple_browsing() {
        let logger_name = "test_simple_browsing";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Info);
        let channel_config_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_utilities");
        let channel_config_file = "messenger_channel_id.toml";
        let channel_id = load_channel_id(&channel_config_path, channel_config_file);
        let log_channel_id = channel_id.clone();
        let slack_messenger = SlackMessenger::new(&channel_id, &log_channel_id, &project_logger);
        let file_io = FileIO::new(&project_logger);
        let mut web_scraper = AsyncWebScraper::new(&project_logger, &slack_messenger, &file_io);
        let browse_action = extra_action;
        let url = Url::parse("http://www.nowgoal.com").unwrap();
        web_scraper.turn_on_chrome_process();
        let scraper_proxy = ScraperProxy::new(Duration::from_secs(10));
        let mut proxy_list = scraper_proxy.generate_proxy().await;
        let mut proxy_iter = ScraperProxy::sample_proxy(&mut proxy_list, 1);
        let browser = web_scraper.get_default_browser();
        let content =
            web_scraper.browse_request_with_proxy(&url, proxy_iter.next().unwrap().browser_proxy.clone(), &browser, &browse_action, AsyncWebScraper::null_check_func).await;
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let file = "test_browse.html".to_owned();
        web_scraper.save_request_content(&folder_path, &file, content.unwrap()).await;
        web_scraper.kill_chrome_process();
    }

    #[tokio::test]
    async fn test_multiple_browsing() {
        let logger_name = "test_multiple_browsing";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Info);
        let channel_config_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Config")
            .join("config_sctys_rust_utilities");
        let channel_config_file = "messenger_channel_id.toml";
        let channel_id = load_channel_id(&channel_config_path, channel_config_file);
        let log_channel_id = channel_id.clone();
        let slack_messenger = SlackMessenger::new(&channel_id, &log_channel_id, &project_logger);
        let file_io = FileIO::new(&project_logger);
        let browse_action = extra_action;
        let mut web_scraper = AsyncWebScraper::new(&project_logger, &slack_messenger, &file_io);
        let url_suffix = ["football/live", "football/results", "football/schedule"];
        let url = Url::parse("http://www.nowgoal.com/").unwrap();
        let file = "test_browse{index}.html".to_owned();
        let url_file_list = Vec::from_iter(url_suffix.iter().enumerate().map(|(i, x)| {
            UrlFile::new(
                url.join(x).unwrap(),
                file.replace("{index}", &i.to_string()),
            )
        }));
        let scraper_proxy = ScraperProxy::new(Duration::from_secs(10));
        let browser = web_scraper.get_default_browser();
        let folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let calling_func = utilities_function::function_name!(true);
        let browse_setting = BrowseSetting {
            restart_web_driver: false,
            calling_func,
            log_only: true,
        };
        web_scraper.turn_on_chrome_process();
        web_scraper.multiple_browse_requests(
            &url_file_list,
            scraper_proxy,
            &browser,
            &folder_path,
            &browse_action,
            AsyncWebScraper::null_check_func,
            browse_setting,
        ).await;
        web_scraper.kill_chrome_process();
    }

}