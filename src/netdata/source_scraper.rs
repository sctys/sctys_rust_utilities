use std::collections::HashMap;

use playwright_rust::{api::Viewport, Playwright};
use rquest_util::Emulation;

use crate::{logger::ProjectLogger, python_utils::PythonPath, time_operation};
use pyo3::{
    prelude::*,
    types::{IntoPyDict, PyDict},
};

use super::{
    data_struct::{BrowseOptions, PyResponse, RequestOptions, Response, ScraperError},
    proxy::ScraperProxy,
    python_struct::{NetdataPythonPath, PythonTxt},
    requests_ip_rotate::{ApiGateway, ApiGatewayConfig, ApiGatewayRegion},
};

const JS_HEADER_INTERCEPION: &str = include_str!("./js/header_interception.js");

pub struct SourceScraper<'a> {
    logger: &'a ProjectLogger,
    scraper_proxy: Option<ScraperProxy<'a>>,
    rquest_client: Option<rquest::Client>,
    curl_cffi_session: Option<Py<PyAny>>,
    playwright_client: Option<Playwright>,
    api_gateway: Option<ApiGateway>,
}

impl Drop for SourceScraper<'_> {
    fn drop(&mut self) {
        if let Some(curl_cffi_session) = self.curl_cffi_session.take() {
            Python::with_gil(|py| {
                let _ = curl_cffi_session.call_method1(
                    py,
                    "__exit__",
                    (py.None(), py.None(), py.None()),
                );
            })
        };
    }
}

impl<'a> SourceScraper<'a> {
    const GOOGLE_SHEET_URL: &'a str = "https://docs.google.com/spreadsheets/d/";
    const GOOGLE_SHEET_REPLACE_TOKEN: (&'a str, &'a str) = ("edit#gid=", "export?format=csv&gid=");
    const RQUEST_BROWSER: Emulation = Emulation::Chrome135;
    const CURL_CFFI_BROWSER: (&'static str, &'static str) = ("impersonate", "chrome");

    pub fn new(logger: &'a ProjectLogger) -> Self {
        Self {
            logger,
            scraper_proxy: None,
            rquest_client: None,
            curl_cffi_session: None,
            playwright_client: None,
            api_gateway: None,
        }
    }

    fn init_proxy(&mut self) {
        if self.scraper_proxy.is_none() {
            self.scraper_proxy = Some(ScraperProxy::new(self.logger));
        }
    }

    fn get_scraper_proxy(&mut self) -> &mut ScraperProxy<'a> {
        self.init_proxy();
        match self.scraper_proxy.as_mut() {
            Some(scraper_proxy) => {
                let debug_str = "Scraper proxy initialized";
                self.logger.log_debug(debug_str);
                scraper_proxy
            }
            None => {
                let error_str = "Scraper proxy initialized by still none.";
                self.logger.log_error(error_str);
                panic!("{error_str}");
            }
        }
    }

    fn get_rquest_client(&mut self) -> Result<&rquest::Client, ScraperError> {
        if self.rquest_client.is_none() {
            self.rquest_client = Some(
                rquest::Client::builder()
                    .emulation(Self::RQUEST_BROWSER)
                    .build()?,
            );
        }
        match self.rquest_client.as_ref() {
            Some(rquest_client) => {
                let debug_str = "Rquest client initialized";
                self.logger.log_debug(debug_str);
                Ok(rquest_client)
            }
            None => {
                let error_str = "Rquest client initialized by still none.";
                self.logger.log_error(error_str);
                panic!("{error_str}");
            }
        }
    }

    fn get_curl_cffi_session(&mut self) -> Result<&Py<PyAny>, ScraperError> {
        if self.curl_cffi_session.is_none() {
            Python::with_gil(|py| -> PyResult<()> {
                let requests = py.import("curl_cffi.requests")?;
                let kwargs = [Self::CURL_CFFI_BROWSER].into_py_dict(py)?;
                let session_obj = requests.call_method("Session", (py.None(),), Some(&kwargs))?;
                let session = session_obj.call_method0("__enter__")?;
                self.curl_cffi_session = Some(session.into());
                Ok(())
            })
            .map_err(ScraperError::from)?
        }
        match self.curl_cffi_session.as_ref() {
            Some(curl_cffi_session) => {
                let debug_str = "Curl cffi session initialized";
                self.logger.log_debug(debug_str);
                Ok(curl_cffi_session)
            }
            None => {
                let error_str = "Curl cffi session initialized by still none.";
                self.logger.log_error(error_str);
                panic!("{error_str}");
            }
        }
    }

    async fn get_playwright_client(&mut self) -> Result<&Playwright, ScraperError> {
        if self.playwright_client.is_none() {
            let playwright = Playwright::initialize().await?;
            playwright.prepare()?;
            self.playwright_client = Some(playwright);
        }
        match self.playwright_client.as_ref() {
            Some(playwright_client) => {
                let debug_str = "Playwright client initialized";
                self.logger.log_debug(debug_str);
                Ok(playwright_client)
            }
            None => {
                let error_str = "Playwright client initialized by still none.";
                self.logger.log_error(error_str);
                panic!("{error_str}");
            }
        }
    }

    async fn get_api_gateway(
        &mut self,
        url: &str,
        regions: Option<Vec<ApiGatewayRegion>>,
    ) -> Result<&ApiGateway, ScraperError> {
        if self.api_gateway.is_none() {
            let api_gateway_config = ApiGatewayConfig::form_config(url, regions);
            self.api_gateway = Some(ApiGateway::new(api_gateway_config));
        }
        match self.api_gateway.as_ref() {
            Some(api_gateway) => {
                let debug_str = "Api gateway initialized";
                self.logger.log_debug(debug_str);
                api_gateway.start(false, false, Vec::new()).await;
                Ok(api_gateway)
            }
            None => {
                let error_str = "Api gateway initialized by still none.";
                self.logger.log_error(error_str);
                panic!("{error_str}");
            }
        }
    }

    fn url_from_google_sheet_link(google_sheet_key: &str) -> String {
        let (replace_token_from, replace_token_to) = Self::GOOGLE_SHEET_REPLACE_TOKEN;
        let csv_link = format!(
            "{}{}",
            Self::GOOGLE_SHEET_URL,
            google_sheet_key.replace(replace_token_from, replace_token_to,)
        );
        csv_link
    }

    pub async fn download_google_sheet(
        &mut self,
        google_sheet_key: &str,
    ) -> Result<Response, ScraperError> {
        let google_sheet_url = Self::url_from_google_sheet_link(google_sheet_key);
        let request_options = RequestOptions {
            proxy: false,
            ..Default::default()
        };
        self.request_with_reqwest(&google_sheet_url, &request_options, false)
            .await
    }

    pub async fn request_with_reqwest(
        &mut self,
        url: &str,
        request_options: &RequestOptions,
        use_gateway: bool,
    ) -> Result<Response, ScraperError> {
        let mut client_builder = reqwest::ClientBuilder::new().timeout(request_options.timeout);
        if let Some(headers) = &request_options.headers {
            client_builder = client_builder.default_headers(headers.clone());
        }
        let response = if use_gateway {
            let api_gateway = self.get_api_gateway(url, None).await?;
            let client = client_builder.build()?;
            let request = client.get(url).build()?;
            api_gateway
                .reqwest_send(&client, request)
                .await
                .map_err(ScraperError::from)?
                .error_for_status()?
        } else {
            if request_options.proxy {
                let scraper_proxy = self.get_scraper_proxy();
                let proxy = scraper_proxy.generate_proxy().await?.get_reqwest_proxy()?;
                client_builder = client_builder.proxy(proxy);
            }
            client_builder
                .build()?
                .get(url)
                .send()
                .await?
                .error_for_status()?
        };
        Response::from_reqwest_response(response).await
    }

    pub async fn request_with_rquest(
        &mut self,
        url: &str,
        request_options: &RequestOptions,
        use_gateway: bool,
    ) -> Result<Response, ScraperError> {
        let client = self.get_rquest_client()?;
        let mut request_builder = client.get(url);
        if let Some(headers) = &request_options.headers {
            request_builder = request_builder.headers(headers.clone());
        }
        let response = if use_gateway {
            let client = client.clone();
            let api_gateway = self.get_api_gateway(url, None).await?;
            let request = request_builder.build()?;
            api_gateway
                .rquest_send(&client, request)
                .await
                .map_err(ScraperError::from)?
                .error_for_status()?
        } else {
            if request_options.proxy {
                let scraper_proxy = self.get_scraper_proxy();
                let proxy = scraper_proxy.generate_proxy().await?.get_rquest_proxy()?;
                request_builder = request_builder.proxy(proxy);
            }
            request_builder.send().await?
        };
        Response::from_rquest_response(response).await
    }

    pub async fn request_with_curl_cffi(
        &mut self,
        url: &str,
        request_options: &RequestOptions,
    ) -> Result<Response, ScraperError> {
        NetdataPythonPath::setup_python_venv();
        let proxy = if request_options.proxy {
            let scraper_proxy = self.get_scraper_proxy();
            Some(scraper_proxy.generate_proxy().await?.get_http_address())
        } else {
            None
        };
        Python::with_gil(|py| {
            NetdataPythonPath::append_script_path(&py)?;
            let request_curl_cffi = py.import(PythonTxt::RequestCurlCffi.to_string())?;
            let kwargs = PyDict::new(py);
            let session = self.get_curl_cffi_session()?;
            kwargs.set_item(
                PythonTxt::Timeout.to_string(),
                request_options.timeout.as_secs(),
            )?;
            if let Some(headers) = &request_options.convert_header_map_to_map() {
                kwargs.set_item(PythonTxt::Headers.to_string(), headers)?;
            } else {
                kwargs.set_item(
                    PythonTxt::Headers.to_string(),
                    HashMap::<String, String>::new(),
                )?;
            }
            if let Some(proxy) = proxy {
                kwargs.set_item(PythonTxt::Proxy.to_string(), proxy)?;
            } else {
                kwargs.set_item(PythonTxt::Proxy.to_string(), py.None())?;
            }
            let response = request_curl_cffi
                .getattr(PythonTxt::RequestsWithCurlCffi.to_string())?
                .call((session, url), Some(&kwargs))?;
            let py_response = response.extract::<PyResponse>()?;
            py_response.to_response()
        })
    }

    pub async fn request_with_playwright(
        &mut self,
        url: &str,
        request_options: &RequestOptions,
        browser_options: &BrowseOptions,
    ) -> Result<Response, ScraperError> {
        let playwright = self.get_playwright_client().await?;
        let chromium = playwright.chromium();
        let mut browser = chromium
            .launcher()
            .timeout(request_options.timeout.as_millis() as f64)
            .headless(browser_options.headless);
        if request_options.proxy {
            let scraper_proxy = self.get_scraper_proxy();
            let proxy = scraper_proxy.generate_proxy().await?.get_playwright_proxy();
            browser = browser.proxy(proxy);
        }
        let browser = browser
            .launch()
            .await
            .map_err(playwright_rust::Error::from)?;
        let context = browser.context_builder()
            .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")
            .viewport(Some(Viewport { width: 1920, height: 1080 }))
            .build().await.map_err(playwright_rust::Error::from)?;
        let page = context
            .new_page()
            .await
            .map_err(playwright_rust::Error::from)?;
        let response = page
            .goto_builder(url)
            .goto()
            .await
            .map_err(playwright_rust::Error::from)?;
        if let Some(response) = response {
            if let Some(page_evaluation) = &browser_options.page_evaluation {
                page.eval::<()>(page_evaluation)
                    .await
                    .map_err(playwright_rust::Error::from)?;
            }
            time_operation::async_sleep(browser_options.browser_wait).await;
            if response.ok()? {
                Ok({
                    Response {
                        content: page.content().await.map_err(playwright_rust::Error::from)?,
                        status_code: match response.status() {
                            Ok(status) => status as u16,
                            Err(e) => return Err(ScraperError::Playwright(e)),
                        },
                        url: page.url()?,
                    }
                })
            } else {
                Err(ScraperError::Other(format!(
                    "Error return from playwright for url {url}, {}",
                    response.status_text()?
                )))
            }
        } else {
            Err(ScraperError::Other(format!(
                "No response from playwright for url {url}"
            )))
        }
    }

    pub async fn get_headers_for_requests(
        &mut self,
        url: &str,
        request_options: &RequestOptions,
        browser_options: &BrowseOptions,
    ) -> Result<HashMap<String, HashMap<String, String>>, ScraperError> {
        let playwright = self.get_playwright_client().await?;
        let chromium = playwright.chromium();
        let mut browser = chromium
            .launcher()
            .timeout(request_options.timeout.as_millis() as f64)
            .headless(browser_options.headless);
        if request_options.proxy {
            let scraper_proxy = self.get_scraper_proxy();
            let proxy = scraper_proxy.generate_proxy().await?.get_playwright_proxy();
            browser = browser.proxy(proxy);
        }
        let browser = browser
            .launch()
            .await
            .map_err(playwright_rust::Error::from)?;
        let context = browser.context_builder()
            .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")
            .viewport(Some(Viewport { width: 1920, height: 1080 }))
            .build().await.map_err(playwright_rust::Error::from)?;
        // Add the script to intercept headers
        context
            .add_init_script(JS_HEADER_INTERCEPION)
            .await
            .map_err(playwright_rust::Error::from)?;

        let page = context
            .new_page()
            .await
            .map_err(playwright_rust::Error::from)?;
        page.goto_builder(url)
            .goto()
            .await
            .map_err(playwright_rust::Error::from)?;
        time_operation::async_sleep(browser_options.browser_wait).await;
        let headers_json: String = page
            .eval("() => JSON.stringify(window.__getInterceptedHeaders())")
            .await
            .map_err(playwright_rust::Error::from)?;
        dbg!(&headers_json);
        let headers_map: HashMap<String, HashMap<String, String>> =
            serde_json::from_str(&headers_json)?;
        browser
            .close()
            .await
            .map_err(playwright_rust::Error::from)?;
        Ok(headers_map)
    }
}

#[cfg(test)]
mod tests {
    use std::{env, path::Path, time::Duration};

    use super::*;
    use log::LevelFilter;

    #[tokio::test]
    async fn test_reqwest() {
        let logger_name = "test_scraping";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let mut scraper = SourceScraper::new(&project_logger);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions {
            timeout: Duration::from_secs(10),
            proxy: false,
            headers: None,
        };
        let response = scraper
            .request_with_reqwest(url, &request_options, false)
            .await
            .unwrap();
        dbg!(response);
        for _ in 0..3 {
            let response = scraper
                .request_with_reqwest(url, &request_options, true)
                .await
                .unwrap();
            dbg!(response);
        }
        let request_options = RequestOptions {
            timeout: Duration::from_secs(10),
            proxy: true,
            headers: None,
        };
        for _ in 0..3 {
            let response = scraper
                .request_with_reqwest(url, &request_options, false)
                .await
                .unwrap();
            dbg!(response);
        }
    }

    #[tokio::test]
    async fn test_rquest() {
        let logger_name = "test_scraping";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let mut scraper = SourceScraper::new(&project_logger);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions {
            timeout: Duration::from_secs(10),
            proxy: false,
            headers: None,
        };
        let response = scraper
            .request_with_rquest(url, &request_options, false)
            .await
            .unwrap();
        dbg!(response);
        for _ in 0..3 {
            let response = scraper
                .request_with_rquest(url, &request_options, true)
                .await
                .unwrap();
            dbg!(response);
        }
        // let request_options = RequestOptions {
        //     timeout: Duration::from_secs(10),
        //     proxy: true,
        //     headers: None,
        // };
        // for _ in 0..3 {
        //     let response = scraper.request_with_rquest(url, &request_options, false).await.unwrap();
        //     dbg!(response);
        // }
    }

    #[tokio::test]
    async fn test_curl_cffi() {
        let logger_name = "test_scraping";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let mut scraper = SourceScraper::new(&project_logger);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions {
            timeout: Duration::from_secs(10),
            proxy: false,
            headers: None,
        };
        let response = scraper
            .request_with_curl_cffi(url, &request_options)
            .await
            .unwrap();
        dbg!(response);
        let request_options = RequestOptions {
            timeout: Duration::from_secs(10),
            proxy: true,
            headers: None,
        };
        for _ in 0..3 {
            let response = scraper
                .request_with_curl_cffi(url, &request_options)
                .await
                .unwrap();
            dbg!(response);
        }
    }

    #[tokio::test]
    async fn test_playwright() {
        let logger_name = "test_scraping";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let mut scraper = SourceScraper::new(&project_logger);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions {
            timeout: Duration::from_secs(10),
            proxy: false,
            headers: None,
        };
        let browse_options = BrowseOptions {
            headless: true,
            browser_wait: Duration::from_secs(3),
            page_evaluation: None,
        };
        let response = scraper
            .request_with_playwright(url, &request_options, &browse_options)
            .await
            .unwrap();
        dbg!(response);
        let request_options = RequestOptions {
            timeout: Duration::from_secs(10),
            proxy: true,
            headers: None,
        };
        for _ in 0..3 {
            let response = scraper
                .request_with_playwright(url, &request_options, &browse_options)
                .await
                .unwrap();
            dbg!(response);
        }
    }

    #[tokio::test]
    async fn test_header_interception() {
        let logger_name = "test_scraping";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let mut scraper = SourceScraper::new(&project_logger);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions {
            timeout: Duration::from_secs(10),
            proxy: false,
            headers: None,
        };
        let browse_options = BrowseOptions {
            headless: true,
            browser_wait: Duration::from_secs(3),
            page_evaluation: None,
        };
        let headers_map = scraper
            .get_headers_for_requests(url, &request_options, &browse_options)
            .await
            .unwrap();
        dbg!(headers_map);
    }
}
