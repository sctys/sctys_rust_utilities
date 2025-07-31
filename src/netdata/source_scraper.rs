use std::{collections::HashMap, time::Duration};

use playwright_rust::{api::Viewport, Playwright};
use rquest_util::Emulation;

use crate::{logger::ProjectLogger, time_operation};

use super::{
    data_struct::{BrowseOptions, CurlCffiClient, RequestOptions, Response, ScraperError},
    proxy::ScraperProxy,
    requests_ip_rotate::{ApiGateway, ApiGatewayConfig, ApiGatewayRegion},
};

const JS_HEADER_INTERCEPION: &str = include_str!("./js/header_interception.js");

pub struct SourceScraper<'a> {
    pub logger: &'a ProjectLogger,
}

impl<'a> SourceScraper<'a> {
    const GOOGLE_SHEET_URL: &'a str = "https://docs.google.com/spreadsheets/d/";
    const GOOGLE_SHEET_REPLACE_TOKEN: (&'a str, &'a str) = ("edit#gid=", "export?format=csv&gid=");
    const RQUEST_BROWSER: Emulation = Emulation::Chrome135;

    pub fn new(logger: &'a ProjectLogger) -> Self {
        Self { logger }
    }

    pub fn get_scraper_proxy(&self) -> ScraperProxy<'a> {
        let scraper_proxy = ScraperProxy::new(self.logger);
        let debug_str = "Scraper proxy initialized";
        self.logger.log_debug(debug_str);
        scraper_proxy
    }

    pub fn get_rquest_client(&self, timeout: Duration) -> Result<rquest::Client, ScraperError> {
        let rquest_client = rquest::Client::builder()
            .emulation(Self::RQUEST_BROWSER)
            .connect_timeout(timeout)
            .build()?;
        let debug_str = "Rquest client initialized";
        self.logger.log_debug(debug_str);
        Ok(rquest_client)
    }

    pub fn get_curl_cffi_client(&self) -> Result<CurlCffiClient, ScraperError> {
        let curl_cffi_client = CurlCffiClient::create_session()?;
        let debug_str = "Curl cffi client initialized";
        self.logger.log_debug(debug_str);
        Ok(curl_cffi_client)
    }

    pub async fn get_playwright_client(&self) -> Result<Playwright, ScraperError> {
        let playwright_client = Playwright::initialize().await?;
        playwright_client.prepare()?;
        let debug_str = "Playwright client initialized";
        self.logger.log_debug(debug_str);
        Ok(playwright_client)
    }

    pub async fn get_api_gateway(
        &self,
        url: &str,
        regions: Option<Vec<ApiGatewayRegion>>,
    ) -> Result<ApiGateway, ScraperError> {
        let api_gateway_config = ApiGatewayConfig::form_config(url, regions);
        let api_gateway = ApiGateway::new(api_gateway_config);
        api_gateway.start(false, false, Vec::new()).await;
        let debug_str = "Api gateway initialized";
        self.logger.log_debug(debug_str);
        Ok(api_gateway)
    }

    pub fn url_site_from_url(url: &str) -> String {
        url.split('/').take(3).collect::<Vec<_>>().join("/")
    }

    pub async fn get_update_domain(
        &self,
        url: &str,
        request_options: &RequestOptions,
    ) -> (String, String) {
        let original_domain = Self::url_site_from_url(url);
        let new_domain = match self
            .request_with_reqwest(&original_domain, request_options, None, None)
            .await
        {
            Ok(response) => {
                if response.ok {
                    Self::url_site_from_url(response.url.as_str())
                } else {
                    original_domain.clone()
                }
            }
            Err(_) => original_domain.clone(),
        };
        (original_domain, new_domain)
    }

    pub fn url_from_google_sheet_link(google_sheet_key: &str) -> String {
        let (replace_token_from, replace_token_to) = Self::GOOGLE_SHEET_REPLACE_TOKEN;
        let csv_link = format!(
            "{}{}",
            Self::GOOGLE_SHEET_URL,
            google_sheet_key.replace(replace_token_from, replace_token_to,)
        );
        csv_link
    }

    pub async fn request_with_reqwest(
        &self,
        url: &str,
        request_options: &RequestOptions,
        scraper_proxy: Option<&mut ScraperProxy<'a>>,
        gateway: Option<&ApiGateway>,
    ) -> Result<Response, ScraperError> {
        let debug_log = format!("Attempting to make a request to {} with reqwest", url);
        self.logger.log_debug(&debug_log);
        let mut client_builder = reqwest::ClientBuilder::new()
            .connect_timeout(request_options.connect_timeout)
            .timeout(request_options.timeout);
        if let Some(headers) = &request_options.headers {
            client_builder = client_builder.default_headers(headers.clone());
        }
        let response = if let Some(api_gateway) = gateway {
            let client = client_builder.build()?;
            let request = client.get(url).build()?;
            api_gateway
                .reqwest_send(&client, request)
                .await
                .map_err(ScraperError::from)?
        } else if let Some(scraper_proxy) = scraper_proxy {
            let proxy_result = scraper_proxy.generate_proxy().await?;
            let proxy = proxy_result.get_reqwest_proxy()?;
            client_builder = client_builder.proxy(proxy);
            let response = client_builder.build()?.get(url).send().await.map_err(|e| {
                if e.is_timeout() {
                    let warn_str = format!(
                        "Proxy request {}:{} timed out",
                        proxy_result.proxy_address, proxy_result.port
                    );
                    self.logger.log_warn(&warn_str);
                    e
                } else {
                    e
                }
            })?;
            if !request_options.allow_forbidden_proxy
                && response.status() == reqwest::StatusCode::FORBIDDEN
            {
                scraper_proxy.add_proxy_block_count(&proxy_result);
            };
            response
        } else {
            client_builder.build()?.get(url).send().await?
        };
        Response::from_reqwest_response(response).await
    }

    pub async fn request_with_rquest(
        &self,
        url: &str,
        request_options: &RequestOptions,
        client: &rquest::Client,
        scraper_proxy: Option<&mut ScraperProxy<'a>>,
        api_gateway: Option<&ApiGateway>,
    ) -> Result<Response, ScraperError> {
        let debug_log = format!("Attempting to make a request to {} with rquest", url);
        self.logger.log_debug(&debug_log);
        let mut request_builder = client.get(url);
        if let Some(headers) = &request_options.headers {
            request_builder = request_builder
                .headers(headers.clone())
                .timeout(request_options.timeout);
        }
        let response = if let Some(api_gateway) = api_gateway {
            let request = request_builder.build()?;
            api_gateway
                .rquest_send(client, request)
                .await
                .map_err(ScraperError::from)?
        } else if let Some(scraper_proxy) = scraper_proxy {
            let proxy_result = scraper_proxy.generate_proxy().await?;
            let proxy = proxy_result.get_rquest_proxy()?;
            request_builder = request_builder.proxy(proxy);
            let response = request_builder.send().await.map_err(|e| {
                if e.is_timeout() {
                    let warn_str = format!(
                        "Proxy request {}:{} timed out",
                        proxy_result.proxy_address, proxy_result.port
                    );
                    self.logger.log_warn(&warn_str);
                    e
                } else {
                    e
                }
            })?;
            if !request_options.allow_forbidden_proxy
                && response.status() == rquest::StatusCode::FORBIDDEN
            {
                scraper_proxy.add_proxy_block_count(&proxy_result);
            };
            response
        } else {
            request_builder.send().await?
        };
        Response::from_rquest_response(response).await
    }

    pub async fn request_with_curl_cffi(
        &self,
        url: &str,
        request_options: &RequestOptions,
        curl_cffi_client: &CurlCffiClient,
        scraper_proxy: Option<&mut ScraperProxy<'a>>,
    ) -> Result<Response, ScraperError> {
        let debug_log = format!("Attempting to make a request to {} with curl_cffi", url);
        self.logger.log_debug(&debug_log);
        if let Some(scraper_proxy) = scraper_proxy {
            let proxy_result = scraper_proxy.generate_proxy().await?;
            let proxy = Some(proxy_result.get_http_address());
            let py_response = curl_cffi_client.request(url, request_options, proxy)?;
            let response = py_response.to_response()?;
            if !request_options.allow_forbidden_proxy && response.status_code == 403 {
                scraper_proxy.add_proxy_block_count(&proxy_result);
            };
            Ok(response)
        } else {
            let py_response = curl_cffi_client.request(url, request_options, None)?;
            py_response.to_response()
        }
    }

    pub async fn request_with_playwright(
        &self,
        url: &str,
        request_options: &RequestOptions,
        browser_options: &BrowseOptions,
        playwright: &Playwright,
        scraper_proxy: Option<&mut ScraperProxy<'a>>,
    ) -> Result<Response, ScraperError> {
        let debug_log = format!("Attempting to make a request to {} with playwright", url);
        self.logger.log_debug(&debug_log);
        let chromium = playwright.chromium();
        let mut browser = chromium
            .launcher()
            .timeout(request_options.connect_timeout.as_millis() as f64)
            .headless(browser_options.headless);
        if let Some(scraper_proxy) = scraper_proxy {
            let proxy_result = scraper_proxy.generate_proxy().await?;
            let proxy = proxy_result.get_playwright_proxy();
            browser = browser.proxy(proxy);
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
                .timeout(request_options.timeout.as_millis() as f64)
                .goto()
                .await
                .map_err(playwright_rust::Error::from)?;
            let cookies = context
                .cookies(&[])
                .await
                .map_err(playwright_rust::Error::from)?
                .iter()
                .map(|c| (c.name.to_string(), c.value.to_string()))
                .collect();
            if let Some(response) = response {
                if let Some(page_evaluation) = &browser_options.page_evaluation {
                    page.eval::<()>(page_evaluation)
                        .await
                        .map_err(playwright_rust::Error::from)?;
                }
                time_operation::async_sleep(browser_options.browser_wait).await;
                let status_code = response.status()? as u16;
                if !request_options.allow_forbidden_proxy && status_code == 403 {
                    scraper_proxy.add_proxy_block_count(&proxy_result);
                }
                let response = {
                    Response {
                        content: page.content().await.map_err(playwright_rust::Error::from)?,
                        status_code: response.status()? as u16,
                        url: page.url()?,
                        ok: response.ok()?,
                        reason: response.status_text()?,
                        cookies,
                    }
                };
                page.close(None)
                    .await
                    .map_err(playwright_rust::Error::from)?;
                context
                    .close()
                    .await
                    .map_err(playwright_rust::Error::from)?;
                browser
                    .close()
                    .await
                    .map_err(playwright_rust::Error::from)?;
                Ok(response)
            } else {
                page.close(None)
                    .await
                    .map_err(playwright_rust::Error::from)?;
                context
                    .close()
                    .await
                    .map_err(playwright_rust::Error::from)?;
                browser
                    .close()
                    .await
                    .map_err(playwright_rust::Error::from)?;
                Err(ScraperError::Other(format!(
                    "No response from playwright for url {url}"
                )))
            }
        } else {
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
                .timeout(request_options.timeout.as_millis() as f64)
                .goto()
                .await
                .map_err(playwright_rust::Error::from)?;
            let cookies = context
                .cookies(&[])
                .await
                .map_err(playwright_rust::Error::from)?
                .iter()
                .map(|c| (c.name.to_string(), c.value.to_string()))
                .collect();
            if let Some(response) = response {
                if let Some(page_evaluation) = &browser_options.page_evaluation {
                    page.eval::<()>(page_evaluation)
                        .await
                        .map_err(playwright_rust::Error::from)?;
                }
                time_operation::async_sleep(browser_options.browser_wait).await;
                let response = {
                    Response {
                        content: page.content().await.map_err(playwright_rust::Error::from)?,
                        status_code: response.status()? as u16,
                        url: page.url()?,
                        ok: response.ok()?,
                        reason: response.status_text()?,
                        cookies,
                    }
                };
                page.close(None)
                    .await
                    .map_err(playwright_rust::Error::from)?;
                context
                    .close()
                    .await
                    .map_err(playwright_rust::Error::from)?;
                browser
                    .close()
                    .await
                    .map_err(playwright_rust::Error::from)?;
                Ok(response)
            } else {
                page.close(None)
                    .await
                    .map_err(playwright_rust::Error::from)?;
                context
                    .close()
                    .await
                    .map_err(playwright_rust::Error::from)?;
                browser
                    .close()
                    .await
                    .map_err(playwright_rust::Error::from)?;
                Err(ScraperError::Other(format!(
                    "No response from playwright for url {url}"
                )))
            }
        }
    }

    pub async fn get_headers_for_requests(
        &self,
        url: &str,
        request_options: &RequestOptions,
        browser_options: &BrowseOptions,
        playwright: &Playwright,
        scraper_proxy: Option<&mut ScraperProxy<'a>>,
    ) -> Result<HashMap<String, HashMap<String, String>>, ScraperError> {
        let chromium = playwright.chromium();
        let mut browser = chromium
            .launcher()
            .timeout(request_options.timeout.as_millis() as f64)
            .headless(browser_options.headless);
        if let Some(scraper_proxy) = scraper_proxy {
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
        let scraper = SourceScraper::new(&project_logger);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions {
            connect_timeout: Duration::from_secs(5),
            timeout: Duration::from_secs(15),
            headers: None,
            allow_forbidden_proxy: false,
        };
        let response = scraper
            .request_with_reqwest(url, &request_options, None, None)
            .await
            .unwrap();
        dbg!(response);
        let api_gateway = scraper.get_api_gateway(url, None).await.unwrap();
        for _ in 0..3 {
            let response = scraper
                .request_with_reqwest(url, &request_options, None, Some(&api_gateway))
                .await
                .unwrap();
            dbg!(response);
        }
        let mut scraper_proxy = scraper.get_scraper_proxy();
        for _ in 0..3 {
            let response = scraper
                .request_with_reqwest(url, &request_options, Some(&mut scraper_proxy), None)
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
        let scraper = SourceScraper::new(&project_logger);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions {
            connect_timeout: Duration::from_secs(5),
            timeout: Duration::from_secs(15),
            headers: None,
            allow_forbidden_proxy: false,
        };
        let rquest_client = scraper
            .get_rquest_client(request_options.connect_timeout)
            .unwrap();
        let response = scraper
            .request_with_rquest(url, &request_options, &rquest_client, None, None)
            .await
            .unwrap();
        dbg!(response);
        let api_gateway = scraper.get_api_gateway(url, None).await.unwrap();
        for _ in 0..3 {
            let response = scraper
                .request_with_rquest(
                    url,
                    &request_options,
                    &rquest_client,
                    None,
                    Some(&api_gateway),
                )
                .await
                .unwrap();
            dbg!(response);
        }
        let mut scraper_proxy = scraper.get_scraper_proxy();
        for _ in 0..3 {
            let response = scraper
                .request_with_rquest(
                    url,
                    &request_options,
                    &rquest_client,
                    Some(&mut scraper_proxy),
                    None,
                )
                .await
                .unwrap();
            dbg!(response);
        }
    }

    #[tokio::test]
    async fn test_curl_cffi() {
        let logger_name = "test_scraping";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let scraper = SourceScraper::new(&project_logger);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions {
            connect_timeout: Duration::from_secs(5),
            timeout: Duration::from_secs(15),
            headers: None,
            allow_forbidden_proxy: false,
        };
        let curl_cffi_client = scraper.get_curl_cffi_client().unwrap();
        let response = scraper
            .request_with_curl_cffi(url, &request_options, &curl_cffi_client, None)
            .await
            .unwrap();
        dbg!(response);
        let mut scraper_proxy = scraper.get_scraper_proxy();
        for _ in 0..3 {
            let response = scraper
                .request_with_curl_cffi(
                    url,
                    &request_options,
                    &curl_cffi_client,
                    Some(&mut scraper_proxy),
                )
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
        let scraper = SourceScraper::new(&project_logger);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions {
            connect_timeout: Duration::from_secs(5),
            timeout: Duration::from_secs(15),
            headers: None,
            allow_forbidden_proxy: false,
        };
        let browse_options = BrowseOptions {
            headless: true,
            browser_wait: Duration::from_secs(3),
            page_evaluation: None,
        };
        let playwright = scraper.get_playwright_client().await.unwrap();
        let response = scraper
            .request_with_playwright(url, &request_options, &browse_options, &playwright, None)
            .await
            .unwrap();
        dbg!(response);
        let mut scraper_proxy = scraper.get_scraper_proxy();
        for _ in 0..3 {
            let response = scraper
                .request_with_playwright(
                    url,
                    &request_options,
                    &browse_options,
                    &playwright,
                    Some(&mut scraper_proxy),
                )
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
        let scraper = SourceScraper::new(&project_logger);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions {
            connect_timeout: Duration::from_secs(5),
            timeout: Duration::from_secs(15),
            headers: None,
            allow_forbidden_proxy: false,
        };
        let browse_options = BrowseOptions {
            headless: true,
            browser_wait: Duration::from_secs(3),
            page_evaluation: None,
        };
        let playwright = scraper.get_playwright_client().await.unwrap();
        let headers_map = scraper
            .get_headers_for_requests(url, &request_options, &browse_options, &playwright, None)
            .await
            .unwrap();
        dbg!(headers_map);
    }

    #[tokio::test]
    async fn test_update_domain() {
        let logger_name = "test_scraping";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let scraper = SourceScraper::new(&project_logger);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions {
            connect_timeout: Duration::from_secs(5),
            timeout: Duration::from_secs(15),
            headers: None,
            allow_forbidden_proxy: false,
        };
        let (original_domain, new_domain) = scraper.get_update_domain(url, &request_options).await;
        dbg!(original_domain);
        dbg!(new_domain);
    }
}
