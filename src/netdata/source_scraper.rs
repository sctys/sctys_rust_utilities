use std::{
    collections::HashMap,
    env, fs,
    future::Future,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use playwright_rust::{
    api::{Browser, BrowserContext, Page, Viewport},
    Playwright,
};
use wreq_util::Emulation;

use crate::{
    logger::ProjectLogger,
    netdata::{capsolver::CapSolver, playwright_js_client::PlaywrightClient},
    secret::aws_secret::Secret,
    time_operation,
};

use super::{
    data_struct::{BrowseOptions, RequestOptions, Response, ScraperError},
    proxy::ScraperProxy,
    requests_ip_rotate::{ApiGateway, ApiGatewayConfig, ApiGatewayRegion},
};

const JS_HEADER_INTERCEPION: &str = include_str!("./js/header_interception.js");
const LETSENCRYPT_R13_CERT: &[u8] = include_bytes!("letsencrypt_r13.pem");

/// Comprehensive stealth init script injected into every page before any JS runs.
/// Patches the most common bot-detection vectors used by Cloudflare Turnstile.
const STEALTH_INIT_SCRIPT: &str = include_str!("./js/stealth_init_script.js");
const PLAYWRIGHT_TMP_ENV: &str = "SCTYS_PLAYWRIGHT_TMP";
const PLAYWRIGHT_TMP_DIR: &str = "sctys_playwright_tmp";

#[derive(Clone, Copy)]
pub enum RquestBrowser {
    Chrome120,
    Chrome135,
}

pub struct SourceScraper<'a> {
    logger: &'a ProjectLogger,
    secret: &'a Secret<'a>,
}

struct PlaywrightRequestTempDir {
    path: PathBuf,
}

impl PlaywrightRequestTempDir {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn downloads_path(&self) -> PathBuf {
        self.path.join("downloads")
    }
}

impl Drop for PlaywrightRequestTempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

impl<'a> SourceScraper<'a> {
    const GOOGLE_SHEET_URL: &'a str = "https://docs.google.com/spreadsheets/d/";
    const GOOGLE_SHEET_REPLACE_TOKEN: (&'a str, &'a str) = ("edit#gid=", "export?format=csv&gid=");
    const RQUEST_BROWSER: RquestBrowser = RquestBrowser::Chrome135;

    pub fn new(logger: &'a ProjectLogger, secret: &'a Secret) -> Self {
        Self { logger, secret }
    }

    pub fn get_logger(&self) -> &'a ProjectLogger {
        self.logger
    }

    pub async fn get_scraper_proxy(&self) -> Result<ScraperProxy<'a>, ScraperError> {
        let scraper_proxy = ScraperProxy::new(self.logger, self.secret).await?;
        let debug_str = "Scraper proxy initialized";
        self.logger.log_debug(debug_str);
        Ok(scraper_proxy)
    }

    pub async fn get_cap_solver(&self) -> Result<CapSolver<'a>, ScraperError> {
        let cap_solver = CapSolver::new(self.logger, self.secret).await?;
        let debug_str = "Cap solver initialized";
        self.logger.log_debug(debug_str);
        Ok(cap_solver)
    }

    pub fn get_rquest_client(
        &self,
        request_options: &RequestOptions,
    ) -> Result<wreq::Client, ScraperError> {
        self.get_rquest_client_with_browser(request_options, Self::RQUEST_BROWSER)
    }

    pub fn get_rquest_client_with_browser(
        &self,
        request_options: &RequestOptions,
        browser: RquestBrowser,
    ) -> Result<wreq::Client, ScraperError> {
        let read_timeout = Self::get_read_timeout(
            request_options.timeout,
            request_options.connect_timeout,
            "Invalid rquest timeout config. timeout must be greater than connect_timeout",
        )?;
        let emulation = match browser {
            RquestBrowser::Chrome120 => Emulation::Chrome120,
            RquestBrowser::Chrome135 => Emulation::Chrome135,
        };
        let rquest_client = wreq::Client::builder()
            .emulation(emulation)
            .connect_timeout(request_options.connect_timeout)
            .timeout(request_options.timeout)
            .read_timeout(read_timeout)
            .build()?;
        let debug_str = "Rquest client initialized";
        self.logger.log_debug(debug_str);
        Ok(rquest_client)
    }

    fn get_read_timeout(
        timeout: Duration,
        connect_timeout: Duration,
        err_msg: &str,
    ) -> Result<Duration, ScraperError> {
        timeout
            .checked_sub(connect_timeout)
            .filter(|d| !d.is_zero())
            .ok_or_else(|| ScraperError::Timeout(err_msg.to_string()))
    }

    fn get_outer_timeout(timeout: Duration) -> Result<Duration, ScraperError> {
        timeout
            .checked_mul(2)
            .ok_or_else(|| ScraperError::Timeout("Outer timeout overflow".to_string()))
    }

    fn duration_as_playwright_millis(timeout: Duration) -> Result<u32, ScraperError> {
        timeout.as_millis().try_into().map_err(|_| {
            ScraperError::Timeout("Playwright timeout exceeds u32::MAX ms".to_string())
        })
    }

    fn playwright_tmp_root() -> Result<PathBuf, ScraperError> {
        match env::var_os(PLAYWRIGHT_TMP_ENV) {
            Some(path) => Ok(PathBuf::from(path)),
            None => Ok(env::current_dir()?.join("target").join(PLAYWRIGHT_TMP_DIR)),
        }
    }

    fn prepare_playwright_tmp_root(&self) -> Result<PathBuf, ScraperError> {
        let tmp_root = Self::playwright_tmp_root()?;
        fs::create_dir_all(&tmp_root)?;
        env::set_var("TMPDIR", &tmp_root);
        Ok(tmp_root)
    }

    fn playwright_request_tmp_dir(
        &self,
        url: &str,
    ) -> Result<PlaywrightRequestTempDir, ScraperError> {
        let tmp_root = self.prepare_playwright_tmp_root()?;
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let pid = std::process::id();
        let mut url_hash = 0_u64;
        for byte in url.as_bytes() {
            url_hash = url_hash.wrapping_mul(31).wrapping_add(u64::from(*byte));
        }
        let request_tmp_dir = tmp_root.join(format!("request-{pid}-{now_nanos}-{url_hash:x}"));
        fs::create_dir_all(&request_tmp_dir)?;
        Ok(PlaywrightRequestTempDir::new(request_tmp_dir))
    }

    async fn with_playwright_timeout<T, E, F>(
        &self,
        url: &str,
        operation: &str,
        timeout: Duration,
        future: F,
    ) -> Result<T, ScraperError>
    where
        F: Future<Output = Result<T, E>>,
        playwright_rust::Error: From<E>,
    {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| {
                ScraperError::Timeout(format!("Playwright {operation} timed out for {url}"))
            })?
            .map_err(|e| ScraperError::from(playwright_rust::Error::from(e)))
    }

    fn reborrow_scraper_proxy<'b>(
        scraper_proxy: &'b mut Option<&mut ScraperProxy<'a>>,
    ) -> Option<&'b mut ScraperProxy<'a>> {
        match scraper_proxy {
            Some(scraper_proxy) => Some(&mut **scraper_proxy),
            None => None,
        }
    }

    fn playwright_error_display_looks_recoverable(error: &playwright_rust::Error) -> bool {
        let error = error.to_string().to_ascii_lowercase();
        error.contains("object not found")
            || error.contains("target page, context or browser has been closed")
            || error.contains("browser has been closed")
            || error.contains("browser closed")
            || error.contains("browser disconnected")
            || error.contains("connection closed")
            || error.contains("channel closed")
            || error.contains("receiver closed")
            || error.contains("disconnected")
            || error.contains("execution context was destroyed")
            || error.contains("page crashed")
            || error.contains("navigation failed because page crashed")
            || error.contains("no space left on device")
            || error.contains("enospc")
            || error.contains("transport")
    }

    fn is_recoverable_playwright_lifecycle_error(error: &playwright_rust::Error) -> bool {
        match error {
            playwright_rust::Error::ObjectNotFound
            | playwright_rust::Error::ReceiverClosed
            | playwright_rust::Error::Channel
            | playwright_rust::Error::Transport(_)
            | playwright_rust::Error::CallbackNotFound => true,
            playwright_rust::Error::Io(e) => {
                matches!(
                    e.kind(),
                    std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::NotConnected
                        | std::io::ErrorKind::UnexpectedEof
                ) || {
                    let error = e.to_string().to_ascii_lowercase();
                    error.contains("no space left on device") || error.contains("enospc")
                }
            }
            playwright_rust::Error::Arc(e) => Self::is_recoverable_playwright_lifecycle_error(e),
            playwright_rust::Error::ErrorResponded(e) => {
                let error = e.to_string().to_ascii_lowercase();
                error.contains("object not found")
                    || error.contains("target page, context or browser has been closed")
                    || error.contains("browser has been closed")
                    || error.contains("browser closed")
                    || error.contains("browser disconnected")
                    || error.contains("connection closed")
                    || error.contains("channel closed")
                    || error.contains("receiver closed")
                    || error.contains("disconnected")
                    || error.contains("execution context was destroyed")
                    || error.contains("page crashed")
                    || error.contains("navigation failed because page crashed")
                    || error.contains("no space left on device")
                    || error.contains("enospc")
            }
            playwright_rust::Error::Event(e) => {
                let error = e.to_string().to_ascii_lowercase();
                error.contains("closed")
                    || error.contains("channel closed")
                    || error.contains("receiver closed")
            }
            _ => Self::playwright_error_display_looks_recoverable(error),
        }
    }

    fn is_stale_playwright_client_error(error: &ScraperError) -> bool {
        match error {
            ScraperError::Playwright(e) => Self::is_recoverable_playwright_lifecycle_error(e),
            _ => false,
        }
    }

    fn is_recoverable_playwright_scraper_error(error: &ScraperError) -> bool {
        Self::is_stale_playwright_client_error(error)
    }

    fn recoverable_playwright_cleanup_error() -> ScraperError {
        ScraperError::Playwright(playwright_rust::Error::ReceiverClosed)
    }

    fn error_after_playwright_cleanup(
        error: ScraperError,
        cleanup_left_client_suspect: bool,
    ) -> ScraperError {
        if cleanup_left_client_suspect && !Self::is_stale_playwright_client_error(&error) {
            Self::recoverable_playwright_cleanup_error()
        } else {
            error
        }
    }

    fn log_playwright_replacement(&self, url: &str, error: &ScraperError) {
        let warn_str = format!(
            "Replacing Playwright client for {url} after recoverable Playwright error: {error}"
        );
        self.logger.log_warn(&warn_str);
    }

    fn log_playwright_cleanup_suspect_temporary_client(&self, url: &str) {
        let warn_str = format!("Playwright cleanup left temporary client suspect for {url}");
        self.logger.log_warn(&warn_str);
    }

    fn log_playwright_cleanup_error(
        &self,
        url: &str,
        resource: &str,
        error: playwright_rust::Error,
    ) {
        let warn_str =
            format!("Playwright cleanup for {url} failed while closing {resource}: {error}");
        self.logger.log_warn(&warn_str);
    }

    async fn close_playwright_resources(
        &self,
        url: &str,
        cleanup_timeout: Duration,
        page: &Page,
        context: &BrowserContext,
        browser: &Browser,
    ) -> bool {
        let mut cleanup_left_client_suspect = false;
        match tokio::time::timeout(cleanup_timeout, page.close(None)).await {
            Ok(Err(e)) => {
                cleanup_left_client_suspect = true;
                self.log_playwright_cleanup_error(url, "page", e.into());
            }
            Err(_) => {
                cleanup_left_client_suspect = true;
                self.logger.log_warn(&format!(
                    "Playwright cleanup for {url} timed out while closing page"
                ));
            }
            Ok(Ok(())) => {}
        }
        match tokio::time::timeout(cleanup_timeout, context.close()).await {
            Ok(Err(e)) => {
                cleanup_left_client_suspect = true;
                self.log_playwright_cleanup_error(url, "context", e.into());
            }
            Err(_) => {
                cleanup_left_client_suspect = true;
                self.logger.log_warn(&format!(
                    "Playwright cleanup for {url} timed out while closing context"
                ));
            }
            Ok(Ok(())) => {}
        }
        match tokio::time::timeout(cleanup_timeout, browser.close()).await {
            Ok(Err(e)) => {
                cleanup_left_client_suspect = true;
                self.log_playwright_cleanup_error(url, "browser", e.into());
            }
            Err(_) => {
                cleanup_left_client_suspect = true;
                self.logger.log_warn(&format!(
                    "Playwright cleanup for {url} timed out while closing browser"
                ));
            }
            Ok(Ok(())) => {}
        }
        cleanup_left_client_suspect
    }

    async fn close_playwright_context_and_browser(
        &self,
        url: &str,
        cleanup_timeout: Duration,
        context: &BrowserContext,
        browser: &Browser,
    ) -> bool {
        let mut cleanup_left_client_suspect = false;
        match tokio::time::timeout(cleanup_timeout, context.close()).await {
            Ok(Err(e)) => {
                cleanup_left_client_suspect = true;
                self.log_playwright_cleanup_error(url, "context", e.into());
            }
            Err(_) => {
                cleanup_left_client_suspect = true;
                self.logger.log_warn(&format!(
                    "Playwright cleanup for {url} timed out while closing context"
                ));
            }
            Ok(Ok(())) => {}
        }
        cleanup_left_client_suspect
            | self
                .close_playwright_browser(url, cleanup_timeout, browser)
                .await
    }

    async fn close_playwright_browser(
        &self,
        url: &str,
        cleanup_timeout: Duration,
        browser: &Browser,
    ) -> bool {
        match tokio::time::timeout(cleanup_timeout, browser.close()).await {
            Ok(Err(e)) => {
                self.log_playwright_cleanup_error(url, "browser", e.into());
                true
            }
            Err(_) => {
                self.logger.log_warn(&format!(
                    "Playwright cleanup for {url} timed out while closing browser"
                ));
                true
            }
            Ok(Ok(())) => false,
        }
    }

    pub async fn get_playwright_client(&self) -> Result<Playwright, ScraperError> {
        self.prepare_playwright_tmp_root()?;
        let playwright_client = Playwright::initialize().await?;
        playwright_client.prepare()?;
        let debug_str = "Playwright client initialized";
        self.logger.log_debug(debug_str);
        Ok(playwright_client)
    }

    pub fn get_playwright_js_client(&self) -> Result<PlaywrightClient, ScraperError> {
        let playwright_client = PlaywrightClient::new()?;
        playwright_client.init()?;
        let debug_str = "Playwright js client initialized";
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

    pub async fn post_with_reqwest(
        &self,
        url: &str,
        request_options: &RequestOptions,
        json_payload: &serde_json::Value,
        scraper_proxy: Option<&mut ScraperProxy<'a>>,
        gateway: Option<&ApiGateway>,
    ) -> Result<Response, ScraperError> {
        let debug_log = format!("Attempting to make a POST request to {} with reqwest", url);
        self.logger.log_debug(&debug_log);
        let cert = reqwest::Certificate::from_pem(LETSENCRYPT_R13_CERT)?;
        let read_timeout = Self::get_read_timeout(
            request_options.timeout,
            request_options.connect_timeout,
            "Invalid reqwest timeout config. timeout must be greater than connect_timeout",
        )?;
        let outer_timeout = Self::get_outer_timeout(request_options.timeout)?;
        let mut client_builder = reqwest::ClientBuilder::new()
            .add_root_certificate(cert)
            .connect_timeout(request_options.connect_timeout)
            .timeout(request_options.timeout)
            .read_timeout(read_timeout);
        if let Some(headers) = &request_options.headers {
            client_builder = client_builder.default_headers(headers.clone());
        }
        let response = if let Some(api_gateway) = gateway {
            let client = client_builder.build()?;
            let request = client.post(url).json(json_payload).build()?;
            tokio::time::timeout(outer_timeout, api_gateway.reqwest_send(&client, request))
                .await
                .map_err(|_| ScraperError::Timeout(format!("reqwest send timed out for {url}")))?
                .map_err(ScraperError::from)?
        } else if let Some(scraper_proxy) = scraper_proxy {
            let proxy_result = scraper_proxy.generate_proxy().await?;
            let proxy = proxy_result.get_reqwest_proxy()?;
            client_builder = client_builder.proxy(proxy);
            let response = tokio::time::timeout(
                outer_timeout,
                client_builder.build()?.post(url).json(json_payload).send(),
            )
            .await
            .map_err(|_| ScraperError::Timeout(format!("reqwest send timed out for {url}")))?
            .map_err(|e| {
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
            if request_options.proxy_block_count > 0
                && response.status() == reqwest::StatusCode::FORBIDDEN
            {
                scraper_proxy.add_proxy_block_count(&proxy_result);
            };
            response
        } else {
            tokio::time::timeout(
                outer_timeout,
                client_builder.build()?.post(url).json(json_payload).send(),
            )
            .await
            .map_err(|_| ScraperError::Timeout(format!("reqwest send timed out for {url}")))??
        };
        Response::from_reqwest_response(response, request_options.timeout).await
    }

    pub async fn post_with_rquest(
        &self,
        url: &str,
        request_options: &RequestOptions,
        json_payload: &serde_json::Value,
        client: &wreq::Client,
        scraper_proxy: Option<&mut ScraperProxy<'a>>,
        api_gateway: Option<&ApiGateway>,
    ) -> Result<Response, ScraperError> {
        let debug_log = format!("Attempting to make a POST request to {} with rquest", url);
        self.logger.log_debug(&debug_log);
        let outer_timeout = Self::get_outer_timeout(request_options.timeout)?;
        let mut request_builder = client.post(url).json(json_payload);
        if let Some(headers) = &request_options.headers {
            request_builder = request_builder
                .headers(headers.clone())
                .timeout(request_options.timeout);
        }
        let response = if let Some(api_gateway) = api_gateway {
            let request = request_builder.build()?;
            tokio::time::timeout(outer_timeout, api_gateway.rquest_send(client, request))
                .await
                .map_err(|_| ScraperError::Timeout(format!("rquest send timed out for {url}")))?
                .map_err(ScraperError::from)?
        } else if let Some(scraper_proxy) = scraper_proxy {
            let proxy_result = scraper_proxy.generate_proxy().await?;
            let proxy = proxy_result.get_rquest_proxy()?;
            request_builder = request_builder.proxy(proxy);
            let response = tokio::time::timeout(outer_timeout, request_builder.send())
                .await
                .map_err(|_| ScraperError::Timeout(format!("rquest send timed out for {url}")))?
                .map_err(|e| {
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
            if request_options.proxy_block_count > 0
                && response.status() == wreq::StatusCode::FORBIDDEN
            {
                scraper_proxy.add_proxy_block_count(&proxy_result);
            };
            response
        } else {
            tokio::time::timeout(outer_timeout, request_builder.send())
                .await
                .map_err(|_| ScraperError::Timeout(format!("rquest send timed out for {url}")))??
        };
        Response::from_rquest_response(response, request_options.timeout).await
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
        let cert = reqwest::Certificate::from_pem(LETSENCRYPT_R13_CERT)?;
        let read_timeout = Self::get_read_timeout(
            request_options.timeout,
            request_options.connect_timeout,
            "Invalid reqwest timeout config. timeout must be greater than connect_timeout",
        )?;
        let outer_timeout = Self::get_outer_timeout(request_options.timeout)?;
        let mut client_builder = reqwest::ClientBuilder::new()
            .add_root_certificate(cert)
            .connect_timeout(request_options.connect_timeout)
            .timeout(request_options.timeout)
            .read_timeout(read_timeout);
        if let Some(headers) = &request_options.headers {
            client_builder = client_builder.default_headers(headers.clone());
        }
        let response = if let Some(api_gateway) = gateway {
            let client = client_builder.build()?;
            let request = client.get(url).build()?;
            tokio::time::timeout(outer_timeout, api_gateway.reqwest_send(&client, request))
                .await
                .map_err(|_| ScraperError::Timeout(format!("reqwest send timed out for {url}")))?
                .map_err(ScraperError::from)?
        } else if let Some(scraper_proxy) = scraper_proxy {
            let proxy_result = scraper_proxy.generate_proxy().await?;
            let proxy = proxy_result.get_reqwest_proxy()?;
            client_builder = client_builder.proxy(proxy);
            let response =
                tokio::time::timeout(outer_timeout, client_builder.build()?.get(url).send())
                    .await
                    .map_err(|_| {
                        ScraperError::Timeout(format!("reqwest send timed out for {url}"))
                    })?
                    .map_err(|e| {
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
            if request_options.proxy_block_count > 0
                && response.status() == reqwest::StatusCode::FORBIDDEN
            {
                scraper_proxy.add_proxy_block_count(&proxy_result);
            };
            response
        } else {
            tokio::time::timeout(outer_timeout, client_builder.build()?.get(url).send())
                .await
                .map_err(|_| ScraperError::Timeout(format!("reqwest send timed out for {url}")))??
        };
        Response::from_reqwest_response(response, request_options.timeout).await
    }

    pub async fn request_with_rquest<'b>(
        &self,
        url: &str,
        request_options: &RequestOptions,
        client: &wreq::Client,
        scraper_proxy: Option<&mut ScraperProxy<'b>>,
        api_gateway: Option<&ApiGateway>,
    ) -> Result<Response, ScraperError> {
        let debug_log = format!("Attempting to make a request to {} with rquest", url);
        self.logger.log_debug(&debug_log);
        let outer_timeout = Self::get_outer_timeout(request_options.timeout)?;
        let mut request_builder = client.get(url);
        if let Some(headers) = &request_options.headers {
            request_builder = request_builder
                .headers(headers.clone())
                .timeout(request_options.timeout);
        }
        let response = if let Some(api_gateway) = api_gateway {
            let request = request_builder.build()?;
            tokio::time::timeout(outer_timeout, api_gateway.rquest_send(client, request))
                .await
                .map_err(|_| ScraperError::Timeout(format!("rquest send timed out for {url}")))?
                .map_err(ScraperError::from)?
        } else if let Some(scraper_proxy) = scraper_proxy {
            let proxy_result = scraper_proxy.generate_proxy().await?;
            let proxy = proxy_result.get_rquest_proxy()?;
            request_builder = request_builder.proxy(proxy);
            let response = tokio::time::timeout(outer_timeout, request_builder.send())
                .await
                .map_err(|_| ScraperError::Timeout(format!("rquest send timed out for {url}")))?
                .map_err(|e| {
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
            if request_options.proxy_block_count > 0
                && response.status() == wreq::StatusCode::FORBIDDEN
            {
                scraper_proxy.add_proxy_block_count(&proxy_result);
                scraper_proxy.clear_sticky_proxy();
            };
            response
        } else {
            tokio::time::timeout(outer_timeout, request_builder.send())
                .await
                .map_err(|_| ScraperError::Timeout(format!("rquest send timed out for {url}")))??
        };
        Response::from_rquest_response(response, request_options.timeout).await
    }

    pub async fn request_with_playwright(
        &self,
        url: &str,
        request_options: &RequestOptions,
        browser_options: &BrowseOptions,
        playwright: &Playwright,
        mut scraper_proxy: Option<&mut ScraperProxy<'a>>,
    ) -> Result<Response, ScraperError> {
        let debug_log = format!("Attempting to make a request to {} with playwright", url);
        self.logger.log_debug(&debug_log);
        let first_result = self
            .request_with_playwright_once(
                url,
                request_options,
                browser_options,
                playwright,
                Self::reborrow_scraper_proxy(&mut scraper_proxy),
            )
            .await;

        match first_result {
            Err(e) if Self::is_recoverable_playwright_scraper_error(&e) => {
                let warn_str = format!(
                    "Using a fresh temporary Playwright client for {url} after recoverable Playwright error: {e}"
                );
                self.logger.log_warn(&warn_str);
                let refreshed_playwright = self.get_playwright_client().await?;
                self.request_with_playwright_once(
                    url,
                    request_options,
                    browser_options,
                    &refreshed_playwright,
                    Self::reborrow_scraper_proxy(&mut scraper_proxy),
                )
                .await
                .map(|(response, _)| response)
            }
            Ok((response, cleanup_left_client_suspect)) => {
                if cleanup_left_client_suspect {
                    self.log_playwright_cleanup_suspect_temporary_client(url);
                }
                Ok(response)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn request_with_playwright_refreshing(
        &self,
        url: &str,
        request_options: &RequestOptions,
        browser_options: &BrowseOptions,
        playwright: &mut Playwright,
        mut scraper_proxy: Option<&mut ScraperProxy<'a>>,
    ) -> Result<Response, ScraperError> {
        let debug_log = format!("Attempting to make a request to {} with playwright", url);
        self.logger.log_debug(&debug_log);
        let first_result = self
            .request_with_playwright_once(
                url,
                request_options,
                browser_options,
                playwright,
                Self::reborrow_scraper_proxy(&mut scraper_proxy),
            )
            .await;

        match first_result {
            Err(e) if Self::is_recoverable_playwright_scraper_error(&e) => {
                self.log_playwright_replacement(url, &e);
                *playwright = self.get_playwright_client().await?;
                self.request_with_playwright_once(
                    url,
                    request_options,
                    browser_options,
                    playwright,
                    Self::reborrow_scraper_proxy(&mut scraper_proxy),
                )
                .await
                .map(|(response, _)| response)
            }
            Ok((response, cleanup_left_client_suspect)) => {
                if cleanup_left_client_suspect {
                    let cleanup_error = Self::recoverable_playwright_cleanup_error();
                    self.log_playwright_replacement(url, &cleanup_error);
                    *playwright = self.get_playwright_client().await?;
                }
                Ok(response)
            }
            Err(e) => Err(e),
        }
    }

    async fn request_with_playwright_once(
        &self,
        url: &str,
        request_options: &RequestOptions,
        browser_options: &BrowseOptions,
        playwright: &Playwright,
        mut scraper_proxy: Option<&mut ScraperProxy<'a>>,
    ) -> Result<(Response, bool), ScraperError> {
        let request_tmp_dir = self.playwright_request_tmp_dir(url)?;
        let downloads_dir = request_tmp_dir.downloads_path();
        fs::create_dir_all(&downloads_dir)?;

        let chromium = playwright.chromium();
        let read_timeout = Self::get_read_timeout(
            request_options.timeout,
            request_options.connect_timeout,
            "Invalid playwright timeout config. timeout must be greater than connect_timeout",
        )?;
        let navigation_timeout = Self::duration_as_playwright_millis(request_options.timeout)?;
        let default_timeout = Self::duration_as_playwright_millis(read_timeout)?;
        let args = [
            "--disable-blink-features=AutomationControlled".to_string(),
            "--disable-features=IsolateOrigins,site-per-process".to_string(),
            "--no-sandbox".to_string(),
            "--disable-setuid-sandbox".to_string(),
            "--disable-dev-shm-usage".to_string(),
            "--disable-web-security".to_string(),
            "--disable-features=VizDisplayCompositor".to_string(),
            "--disable-background-networking".to_string(),
            "--disable-background-timer-throttling".to_string(),
            "--disable-backgrounding-occluded-windows".to_string(),
            "--disable-breakpad".to_string(),
            "--disable-client-side-phishing-detection".to_string(),
            "--disable-component-extensions-with-background-pages".to_string(),
            "--disable-default-apps".to_string(),
            "--disable-remote-debugging".to_string(),
            "--disable-extensions".to_string(),
            "--disable-features=TranslateUI".to_string(),
            "--disable-hang-monitor".to_string(),
            "--disable-ipc-flooding-protection".to_string(),
            "--disable-popup-blocking".to_string(),
            "--disable-prompt-on-repost".to_string(),
            "--disable-renderer-backgrounding".to_string(),
            "--disable-sync".to_string(),
            "--force-color-profile=srgb".to_string(),
            "--metrics-recording-only".to_string(),
            "--no-first-run".to_string(),
            "--enable-automation=false".to_string(),
            "--password-store=basic".to_string(),
            "--use-mock-keychain".to_string(),
            "--window-size=1920,1080".to_string(),
            "--start-maximized".to_string(),
            "--disable-gpu".to_string(),
            "--disable-software-rasterizer".to_string(),
        ];
        let mut browser = chromium
            .launcher()
            .timeout(request_options.connect_timeout.as_millis() as f64)
            .headless(browser_options.headless)
            .downloads(&downloads_dir)
            .args(&args);
        let proxy_result = if let Some(scraper_proxy) = scraper_proxy.as_deref_mut() {
            let proxy_result = scraper_proxy.generate_proxy().await?;
            let proxy = proxy_result.get_playwright_proxy();
            browser = browser.proxy(proxy);
            Some(proxy_result)
        } else {
            None
        };
        let browser = self
            .with_playwright_timeout(
                url,
                "browser launch",
                request_options.connect_timeout,
                browser.launch(),
            )
            .await?;
        let context = match self
            .with_playwright_timeout(
                url,
                "context creation",
                read_timeout,
                browser
                    .context_builder()
                    .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
                    .viewport(Some(Viewport { width: 1920, height: 1080 }))
                    .locale("en-GB")
                    .timezone_id("Europe/London")
                    .build(),
            )
            .await
        {
            Ok(context) => context,
            Err(e) => {
                let cleanup_left_client_suspect = self
                    .close_playwright_browser(url, request_options.connect_timeout, &browser)
                    .await;
                return Err(Self::error_after_playwright_cleanup(
                    e,
                    cleanup_left_client_suspect,
                ));
            }
        };
        let page = match self
            .with_playwright_timeout(url, "page creation", read_timeout, context.new_page())
            .await
        {
            Ok(page) => page,
            Err(e) => {
                let cleanup_left_client_suspect = self
                    .close_playwright_context_and_browser(
                        url,
                        request_options.connect_timeout,
                        &context,
                        &browser,
                    )
                    .await;
                return Err(Self::error_after_playwright_cleanup(
                    e,
                    cleanup_left_client_suspect,
                ));
            }
        };
        if let Err(e) = self
            .with_playwright_timeout(
                url,
                "default context timeout setup",
                read_timeout,
                context.set_default_timeout(default_timeout),
            )
            .await
        {
            let cleanup_left_client_suspect = self
                .close_playwright_resources(
                    url,
                    request_options.connect_timeout,
                    &page,
                    &context,
                    &browser,
                )
                .await;
            return Err(Self::error_after_playwright_cleanup(
                e,
                cleanup_left_client_suspect,
            ));
        }
        if let Err(e) = self
            .with_playwright_timeout(
                url,
                "default context navigation timeout setup",
                read_timeout,
                context.set_default_navigation_timeout(navigation_timeout),
            )
            .await
        {
            let cleanup_left_client_suspect = self
                .close_playwright_resources(
                    url,
                    request_options.connect_timeout,
                    &page,
                    &context,
                    &browser,
                )
                .await;
            return Err(Self::error_after_playwright_cleanup(
                e,
                cleanup_left_client_suspect,
            ));
        }
        if let Some(header_map) = request_options.convert_header_map_to_map() {
            if let Err(e) = self
                .with_playwright_timeout(
                    url,
                    "extra HTTP headers setup",
                    read_timeout,
                    page.set_extra_http_headers(header_map),
                )
                .await
            {
                let cleanup_left_client_suspect = self
                    .close_playwright_resources(
                        url,
                        request_options.connect_timeout,
                        &page,
                        &context,
                        &browser,
                    )
                    .await;
                return Err(Self::error_after_playwright_cleanup(
                    e,
                    cleanup_left_client_suspect,
                ));
            }
        }
        if let Err(e) = self
            .with_playwright_timeout(
                url,
                "init script setup",
                read_timeout,
                page.add_init_script(STEALTH_INIT_SCRIPT),
            )
            .await
        {
            let cleanup_left_client_suspect = self
                .close_playwright_resources(
                    url,
                    request_options.connect_timeout,
                    &page,
                    &context,
                    &browser,
                )
                .await;
            return Err(Self::error_after_playwright_cleanup(
                e,
                cleanup_left_client_suspect,
            ));
        }
        let goto_result = self
            .with_playwright_timeout(
                url,
                "navigation",
                request_options.timeout,
                page.goto_builder(url)
                    .timeout(request_options.timeout.as_millis() as f64)
                    .goto(),
            )
            .await;
        match goto_result {
            Ok(response) => {
                let cookies = match self
                    .with_playwright_timeout(url, "cookie read", read_timeout, context.cookies(&[]))
                    .await
                {
                    Ok(cookies) => cookies,
                    Err(e) => {
                        let cleanup_left_client_suspect = self
                            .close_playwright_resources(
                                url,
                                request_options.connect_timeout,
                                &page,
                                &context,
                                &browser,
                            )
                            .await;
                        return Err(Self::error_after_playwright_cleanup(
                            e,
                            cleanup_left_client_suspect,
                        ));
                    }
                }
                .iter()
                .map(|c| (c.name.to_string(), c.value.to_string()))
                .collect();
                if let Some(response) = response {
                    if let Some(page_evaluation) = &browser_options.page_evaluation {
                        if let Err(e) = self
                            .with_playwright_timeout(
                                url,
                                "page evaluation",
                                read_timeout,
                                page.eval::<()>(page_evaluation),
                            )
                            .await
                        {
                            let cleanup_left_client_suspect = self
                                .close_playwright_resources(
                                    url,
                                    request_options.connect_timeout,
                                    &page,
                                    &context,
                                    &browser,
                                )
                                .await;
                            return Err(Self::error_after_playwright_cleanup(
                                e,
                                cleanup_left_client_suspect,
                            ));
                        }
                    }
                    time_operation::async_sleep(browser_options.browser_wait).await;
                    let content = match self
                        .with_playwright_timeout(url, "content read", read_timeout, page.content())
                        .await
                    {
                        Ok(content) => content,
                        Err(e) => {
                            let cleanup_left_client_suspect = self
                                .close_playwright_resources(
                                    url,
                                    request_options.connect_timeout,
                                    &page,
                                    &context,
                                    &browser,
                                )
                                .await;
                            return Err(Self::error_after_playwright_cleanup(
                                e,
                                cleanup_left_client_suspect,
                            ));
                        }
                    };
                    let status_code = match response.status() {
                        Ok(status_code) => status_code as u16,
                        Err(e) => {
                            let cleanup_left_client_suspect = self
                                .close_playwright_resources(
                                    url,
                                    request_options.connect_timeout,
                                    &page,
                                    &context,
                                    &browser,
                                )
                                .await;
                            return Err(Self::error_after_playwright_cleanup(
                                ScraperError::from(e),
                                cleanup_left_client_suspect,
                            ));
                        }
                    };
                    let final_url = match page.url() {
                        Ok(final_url) => final_url,
                        Err(e) => {
                            let cleanup_left_client_suspect = self
                                .close_playwright_resources(
                                    url,
                                    request_options.connect_timeout,
                                    &page,
                                    &context,
                                    &browser,
                                )
                                .await;
                            return Err(Self::error_after_playwright_cleanup(
                                ScraperError::from(e),
                                cleanup_left_client_suspect,
                            ));
                        }
                    };
                    let ok = match response.ok() {
                        Ok(ok) => ok,
                        Err(e) => {
                            let cleanup_left_client_suspect = self
                                .close_playwright_resources(
                                    url,
                                    request_options.connect_timeout,
                                    &page,
                                    &context,
                                    &browser,
                                )
                                .await;
                            return Err(Self::error_after_playwright_cleanup(
                                ScraperError::from(e),
                                cleanup_left_client_suspect,
                            ));
                        }
                    };
                    let reason = match response.status_text() {
                        Ok(reason) => reason,
                        Err(e) => {
                            let cleanup_left_client_suspect = self
                                .close_playwright_resources(
                                    url,
                                    request_options.connect_timeout,
                                    &page,
                                    &context,
                                    &browser,
                                )
                                .await;
                            return Err(Self::error_after_playwright_cleanup(
                                ScraperError::from(e),
                                cleanup_left_client_suspect,
                            ));
                        }
                    };
                    if request_options.proxy_block_count > 0 && status_code == 403 {
                        if let (Some(scraper_proxy), Some(proxy_result)) =
                            (scraper_proxy.as_mut(), proxy_result.as_ref())
                        {
                            scraper_proxy.add_proxy_block_count(proxy_result);
                        }
                    }
                    let response = Response {
                        content,
                        status_code,
                        url: final_url,
                        ok,
                        reason,
                        cookies,
                    };
                    let cleanup_left_client_suspect = self
                        .close_playwright_resources(
                            url,
                            request_options.connect_timeout,
                            &page,
                            &context,
                            &browser,
                        )
                        .await;
                    Ok((response, cleanup_left_client_suspect))
                } else {
                    let cleanup_left_client_suspect = self
                        .close_playwright_resources(
                            url,
                            request_options.connect_timeout,
                            &page,
                            &context,
                            &browser,
                        )
                        .await;
                    let error =
                        ScraperError::Other(format!("No response from playwright for url {url}"));
                    Err(Self::error_after_playwright_cleanup(
                        error,
                        cleanup_left_client_suspect,
                    ))
                }
            }
            Err(e) => {
                let cleanup_left_client_suspect = self
                    .close_playwright_resources(
                        url,
                        request_options.connect_timeout,
                        &page,
                        &context,
                        &browser,
                    )
                    .await;
                Err(Self::error_after_playwright_cleanup(
                    e,
                    cleanup_left_client_suspect,
                ))
            }
        }
    }

    pub async fn request_with_playwright_js(
        &self,
        url: &str,
        request_options: &RequestOptions,
        playwright: &PlaywrightClient,
        scraper_proxy: Option<&mut ScraperProxy<'a>>,
    ) -> Result<Response, ScraperError> {
        let debug_log = format!("Attempting to make a request to {} with playwright js", url);
        self.logger.log_debug(&debug_log);
        let headers = request_options.convert_header_map_to_map();
        if let Some(scraper_proxy) = scraper_proxy {
            let proxy_result = scraper_proxy.generate_proxy().await?;
            let proxy = proxy_result.get_playwright_proxy();
            let context_id = playwright.create_context(Some(proxy), headers)?;
            match playwright.navigate(
                &context_id,
                url,
                Some(request_options.timeout.as_millis() as u64),
            ) {
                Ok(response) => {
                    if request_options.proxy_block_count > 0 && response.status_code == 403 {
                        scraper_proxy.add_proxy_block_count(&proxy_result);
                    }
                    playwright.close_context(&context_id)?;
                    Ok(response)
                }
                Err(e) => {
                    playwright.close_context(&context_id)?;
                    Err(e)
                }
            }
        } else {
            let context_id = playwright.create_context(None, headers)?;
            match playwright.navigate(
                &context_id,
                url,
                Some(request_options.timeout.as_millis() as u64),
            ) {
                Ok(response) => {
                    playwright.close_context(&context_id)?;
                    Ok(response)
                }
                Err(e) => {
                    playwright.close_context(&context_id)?;
                    Err(e)
                }
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
        let request_tmp_dir = self.playwright_request_tmp_dir(url)?;
        let downloads_dir = request_tmp_dir.downloads_path();
        fs::create_dir_all(&downloads_dir)?;
        let chromium = playwright.chromium();
        let mut browser = chromium
            .launcher()
            .timeout(request_options.timeout.as_millis() as f64)
            .headless(browser_options.headless)
            .downloads(&downloads_dir);
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
    use std::{env, io, path::Path, sync::Arc, time::Duration};

    use super::*;
    use log::LevelFilter;

    #[test]
    fn test_playwright_lifecycle_error_classification() {
        assert!(SourceScraper::is_recoverable_playwright_lifecycle_error(
            &playwright_rust::Error::ObjectNotFound
        ));
        assert!(SourceScraper::is_recoverable_playwright_lifecycle_error(
            &playwright_rust::Error::ReceiverClosed
        ));
        assert!(SourceScraper::is_recoverable_playwright_lifecycle_error(
            &playwright_rust::Error::Channel
        ));
        assert!(!SourceScraper::is_recoverable_playwright_lifecycle_error(
            &playwright_rust::Error::Timeout
        ));
        assert!(SourceScraper::is_recoverable_playwright_lifecycle_error(
            &playwright_rust::Error::Io(io::Error::from(io::ErrorKind::BrokenPipe))
        ));
        assert!(SourceScraper::is_recoverable_playwright_lifecycle_error(
            &playwright_rust::Error::Io(io::Error::from(io::ErrorKind::NotConnected))
        ));
        assert!(!SourceScraper::is_recoverable_playwright_lifecycle_error(
            &playwright_rust::Error::InvalidParams
        ));
    }

    #[test]
    fn test_playwright_stale_client_error_classification() {
        let stale_client_error = ScraperError::Playwright(playwright_rust::Error::ObjectNotFound);
        assert!(SourceScraper::is_stale_playwright_client_error(
            &stale_client_error
        ));
        assert!(SourceScraper::is_recoverable_playwright_scraper_error(
            &stale_client_error
        ));

        let lifecycle_error = ScraperError::Playwright(playwright_rust::Error::ReceiverClosed);
        assert!(SourceScraper::is_stale_playwright_client_error(
            &lifecycle_error
        ));
        assert!(SourceScraper::is_recoverable_playwright_scraper_error(
            &lifecycle_error
        ));
    }

    #[test]
    fn test_playwright_wrapped_object_not_found_is_stale() {
        let wrapped_object_not_found = ScraperError::Playwright(playwright_rust::Error::Arc(
            Arc::new(playwright_rust::Error::ObjectNotFound),
        ));

        assert!(SourceScraper::is_stale_playwright_client_error(
            &wrapped_object_not_found
        ));
        assert!(wrapped_object_not_found
            .to_string()
            .contains("Object not found"));
    }

    #[test]
    fn test_playwright_page_crash_is_stale() {
        let page_crash = ScraperError::Playwright(playwright_rust::Error::GuidNotFound(
            serde_json::json!("Navigation failed because page crashed!"),
        ));

        assert!(SourceScraper::is_stale_playwright_client_error(&page_crash));
    }

    #[test]
    fn test_playwright_enospc_is_stale() {
        let enospc = ScraperError::Playwright(playwright_rust::Error::Io(io::Error::other(
            "ENOSPC: no space left on device, mkdtemp '/tmp/playwright_downloads-test'",
        )));

        assert!(SourceScraper::is_stale_playwright_client_error(&enospc));
    }

    #[test]
    fn test_non_playwright_errors_are_not_stale() {
        let non_playwright_error =
            ScraperError::Other("Playwright error: Object not found".to_string());

        assert!(!SourceScraper::is_stale_playwright_client_error(
            &non_playwright_error
        ));
    }

    #[test]
    fn test_playwright_timeout_config() {
        let read_timeout = SourceScraper::get_read_timeout(
            Duration::from_secs(15),
            Duration::from_secs(5),
            "invalid",
        )
        .unwrap();
        assert_eq!(read_timeout, Duration::from_secs(10));

        assert!(SourceScraper::get_read_timeout(
            Duration::from_secs(5),
            Duration::from_secs(5),
            "invalid",
        )
        .is_err());
    }

    #[tokio::test]
    async fn test_reqwest() {
        let logger_name = "test_scraping";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let secret = Secret::new(&project_logger).await;
        let scraper = SourceScraper::new(&project_logger, &secret);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions::default();
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
        let mut scraper_proxy = scraper.get_scraper_proxy().await.unwrap();
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
        let secret = Secret::new(&project_logger).await;
        let scraper = SourceScraper::new(&project_logger, &secret);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions::default();
        let rquest_client = scraper.get_rquest_client(&request_options).unwrap();
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
        let mut scraper_proxy = scraper.get_scraper_proxy().await.unwrap();
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
    async fn test_playwright() {
        let logger_name = "test_scraping";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_netdata");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let secret = Secret::new(&project_logger).await;
        let scraper = SourceScraper::new(&project_logger, &secret);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions::default();
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
        let mut scraper_proxy = scraper.get_scraper_proxy().await.unwrap();
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
        let secret = Secret::new(&project_logger).await;
        let scraper = SourceScraper::new(&project_logger, &secret);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions::default();
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
        let secret = Secret::new(&project_logger).await;
        let scraper = SourceScraper::new(&project_logger, &secret);
        let url = "https://browserleaks.com/ip";
        let request_options = RequestOptions::default();
        let (original_domain, new_domain) = scraper.get_update_domain(url, &request_options).await;
        dbg!(original_domain);
        dbg!(new_domain);
    }
}
