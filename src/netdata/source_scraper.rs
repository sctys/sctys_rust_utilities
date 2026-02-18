use std::{collections::HashMap, time::Duration};

use playwright_rust::{api::Viewport, Playwright};
use rquest_util::Emulation;

use crate::{
    logger::ProjectLogger, netdata::playwright_js_client::PlaywrightClient,
    secret::aws_secret::Secret, time_operation,
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
const STEALTH_INIT_SCRIPT: &str = r#"
(() => {
    // 1. Hide webdriver property
    try {
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined,
            configurable: true
        });
        delete navigator.__proto__.webdriver;
    } catch(e) {}

    // 2. Full chrome object matching real Chrome
    window.chrome = {
        app: {
            isInstalled: false,
            InstallState: { DISABLED: 'disabled', INSTALLED: 'installed', NOT_INSTALLED: 'not_installed' },
            RunningState: { CANNOT_RUN: 'cannot_run', READY_TO_RUN: 'ready_to_run', RUNNING: 'running' }
        },
        runtime: {
            OnInstalledReason: { CHROME_UPDATE: 'chrome_update', INSTALL: 'install', SHARED_MODULE_UPDATE: 'shared_module_update', UPDATE: 'update' },
            OnRestartRequiredReason: { APP_UPDATE: 'app_update', OS_UPDATE: 'os_update', PERIODIC: 'periodic' },
            PlatformArch: { ARM: 'arm', ARM64: 'arm64', MIPS: 'mips', MIPS64: 'mips64', X86_32: 'x86-32', X86_64: 'x86-64' },
            PlatformNaclArch: { ARM: 'arm', MIPS: 'mips', MIPS64: 'mips64', X86_32: 'x86-32', X86_64: 'x86-64' },
            PlatformOs: { ANDROID: 'android', CROS: 'cros', LINUX: 'linux', MAC: 'mac', OPENBSD: 'openbsd', WIN: 'win' },
            RequestUpdateCheckStatus: { NO_UPDATE: 'no_update', THROTTLED: 'throttled', UPDATE_AVAILABLE: 'update_available' }
        },
        loadTimes: function() { return {}; },
        csi: function() { return {}; }
    };

    // 3. Permissions API
    try {
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.__proto__.query = function(parameters) {
            if (parameters.name === 'notifications') {
                return Promise.resolve({ state: Notification.permission });
            }
            return originalQuery.call(this, parameters);
        };
    } catch(e) {}

    // 4. Realistic plugins
    try {
        Object.defineProperty(navigator, 'plugins', {
            get: () => {
                return [
                    {
                        0: {type: 'application/x-google-chrome-pdf', suffixes: 'pdf', description: 'Portable Document Format'},
                        description: 'Portable Document Format',
                        filename: 'internal-pdf-viewer',
                        length: 1,
                        name: 'Chrome PDF Plugin'
                    },
                    {
                        0: {type: 'application/pdf', suffixes: 'pdf', description: ''},
                        description: '',
                        filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai',
                        length: 1,
                        name: 'Chrome PDF Viewer'
                    },
                    {
                        0: {type: 'application/x-nacl', suffixes: '', description: 'Native Client Executable'},
                        1: {type: 'application/x-pnacl', suffixes: '', description: 'Portable Native Client Executable'},
                        description: '',
                        filename: 'internal-nacl-plugin',
                        length: 2,
                        name: 'Native Client'
                    }
                ];
            },
            configurable: true
        });
    } catch(e) {}

    // 5. MimeTypes (empty = bot signal)
    try {
        Object.defineProperty(navigator, 'mimeTypes', {
            get: () => {
                return [
                    { type: 'application/pdf', suffixes: 'pdf', description: '' },
                    { type: 'application/x-google-chrome-pdf', suffixes: 'pdf', description: 'Portable Document Format' },
                    { type: 'application/x-nacl', suffixes: '', description: 'Native Client Executable' },
                    { type: 'application/x-pnacl', suffixes: '', description: 'Portable Native Client Executable' }
                ];
            },
            configurable: true
        });
    } catch(e) {}

    // 6. Languages
    try {
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-GB', 'en', 'en-US'],
            configurable: true
        });
    } catch(e) {}

    // 7. Hardware concurrency
    try {
        Object.defineProperty(navigator, 'hardwareConcurrency', {
            get: () => 8,
            configurable: true
        });
    } catch(e) {}

    // 8. Device memory
    try {
        Object.defineProperty(navigator, 'deviceMemory', {
            get: () => 8,
            configurable: true
        });
    } catch(e) {}

    // 9. Platform
    try {
        Object.defineProperty(navigator, 'platform', {
            get: () => 'Linux x86_64',
            configurable: true
        });
    } catch(e) {}

    // 10. Vendor
    try {
        Object.defineProperty(navigator, 'vendor', {
            get: () => 'Google Inc.',
            configurable: true
        });
    } catch(e) {}

    // 11. maxTouchPoints (desktop = 0)
    try {
        Object.defineProperty(navigator, 'maxTouchPoints', {
            get: () => 0,
            configurable: true
        });
    } catch(e) {}

    // 12. Network connection
    try {
        Object.defineProperty(navigator, 'connection', {
            get: () => ({
                effectiveType: '4g',
                rtt: 50,
                downlink: 10,
                saveData: false,
                type: 'wifi',
                addEventListener: () => {},
                removeEventListener: () => {}
            }),
            configurable: true
        });
    } catch(e) {}

    // 13. Fix outerWidth/outerHeight (0 in headless)
    try {
        if (window.outerWidth === 0) {
            Object.defineProperty(window, 'outerWidth', { get: () => window.innerWidth, configurable: true });
        }
        if (window.outerHeight === 0) {
            Object.defineProperty(window, 'outerHeight', { get: () => window.innerHeight, configurable: true });
        }
    } catch(e) {}

    // 14. Screen properties
    try {
        Object.defineProperty(screen, 'availWidth', { get: () => 1920, configurable: true });
        Object.defineProperty(screen, 'availHeight', { get: () => 1080, configurable: true });
        Object.defineProperty(screen, 'width', { get: () => 1920, configurable: true });
        Object.defineProperty(screen, 'height', { get: () => 1080, configurable: true });
        Object.defineProperty(screen, 'colorDepth', { get: () => 24, configurable: true });
        Object.defineProperty(screen, 'pixelDepth', { get: () => 24, configurable: true });
    } catch(e) {}

    // 15. Canvas fingerprint noise
    try {
        const origToDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function(type, ...args) {
            const ctx2d = this.getContext('2d');
            if (ctx2d) {
                const imgData = ctx2d.getImageData(0, 0, this.width, this.height);
                for (let i = 0; i < imgData.data.length; i += 4) {
                    imgData.data[i] = imgData.data[i] ^ (Math.random() * 2 | 0);
                }
                ctx2d.putImageData(imgData, 0, 0);
            }
            return origToDataURL.apply(this, [type, ...args]);
        };
    } catch(e) {}

    // 16. WebGL vendor/renderer spoofing
    try {
        const getParam = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return 'Intel Inc.';
            if (parameter === 37446) return 'Intel Iris OpenGL Engine';
            return getParam.apply(this, arguments);
        };
        const getParam2 = WebGL2RenderingContext.prototype.getParameter;
        WebGL2RenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return 'Intel Inc.';
            if (parameter === 37446) return 'Intel Iris OpenGL Engine';
            return getParam2.apply(this, arguments);
        };
    } catch(e) {}

    // 17. Notification permission
    try {
        Object.defineProperty(Notification, 'permission', {
            get: () => 'default',
            configurable: true
        });
    } catch(e) {}
})();
"#;

pub struct SourceScraper<'a> {
    logger: &'a ProjectLogger,
    secret: &'a Secret<'a>,
}

impl<'a> SourceScraper<'a> {
    const GOOGLE_SHEET_URL: &'a str = "https://docs.google.com/spreadsheets/d/";
    const GOOGLE_SHEET_REPLACE_TOKEN: (&'a str, &'a str) = ("edit#gid=", "export?format=csv&gid=");
    const RQUEST_BROWSER: Emulation = Emulation::Chrome135;

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

    pub fn get_rquest_client(&self, timeout: Duration) -> Result<rquest::Client, ScraperError> {
        let rquest_client = rquest::Client::builder()
            .emulation(Self::RQUEST_BROWSER)
            .connect_timeout(timeout)
            .build()?;
        let debug_str = "Rquest client initialized";
        self.logger.log_debug(debug_str);
        Ok(rquest_client)
    }

    pub async fn get_playwright_client(&self) -> Result<Playwright, ScraperError> {
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
        let mut client_builder = reqwest::ClientBuilder::new()
            .add_root_certificate(cert)
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
            .args(&args);
        if let Some(scraper_proxy) = scraper_proxy {
            let proxy_result = scraper_proxy.generate_proxy().await?;
            let proxy = proxy_result.get_playwright_proxy();
            browser = browser.proxy(proxy);
            let browser = browser
                .launch()
                .await
                .map_err(playwright_rust::Error::from)?;
            let context = browser.context_builder()
                .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
                .viewport(Some(Viewport { width: 1920, height: 1080 }))
                .locale("en-GB")
                .timezone_id("Europe/London")
                .build().await.map_err(playwright_rust::Error::from)?;
            let page = context
                .new_page()
                .await
                .map_err(playwright_rust::Error::from)?;
            page.add_init_script(STEALTH_INIT_SCRIPT)
            .await
            .map_err(playwright_rust::Error::from)?;
            if let Some(header_map) = request_options.convert_header_map_to_map() {
                page.set_extra_http_headers(header_map)
                    .await
                    .map_err(playwright_rust::Error::from)?;
            }
            match page
                .goto_builder(url)
                .timeout(request_options.timeout.as_millis() as f64)
                .goto()
                .await
            {
                Ok(response) => {
                    let cookies = context
                        .cookies(&[])
                        .await
                        .map_err(playwright_rust::Error::from)?
                        .iter()
                        .map(|c| (c.name.to_string(), c.value.to_string()))
                        .collect();
                    if let Some(response) = response {
                        let content = page
                            .content()
                            .await
                            .map_err(playwright_rust::Error::from)?;
                        let status_code = response.status()? as u16;
                        let url = page.url()?;
                        let ok = response.ok()?;
                        let reason = response.status_text()?;
                        if let Some(page_evaluation) = &browser_options.page_evaluation {
                            page.eval::<()>(page_evaluation)
                                .await
                                .map_err(playwright_rust::Error::from)?;
                        }
                        time_operation::async_sleep(browser_options.browser_wait).await;
                        if !request_options.allow_forbidden_proxy && status_code == 403 {
                            scraper_proxy.add_proxy_block_count(&proxy_result);
                        }
                        let response = {
                            Response {
                                content,
                                status_code,
                                url,
                                ok,
                                reason,
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
                Err(e) => {
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
                    Err(ScraperError::from(playwright_rust::Error::from(e)))
                }
            }
        } else {
            let browser = browser
                .launch()
                .await
                .map_err(playwright_rust::Error::from)?;
            let context = browser.context_builder()
                .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
                .viewport(Some(Viewport { width: 1920, height: 1080 }))
                .locale("en-GB")
                .timezone_id("Europe/London")
                .build().await.map_err(playwright_rust::Error::from)?;
            let page = context
                .new_page()
                .await
                .map_err(playwright_rust::Error::from)?;
            if let Some(header_map) = request_options.convert_header_map_to_map() {
                page.set_extra_http_headers(header_map)
                    .await
                    .map_err(playwright_rust::Error::from)?;
            }
            page.add_init_script(STEALTH_INIT_SCRIPT)
            .await
            .map_err(playwright_rust::Error::from)?;
            match page
                .goto_builder(url)
                .timeout(request_options.timeout.as_millis() as f64)
                .goto()
                .await
            {
                Ok(response) => {
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
                        let content = page
                            .content()
                            .await
                            .map_err(playwright_rust::Error::from)?;
                        let status_code = response.status()? as u16;
                        let url = page.url()?;
                        let ok = response.ok()?;
                        let reason = response.status_text()?;
                        let response = {
                            Response {
                                content,
                                status_code,
                                url,
                                ok,
                                reason,
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
                Err(e) => {
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
                    Err(ScraperError::from(playwright_rust::Error::from(e)))
                }
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
                    if !request_options.allow_forbidden_proxy && response.status_code == 403 {
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
        let secret = Secret::new(&project_logger).await;
        let scraper = SourceScraper::new(&project_logger, &secret);
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
        let secret = Secret::new(&project_logger).await;
        let scraper = SourceScraper::new(&project_logger, &secret);
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
