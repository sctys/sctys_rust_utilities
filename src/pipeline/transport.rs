//! Per-run clients and sessions for the existing transport implementations.
//! Provider hooks can mutate this session; no provider protocol is encoded here.
use crate::netdata::{
    cloudflare_pool::CloudflarePool,
    data_struct::{RequestOptions, Response, Scraper, ScraperClient, ScraperError},
    playwright_js_client::PlaywrightClient,
    proxy::ScraperProxy,
    requests_ip_rotate::ApiGateway,
};
use crate::source_scraper::{RquestBrowser, SourceScraper};
use playwright_rust::Playwright;
use std::time::Duration;

#[derive(Default)]
pub struct CloudflareSettings {
    pub max_sessions: Option<usize>,
    pub clearance_ttl: Option<Duration>,
    pub persistence_ttl: Option<Duration>,
    pub redis_persistence: RedisPersistence,
}
#[derive(Clone, Copy, Debug, Default)]
pub enum RedisPersistence {
    #[default]
    Disabled,
    BestEffort,
    Required,
}

/// Disjoint borrows let async header/provider hooks mutate request and proxy
/// state while still accessing the selected client and gateway.
pub struct HeaderContext<'session, 'owner> {
    pub request_options: &'session mut RequestOptions,
    pub client: Option<ScraperClient<'session>>,
    pub gateway: Option<&'session ApiGateway>,
    pub proxy: Option<&'session mut ScraperProxy<'owner>>,
    pub domain_update: &'session mut Option<(String, String)>,
}
pub struct TransportSettings {
    pub scraper: Scraper,
    pub request: RequestOptions,
    pub browser: RquestBrowser,
    pub use_proxy: bool,
    pub clearance_persistence_ttl: Duration,
    pub cloudflare: CloudflareSettings,
}

pub struct TransportSession<'a> {
    pub source: &'a SourceScraper<'a>,
    pub request_options: RequestOptions,
    pub scraper: Scraper,
    pub gateway: Option<ApiGateway>,
    pub rquest: Option<wreq::Client>,
    pub playwright: Option<Playwright>,
    pub playwright_js: Option<PlaywrightClient>,
    pub proxy: Option<ScraperProxy<'a>>,
    pub cloudflare: Option<CloudflarePool<'a>>,
    pub domain_update: Option<(String, String)>,
    pub clearance_persistence_ttl: Duration,
}
impl<'a> TransportSession<'a> {
    /// Creates only the selected clients. Empty input may omit gateway creation,
    /// matching existing setup behavior without inventing a URL.
    pub async fn open(
        source: &'a SourceScraper<'a>,
        settings: TransportSettings,
        first_url: Option<&str>,
    ) -> Result<Self, ScraperError> {
        if matches!(settings.scraper, Scraper::RquestCf) && !settings.use_proxy {
            return Err(ScraperError::Other(
                "Cloudflare transport requires a proxy".into(),
            ));
        }
        let gateway = match &settings.scraper {
            Scraper::Reqwest(true) | Scraper::Rquest(true) => match first_url {
                Some(url) => Some(source.get_api_gateway(url, None).await?),
                None => None,
            },
            _ => None,
        };
        let rquest = if matches!(settings.scraper, Scraper::Rquest(_) | Scraper::RquestCf) {
            Some(source.get_rquest_client_with_browser(&settings.request, settings.browser)?)
        } else {
            None
        };
        let cloudflare = if matches!(settings.scraper, Scraper::RquestCf) {
            let mut pool = CloudflarePool::new(source.get_cap_solver().await?);
            if let Some(width) = settings.cloudflare.max_sessions {
                pool = pool.with_max_sessions(width);
            }
            if let Some(ttl) = settings.cloudflare.clearance_ttl {
                pool = pool.with_clearance_ttl(ttl);
            }
            if let Some(ttl) = settings.cloudflare.persistence_ttl {
                pool = pool.with_persistence_ttl(ttl);
            }
            if !matches!(
                settings.cloudflare.redis_persistence,
                RedisPersistence::Disabled
            ) {
                pool = pool.with_redis_persistence();
                if let Err(error) = pool.connect_redis_persistence().await {
                    if matches!(
                        settings.cloudflare.redis_persistence,
                        RedisPersistence::Required
                    ) {
                        return Err(ScraperError::Other(format!(
                            "Unable to initialize clearance persistence: {error}"
                        )));
                    }
                    source.get_logger().log_warn(&format!(
                        "Clearance persistence disabled for this run: {error}"
                    ));
                }
            }
            Some(pool)
        } else {
            None
        };
        let playwright = if matches!(settings.scraper, Scraper::Playwright(_)) {
            Some(source.get_playwright_client().await?)
        } else {
            None
        };
        let playwright_js = if matches!(settings.scraper, Scraper::PlaywrightJs) {
            Some(source.get_playwright_js_client()?)
        } else {
            None
        };
        let proxy = if settings.use_proxy && gateway.is_none() {
            let mut proxy = source.get_scraper_proxy().await?;
            proxy.set_block_count(settings.request.proxy_block_count);
            Some(proxy)
        } else {
            None
        };
        Ok(Self {
            source,
            request_options: settings.request,
            scraper: settings.scraper,
            gateway,
            rquest,
            playwright,
            playwright_js,
            proxy,
            cloudflare,
            domain_update: None,
            clearance_persistence_ttl: settings.clearance_persistence_ttl,
        })
    }
    pub fn client(&mut self) -> Option<ScraperClient<'_>> {
        if let Some(client) = &self.rquest {
            Some(ScraperClient::Rquest(client))
        } else if let Some(client) = &mut self.playwright {
            Some(ScraperClient::Playwright(client))
        } else {
            self.playwright_js.as_ref().map(ScraperClient::PlaywrightJs)
        }
    }
    pub fn header_context(&mut self) -> HeaderContext<'_, 'a> {
        let client = if let Some(client) = &self.rquest {
            Some(ScraperClient::Rquest(client))
        } else if let Some(client) = &mut self.playwright {
            Some(ScraperClient::Playwright(client))
        } else {
            self.playwright_js.as_ref().map(ScraperClient::PlaywrightJs)
        };
        HeaderContext {
            request_options: &mut self.request_options,
            client,
            gateway: self.gateway.as_ref(),
            proxy: self.proxy.as_mut(),
            domain_update: &mut self.domain_update,
        }
    }
    pub async fn refresh_domain(&mut self, url: &str) {
        let (original, updated) = self
            .source
            .get_update_domain(url, &self.request_options)
            .await;
        self.domain_update = (original != updated).then_some((original, updated));
    }
    /// Applies a caller-owned refresh cadence to the first input URL.
    pub async fn refresh_domain_if_due(
        &mut self,
        attempted: usize,
        cadence: usize,
        enabled: bool,
        first_url: &str,
    ) {
        if cadence > 0 && attempted.is_multiple_of(cadence) {
            self.domain_update = None;
        }
        if enabled && self.domain_update.is_none() {
            self.refresh_domain(first_url).await;
        }
    }
    pub fn rewrite_url(&self, url: &str) -> String {
        self.domain_update
            .as_ref()
            .map_or_else(|| url.to_string(), |(from, to)| url.replace(from, to))
    }
    pub fn close_input(&mut self) {
        if let Some(proxy) = &mut self.proxy {
            proxy.clear_sticky_proxy();
        }
    }
    /// One physical request. Retry/replay bookkeeping is owned by run_scraper.
    pub async fn request(&mut self, url: &str) -> Result<Response, ScraperError> {
        let missing = || ScraperError::Other("selected transport client is not initialized".into());
        match &self.scraper {
            Scraper::Reqwest(_) => {
                self.source
                    .request_with_reqwest(
                        url,
                        &self.request_options,
                        self.proxy.as_mut(),
                        self.gateway.as_ref(),
                    )
                    .await
            }
            Scraper::Rquest(_) => {
                self.source
                    .request_with_rquest(
                        url,
                        &self.request_options,
                        self.rquest.as_ref().ok_or_else(missing)?,
                        self.proxy.as_mut(),
                        self.gateway.as_ref(),
                    )
                    .await
            }
            Scraper::RquestCf => {
                self.cloudflare
                    .as_mut()
                    .ok_or_else(missing)?
                    .request(
                        url,
                        &self.request_options,
                        self.rquest.as_ref().ok_or_else(missing)?,
                        self.proxy.as_mut().ok_or_else(missing)?,
                        self.source,
                        self.source.get_logger(),
                    )
                    .await
            }
            Scraper::Playwright(options) => {
                self.source
                    .request_with_playwright_refreshing(
                        url,
                        &self.request_options,
                        options,
                        self.playwright.as_mut().ok_or_else(missing)?,
                        self.proxy.as_mut(),
                    )
                    .await
            }
            Scraper::PlaywrightJs => {
                self.source
                    .request_with_playwright_js_with_clearance_ttl(
                        url,
                        &self.request_options,
                        self.playwright_js.as_ref().ok_or_else(missing)?,
                        self.proxy.as_mut(),
                        self.clearance_persistence_ttl,
                    )
                    .await
            }
        }
    }
}
