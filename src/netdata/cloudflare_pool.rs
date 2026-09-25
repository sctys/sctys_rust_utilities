use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use wreq::header::{HeaderName, HeaderValue, COOKIE, USER_AGENT};

use crate::{
    io::redis::{RedisKvClient, RedisSnapshotConfig, REDIS_PATH},
    logger::ProjectLogger,
    netdata::{
        capsolver::CapSolver,
        data_struct::{RequestOptions, Response, ScraperError},
        proxy::{ProxyResult, ScraperProxy},
        source_scraper::SourceScraper,
    },
};

/// Clearance state that survives process restarts via Redis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedClearance {
    pub site_host: String,
    pub proxy_address: String,
    pub port: u32,
    pub user_agent: String,
    pub cookie_header: String,
    pub extra_headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CfBlock {
    Challenge,
    HardBlock,
}

pub fn classify_cf_block(status_code: u16, content: &str) -> Option<CfBlock> {
    if status_code != 403 {
        return None;
    }
    if content.contains("Just a moment")
        || content.contains("challenges.cloudflare.com")
        || content.contains("cf_chl")
    {
        Some(CfBlock::Challenge)
    } else {
        Some(CfBlock::HardBlock)
    }
}

struct CfSession {
    proxy: ProxyResult,
    user_agent: String,
    cookie_header: String,
    extra_headers: Vec<(String, String)>,
    cleared_at: Option<Instant>,
    failures: u8,
    dirty_cookies: bool,
    last_cookie_flush: Option<Instant>,
}

impl CfSession {
    fn new(proxy: ProxyResult, user_agent: &str) -> Self {
        Self {
            proxy,
            user_agent: user_agent.to_string(),
            cookie_header: String::new(),
            extra_headers: Vec::new(),
            cleared_at: None,
            failures: 0,
            dirty_cookies: false,
            last_cookie_flush: None,
        }
    }

    fn apply_solution(&mut self, solution: &crate::netdata::capsolver::CloudflareSolution) {
        let cookie_pairs: Vec<String> = solution
            .cookies()
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        self.cookie_header = cookie_pairs.join("; ");
        self.user_agent = solution.user_agent().to_string();
        self.extra_headers = solution
            .headers()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        self.cleared_at = Some(Instant::now());
    }

    /// Merges cookies observed on a response (e.g. the short-lived `__cf_bm`
    /// bot-management cookie Cloudflare refreshes per exchange) into the session
    /// cookie header, replacing existing values and appending new names. Returns
    /// whether the header changed.
    fn merge_response_cookies(&mut self, response_cookies: &HashMap<String, String>) -> bool {
        let mut changed = false;
        for (name, value) in response_cookies {
            let full = format!("{name}={value}");
            let existing = self.cookie_header.split("; ").find_map(|pair| {
                let (existing_name, _) = pair.split_once('=')?;
                (existing_name == name).then_some(pair)
            });
            match existing {
                Some(pair) if pair == full.as_str() => {}
                Some(_) => {
                    let rebuilt: Vec<String> = self
                        .cookie_header
                        .split("; ")
                        .map(|pair| {
                            let (existing_name, _) = pair.split_once('=').unwrap();
                            if existing_name == name {
                                full.clone()
                            } else {
                                pair.to_string()
                            }
                        })
                        .collect();
                    self.cookie_header = rebuilt.join("; ");
                    self.dirty_cookies = true;
                    changed = true;
                }
                None if self.cookie_header.is_empty() => {
                    self.cookie_header = full;
                    self.dirty_cookies = true;
                    changed = true;
                }
                None => {
                    self.cookie_header = format!("{}; {}", self.cookie_header, full);
                    self.dirty_cookies = true;
                    changed = true;
                }
            }
        }
        changed
    }

    fn request_options(&self, base: &RequestOptions) -> RequestOptions {
        let mut options = base.clone();
        options.proxy_block_count = 0;
        let mut headers = base.headers.clone().unwrap_or_default();
        if !self.user_agent.is_empty() {
            if let Ok(value) = HeaderValue::from_str(&self.user_agent) {
                headers.insert(USER_AGENT, value);
            }
        }
        if !self.cookie_header.is_empty() {
            let cookie_header = match headers.get(COOKIE) {
                Some(existing) => match existing.to_str() {
                    Ok(existing) if !existing.is_empty() => {
                        format!("{existing}; {}", self.cookie_header)
                    }
                    _ => self.cookie_header.clone(),
                },
                None => self.cookie_header.clone(),
            };
            if let Ok(value) = HeaderValue::from_str(&cookie_header) {
                headers.insert(COOKIE, value);
            }
        }
        for (key, value) in &self.extra_headers {
            let lower_key = key.to_lowercase();
            if lower_key == "cookie" || lower_key == "user-agent" || lower_key == "host" {
                continue;
            }
            if let (Ok(name), Ok(header_value)) = (
                HeaderName::from_bytes(key.as_bytes()),
                HeaderValue::from_str(value),
            ) {
                headers.insert(name, header_value);
            }
        }
        options.headers = Some(headers);
        options
    }
}

pub struct CloudflarePool<'a> {
    cap_solver: CapSolver<'a>,
    sessions: Vec<CfSession>,
    cursor: usize,
    max_sessions: usize,
    clearance_ttl: Duration,
    max_solves_per_request: u8,
    max_bootstrap_attempts: u8,
    solve_count: u32,
    kv_config: Option<RedisSnapshotConfig>,
    kv: Option<RedisKvClient>,
    persistence_ttl: Duration,
}

impl<'a> CloudflarePool<'a> {
    const CHROME135_USER_AGENT: &'static str =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
         Chrome/135.0.0.0 Safari/537.36";
    const DEFAULT_MAX_SESSIONS: usize = 12;
    const DEFAULT_CLEARANCE_TTL: Duration = Duration::from_secs(1500);
    const DEFAULT_MAX_SOLVES_PER_REQUEST: u8 = 3;
    const DEFAULT_MAX_BOOTSTRAP_ATTEMPTS: u8 = 8;
    const MAX_SESSION_FAILURES: u8 = 3;
    const TOP_UP_PROBES: u8 = 4;
    const CLEARANCE_KEY_PREFIX: &'static str = "cf_clearance";
    const SOLVE_LEASE_KEY_PREFIX: &'static str = "cf_solve_lease";
    const DEFAULT_PERSISTENCE_TTL: Duration = Duration::from_secs(30 * 24 * 3600);
    const SOLVE_LEASE_TTL: Duration = Duration::from_secs(180);
    const COOKIE_FLUSH_INTERVAL: Duration = Duration::from_secs(60);

    /// Refreshes a session's cookies from a response and periodically mirrors the
    /// surviving record (including refreshed short-lived cookies like `__cf_bm`)
    /// back to Redis persistence. Merge-heavy but write-throttled per session.
    #[allow(clippy::too_many_arguments)]
    async fn flush_session_cookies(
        kv: Option<&RedisKvClient>,
        persistence_ttl: Duration,
        site_host: &str,
        session: &mut CfSession,
        response_cookies: &HashMap<String, String>,
        logger: &ProjectLogger,
    ) {
        session.merge_response_cookies(response_cookies);
        let flush_due = match session.last_cookie_flush {
            Some(last) => last.elapsed() > Self::COOKIE_FLUSH_INTERVAL,
            None => true,
        };
        if !flush_due {
            return;
        }
        session.last_cookie_flush = Some(Instant::now());
        if session.dirty_cookies {
            session.dirty_cookies = false;
            Self::persist_clearance(
                kv,
                persistence_ttl,
                site_host,
                &session.proxy,
                session,
                logger,
            )
            .await;
        }
    }

    /// After a successful serve, add more clean (200, no-challenge) proxies to the pool so
    /// consecutive requests rotate across IPs instead of reusing one. Capped per call and
    /// overall by `max_sessions`; only proxies that pass without a challenge are added.
    async fn top_up(
        &mut self,
        url: &str,
        request_options: &RequestOptions,
        client: &wreq::Client,
        scraper_proxy: &mut ScraperProxy<'a>,
        source_scraper: &SourceScraper<'a>,
        logger: &ProjectLogger,
    ) {
        let site_host = Self::site_host(url).unwrap_or("unknown");
        let kv = self.kv.clone();
        let Self {
            sessions,
            max_sessions,
            ..
        } = self;
        let existing_ips: Vec<String> = sessions
            .iter()
            .map(|s| s.proxy.proxy_address.clone())
            .collect();
        let mut cached_proxies =
            Self::cached_clearance_proxies(kv.as_ref(), site_host, scraper_proxy, logger)
                .await
                .into_iter()
                .filter(|proxy| !existing_ips.contains(&proxy.proxy_address))
                .collect::<Vec<_>>();
        let mut probes = 0u8;
        while sessions.len() < *max_sessions && probes < Self::TOP_UP_PROBES {
            probes += 1;
            let proxy = match cached_proxies.pop() {
                Some(proxy) => proxy,
                None => match scraper_proxy.generate_proxy().await {
                    Ok(proxy) => proxy,
                    Err(_) => break,
                },
            };
            if existing_ips.contains(&proxy.proxy_address) {
                continue;
            }
            let mut session = CfSession::new(proxy.clone(), Self::CHROME135_USER_AGENT);
            if Self::hydrate_clearance(kv.as_ref(), site_host, &proxy, &mut session, logger).await {
                sessions.push(session);
                continue;
            }
            let options = session.request_options(request_options);
            let response = Self::send_with_session(
                url,
                &options,
                &proxy,
                client,
                scraper_proxy,
                source_scraper,
                logger,
            )
            .await;
            match response {
                Ok(response) => match classify_cf_block(response.status_code, &response.content) {
                    None => sessions.push(session),
                    Some(CfBlock::HardBlock) => {
                        Self::evict_clearance(kv.as_ref(), site_host, &proxy, logger).await;
                        scraper_proxy.add_proxy_block_count(&proxy);
                    }
                    Some(CfBlock::Challenge) => {}
                },
                Err(_) => break,
            }
        }
    }

    pub fn new(cap_solver: CapSolver<'a>) -> Self {
        Self {
            cap_solver,
            sessions: Vec::new(),
            cursor: 0,
            max_sessions: Self::DEFAULT_MAX_SESSIONS,
            clearance_ttl: Self::DEFAULT_CLEARANCE_TTL,
            max_solves_per_request: Self::DEFAULT_MAX_SOLVES_PER_REQUEST,
            max_bootstrap_attempts: Self::DEFAULT_MAX_BOOTSTRAP_ATTEMPTS,
            solve_count: 0,
            kv_config: None,
            kv: None,
            persistence_ttl: Self::DEFAULT_PERSISTENCE_TTL,
        }
    }

    pub fn with_max_sessions(mut self, max_sessions: usize) -> Self {
        self.max_sessions = max_sessions.max(1);
        self
    }

    pub fn with_clearance_ttl(mut self, clearance_ttl: Duration) -> Self {
        self.clearance_ttl = clearance_ttl;
        self
    }

    /// Mirrors clearances in Redis so they survive process restarts and are shared
    /// across concurrent pools. Connects once on the next `connect_redis_persistence`
    /// call; until then persistence helpers are no-ops.
    pub fn with_redis_persistence(mut self) -> Self {
        self.kv_config = Some(RedisSnapshotConfig::new(REDIS_PATH, "cloudflare_pool"));
        self
    }

    /// Safety TTL applied to persisted clearance records. Set below the site's real
    /// `cf_clearance` expiry when it is short, so stale records lapse in storage
    /// instead of being re-tested on contact.
    pub fn with_persistence_ttl(mut self, persistence_ttl: Duration) -> Self {
        self.persistence_ttl = persistence_ttl;
        self
    }

    /// Establishes the Redis connection requested by [`Self::with_redis_persistence`].
    /// An error disables persistence silently; the pool keeps working in-memory.
    pub async fn connect_redis_persistence(
        &mut self,
    ) -> Result<(), crate::io::redis::SnapshotError> {
        let Some(config) = self.kv_config.take() else {
            return Ok(());
        };
        let kv = RedisKvClient::connect(config).await?;
        self.kv = Some(kv);
        Ok(())
    }

    pub fn solve_count(&self) -> u32 {
        self.solve_count
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn request(
        &mut self,
        url: &str,
        request_options: &RequestOptions,
        client: &wreq::Client,
        scraper_proxy: &mut ScraperProxy<'a>,
        source_scraper: &SourceScraper<'a>,
        logger: &ProjectLogger,
    ) -> Result<Response, ScraperError> {
        let response = self
            .request_inner(
                url,
                request_options,
                client,
                scraper_proxy,
                source_scraper,
                logger,
            )
            .await?;
        if response.status_code != 403 {
            self.top_up(
                url,
                request_options,
                client,
                scraper_proxy,
                source_scraper,
                logger,
            )
            .await;
        }
        Ok(response)
    }

    async fn request_inner(
        &mut self,
        url: &str,
        request_options: &RequestOptions,
        client: &wreq::Client,
        scraper_proxy: &mut ScraperProxy<'a>,
        source_scraper: &SourceScraper<'a>,
        logger: &ProjectLogger,
    ) -> Result<Response, ScraperError> {
        let site_host = Self::site_host(url).unwrap_or("unknown");
        let kv = self.kv.clone();
        let Self {
            cap_solver,
            sessions,
            cursor,
            max_sessions,
            clearance_ttl,
            max_solves_per_request,
            max_bootstrap_attempts,
            solve_count,
            persistence_ttl,
            ..
        } = self;
        let mut solves = 0u8;
        let mut last_response: Option<Response> = None;
        let mut attempted = 0usize;
        let total = sessions.len();
        while attempted < total && !sessions.is_empty() {
            let index = *cursor % sessions.len();
            attempted += 1;
            let session = &mut sessions[index];
            if session
                .cleared_at
                .is_some_and(|cleared_at| cleared_at.elapsed() > *clearance_ttl)
            {
                session.cleared_at = None;
                session.cookie_header.clear();
                session.extra_headers.clear();
                session.user_agent = Self::CHROME135_USER_AGENT.to_string();
            }
            let options = session.request_options(request_options);
            let response = Self::send_with_session(
                url,
                &options,
                &session.proxy,
                client,
                scraper_proxy,
                source_scraper,
                logger,
            )
            .await;
            match response {
                Ok(response) => match classify_cf_block(response.status_code, &response.content) {
                    None => {
                        session.failures = 0;
                        Self::flush_session_cookies(
                            kv.as_ref(),
                            *persistence_ttl,
                            site_host,
                            session,
                            &response.cookies,
                            logger,
                        )
                        .await;
                        *cursor = (index + 1) % sessions.len().max(1);
                        return Ok(response);
                    }
                    Some(CfBlock::Challenge) => {
                        if solves >= *max_solves_per_request {
                            *cursor = (index + 1) % sessions.len().max(1);
                            last_response = Some(response);
                            continue;
                        }
                        let proxy = session.proxy.clone();
                        if !Self::try_acquire_solve_lease(kv.as_ref(), site_host, &proxy, logger)
                            .await
                        {
                            if Self::hydrate_clearance(
                                kv.as_ref(),
                                site_host,
                                &proxy,
                                session,
                                logger,
                            )
                            .await
                            {
                                let options = session.request_options(request_options);
                                let retry = Self::send_with_session(
                                    url,
                                    &options,
                                    &proxy,
                                    client,
                                    scraper_proxy,
                                    source_scraper,
                                    logger,
                                )
                                .await;
                                match retry {
                                    Ok(retry)
                                        if classify_cf_block(retry.status_code, &retry.content)
                                            .is_none() =>
                                    {
                                        session.failures = 0;
                                        Self::flush_session_cookies(
                                            kv.as_ref(),
                                            *persistence_ttl,
                                            site_host,
                                            session,
                                            &retry.cookies,
                                            logger,
                                        )
                                        .await;
                                        *cursor = (index + 1) % sessions.len().max(1);
                                        return Ok(retry);
                                    }
                                    _ => {
                                        *cursor = (index + 1) % sessions.len().max(1);
                                        last_response = Some(response);
                                        continue;
                                    }
                                }
                            }
                            *cursor = (index + 1) % sessions.len().max(1);
                            last_response = Some(response);
                            continue;
                        }
                        let solve_result = cap_solver
                            .solve_cloudflare(
                                url,
                                &proxy.get_cap_solver_proxy(),
                                &session.user_agent,
                                &response.content,
                            )
                            .await;
                        Self::release_solve_lease(kv.as_ref(), site_host, &proxy, logger).await;
                        solves += 1;
                        match solve_result {
                            Ok(solution) => {
                                *solve_count += 1;
                                session.apply_solution(&solution);
                                Self::persist_clearance(
                                    kv.as_ref(),
                                    *persistence_ttl,
                                    site_host,
                                    &proxy,
                                    session,
                                    logger,
                                )
                                .await;
                                logger.log_debug(&format!(
                                    "Cloudflare pool solved challenge for proxy {}:{} ({} solves total)",
                                    proxy.proxy_address, proxy.port, *solve_count
                                ));
                                let options = session.request_options(request_options);
                                let retry = Self::send_with_session(
                                    url,
                                    &options,
                                    &proxy,
                                    client,
                                    scraper_proxy,
                                    source_scraper,
                                    logger,
                                )
                                .await;
                                match retry {
                                    Ok(retry)
                                        if classify_cf_block(retry.status_code, &retry.content)
                                            .is_none() =>
                                    {
                                        session.failures = 0;
                                        Self::flush_session_cookies(
                                            kv.as_ref(),
                                            *persistence_ttl,
                                            site_host,
                                            session,
                                            &retry.cookies,
                                            logger,
                                        )
                                        .await;
                                        *cursor = (index + 1) % sessions.len().max(1);
                                        return Ok(retry);
                                    }
                                    Ok(retry) => {
                                        let warn_str = format!(
                                            "Cloudflare pool session {}:{} still blocked after fresh clearance, evicting",
                                            proxy.proxy_address, proxy.port
                                        );
                                        logger.log_warn(&warn_str);
                                        Self::evict_clearance(
                                            kv.as_ref(),
                                            site_host,
                                            &proxy,
                                            logger,
                                        )
                                        .await;
                                        last_response = Some(retry);
                                        sessions.remove(index);
                                        continue;
                                    }
                                    Err(e) => {
                                        logger.log_warn(&format!(
                                            "Cloudflare pool request failed after clearance: {e}"
                                        ));
                                        Self::evict_clearance(
                                            kv.as_ref(),
                                            site_host,
                                            &proxy,
                                            logger,
                                        )
                                        .await;
                                        sessions.remove(index);
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                let warn_str = format!(
                                    "Cloudflare pool failed to solve challenge for proxy {}:{}, evicting. {e}",
                                    proxy.proxy_address, proxy.port
                                );
                                logger.log_warn(&warn_str);
                                Self::evict_clearance(kv.as_ref(), site_host, &proxy, logger).await;
                                scraper_proxy.add_proxy_block_count(&proxy);
                                sessions.remove(index);
                                continue;
                            }
                        }
                    }
                    Some(CfBlock::HardBlock) => {
                        let proxy = session.proxy.clone();
                        let warn_str = format!(
                            "Cloudflare pool session {}:{} hit hard block, evicting",
                            proxy.proxy_address, proxy.port
                        );
                        logger.log_warn(&warn_str);
                        Self::evict_clearance(kv.as_ref(), site_host, &proxy, logger).await;
                        scraper_proxy.add_proxy_block_count(&proxy);
                        last_response = Some(response);
                        sessions.remove(index);
                        continue;
                    }
                },
                Err(e) => {
                    logger.log_warn(&format!("Cloudflare pool session request failed: {e}"));
                    session.failures += 1;
                    if session.failures >= Self::MAX_SESSION_FAILURES {
                        sessions.remove(index);
                    } else {
                        *cursor = (index + 1) % sessions.len().max(1);
                    }
                    continue;
                }
            }
        }
        let existing_addresses: Vec<String> = sessions
            .iter()
            .map(|session| session.proxy.proxy_address.clone())
            .collect();
        let mut cached_proxies =
            Self::cached_clearance_proxies(kv.as_ref(), site_host, scraper_proxy, logger)
                .await
                .into_iter()
                .filter(|proxy| !existing_addresses.contains(&proxy.proxy_address))
                .collect::<Vec<_>>();
        let mut bootstraps = 0u8;
        while bootstraps < *max_bootstrap_attempts && sessions.len() < *max_sessions {
            bootstraps += 1;
            let proxy = match cached_proxies.pop() {
                Some(proxy) => proxy,
                None => scraper_proxy.generate_proxy().await?,
            };
            let mut session = CfSession::new(proxy.clone(), Self::CHROME135_USER_AGENT);
            if Self::hydrate_clearance(kv.as_ref(), site_host, &proxy, &mut session, logger).await {
                let options = session.request_options(request_options);
                let response = Self::send_with_session(
                    url,
                    &options,
                    &proxy,
                    client,
                    scraper_proxy,
                    source_scraper,
                    logger,
                )
                .await;
                match response {
                    Ok(response)
                        if classify_cf_block(response.status_code, &response.content).is_none() =>
                    {
                        sessions.push(session);
                        *cursor = sessions.len() % (*max_sessions).max(1);
                        return Ok(response);
                    }
                    Ok(response) => {
                        let warn_str = format!(
                            "Cloudflare pool hydrated clearance for proxy {}:{} and site {site_host} was rejected, deleting record",
                            proxy.proxy_address, proxy.port
                        );
                        logger.log_warn(&warn_str);
                        Self::evict_clearance(kv.as_ref(), site_host, &proxy, logger).await;
                        last_response = Some(response);
                        continue;
                    }
                    Err(e) => {
                        logger.log_warn(&format!(
                            "Cloudflare pool bootstrap request with hydrated clearance for {site_host} failed: {e}"
                        ));
                        continue;
                    }
                }
            }
            let options = session.request_options(request_options);
            let response = Self::send_with_session(
                url,
                &options,
                &proxy,
                client,
                scraper_proxy,
                source_scraper,
                logger,
            )
            .await;
            match response {
                Ok(response) => match classify_cf_block(response.status_code, &response.content) {
                    None => {
                        sessions.push(session);
                        *cursor = sessions.len() % (*max_sessions).max(1);
                        return Ok(response);
                    }
                    Some(CfBlock::Challenge) => {
                        if solves >= *max_solves_per_request {
                            last_response = Some(response);
                            continue;
                        }
                        if !Self::try_acquire_solve_lease(kv.as_ref(), site_host, &proxy, logger)
                            .await
                        {
                            last_response = Some(response);
                            continue;
                        }
                        let solve_result = cap_solver
                            .solve_cloudflare(
                                url,
                                &proxy.get_cap_solver_proxy(),
                                &session.user_agent,
                                &response.content,
                            )
                            .await;
                        Self::release_solve_lease(kv.as_ref(), site_host, &proxy, logger).await;
                        solves += 1;
                        match solve_result {
                            Ok(solution) => {
                                *solve_count += 1;
                                session.apply_solution(&solution);
                                Self::persist_clearance(
                                    kv.as_ref(),
                                    *persistence_ttl,
                                    site_host,
                                    &proxy,
                                    &session,
                                    logger,
                                )
                                .await;
                                logger.log_debug(&format!(
                                    "Cloudflare pool bootstrapped cleared session for proxy {}:{} ({} solves total)",
                                    proxy.proxy_address, proxy.port, *solve_count
                                ));
                                let options = session.request_options(request_options);
                                let retry = Self::send_with_session(
                                    url,
                                    &options,
                                    &proxy,
                                    client,
                                    scraper_proxy,
                                    source_scraper,
                                    logger,
                                )
                                .await;
                                match retry {
                                    Ok(retry) => {
                                        if classify_cf_block(retry.status_code, &retry.content)
                                            .is_none()
                                        {
                                            sessions.push(session);
                                        } else {
                                            Self::evict_clearance(
                                                kv.as_ref(),
                                                site_host,
                                                &proxy,
                                                logger,
                                            )
                                            .await;
                                            scraper_proxy.add_proxy_block_count(&proxy);
                                        }
                                        return Ok(retry);
                                    }
                                    Err(e) => {
                                        let warn_str = format!(
                                            "Cloudflare pool bootstrap request failed after clearance: {e}"
                                        );
                                        logger.log_warn(&warn_str);
                                        Self::evict_clearance(
                                            kv.as_ref(),
                                            site_host,
                                            &proxy,
                                            logger,
                                        )
                                        .await;
                                        scraper_proxy.add_proxy_block_count(&proxy);
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                let warn_str = format!(
                                    "Cloudflare pool failed to bootstrap clearance for proxy {}:{}, skipping. {e}",
                                    proxy.proxy_address, proxy.port
                                );
                                logger.log_warn(&warn_str);
                                Self::evict_clearance(kv.as_ref(), site_host, &proxy, logger).await;
                                scraper_proxy.add_proxy_block_count(&proxy);
                                continue;
                            }
                        }
                    }
                    Some(CfBlock::HardBlock) => {
                        Self::evict_clearance(kv.as_ref(), site_host, &proxy, logger).await;
                        scraper_proxy.add_proxy_block_count(&proxy);
                        last_response = Some(response);
                        continue;
                    }
                },
                Err(e) => {
                    let warn_str =
                        format!("Cloudflare pool bootstrap request with new proxy failed: {e}");
                    logger.log_warn(&warn_str);
                    continue;
                }
            }
        }
        match last_response {
            Some(response) => Ok(response),
            None => Err(ScraperError::Other(format!(
                "Cloudflare pool exhausted for url {url}. Sessions {}, bootstraps {bootstraps}, solves {solves}",
                sessions.len(),
            ))),
        }
    }

    fn site_host(url: &str) -> Option<&str> {
        let after_scheme = url.split_once("://")?.1;
        let host = after_scheme.split(['/', '?', '#']).next()?;
        let host = host.rsplit('@').next()?;
        if host.is_empty() {
            None
        } else {
            Some(host)
        }
    }

    /// Proxies of the current active pool whose `cf_clearance` for `site_host` is
    /// already persisted. Resolution and pruning mirror the PlaywrightJs clearance
    /// preference: stale records for proxies no longer in the pool are removed.
    async fn cached_clearance_proxies(
        kv: Option<&RedisKvClient>,
        site_host: &str,
        scraper_proxy: &mut ScraperProxy<'a>,
        logger: &ProjectLogger,
    ) -> Vec<ProxyResult> {
        let Some(kv) = kv else {
            return Vec::new();
        };
        if let Err(e) = scraper_proxy.refresh_pool_list().await {
            let warn_str = format!(
                "Cloudflare pool failed to refresh proxy pool for site {site_host}, skipping clearance preference. {e}"
            );
            logger.log_warn(&warn_str);
            return Vec::new();
        }
        let keys = match kv.global_pattern_keys("*").await {
            Ok(keys) => keys,
            Err(e) => {
                let warn_str = format!(
                    "Cloudflare pool failed to scan persisted clearances for site {site_host}. {e}"
                );
                logger.log_warn(&warn_str);
                return Vec::new();
            }
        };
        let site_marker = format!("{}:{}:", Self::CLEARANCE_KEY_PREFIX, site_host);
        let mut proxies = Vec::new();
        for key in keys {
            let Some((_, suffix)) = key.rsplit_once(&site_marker) else {
                continue;
            };
            let mut parts = suffix.split(':').rev();
            let port = match parts.next().and_then(|port| port.parse::<u32>().ok()) {
                Some(port) => port,
                None => continue,
            };
            let Some(proxy_address) = parts.next() else {
                continue;
            };
            if let Some(proxy) = scraper_proxy.find_proxy(proxy_address, port) {
                proxies.push(proxy);
            }
        }
        proxies
    }

    fn storage_key(prefix: &str, site_host: &str, proxy: &ProxyResult) -> String {
        format!(
            "{prefix}:{site_host}:{}:{}",
            proxy.proxy_address, proxy.port
        )
    }

    async fn persist_clearance(
        kv: Option<&RedisKvClient>,
        persistence_ttl: Duration,
        site_host: &str,
        proxy: &ProxyResult,
        session: &CfSession,
        logger: &ProjectLogger,
    ) {
        let Some(kv) = kv else {
            return;
        };
        if session.cookie_header.is_empty() {
            return;
        }
        let record = PersistedClearance {
            site_host: site_host.to_owned(),
            proxy_address: proxy.proxy_address.clone(),
            port: proxy.port,
            user_agent: session.user_agent.clone(),
            cookie_header: session.cookie_header.clone(),
            extra_headers: session.extra_headers.clone(),
        };
        let Ok(json) = serde_json::to_string(&record) else {
            return;
        };
        let key = Self::storage_key(Self::CLEARANCE_KEY_PREFIX, site_host, proxy);
        if let Err(e) = kv.set_string_with_ttl(&key, &json, persistence_ttl).await {
            let warn_str = format!(
                "Cloudflare pool failed to persist clearance for proxy {}:{} and site {site_host}. {e}",
                proxy.proxy_address, proxy.port
            );
            logger.log_warn(&warn_str);
        }
    }

    async fn evict_clearance(
        kv: Option<&RedisKvClient>,
        site_host: &str,
        proxy: &ProxyResult,
        logger: &ProjectLogger,
    ) {
        let Some(kv) = kv else {
            return;
        };
        let key = Self::storage_key(Self::CLEARANCE_KEY_PREFIX, site_host, proxy);
        if let Err(e) = kv.delete(&key).await {
            let warn_str = format!(
                "Cloudflare pool failed to evict clearance for proxy {}:{} and site {site_host}. {e}",
                proxy.proxy_address, proxy.port
            );
            logger.log_warn(&warn_str);
        }
    }

    async fn hydrate_clearance(
        kv: Option<&RedisKvClient>,
        site_host: &str,
        proxy: &ProxyResult,
        session: &mut CfSession,
        logger: &ProjectLogger,
    ) -> bool {
        let Some(kv) = kv else {
            return false;
        };
        let key = Self::storage_key(Self::CLEARANCE_KEY_PREFIX, site_host, proxy);
        let value = match kv.get_string(&key).await {
            Ok(Some(value)) => value,
            Ok(None) => return false,
            Err(e) => {
                let warn_str = format!(
                    "Cloudflare pool failed to read persisted clearance for proxy {}:{} and site {site_host}. {e}",
                    proxy.proxy_address, proxy.port
                );
                logger.log_warn(&warn_str);
                return false;
            }
        };
        let Ok(record) = serde_json::from_str::<PersistedClearance>(&value) else {
            let warn_str = format!(
                "Cloudflare pool found malformed persisted clearance for proxy {}:{} and site {site_host}, deleting",
                proxy.proxy_address, proxy.port
            );
            logger.log_warn(&warn_str);
            let _ = kv.delete(&key).await;
            return false;
        };
        session.user_agent = record.user_agent;
        session.cookie_header = record.cookie_header;
        session.extra_headers = record.extra_headers;
        session.cleared_at = Some(Instant::now());
        true
    }

    /// `true` when no Redis is configured (no coordination) or the lease was acquired.
    async fn try_acquire_solve_lease(
        kv: Option<&RedisKvClient>,
        site_host: &str,
        proxy: &ProxyResult,
        logger: &ProjectLogger,
    ) -> bool {
        let Some(kv) = kv else {
            return true;
        };
        let key = Self::storage_key(Self::SOLVE_LEASE_KEY_PREFIX, site_host, proxy);
        match kv
            .set_string_if_absent_ttl(&key, "1", Self::SOLVE_LEASE_TTL)
            .await
        {
            Ok(acquired) => acquired,
            Err(e) => {
                let warn_str = format!(
                    "Cloudflare pool failed to acquire solve lease for proxy {}:{} and site {site_host}. {e}",
                    proxy.proxy_address, proxy.port
                );
                logger.log_warn(&warn_str);
                false
            }
        }
    }

    async fn release_solve_lease(
        kv: Option<&RedisKvClient>,
        site_host: &str,
        proxy: &ProxyResult,
        logger: &ProjectLogger,
    ) {
        if let Some(kv) = kv {
            let key = Self::storage_key(Self::SOLVE_LEASE_KEY_PREFIX, site_host, proxy);
            if let Err(e) = kv.delete(&key).await {
                logger.log_warn(&format!(
                    "Cloudflare pool failed to release solve lease for proxy {}:{} and site {site_host}. {e}",
                    proxy.proxy_address, proxy.port
                ));
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_with_session(
        url: &str,
        options: &RequestOptions,
        proxy: &ProxyResult,
        client: &wreq::Client,
        scraper_proxy: &mut ScraperProxy<'a>,
        source_scraper: &SourceScraper<'a>,
        logger: &ProjectLogger,
    ) -> Result<Response, ScraperError> {
        logger.log_debug(&format!(
            "Cloudflare pool request to {url} via proxy {}:{}",
            proxy.proxy_address, proxy.port
        ));
        scraper_proxy.set_sticky_proxy(proxy.clone());
        let response = source_scraper
            .request_with_rquest(url, options, client, Some(scraper_proxy), None)
            .await;
        scraper_proxy.clear_sticky_proxy();
        response
    }
}

#[cfg(test)]
mod tests {
    type Pool = CloudflarePool<'static>;
    use super::*;

    #[test]
    fn test_site_host_extracts_domains() {
        assert_eq!(
            Pool::site_host("https://oddspedia.com/api/v1/getOddsMovements?matchId=1"),
            Some("oddspedia.com")
        );
        assert_eq!(
            Pool::site_host("https://sub.site.co.uk/path?q=1"),
            Some("sub.site.co.uk")
        );
        assert_eq!(Pool::site_host("not-a-url"), None);
    }

    fn proxy_for(address: &str, port: u32) -> PersistedClearance {
        PersistedClearance {
            site_host: "oddspedia.com".to_owned(),
            proxy_address: address.to_owned(),
            port,
            user_agent: "ua".to_owned(),
            cookie_header: "cf_clearance=t; __cf_bm=b".to_owned(),
            extra_headers: vec![("X-Extra".to_owned(), "1".to_owned())],
        }
    }

    #[test]
    fn test_persisted_clearance_roundtrip() {
        let record = proxy_for("10.0.0.1", 8080);
        let json = serde_json::to_string(&record).unwrap();
        assert_eq!(
            serde_json::from_str::<PersistedClearance>(&json).unwrap(),
            record
        );
    }

    #[test]
    fn test_storage_keys_isolate_site_and_proxy() {
        let proxy: ProxyResult = serde_json::from_str(
            r#"{"username":"user","password":"pass","proxy_address":"10.0.0.1","port":8080,"valid":true}"#,
        )
        .unwrap();
        assert_eq!(
            Pool::storage_key(Pool::CLEARANCE_KEY_PREFIX, "oddspedia.com", &proxy),
            "cf_clearance:oddspedia.com:10.0.0.1:8080"
        );
        assert_eq!(
            Pool::storage_key(Pool::SOLVE_LEASE_KEY_PREFIX, "oddspedia.com", &proxy),
            "cf_solve_lease:oddspedia.com:10.0.0.1:8080"
        );
        let other_site = Pool::storage_key(Pool::CLEARANCE_KEY_PREFIX, "other.com", &proxy);
        assert_ne!(
            other_site,
            Pool::storage_key(Pool::CLEARANCE_KEY_PREFIX, "oddspedia.com", &proxy)
        );
        assert_eq!(
            Pool::storage_key("prefix", "site", &proxy),
            Pool::storage_key("prefix", "site", &proxy)
        );
    }
}
