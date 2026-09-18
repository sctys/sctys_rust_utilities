use std::time::{Duration, Instant};

use wreq::header::{HeaderName, HeaderValue, COOKIE, USER_AGENT};

use crate::{
    logger::ProjectLogger,
    netdata::{
        capsolver::CapSolver,
        data_struct::{RequestOptions, Response, ScraperError},
        proxy::{ProxyResult, ScraperProxy},
        source_scraper::SourceScraper,
    },
};

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
        let Self {
            sessions,
            max_sessions,
            ..
        } = self;
        let existing_ips: Vec<String> = sessions
            .iter()
            .map(|s| s.proxy.proxy_address.clone())
            .collect();
        let mut probes = 0u8;
        while sessions.len() < *max_sessions && probes < Self::TOP_UP_PROBES {
            probes += 1;
            let proxy = match scraper_proxy.generate_proxy().await {
                Ok(proxy) => proxy,
                Err(_) => break,
            };
            if existing_ips.contains(&proxy.proxy_address) {
                continue;
            }
            let session = CfSession::new(proxy.clone(), Self::CHROME135_USER_AGENT);
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
        let Self {
            cap_solver,
            sessions,
            cursor,
            max_sessions,
            clearance_ttl,
            max_solves_per_request,
            max_bootstrap_attempts,
            solve_count,
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
                        let solve_result = cap_solver
                            .solve_cloudflare(
                                url,
                                &proxy.get_cap_solver_proxy(),
                                &session.user_agent,
                                &response.content,
                            )
                            .await;
                        solves += 1;
                        match solve_result {
                            Ok(solution) => {
                                *solve_count += 1;
                                session.apply_solution(&solution);
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
                                        *cursor = (index + 1) % sessions.len().max(1);
                                        return Ok(retry);
                                    }
                                    Ok(retry) => {
                                        let warn_str = format!(
                                            "Cloudflare pool session {}:{} still blocked after fresh clearance, evicting",
                                            proxy.proxy_address, proxy.port
                                        );
                                        logger.log_warn(&warn_str);
                                        last_response = Some(retry);
                                        sessions.remove(index);
                                        continue;
                                    }
                                    Err(e) => {
                                        logger.log_warn(&format!(
                                            "Cloudflare pool request failed after clearance: {e}"
                                        ));
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
        let mut bootstraps = 0u8;
        while bootstraps < *max_bootstrap_attempts && sessions.len() < *max_sessions {
            bootstraps += 1;
            let proxy = scraper_proxy.generate_proxy().await?;
            let mut session = CfSession::new(proxy.clone(), Self::CHROME135_USER_AGENT);
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
                        let solve_result = cap_solver
                            .solve_cloudflare(
                                url,
                                &proxy.get_cap_solver_proxy(),
                                &session.user_agent,
                                &response.content,
                            )
                            .await;
                        solves += 1;
                        match solve_result {
                            Ok(solution) => {
                                *solve_count += 1;
                                session.apply_solution(&solution);
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
                                        }
                                        return Ok(retry);
                                    }
                                    Err(e) => {
                                        let warn_str = format!(
                                            "Cloudflare pool bootstrap request failed after clearance: {e}"
                                        );
                                        logger.log_warn(&warn_str);
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
                                scraper_proxy.add_proxy_block_count(&proxy);
                                continue;
                            }
                        }
                    }
                    Some(CfBlock::HardBlock) => {
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
