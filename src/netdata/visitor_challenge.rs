//! Provider hooks and session state for visitor-challenge cookies and proxy pinning.
//!
//! Providers own challenge detection and solving. Callers own physical request
//! replay and retry limits; this module prepares the session for that replay.

use crate::netdata::{
    data_struct::{RequestOptions, Response, ScraperError},
    proxy::{ProxyResult, ScraperProxy},
    source_scraper::SourceScraper,
};
use async_trait::async_trait;
use fxhash::FxHashMap;

const DIRECT_CACHE_KEY: &str = "direct";

/// Inputs a provider needs to solve its challenge without making the shared crate
/// depend on provider-specific HTTP protocols, hosts, or credentials.
pub struct VisitorSolveContext<'a, 'source> {
    pub url: &'a str,
    pub request_options: &'a RequestOptions,
    pub proxy: Option<&'a ProxyResult>,
    pub response: &'a Response,
    pub client: &'a wreq::Client,
    pub source: &'a SourceScraper<'source>,
}

/// Provider-specific visitor-challenge behavior. The shared session owns
/// cookie storage, proxy pinning, replay orchestration, and TTL bookkeeping;
/// providers own applicability, wire format, headers, and solving.
#[async_trait]
pub trait VisitorChallenge: Send + Sync {
    fn is_applicable(&self, url: &str) -> bool;
    fn cache_key(&self, url: &str, proxy_key: &str) -> String;
    fn is_challenge_response(&self, url: &str, response: &Response) -> bool;
    fn cookie_ttl_seconds(&self, url: &str) -> i64;
    fn attach_cookie(
        &self,
        request_options: &mut RequestOptions,
        cache_key: &str,
        url: &str,
        cookie: Option<&str>,
    );
    fn clear_on_response(&self, url: &str, response: &Response) -> bool;
    async fn solve(&self, context: &VisitorSolveContext<'_, '_>) -> Option<String>;
}

/// Default handler for modules that do not use a visitor challenge.
pub struct NoopVisitorChallenge;

pub static NOOP_VISITOR_CHALLENGE: NoopVisitorChallenge = NoopVisitorChallenge;

#[async_trait]
impl VisitorChallenge for NoopVisitorChallenge {
    fn is_applicable(&self, _: &str) -> bool {
        false
    }

    fn cache_key(&self, _: &str, proxy_key: &str) -> String {
        proxy_key.to_string()
    }

    fn is_challenge_response(&self, _: &str, _: &Response) -> bool {
        false
    }

    fn cookie_ttl_seconds(&self, _: &str) -> i64 {
        0
    }

    fn attach_cookie(&self, _: &mut RequestOptions, _: &str, _: &str, _: Option<&str>) {}

    fn clear_on_response(&self, _: &str, _: &Response) -> bool {
        false
    }

    async fn solve(&self, _: &VisitorSolveContext<'_, '_>) -> Option<String> {
        None
    }
}

// Injectable provider I/O keeps the same production proxy and verification
// path under offline tests. Physical scrape replay remains in the shared engine.
#[async_trait]
trait VisitorProxyAccess: Send {
    async fn pin(&mut self) -> Result<String, ScraperError>;
    fn clear(&mut self);
}
#[async_trait]
trait VisitorChallengeAccess: VisitorProxyAccess {
    async fn verify(
        &mut self,
        url: &str,
        options: &RequestOptions,
        response: &Response,
        challenge: &dyn VisitorChallenge,
    ) -> Option<String>;
}
struct ProxyAccess<'a, 'logger> {
    proxy: Option<&'a mut ScraperProxy<'logger>>,
    pinned: Option<ProxyResult>,
}
#[async_trait]
impl VisitorProxyAccess for ProxyAccess<'_, '_> {
    async fn pin(&mut self) -> Result<String, ScraperError> {
        match &mut self.proxy {
            Some(proxy) => {
                let pinned = proxy.generate_proxy().await.map_err(ScraperError::Proxy)?;
                proxy.set_sticky_proxy(pinned.clone());
                let key = pinned.get_http_address();
                self.pinned = Some(pinned);
                Ok(key)
            }
            None => Ok(DIRECT_CACHE_KEY.into()),
        }
    }
    fn clear(&mut self) {
        if let Some(proxy) = &mut self.proxy {
            proxy.clear_sticky_proxy();
        }
    }
}
struct NetworkAccess<'a, 'proxy, 'logger, 'source> {
    proxy: ProxyAccess<'proxy, 'logger>,
    client: &'a wreq::Client,
    source: &'a SourceScraper<'source>,
}
#[async_trait]
impl VisitorProxyAccess for NetworkAccess<'_, '_, '_, '_> {
    async fn pin(&mut self) -> Result<String, ScraperError> {
        self.proxy.pin().await
    }
    fn clear(&mut self) {
        self.proxy.clear();
    }
}
#[async_trait]
impl VisitorChallengeAccess for NetworkAccess<'_, '_, '_, '_> {
    async fn verify(
        &mut self,
        url: &str,
        options: &RequestOptions,
        response: &Response,
        challenge: &dyn VisitorChallenge,
    ) -> Option<String> {
        challenge
            .solve(&VisitorSolveContext {
                url,
                request_options: options,
                proxy: self.proxy.pinned.as_ref(),
                response,
                client: self.client,
                source: self.source,
            })
            .await
    }
}

pub struct VisitorSession {
    cookies: FxHashMap<String, (String, i64)>,
    challenge: &'static dyn VisitorChallenge,
}

impl Default for VisitorSession {
    fn default() -> Self {
        Self::new()
    }
}

impl VisitorSession {
    pub fn new() -> Self {
        Self::with_challenge(&NOOP_VISITOR_CHALLENGE)
    }

    pub fn with_challenge(challenge: &'static dyn VisitorChallenge) -> Self {
        Self {
            cookies: FxHashMap::default(),
            challenge,
        }
    }

    pub fn get_cookie(&self, cache_key: &str) -> Option<String> {
        let now = chrono::Utc::now().timestamp();
        self.cookies
            .get(cache_key)
            .filter(|(_, expires_at)| *expires_at > now)
            .map(|(value, _)| value.clone())
    }

    pub fn store_cookie_with_ttl(&mut self, cache_key: &str, cookie: &str, ttl_seconds: i64) {
        let expires_at = chrono::Utc::now().timestamp() + ttl_seconds;
        self.cookies
            .insert(cache_key.to_string(), (cookie.to_string(), expires_at));
    }

    /// Draws (and pins) a proxy for the attempt and attaches the cached visitor
    /// cookie, if any. On proxy exhaustion the returned error is fed into the
    /// regular response flow for the NoValidProxy panic path.
    pub async fn attach_for_attempt(
        &mut self,
        request_options: &mut RequestOptions,
        scraper_proxy: Option<&mut ScraperProxy<'_>>,
        url: &str,
    ) -> Result<(), ScraperError> {
        self.attach_with(
            request_options,
            &mut ProxyAccess {
                proxy: scraper_proxy,
                pinned: None,
            },
            url,
        )
        .await
    }

    pub async fn prepare_challenge_replay(
        &mut self,
        response: &Response,
        url: &str,
        request_options: &mut RequestOptions,
        rquest_client: &wreq::Client,
        scraper_proxy: Option<&mut ScraperProxy<'_>>,
        source_scraper: &SourceScraper<'_>,
    ) -> Result<bool, ScraperError> {
        let mut access = NetworkAccess {
            proxy: ProxyAccess {
                proxy: scraper_proxy,
                pinned: None,
            },
            client: rquest_client,
            source: source_scraper,
        };
        let ready = self
            .prepare_with(response, url, request_options, &mut access)
            .await?;
        if !ready
            && self.challenge.is_applicable(url)
            && self.challenge.is_challenge_response(url, response)
        {
            source_scraper
                .get_logger()
                .log_warn(&format!("Unable to solve visitor challenge for {url}"));
        }
        Ok(ready)
    }

    async fn attach_with<A: VisitorProxyAccess>(
        &mut self,
        options: &mut RequestOptions,
        access: &mut A,
        url: &str,
    ) -> Result<(), ScraperError> {
        if self.challenge.is_applicable(url) {
            let key = self.challenge.cache_key(url, &access.pin().await?);
            let cookie = self.get_cookie(&key);
            self.challenge
                .attach_cookie(options, &key, url, cookie.as_deref());
        }
        Ok(())
    }
    async fn prepare_with<A: VisitorChallengeAccess>(
        &mut self,
        response: &Response,
        url: &str,
        options: &mut RequestOptions,
        access: &mut A,
    ) -> Result<bool, ScraperError> {
        if !(self.challenge.is_applicable(url)
            && self.challenge.is_challenge_response(url, response))
        {
            if self.challenge.is_applicable(url) && self.challenge.clear_on_response(url, response)
            {
                access.clear();
            }
            return Ok(false);
        }
        let key = self.challenge.cache_key(url, &access.pin().await?);
        let Some(cookie) = access.verify(url, options, response, self.challenge).await else {
            access.clear();
            return Ok(false);
        };
        self.store_cookie_with_ttl(&key, &cookie, self.challenge.cookie_ttl_seconds(url));
        Ok(true)
    }
}

pub fn extract_host(url: &str) -> Option<String> {
    let after_scheme = url.split("://").nth(1)?;
    let host = after_scheme.split('/').next()?;
    Some(host.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_cookie_ttl() {
        let mut session = VisitorSession::new();
        session.store_cookie_with_ttl("key", "value", 60);
        assert_eq!(session.get_cookie("key"), Some("value".to_string()));
        assert_eq!(session.get_cookie("other"), None);
        session
            .cookies
            .insert("expired".to_string(), ("old".to_string(), 1));
        assert_eq!(session.get_cookie("expired"), None);
    }
}

#[cfg(test)]
mod provider_hook_tests {
    use super::*;

    struct MockChallenge {
        applicable: bool,
        challenge_response: bool,
        ttl_seconds: i64,
        clear_on_403: bool,
    }

    static ACTIVE_CHALLENGE: MockChallenge = MockChallenge {
        applicable: true,
        challenge_response: true,
        ttl_seconds: 60,
        clear_on_403: true,
    };
    static FORBIDDEN_CHALLENGE: MockChallenge = MockChallenge {
        applicable: true,
        challenge_response: false,
        ttl_seconds: 60,
        clear_on_403: true,
    };
    static INACTIVE_CHALLENGE: MockChallenge = MockChallenge {
        applicable: false,
        challenge_response: false,
        ttl_seconds: 60,
        clear_on_403: false,
    };

    #[async_trait]
    impl VisitorChallenge for MockChallenge {
        fn is_applicable(&self, _: &str) -> bool {
            self.applicable
        }

        fn cache_key(&self, _: &str, proxy_key: &str) -> String {
            proxy_key.to_string()
        }

        fn is_challenge_response(&self, _: &str, _: &Response) -> bool {
            self.challenge_response
        }

        fn cookie_ttl_seconds(&self, _: &str) -> i64 {
            self.ttl_seconds
        }

        fn attach_cookie(
            &self,
            request_options: &mut RequestOptions,
            _: &str,
            _: &str,
            cookie: Option<&str>,
        ) {
            if let Some(cookie) = cookie {
                request_options.try_insert_to_header_map("Cookie", format!("mock={cookie}"));
            }
        }

        fn clear_on_response(&self, _: &str, response: &Response) -> bool {
            self.clear_on_403 && response.status_code == 403
        }

        async fn solve(&self, _: &VisitorSolveContext<'_, '_>) -> Option<String> {
            None
        }
    }

    #[derive(Default)]
    struct MockAccess {
        events: Vec<&'static str>,
        fail_pin: bool,
        cookie: Option<String>,
    }
    #[async_trait]
    impl VisitorProxyAccess for MockAccess {
        async fn pin(&mut self) -> Result<String, ScraperError> {
            self.events.push("pin");
            if self.fail_pin {
                Err(ScraperError::Other("proxy failure".into()))
            } else {
                Ok("proxy-A".into())
            }
        }
        fn clear(&mut self) {
            self.events.push("clear");
        }
    }
    #[async_trait]
    impl VisitorChallengeAccess for MockAccess {
        async fn verify(
            &mut self,
            _: &str,
            _: &RequestOptions,
            _: &Response,
            _: &dyn VisitorChallenge,
        ) -> Option<String> {
            self.events.push("verify");
            self.cookie.clone()
        }
    }
    fn response() -> Response {
        Response {
            content: "challenge-body".into(),
            status_code: 400,
            url: "https://example.com/fixture".into(),
            ok: false,
            reason: "challenge".into(),
            cookies: Default::default(),
        }
    }
    #[tokio::test]
    async fn proxy_pin_cookie_cache_and_replay_preparation_share_one_provider_path() {
        let mut session = VisitorSession::with_challenge(&ACTIVE_CHALLENGE);
        let mut access = MockAccess {
            cookie: Some("visitor-cookie".into()),
            ..Default::default()
        };
        let mut options = RequestOptions::default();
        let response = response();
        session
            .attach_with(&mut options, &mut access, &response.url)
            .await
            .unwrap();
        assert!(session
            .prepare_with(&response, &response.url, &mut options, &mut access)
            .await
            .unwrap());
        assert_eq!(access.events, ["pin", "pin", "verify"]);
        assert_eq!(
            session.get_cookie("proxy-A").as_deref(),
            Some("visitor-cookie")
        );
        assert!(
            options.headers.is_none(),
            "prepare must not mutate the live request options"
        );
        session
            .attach_with(&mut options, &mut access, &response.url)
            .await
            .unwrap();
        assert_eq!(
            options.headers.as_ref().unwrap().get("Cookie").unwrap(),
            "mock=visitor-cookie"
        );
        options.headers = None;
        session
            .attach_with(&mut options, &mut access, &response.url)
            .await
            .unwrap();
        assert!(options.headers.is_some());
        let before = access.events.len();
        let mut inactive = VisitorSession::with_challenge(&INACTIVE_CHALLENGE);
        inactive
            .attach_with(&mut options, &mut access, "https://example.com/fixture")
            .await
            .unwrap();
        assert_eq!(access.events.len(), before);
    }
    #[tokio::test]
    async fn forbidden_response_clears_pinned_proxy_without_verifying() {
        let mut session = VisitorSession::with_challenge(&FORBIDDEN_CHALLENGE);
        let mut options = RequestOptions::default();
        let mut access = MockAccess::default();
        let response = Response {
            content: "forbidden".into(),
            status_code: 403,
            url: "https://example.com/api/challenge".into(),
            ok: false,
            reason: "Forbidden".into(),
            cookies: Default::default(),
        };
        assert!(!session
            .prepare_with(&response, &response.url, &mut options, &mut access)
            .await
            .unwrap());
        assert_eq!(access.events, ["clear"]);
    }
    #[tokio::test]
    async fn failed_verification_clears_pinning_and_proxy_failure_does_not_verify() {
        let mut session = VisitorSession::with_challenge(&ACTIVE_CHALLENGE);
        let mut options = RequestOptions::default();
        let response = response();
        let mut access = MockAccess::default();
        assert!(!session
            .prepare_with(&response, &response.url, &mut options, &mut access)
            .await
            .unwrap());
        assert_eq!(access.events, ["pin", "verify", "clear"]);
        assert!(session.get_cookie("proxy-A").is_none());
        access.events.clear();
        access.fail_pin = true;
        assert!(session
            .prepare_with(&response, &response.url, &mut options, &mut access)
            .await
            .is_err());
        assert_eq!(access.events, ["pin"]);
    }
}
