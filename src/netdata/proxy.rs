use std::{env, error::Error, fmt::Display, fs, path::Path, time::Duration};

use async_trait::async_trait;
use chrono::{Duration as LongDuration, Utc};
use fxhash::FxHashMap;
use playwright_rust::api::ProxySettings;
use rand::Rng;
use reqwest::{
    header::{HeaderMap, HeaderValue, InvalidHeaderValue, AUTHORIZATION},
    ClientBuilder,
};
use serde::{Deserialize, Serialize};

use crate::{
    logger::ProjectLogger,
    time_operation::{self, async_sleep},
};

pub struct ScraperProxy<'a> {
    logger: &'a ProjectLogger,
    full_proxy_list: Vec<ProxyResult>,
    active_proxy_list: Vec<ProxyResult>,
    block_proxy_dict: FxHashMap<String, u8>,
    last_update: Option<i64>,
    next_refresh_time: Option<i64>,
    proxy_config: ProxyConfig,
}

impl<'a> ScraperProxy<'a> {
    const BLOCK_COUNT: u8 = 3;
    const REFRESH_PERIOD: LongDuration = LongDuration::minutes(30);

    pub fn new(logger: &'a ProjectLogger) -> Self {
        let proxy_config = ProxyConfig::load_proxy_config();
        Self {
            logger,
            full_proxy_list: Vec::new(),
            active_proxy_list: Vec::new(),
            block_proxy_dict: FxHashMap::default(),
            last_update: None,
            next_refresh_time: None,
            proxy_config,
        }
    }

    async fn get_full_proxy_list(&mut self) -> Result<(), ProxyError> {
        let proxy_list = ProxyResult::get_proxy_result_list(
            &self.proxy_config.proxy_list_url,
            &self.proxy_config.proxy_token,
        )
        .await
        .map_err(|e| {
            let error_str = format!("Fail to get proxy list. {e}");
            self.logger.log_error(&error_str);
            e
        })?;
        self.full_proxy_list = proxy_list
            .into_iter()
            .filter(|proxy| !self.is_proxy_blocked(proxy))
            .collect();
        if self.full_proxy_list.is_empty() {
            let error_str = "Proxy list is empty. All proxies are blocked";
            self.logger.log_error(error_str);
            panic!("{error_str}");
        }
        self.last_update = Some(Utc::now().timestamp());
        self.reset_active_list();
        let debug_str = format!(
            "Blocked proxy list: {:#?}\nNumber of proxy: {}\nNext update time: {:?}",
            self.block_proxy_dict,
            self.full_proxy_list.len(),
            self.next_refresh_time
        );
        self.logger.log_debug(&debug_str);
        Ok(())
    }

    async fn get_next_refresh_time(&mut self) -> Result<(), ProxyError> {
        let next_refresh_time = PlanResult::get_next_refresh_time(
            &self.proxy_config.plan_url,
            &self.proxy_config.proxy_token,
        )
        .await
        .map_err(|e| {
            let error_str = format!("Fail to get next refresh time. {e}");
            self.logger.log_error(&error_str);
            e
        })?;
        self.next_refresh_time = next_refresh_time;
        Ok(())
    }

    fn reset_active_list(&mut self) {
        self.active_proxy_list = self.full_proxy_list.clone();
    }

    fn random_draw_from_active_list(&mut self) -> ProxyResult {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..self.active_proxy_list.len());
        self.active_proxy_list.remove(index)
    }

    pub fn add_proxy_block_count(&mut self, proxy: &ProxyResult) {
        let proxy_address = proxy.get_http_address();
        self.block_proxy_dict
            .entry(proxy_address)
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }

    fn is_proxy_blocked(&self, proxy: &ProxyResult) -> bool {
        let proxy_address = proxy.get_http_address();
        self.block_proxy_dict
            .get(&proxy_address)
            .is_some_and(|count| *count >= Self::BLOCK_COUNT)
    }

    async fn maybe_refresh_list(&mut self) -> Result<(), ProxyError> {
        let current_time = Utc::now().timestamp();
        match self.next_refresh_time {
            Some(next_refresh_time) => {
                if self
                    .last_update
                    .is_some_and(|last_update| last_update >= next_refresh_time)
                {
                    self.get_next_refresh_time().await?;
                }
            }
            None => {
                self.get_next_refresh_time().await?;
            }
        };
        if self.full_proxy_list.is_empty() || self.last_update.is_none() {
            self.get_full_proxy_list().await?;
        } else if self.last_update.is_some_and(|last_update| {
            current_time - last_update >= Self::REFRESH_PERIOD.num_seconds()
        }) {
            let debug_str = format!(
                "Refresh proxy list because last update time is over {} minutes",
                Self::REFRESH_PERIOD.num_minutes()
            );
            self.logger.log_debug(&debug_str);
            self.get_full_proxy_list().await?;
        } else if self
            .next_refresh_time
            .is_some_and(|next_refresh_time| current_time >= next_refresh_time)
        {
            let debug_str = "Refresh proxy list because next refresh time is over";
            self.logger.log_debug(debug_str);
            async_sleep(Duration::from_secs(60)).await;
            self.get_full_proxy_list().await?;
        }
        Ok(())
    }

    pub async fn generate_proxy(&mut self) -> Result<ProxyResult, ProxyError> {
        loop {
            self.maybe_refresh_list().await?;
            if self.active_proxy_list.is_empty() {
                self.reset_active_list();
            }
            let proxy = self.random_draw_from_active_list();
            if !self.is_proxy_blocked(&proxy) {
                return Ok(proxy);
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyResult {
    username: String,
    password: String,
    pub proxy_address: String,
    pub port: u32,
    valid: bool,
}

impl ProxyResult {
    const HTTP_ADDRESS: &str = "http://{user_name}:{password}@{proxy_address}:{port}";
    const SERVER_ADDRESS: &str = "http://{proxy_address}:{port}";

    fn is_valid(&self) -> bool {
        self.valid
    }

    pub fn get_http_address(&self) -> String {
        Self::HTTP_ADDRESS
            .replace("{user_name}", &self.username)
            .replace("{password}", &self.password)
            .replace("{proxy_address}", &self.proxy_address)
            .replace("{port}", &self.port.to_string())
    }

    fn get_server_address(&self) -> String {
        Self::SERVER_ADDRESS
            .replace("{proxy_address}", &self.proxy_address)
            .replace("{port}", &self.port.to_string())
    }

    pub fn get_reqwest_proxy(&self) -> Result<reqwest::Proxy, ProxyError> {
        let proxy = reqwest::Proxy::all(self.get_http_address())?;
        Ok(proxy)
    }

    pub fn get_rquest_proxy(&self) -> Result<rquest::Proxy, ProxyError> {
        let proxy = rquest::Proxy::all(self.get_http_address())?;
        Ok(proxy)
    }

    pub fn get_playwright_proxy(&self) -> ProxySettings {
        ProxySettings {
            server: self.get_server_address(),
            bypass: None,
            username: Some(self.username.clone()),
            password: Some(self.password.clone()),
        }
    }

    async fn get_proxy_result_list(
        proxy_list_url: &str,
        proxy_token: &str,
    ) -> Result<Vec<Self>, ProxyError> {
        let mut proxy_list = Vec::new();
        let mut response = ProxyList::request_proxy_list(proxy_list_url, proxy_token).await?;
        proxy_list.extend(
            response
                .results
                .into_iter()
                .filter(|proxy| proxy.is_valid()),
        );
        while let Some(next_url) = response.next {
            response = ProxyList::request_proxy_list(&next_url, proxy_token).await?;
            proxy_list.extend(
                response
                    .results
                    .into_iter()
                    .filter(|proxy| proxy.is_valid()),
            );
        }
        Ok(proxy_list)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ProxyList {
    next: Option<String>,
    results: Vec<ProxyResult>,
}

impl ProxyRequest for ProxyList {}

impl ProxyList {
    async fn request_proxy_list(
        proxy_list_url: &str,
        proxy_token: &str,
    ) -> Result<Self, ProxyError> {
        let response = Self::request_in_proxy_site(proxy_list_url, proxy_token).await?;
        Ok(serde_json::from_str(&response)
            .unwrap_or_else(|e| panic!("Unable to parse proxy list {e}")))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PlanResult {
    id: i32,
    status: String,
    automatic_refresh_next_at: Option<String>,
}

impl ProxyRequest for PlanResult {}

impl PlanResult {
    async fn request_proxy_plan(
        proxy_plan_url: &str,
        proxy_token: &str,
    ) -> Result<Self, ProxyError> {
        let response = Self::request_in_proxy_site(proxy_plan_url, proxy_token).await?;
        Ok(serde_json::from_str(&response)?)
    }
}

impl PlanResult {
    const ACTIVE: &str = "active";
    async fn get_next_refresh_time(
        proxy_plan_url: &str,
        proxy_token: &str,
    ) -> Result<Option<i64>, ProxyError> {
        let proxy_plan = PlanList::request_proxy_plan(proxy_plan_url, proxy_token).await?;
        if let Some(plan_id) = proxy_plan
            .results
            .iter()
            .find(|result| result.status.as_str() == Self::ACTIVE)
            .map(|plan| plan.id)
        {
            let proxy_plan_url = format!("{proxy_plan_url}{plan_id}/");
            let proxy_plan = PlanResult::request_proxy_plan(&proxy_plan_url, proxy_token).await?;
            if let Some(time_str) = proxy_plan.automatic_refresh_next_at.as_ref() {
                Ok(Some(
                    time_operation::date_time_timezone_from_string(time_str, "%Y-%m-%dT%H:%M:%SZ")?
                        .timestamp(),
                ))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PlanList {
    results: Vec<PlanResult>,
}

impl ProxyRequest for PlanList {}

impl PlanList {
    async fn request_proxy_plan(
        proxy_plan_url: &str,
        proxy_token: &str,
    ) -> Result<Self, ProxyError> {
        let response = Self::request_in_proxy_site(proxy_plan_url, proxy_token).await?;
        Ok(serde_json::from_str(&response)?)
    }
}

#[async_trait]
trait ProxyRequest {
    const RETRY_COUNT: u32 = 30;
    const TIMEOUT: Duration = Duration::from_secs(5);
    const RETRY_SLEEP: Duration = Duration::from_secs(3);

    async fn request_in_proxy_site(
        proxy_url: &str,
        proxy_token: &str,
    ) -> Result<String, ProxyError> {
        let client_builder = ClientBuilder::new();
        let mut headers = HeaderMap::new();
        let auth_value = HeaderValue::from_str(proxy_token)?;
        headers.insert(AUTHORIZATION, auth_value);
        let client = client_builder
            .default_headers(headers)
            .timeout(Self::TIMEOUT)
            .build()?;
        let mut error = None;
        for _ in 0..Self::RETRY_COUNT {
            let res = client.get(proxy_url).send().await;
            match res {
                Ok(r) => match r.error_for_status() {
                    Ok(response) => {
                        return Ok(response.text().await?);
                    }
                    Err(e) => {
                        error = Some(e);
                    }
                },
                Err(e) => {
                    error = Some(e);
                }
            }
            time_operation::async_sleep(Self::RETRY_SLEEP).await;
        }
        if let Some(e) = error {
            Err(ProxyError::Reqwest(e))
        } else {
            panic!("Unable to get proxy list and no error found")
        }
    }
}

#[derive(Debug)]
pub enum ProxyError {
    Reqwest(reqwest::Error),
    Rquest(rquest::Error),
    Json(serde_json::Error),
    Chrono(chrono::ParseError),
    Header(InvalidHeaderValue),
    NoValidProxy(String),
}

impl Display for ProxyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProxyError::Reqwest(e) => write!(f, "Reqwest error: {e}"),
            ProxyError::Rquest(e) => write!(f, "Rquest error: {e}"),
            ProxyError::Json(e) => write!(f, "Json error: {e}"),
            ProxyError::Chrono(e) => write!(f, "Chrono error: {e}"),
            ProxyError::Header(e) => write!(f, "Header error: {e}"),
            ProxyError::NoValidProxy(e) => write!(f, "Proxy error: {e}"),
        }
    }
}

impl Error for ProxyError {}

impl From<reqwest::Error> for ProxyError {
    fn from(value: reqwest::Error) -> Self {
        ProxyError::Reqwest(value)
    }
}

impl From<rquest::Error> for ProxyError {
    fn from(value: rquest::Error) -> Self {
        ProxyError::Rquest(value)
    }
}

impl From<serde_json::Error> for ProxyError {
    fn from(value: serde_json::Error) -> Self {
        ProxyError::Json(value)
    }
}

impl From<chrono::ParseError> for ProxyError {
    fn from(value: chrono::ParseError) -> Self {
        ProxyError::Chrono(value)
    }
}

impl From<InvalidHeaderValue> for ProxyError {
    fn from(value: InvalidHeaderValue) -> Self {
        ProxyError::Header(value)
    }
}

#[derive(Deserialize)]
struct ProxyConfig {
    proxy_list_url: String,
    plan_url: String,
    proxy_token: String,
}

impl ProxyConfig {
    const PROJECT_KEY: &str = "SCTYS_PROJECT";
    const PROXY_CONFIG_PATH: &str = "Secret/secret_sctys_rust_utilities";
    const PRXOY_CONFIG_FILE: &str = "proxy.toml";

    fn load_proxy_config() -> Self {
        let full_proxy_path =
            Path::new(&env::var(Self::PROJECT_KEY).expect("Unable to find project path"))
                .join(Self::PROXY_CONFIG_PATH)
                .join(Self::PRXOY_CONFIG_FILE);
        let proxy_str = fs::read_to_string(&full_proxy_path).unwrap_or_else(|e| {
            panic!(
                "Unable to load the proxy file {}, {e}",
                full_proxy_path.display()
            )
        });
        let proxy_data: ProxyConfig = toml::from_str(&proxy_str).unwrap_or_else(|e| {
            panic!(
                "Unable to parse the proxy file {}, {e}",
                full_proxy_path.display()
            )
        });
        proxy_data
    }
}

#[cfg(test)]
mod tests {

    use log::LevelFilter;

    use super::*;

    #[tokio::test]
    async fn test_generate_proxy() {
        let logger_name = "test_proxy";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_proxy");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let mut scraper_proxy = ScraperProxy::new(&project_logger);
        for _ in 0..3 {
            let proxy = scraper_proxy.generate_proxy().await.unwrap();
            dbg!(proxy);
        }
    }
}
