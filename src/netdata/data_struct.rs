use std::{collections::HashMap, error::Error, fmt::Display, time::Duration};

use pyo3::prelude::*;
use reqwest::{header::HeaderMap, Url};

use super::proxy::ProxyError;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct UrlFile {
    pub url: Url,
    pub file_name: String,
}

impl UrlFile {
    pub fn new(url: Url, file_name: String) -> Self {
        Self { url, file_name }
    }
}

#[derive(Debug, Clone)]
pub struct RequestSetting<'a> {
    pub calling_func: &'a str,
    pub log_only: bool,
    pub in_s3: bool,
}

#[derive(Debug, Clone)]
pub struct BrowseSetting<'a> {
    pub restart_web_driver: bool,
    pub calling_func: &'a str,
    pub log_only: bool,
    pub in_s3: bool,
}

pub enum ResponseCheckResult {
    Ok(String),
    ErrContinue(String),
    ErrTerminate(String),
}

impl ResponseCheckResult {
    pub fn get_content(&self) -> Option<String> {
        match self {
            Self::Ok(content) => Some(content.to_string()),
            _ => None,
        }
    }

    pub fn get_error(&self) -> Option<String> {
        match self {
            Self::ErrContinue(e) | Self::ErrTerminate(e) => Some(e.to_string()),
            _ => None,
        }
    }
}

pub struct RequestOptions {
    pub timeout: Duration,
    pub headers: Option<HeaderMap>,
    pub proxy: bool,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self {
            timeout: RequestOptions::DEFAULT_TIMEOUT,
            headers: None,
            proxy: true,
        }
    }
}

impl RequestOptions {
    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(15);

    pub fn convert_header_map_to_map(&self) -> Option<HashMap<String, String>> {
        self.headers.as_ref().map(|headers| {
            headers
                .iter()
                .filter_map(|(k, v)| v.to_str().ok().map(|v| (k.to_string(), v.to_string())))
                .collect()
        })
    }
}

pub struct BrowseOptions {
    pub headless: bool,
    pub browser_wait: Duration,
    pub page_evaluation: Option<String>,
}

#[derive(Debug)]
pub struct Response {
    pub content: String,
    pub status_code: u16,
    pub url: String,
}

impl Response {
    pub async fn from_reqwest_response(response: reqwest::Response) -> Result<Self, ScraperError> {
        let status_code = response.status().as_u16();
        let url = response.url().to_string();
        Ok(Self {
            content: response.text().await?,
            status_code,
            url,
        })
    }

    pub async fn from_rquest_response(response: rquest::Response) -> Result<Self, ScraperError> {
        let status_code = response.status().as_u16();
        let url = response.url().to_string();
        Ok(Self {
            content: response.text().await?,
            status_code,
            url,
        })
    }
}

#[derive(FromPyObject, Debug)]
pub struct PyResponse {
    content: String,
    status_code: u16,
    url: String,
    ok: bool,
    reason: String,
}

impl PyResponse {
    pub fn to_response(self) -> Result<Response, ScraperError> {
        if self.ok {
            Ok(Response {
                content: self.content,
                status_code: self.status_code,
                url: self.url,
            })
        } else {
            Err(ScraperError::PyScraper(self.reason))
        }
    }
}

#[derive(Debug)]
pub enum ScraperError {
    Reqwest(reqwest::Error),
    Rquest(rquest::Error),
    PyRequest(pyo3::PyErr),
    PyScraper(String),
    Proxy(ProxyError),
    Playwright(playwright_rust::Error),
    SerdeJsonError(serde_json::Error),
    IoError(std::io::Error),
    ApiGatewayError(Box<dyn Error + Send + Sync>),
    Other(String),
}

impl Display for ScraperError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScraperError::Reqwest(e) => write!(f, "Reqwest error: {e}"),
            ScraperError::Rquest(e) => write!(f, "Rquest error: {e}"),
            ScraperError::PyRequest(e) => write!(f, "PyRequest error: {e}"),
            ScraperError::PyScraper(e) => write!(f, "PyScraper error: {e}"),
            ScraperError::Proxy(e) => write!(f, "Proxy error: {e}"),
            ScraperError::Playwright(e) => write!(f, "Playwright error: {e}"),
            ScraperError::SerdeJsonError(e) => write!(f, "SerdeJsonError error: {e}"),
            ScraperError::IoError(e) => write!(f, "IO error: {e}"),
            ScraperError::ApiGatewayError(e) => write!(f, "ApiGatewayError error: {e}"),
            ScraperError::Other(e) => write!(f, "Other error: {e}"),
        }
    }
}

impl Error for ScraperError {}

impl From<reqwest::Error> for ScraperError {
    fn from(value: reqwest::Error) -> Self {
        Self::Reqwest(value)
    }
}

impl From<rquest::Error> for ScraperError {
    fn from(value: rquest::Error) -> Self {
        Self::Rquest(value)
    }
}

impl From<pyo3::PyErr> for ScraperError {
    fn from(value: pyo3::PyErr) -> Self {
        Self::PyRequest(value)
    }
}

impl From<ProxyError> for ScraperError {
    fn from(value: ProxyError) -> Self {
        Self::Proxy(value)
    }
}

impl From<playwright_rust::Error> for ScraperError {
    fn from(value: playwright_rust::Error) -> Self {
        Self::Playwright(value)
    }
}

impl From<serde_json::Error> for ScraperError {
    fn from(value: serde_json::Error) -> Self {
        Self::SerdeJsonError(value)
    }
}

impl From<std::io::Error> for ScraperError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(value)
    }
}

impl From<Box<dyn Error + Send + Sync>> for ScraperError {
    fn from(value: Box<dyn Error + Send + Sync>) -> Self {
        Self::ApiGatewayError(value)
    }
}
