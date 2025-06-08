use std::{collections::HashMap, error::Error, fmt::Display, time::Duration};

use chrono::{DateTime, Utc};
use pyo3::{
    prelude::*,
    types::{IntoPyDict, PyDict},
};
use reqwest::{header::HeaderMap, Url};

use crate::python_utils::PythonPath;

use super::{
    proxy::ProxyError,
    python_struct::{NetdataPythonPath, PythonTxt},
};

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
    ResultOk(String),
    ErrContinue(String),
    ErrTerminate(String),
}

pub enum Scraper {
    Reqwest(bool),
    Rquest(bool),
    CurlCffi,
    Playwright(BrowseOptions),
}

pub struct ScrapeOptions {
    pub num_retry: u8,
    pub retry_sleep: Duration,
    pub consecutive_sleep: (Duration, Duration),
    pub use_proxy: bool,
    pub scraper: Scraper,
    pub update_domain: bool,
}

pub struct FilterOptions {
    pub cutoff_date: Option<DateTime<Utc>>,
    pub filter_scraped: bool,
    pub filter_attempted: bool,
}

impl FilterOptions {
    fn override_cutoff_date(&mut self, cutoff_date: DateTime<Utc>) {
        self.cutoff_date = Some(cutoff_date);
    }

    fn override_filter_scraped(&mut self, filter_scraped: bool) {
        self.filter_scraped = filter_scraped;
    }

    fn override_filter_attempted(&mut self, filter_attempted: bool) {
        self.filter_attempted = filter_attempted;
    }

    pub fn override_filter_options(
        &mut self,
        cutoff_date: Option<DateTime<Utc>>,
        filter_scraped: Option<bool>,
        filter_attempted: Option<bool>,
    ) {
        if let Some(cutoff_date) = cutoff_date {
            self.override_cutoff_date(cutoff_date);
        }
        if let Some(filter_scraped) = filter_scraped {
            self.override_filter_scraped(filter_scraped);
        }
        if let Some(filter_attempted) = filter_attempted {
            self.override_filter_attempted(filter_attempted);
        }
    }
}

pub struct RequestOptions {
    pub timeout: Duration,
    pub headers: Option<HeaderMap>,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self {
            timeout: RequestOptions::DEFAULT_TIMEOUT,
            headers: None,
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

    pub fn convert_map_to_header_map(normal_map: HashMap<&'static str, String>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (key, value) in normal_map {
            let header_value = value
                .parse()
                .unwrap_or_else(|e| panic!("Unable to parse header value {value}. {e}"));
            headers.insert(key, header_value);
        }
        headers
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
    pub ok: bool,
    pub reason: String,
}

impl Response {
    pub async fn from_reqwest_response(response: reqwest::Response) -> Result<Self, ScraperError> {
        let status_code = response.status().as_u16();
        let url = response.url().to_string();
        let ok = response.status().is_success();
        let reason = response
            .status()
            .canonical_reason()
            .unwrap_or_default()
            .to_string();
        Ok(Self {
            content: response.text().await?,
            status_code,
            url,
            ok,
            reason,
        })
    }

    pub async fn from_rquest_response(response: rquest::Response) -> Result<Self, ScraperError> {
        let status_code = response.status().as_u16();
        let url = response.url().to_string();
        let ok = response.status().is_success();
        let reason = response
            .status()
            .canonical_reason()
            .unwrap_or_default()
            .to_string();
        Ok(Self {
            content: response.text().await?,
            status_code,
            url,
            ok,
            reason,
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
        Ok(Response {
            content: self.content,
            status_code: self.status_code,
            url: self.url,
            ok: self.ok,
            reason: self.reason,
        })
    }
}

pub struct CurlCffiClient {
    curl_cffi_session: Py<PyAny>,
}

impl Drop for CurlCffiClient {
    fn drop(&mut self) {
        Python::with_gil(|py| {
            let _ = self.curl_cffi_session.call_method1(
                py,
                "__exit__",
                (py.None(), py.None(), py.None()),
            );
        })
    }
}

impl CurlCffiClient {
    const CURL_CFFI_BROWSER: (&'static str, &'static str) = ("impersonate", "chrome");

    pub fn create_session() -> Result<Self, ScraperError> {
        NetdataPythonPath::setup_python_venv();
        Python::with_gil(|py| -> PyResult<CurlCffiClient> {
            let requests = py.import("curl_cffi.requests")?;
            let kwargs = [Self::CURL_CFFI_BROWSER].into_py_dict(py)?;
            let session_obj = requests.call_method("Session", (py.None(),), Some(&kwargs))?;
            let session = session_obj.call_method0("__enter__")?;
            Ok(Self {
                curl_cffi_session: session.into(),
            })
        })
        .map_err(ScraperError::from)
    }

    pub fn request(
        &self,
        url: &str,
        request_options: &RequestOptions,
        proxy: Option<String>,
    ) -> Result<PyResponse, ScraperError> {
        Python::with_gil(|py| {
            NetdataPythonPath::append_script_path(&py)?;
            let request_curl_cffi = py.import(PythonTxt::RequestCurlCffi.to_string())?;
            let kwargs = PyDict::new(py);
            let session = &self.curl_cffi_session;
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
            Ok(response.extract::<PyResponse>()?)
        })
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
