use reqwest::Url;

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

#[derive(Debug)]
pub struct RequestSetting<'a> {
    pub calling_func: &'a str,
    pub log_only: bool,
    pub in_s3: bool
}

#[derive(Debug)]
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
            _ => None
        }
    }

    pub fn get_error(&self) -> Option<String> {
        match self {
            Self::ErrContinue(e) | Self::ErrTerminate(e) => Some(e.to_string()),
            _ => None
        }
    }
}
