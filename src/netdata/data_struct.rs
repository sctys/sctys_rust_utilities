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
}

#[derive(Debug)]
pub struct BrowseSetting<'a> {
    pub restart_web_driver: bool,
    pub calling_func: &'a str,
    pub log_only: bool,
}

pub enum ResponseCheckResult {
    Ok,
    ErrContinue(String),
    ErrTerminate(String),
}