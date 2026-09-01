use std::{collections::HashMap, error::Error, fmt::Display, time::Duration};

use reqwest::ClientBuilder;
use serde::{Deserialize, Serialize};

use crate::{logger::ProjectLogger, secret::aws_secret::Secret, time_operation, PROJECT};

pub struct CapSolver<'a> {
    logger: &'a ProjectLogger,
    config: CapSolverConfig,
}

impl<'a> CapSolver<'a> {
    const RETRY_COUNT: u32 = 30;
    const TIMEOUT: Duration = Duration::from_secs(5);
    const RETRY_SLEEP: Duration = Duration::from_secs(3);

    pub async fn new(
        logger: &'a ProjectLogger,
        secret: &Secret<'a>,
    ) -> Result<Self, CapSolverError> {
        let config = CapSolverConfig::load_cap_solver_config(logger, secret).await?;
        Ok(Self { logger, config })
    }

    pub async fn solve_turnstile(
        &self,
        website_url: &str,
        website_key: &str,
    ) -> Result<String, CapSolverError> {
        let cap_solver_task_response = self
            .create_task(CapSolverTask::create_task(website_url, website_key))
            .await?;
        self.logger.log_debug(&format!(
            "CapSolver task created: {}",
            cap_solver_task_response.task_id
        ));
        let solution = self.get_solution(&cap_solver_task_response).await?;
        if solution.token.is_empty() {
            return Err(CapSolverError::Json(serde::de::Error::custom(
                "No Turnstile token found",
            )));
        }
        self.logger.log_debug("CapSolver Turnstile task solved");
        Ok(solution.token)
    }

    pub async fn solve_cloudflare(
        &self,
        website_url: &str,
        proxy: &str,
        user_agent: &str,
        html: &str,
    ) -> Result<CloudflareSolution, CapSolverError> {
        let cap_solver_task_response = self
            .create_task(CloudflareTask::create_task(
                website_url,
                proxy,
                user_agent,
                html,
            ))
            .await?;
        self.logger.log_debug(&format!(
            "CapSolver Cloudflare task created: {}",
            cap_solver_task_response.task_id
        ));
        let solution = self.get_solution(&cap_solver_task_response).await?;
        let token = solution.token;
        let mut cookies = solution.cookies.unwrap_or_default();
        if cookies.get("cf_clearance").is_none_or(String::is_empty) && !token.is_empty() {
            cookies.insert("cf_clearance".to_string(), token);
        }
        if cookies.get("cf_clearance").is_none_or(String::is_empty) {
            return Err(CapSolverError::Json(serde::de::Error::custom(
                "No Cloudflare cf_clearance cookie found",
            )));
        }
        let user_agent = solution
            .user_agent
            .unwrap_or_else(|| user_agent.to_string());
        let headers = solution.headers.unwrap_or_default();
        self.logger.log_debug(&format!(
            "CapSolver Cloudflare solution context: user agent {user_agent}, {} cookies, {} headers",
            cookies.len(),
            headers.len()
        ));
        self.logger.log_debug("CapSolver Cloudflare task solved");
        Ok(CloudflareSolution {
            cookies,
            user_agent,
            headers,
        })
    }

    async fn get_solution(
        &self,
        cap_solver_task_response: &CapSolverResponse,
    ) -> Result<CapSolverSolution, CapSolverError> {
        time_operation::async_sleep(Self::RETRY_SLEEP).await;
        for _ in 0..Self::RETRY_COUNT {
            let cap_solver_get_result_response =
                self.get_cap_solver_result(cap_solver_task_response).await?;
            if cap_solver_get_result_response.is_ready() {
                return cap_solver_get_result_response.solution.ok_or_else(|| {
                    CapSolverError::Json(serde::de::Error::custom("No solution found"))
                });
            }
            if cap_solver_get_result_response.is_failed() {
                self.logger.log_error(&format!(
                    "CapSolver task failed: {:?}",
                    cap_solver_get_result_response.error_description
                ));
                return Err(CapSolverError::Json(serde::de::Error::custom(
                    "CapSolver task failed",
                )));
            }
            time_operation::async_sleep(Self::RETRY_SLEEP).await;
        }
        Err(CapSolverError::Json(serde::de::Error::custom(format!(
            "Unable to get capsolver response after {} retries",
            Self::RETRY_COUNT
        ))))
    }

    async fn create_task<T: Serialize>(
        &self,
        task: T,
    ) -> Result<CapSolverResponse, CapSolverError> {
        let task_request = CapSolverTaskRequest::create_task(&self.config.api_key, task);
        let client_builder = ClientBuilder::new();
        let client = client_builder.build()?;
        let mut error = None;
        for _ in 0..Self::RETRY_COUNT {
            let res = client
                .post(CapSolverTaskRequest::<T>::CREATE_TASK_URL)
                .timeout(Self::TIMEOUT)
                .json(&task_request)
                .send()
                .await;
            match res {
                Ok(r) => match r.error_for_status() {
                    Ok(response) => match response.json::<CapSolverResponse>().await {
                        Ok(cap_solver_response) => {
                            if cap_solver_response.error_id == 0 {
                                return Ok(cap_solver_response);
                            } else {
                                error = Some(CapSolverError::Json(serde::de::Error::custom(
                                    cap_solver_response.status,
                                )))
                            }
                        }
                        Err(e) => {
                            error = Some(CapSolverError::Reqwest(e));
                        }
                    },
                    Err(e) => {
                        error = Some(CapSolverError::Reqwest(e));
                    }
                },
                Err(e) => {
                    error = Some(CapSolverError::Reqwest(e));
                }
            }
            time_operation::async_sleep(Self::RETRY_SLEEP).await;
        }
        if let Some(e) = error {
            Err(e)
        } else {
            Err(CapSolverError::Json(serde::de::Error::custom(format!(
                "Unable to create capsolver task after {} retries",
                Self::RETRY_COUNT
            ))))
        }
    }

    async fn get_cap_solver_result(
        &self,
        cap_solver_response: &CapSolverResponse,
    ) -> Result<CapSolverGetResultResponse, CapSolverError> {
        let get_result =
            CapSolverGetResult::get_result(&self.config.api_key, &cap_solver_response.task_id);
        let client_builder = ClientBuilder::new();
        let client = client_builder.build()?;
        let mut error = None;
        for _ in 0..Self::RETRY_COUNT {
            let res = client
                .post(CapSolverGetResult::GET_RESULT_URL)
                .timeout(Self::TIMEOUT)
                .json(&get_result)
                .send()
                .await;
            match res {
                Ok(r) => match r.error_for_status() {
                    Ok(response) => match response.json::<CapSolverGetResultResponse>().await {
                        Ok(cap_solver_get_result_response) => {
                            return Ok(cap_solver_get_result_response)
                        }
                        Err(e) => {
                            error = Some(CapSolverError::Json(serde::de::Error::custom(
                                e.to_string(),
                            )))
                        }
                    },
                    Err(e) => error = Some(CapSolverError::Reqwest(e)),
                },
                Err(e) => error = Some(CapSolverError::Reqwest(e)),
            }
            time_operation::async_sleep(Self::RETRY_SLEEP).await;
        }
        if let Some(e) = error {
            Err(e)
        } else {
            Err(CapSolverError::Json(serde::de::Error::custom(format!(
                "Unable to get capsolver result after {} retries",
                Self::RETRY_COUNT
            ))))
        }
    }
}

pub struct CloudflareSolution {
    cookies: HashMap<String, String>,
    user_agent: String,
    headers: HashMap<String, String>,
}

impl CloudflareSolution {
    pub fn cookies(&self) -> &HashMap<String, String> {
        &self.cookies
    }

    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }

    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MetaData {
    challenge_type: String,
}

impl Default for MetaData {
    fn default() -> Self {
        Self {
            challenge_type: Self::CHALLENGE_TYPE.to_string(),
        }
    }
}

impl MetaData {
    const CHALLENGE_TYPE: &str = "turnstile";
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CapSolverTask {
    #[serde(rename = "type")]
    type_: String,
    website_url: String,
    website_key: String,
    metadata: MetaData,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CloudflareTask {
    #[serde(rename = "type")]
    type_: String,
    website_url: String,
    proxy: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    html: Option<String>,
}

impl CloudflareTask {
    const TASK_TYPE: &str = "AntiCloudflareTask";

    fn create_task(website_url: &str, proxy: &str, user_agent: &str, html: &str) -> Self {
        Self {
            type_: Self::TASK_TYPE.to_string(),
            website_url: website_url.to_string(),
            proxy: proxy.to_string(),
            user_agent: (!user_agent.is_empty()).then(|| user_agent.to_string()),
            html: (!html.is_empty()).then(|| html.to_string()),
        }
    }
}

impl CapSolverTask {
    const TASK_TYPE: &str = "AntiTurnstileTaskProxyLess";

    pub fn create_task(website_url: &str, website_key: &str) -> Self {
        Self {
            type_: Self::TASK_TYPE.to_string(),
            website_url: website_url.to_string(),
            website_key: website_key.to_string(),
            metadata: MetaData::default(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CapSolverTaskRequest<T> {
    client_key: String,
    task: T,
}

impl<T> CapSolverTaskRequest<T> {
    const CREATE_TASK_URL: &str = "https://api.capsolver.com/createTask";

    fn create_task(client_key: &str, task: T) -> Self {
        Self {
            client_key: client_key.to_string(),
            task,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CapSolverResponse {
    error_id: i32,
    status: String,
    task_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CapSolverGetResult {
    client_key: String,
    task_id: String,
}

impl CapSolverGetResult {
    const GET_RESULT_URL: &str = "https://api.capsolver.com/getTaskResult";

    pub fn get_result(client_key: &str, task_id: &str) -> Self {
        Self {
            client_key: client_key.to_string(),
            task_id: task_id.to_string(),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CapSolverSolution {
    #[serde(default)]
    token: String,
    #[serde(default)]
    cookies: Option<HashMap<String, String>>,
    #[serde(default)]
    user_agent: Option<String>,
    #[serde(default)]
    headers: Option<HashMap<String, String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CapSolverGetResultResponse {
    error_id: i32,
    status: String,
    error_description: Option<String>,
    solution: Option<CapSolverSolution>,
}

impl CapSolverGetResultResponse {
    fn is_ready(&self) -> bool {
        self.status == "ready"
    }

    fn is_failed(&self) -> bool {
        self.status == "failed" || self.error_id != 0
    }
}

#[derive(Debug)]
pub enum CapSolverError {
    Reqwest(reqwest::Error),
    Json(serde_json::Error),
    Secret(String),
}

impl Display for CapSolverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CapSolverError::Reqwest(e) => write!(f, "Reqwest error: {}", e),
            CapSolverError::Json(e) => write!(f, "Json error: {}", e),
            CapSolverError::Secret(e) => write!(f, "Secret error: {}", e),
        }
    }
}

impl Error for CapSolverError {}

impl From<reqwest::Error> for CapSolverError {
    fn from(value: reqwest::Error) -> Self {
        Self::Reqwest(value)
    }
}

impl From<serde_json::Error> for CapSolverError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

struct CapSolverConfig {
    api_key: String,
}

impl CapSolverConfig {
    async fn load_cap_solver_config(
        logger: &ProjectLogger,
        secret: &Secret<'_>,
    ) -> Result<Self, CapSolverError> {
        const CATEGORY: &str = "capsolver";
        const APIKEY: &str = "apikey";

        let api_key = secret
            .get_secret_value(PROJECT, CATEGORY, APIKEY)
            .await
            .map_err(|e| {
                let error_str = format!("Fail to get api key. {}", e);
                logger.log_error(&error_str);
                CapSolverError::Secret(error_str)
            })?;
        Ok(Self { api_key })
    }
}

#[cfg(test)]
mod tests {
    use std::{env, path::Path};

    use log::LevelFilter;

    use crate::{logger::ProjectLogger, netdata::capsolver::CapSolver, secret::aws_secret::Secret};

    #[tokio::test]
    async fn test_solve_turnstile() {
        let logger_name = "test_cap_solver";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_proxy");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let secret = Secret::new(&project_logger).await;
        let cap_solver = CapSolver::new(&project_logger, &secret).await.unwrap();
        let website_url = "https://www.fotmob.com/";
        let website_id = "0x4AAAAAACOZughTsLoeXwvg";
        let token = cap_solver
            .solve_turnstile(website_url, website_id)
            .await
            .unwrap();
        println!("Token: {}", &token);
        let verify_url = "https://www.fotmob.com/api/turnstile/verify";
        let client = reqwest::Client::new();
        let res = client.post(verify_url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        .header("Origin", "https://www.fotmob.com")
        .header("Referer", "https://www.fotmob.com/")
        .json(&serde_json::json!({"token": token})).send().await.unwrap();
        let header = res.headers();
        println!("Header: {:?}", header);
    }
}
