use playwright_rust::api::ProxySettings;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};

use crate::netdata::data_struct::{Response, ScraperError};

const JS_SCRIPT_PATH: &str = "src/netdata/js";
const PLAYWRIGHT_JS: &str = "playwright_js.js";

#[derive(Debug, Serialize)]
struct CommandRequest {
    action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    proxy: Option<ProxySettings>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "contextId")]
    context_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cookies: Option<Vec<HashMap<String, String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Cookie {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommandResponse {
    success: bool,
    context_id: Option<String>,
    content: Option<String>,
    status_code: Option<u16>,
    url: Option<String>,
    ok: Option<bool>,
    reason: Option<String>,
    cookies: Option<HashMap<String, String>>, // Changed to HashMap
}

pub struct PlaywrightClient {
    process: Arc<Mutex<Child>>,
    stdin: Arc<Mutex<std::process::ChildStdin>>,
    stdout: Arc<Mutex<BufReader<std::process::ChildStdout>>>,
}

impl PlaywrightClient {
    fn get_node_path() -> String {
        let node_path = PathBuf::from(env!("NVM_BIN"));
        node_path.join("node").display().to_string()
    }

    fn get_script_path() -> String {
        let current_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        current_path
            .join(JS_SCRIPT_PATH)
            .join(PLAYWRIGHT_JS)
            .display()
            .to_string()
    }

    pub fn new() -> Result<Self, ScraperError> {
        // Check if xvfb is available
        let use_xvfb = Command::new("which")
            .arg("xvfb-run")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

        let mut child = if use_xvfb {
            println!("Using Xvfb for virtual display");
            Command::new("xvfb-run")
                .arg("-a")
                .arg(Self::get_node_path())
                .arg(Self::get_script_path()) // Use original server with headless: false
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()?
        } else {
            println!("Running without Xvfb (may be detected)");
            Command::new(Self::get_node_path())
                .arg(Self::get_script_path())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()?
        };

        let stdin = child.stdin.take().expect("Failed to open stdin");
        let stdout = child.stdout.take().expect("Failed to open stdout");

        Ok(Self {
            process: Arc::new(Mutex::new(child)),
            stdin: Arc::new(Mutex::new(stdin)),
            stdout: Arc::new(Mutex::new(BufReader::new(stdout))),
        })
    }

    fn send_command(&self, cmd: CommandRequest) -> Result<CommandResponse, ScraperError> {
        let json = serde_json::to_string(&cmd)?;

        // Send command
        {
            let mut stdin = self.stdin.lock().unwrap();
            writeln!(stdin, "{}", json)?;
            stdin.flush()?;
        }

        // Read response
        let mut stdout = self.stdout.lock().unwrap();
        let mut response_line = String::new();
        stdout.read_line(&mut response_line)?;

        if cmd.action == "shutdown" {
            return Ok(CommandResponse {
                success: true,
                context_id: None,
                content: None,
                status_code: None,
                url: None,
                ok: None,
                reason: None,
                cookies: None,
            });
        }
        let response: CommandResponse = serde_json::from_str(&response_line)?;
        Ok(response)
    }

    pub fn init(&self) -> Result<(), ScraperError> {
        let cmd = CommandRequest {
            action: "init".to_string(),
            proxy: None,
            context_id: None,
            url: None,
            timeout: None,
            cookies: None,
            headers: None,
        };

        let resp = self.send_command(cmd)?;
        if !resp.success {
            return Err(ScraperError::PlaywrightJs(format!(
                "Failed to init: {:?}",
                resp.reason
            )));
        }
        Ok(())
    }

    pub fn create_context(
        &self,
        proxy: Option<ProxySettings>,
        headers: Option<HashMap<String, String>>,
    ) -> Result<String, ScraperError> {
        let cmd = CommandRequest {
            action: "create_context".to_string(),
            proxy,
            context_id: None,
            url: None,
            timeout: None,
            cookies: None,
            headers,
        };

        let resp = self.send_command(cmd)?;
        if !resp.success {
            return Err(ScraperError::PlaywrightJs(format!(
                "Failed to create context: {:?}",
                resp.reason
            )));
        }

        Ok(resp.context_id.unwrap())
    }

    pub fn navigate(
        &self,
        context_id: &str,
        url: &str,
        timeout: Option<u64>,
    ) -> Result<Response, ScraperError> {
        let cmd = CommandRequest {
            action: "navigate".to_string(),
            proxy: None,
            context_id: Some(context_id.to_string()),
            url: Some(url.to_string()),
            timeout,
            cookies: None,
            headers: None,
        };

        let resp = self.send_command(cmd)?;

        if !resp.success {
            return Err(ScraperError::PlaywrightJs(format!(
                "Navigation failed: {:?}",
                resp.reason
            )));
        }

        Ok(Response {
            content: resp.content.unwrap_or_default(),
            status_code: resp.status_code.unwrap_or(0),
            url: resp.url.unwrap_or_default(),
            ok: resp.ok.unwrap_or(false),
            reason: resp.reason.unwrap_or_default(),
            cookies: resp.cookies.unwrap_or_default(),
        })
    }

    pub fn get_content(&self, context_id: &str) -> Result<String, ScraperError> {
        let cmd = CommandRequest {
            action: "get_content".to_string(),
            proxy: None,
            context_id: Some(context_id.to_string()),
            url: None,
            timeout: None,
            cookies: None,
            headers: None,
        };

        let resp = self.send_command(cmd)?;
        if !resp.success {
            return Err(ScraperError::PlaywrightJs(format!(
                "Failed to get content: {:?}",
                resp.reason
            )));
        }

        Ok(resp.content.unwrap_or_default())
    }

    pub fn set_cookies(
        &self,
        context_id: &str,
        cookies: HashMap<String, String>,
    ) -> Result<(), ScraperError> {
        // Convert HashMap to Vec<Cookie> for the API
        let cookie_vec: Vec<Cookie> = cookies
            .iter()
            .map(|(name, value)| Cookie {
                name: name.clone(),
                value: value.clone(),
            })
            .collect();

        let cmd = CommandRequest {
            action: "set_cookies".to_string(),
            proxy: None,
            context_id: Some(context_id.to_string()),
            url: None,
            timeout: None,
            cookies: Some(
                cookie_vec
                    .into_iter()
                    .map(|c| {
                        let mut map = HashMap::new();
                        map.insert("name".to_string(), c.name);
                        map.insert("value".to_string(), c.value);
                        map
                    })
                    .collect(),
            ),
            headers: None,
        };

        let resp = self.send_command(cmd)?;
        if !resp.success {
            return Err(ScraperError::PlaywrightJs(format!(
                "Failed to set cookies: {:?}",
                resp.reason
            )));
        }
        Ok(())
    }

    pub fn close_context(&self, context_id: &str) -> Result<(), ScraperError> {
        let cmd = CommandRequest {
            action: "close_context".to_string(),
            proxy: None,
            context_id: Some(context_id.to_string()),
            url: None,
            timeout: None,
            cookies: None,
            headers: None,
        };

        let resp = self.send_command(cmd)?;
        if !resp.success {
            return Err(ScraperError::PlaywrightJs(
                "Failed to close context".to_string(),
            ));
        }
        Ok(())
    }

    pub fn shutdown(&self) -> Result<(), ScraperError> {
        let cmd = CommandRequest {
            action: "shutdown".to_string(),
            proxy: None,
            context_id: None,
            url: None,
            timeout: None,
            cookies: None,
            headers: None,
        };

        let _ = self.send_command(cmd)?;
        Ok(())
    }
}

impl Drop for PlaywrightClient {
    fn drop(&mut self) {
        self.shutdown().unwrap_or(());
        if let Ok(mut process) = self.process.lock() {
            let _ = process.kill();
        }
    }
}
