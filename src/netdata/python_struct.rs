use std::{env, path::PathBuf};

use strum_macros::Display;

use crate::python_utils::PythonPath;

pub struct NetdataPythonPath;

impl NetdataPythonPath {
    const PYTHON_ENV_PATH: &'static str = "/home/sctys/anaconda3/envs/netdata_rust";
    const PYTHON_PACKAGE_PATH: &'static str = "lib/python3.12/site-packages";
    const PYTHON_SCRIPT_PATH: &'static str = "src/netdata/python";
}

impl PythonPath for NetdataPythonPath {
    fn get_env_path() -> PathBuf {
        PathBuf::from(Self::PYTHON_ENV_PATH)
    }

    fn get_package_path() -> PathBuf {
        Self::get_env_path().join(Self::PYTHON_PACKAGE_PATH)
    }

    fn get_script_path() -> PathBuf {
        let current_path = env::current_dir().unwrap();
        current_path.join(Self::PYTHON_SCRIPT_PATH)
    }
}

#[derive(Debug, Display)]
#[strum(serialize_all = "snake_case")]
pub enum PythonTxt {
    Timeout,
    Headers,
    Proxy,
    Impersonate,
    Chrome,
    CurlCffi,
    Requests,
    RequestCurlCffi,
    RequestsWithCurlCffi,
    Session,
    Playwright,
    Headless,
    BrowserWait,
    PageEvaluation,
    RequestPlaywright,
    RequestsWithPlaywright,
    GetHeaderForRequests,
}
