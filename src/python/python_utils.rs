use std::{env, path::PathBuf};

use pyo3::{types::PyAnyMethods, PyResult, Python};

pub trait PythonPath {
    const PYTHON_HOME: &'static str = "PYTHONHOME";
    const PYTHON_PATH: &'static str = "PYTHONPATH";
    const SYS: &'static str = "sys";
    const PATH: &'static str = "path";
    const APPEND: &'static str = "append";

    fn get_env_path() -> PathBuf;

    fn get_package_path() -> PathBuf;

    fn get_script_path() -> PathBuf;

    fn setup_python_venv() {
        let venv_path = Self::get_env_path();
        let package_path = Self::get_package_path();
        env::set_var(Self::PYTHON_HOME, &venv_path);
        env::set_var(Self::PYTHON_PATH, &package_path);
    }

    fn append_script_path(py: &Python) -> PyResult<()> {
        let script_path = Self::get_script_path();
        let sys = py.import(Self::SYS)?;
        let sys_path = sys.getattr(Self::PATH)?;
        sys_path.call_method1(Self::APPEND, (script_path.as_os_str(),))?;
        Ok(())
    }
}
