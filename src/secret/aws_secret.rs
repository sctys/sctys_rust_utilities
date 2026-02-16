use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_ssm::{
    error::SdkError, operation::get_parameter::GetParameterError,
    types::error::builders::ParameterNotFoundBuilder, Client,
};

use crate::logger::ProjectLogger;

#[derive(Debug, Clone)]
pub struct Secret<'a> {
    project_logger: &'a ProjectLogger,
    client: Client,
}

impl<'a> Secret<'a> {
    pub async fn new(project_logger: &'a ProjectLogger) -> Self {
        let region = RegionProviderChain::default_provider();
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region)
            .load()
            .await;
        let ssm = Client::new(&config);
        Self {
            project_logger,
            client: ssm,
        }
    }

    pub fn get_logger(&self) -> &'a ProjectLogger {
        self.project_logger
    }

    pub async fn get_secret_value(
        &self,
        project: &str,
        category: &str,
        name: &str,
    ) -> Result<String, SdkError<GetParameterError, HttpResponse>> {
        self.client
            .get_parameter()
            .name(format!("/{project}/{category}/{name}"))
            .with_decryption(true)
            .send()
            .await
            .map_or_else(
                |e| {
                    let error_str = format!("Unable to get secret. {e}");
                    self.project_logger.log_error(&error_str);
                    Err(e)
                },
                |param| match param.parameter.and_then(|parameter| parameter.value) {
                    Some(value) => {
                        let debug_str = format!("Secret for {project}/{category}/{name} loaded.");
                        self.project_logger.log_debug(&debug_str);
                        Ok(value)
                    }
                    None => {
                        let error_str =
                            format!("Unable to find secret for {project}/{category}/{name}.");
                        self.project_logger.log_error(&error_str);
                        let parameter_not_found = ParameterNotFoundBuilder::default().build();
                        Err(SdkError::construction_failure(
                            GetParameterError::ParameterNotFound(parameter_not_found),
                        ))
                    }
                },
            )
    }
}

#[cfg(test)]
mod tests {
    use std::{env, path::Path};

    use log::LevelFilter;

    use crate::{logger::ProjectLogger, secret::aws_secret::Secret};

    #[tokio::test]
    async fn test_get_secret_value() {
        let logger_name = "test_aws_secret";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_secret");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let aws_secret = Secret::new(&project_logger).await;
        let project = "sctys_rust_utilities";
        let category = "test";
        let name = "test_secret";
        let content = aws_secret
            .get_secret_value(project, category, name)
            .await
            .unwrap();
        dbg!(content);
    }
}
