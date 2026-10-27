//! Staged factories: callers create a logger, then secrets, then only the services
//! required for the run mode. Contexts borrow those owners rather than referring
//! to fields of the same owning object.
use crate::{
    aws_s3::AWSFileIO, click_house::ClickHouse, file_compress::FileCompress, file_io::FileIO,
    logger::ProjectLogger, mongo_db::MongoDB, secret::aws_secret::Secret,
    slack_messenger::SlackMessenger, source_scraper::SourceScraper,
};
use async_trait::async_trait;
use std::{error::Error, future::Future, path::Path};

pub const SLACK_SECRET_CATEGORY: &str = "slack";
pub const REPORT_CHANNEL_SECRET_NAME: &str = "report_channel_id";
pub const LOG_CHANNEL_SECRET_NAME: &str = "log_channel_id";
pub const ERROR_CHANNEL_SECRET_NAME: &str = "error_channel_id";

#[derive(Debug, thiserror::Error)]
#[error("Unable to initialize {service}: {source}")]
pub struct InitializationError {
    pub service: &'static str,
    #[source]
    pub source: Box<dyn Error + Send + Sync>,
}

/// Shared error context; no notification or termination occurs inside factories.
pub async fn initialize_with<T, E, Fut>(
    service: &'static str,
    future: Fut,
) -> Result<T, InitializationError>
where
    E: Error + Send + Sync + 'static,
    Fut: Future<Output = Result<T, E>>,
{
    future.await.map_err(|source| InitializationError {
        service,
        source: Box::new(source),
    })
}

#[derive(Clone, Copy, Debug)]
pub struct SecretSelector<'a> {
    pub project: &'a str,
    pub category: &'a str,
    pub name: &'a str,
}

pub fn slack_channel_selectors(project: &str) -> [SecretSelector<'_>; 3] {
    [
        SecretSelector {
            project,
            category: SLACK_SECRET_CATEGORY,
            name: REPORT_CHANNEL_SECRET_NAME,
        },
        SecretSelector {
            project,
            category: SLACK_SECRET_CATEGORY,
            name: LOG_CHANNEL_SECRET_NAME,
        },
        SecretSelector {
            project,
            category: SLACK_SECRET_CATEGORY,
            name: ERROR_CHANNEL_SECRET_NAME,
        },
    ]
}

#[async_trait]
pub trait SecretReader: Sync {
    type Error: Error + Send + Sync + 'static;
    async fn read(&self, selector: SecretSelector<'_>) -> Result<String, Self::Error>;
}

#[async_trait]
impl SecretReader for Secret<'_> {
    type Error = aws_sdk_ssm::error::SdkError<
        aws_sdk_ssm::operation::get_parameter::GetParameterError,
        aws_sdk_s3::config::http::HttpResponse,
    >;
    async fn read(&self, selector: SecretSelector<'_>) -> Result<String, Self::Error> {
        self.get_secret_value(selector.project, selector.category, selector.name)
            .await
    }
}

/// Channel order is report, log, error, matching existing secret lookup order.
pub async fn resolve_channels(
    reader: &impl SecretReader,
    selectors: [SecretSelector<'_>; 3],
) -> Result<[String; 3], InitializationError> {
    let mut values = [String::new(), String::new(), String::new()];
    for (index, selector) in selectors.into_iter().enumerate() {
        values[index] = initialize_with("notification channel", reader.read(selector)).await?;
    }
    Ok(values)
}

pub fn logger(path: &Path, name: &str, level: log::LevelFilter) -> ProjectLogger {
    let logger = ProjectLogger::new_logger(path, name);
    logger.set_logger(level);
    logger
}

pub async fn secret(logger: &ProjectLogger) -> Secret<'_> {
    Secret::new(logger).await
}

pub struct ServiceContext<'a, 's> {
    pub logger: &'a ProjectLogger,
    pub secret: &'s Secret<'a>,
}

impl<'a> ServiceContext<'a, '_> {
    pub fn file_io(&self) -> FileIO<'a> {
        FileIO::new(self.logger)
    }
    pub fn compression(&self) -> FileCompress<'a> {
        FileCompress::new(self.logger)
    }
    pub async fn aws_file_io(&self) -> AWSFileIO<'a> {
        AWSFileIO::new(self.logger).await
    }
    pub async fn mongo_db(&self) -> Result<MongoDB<'a>, InitializationError> {
        initialize_with("MongoDB", MongoDB::new(self.logger, self.secret)).await
    }
    pub async fn click_house(&self) -> Result<ClickHouse<'a>, InitializationError> {
        initialize_with("ClickHouse", ClickHouse::new(self.logger, self.secret)).await
    }
    pub async fn messenger(
        &self,
        selectors: [SecretSelector<'_>; 3],
    ) -> Result<SlackMessenger<'a>, InitializationError> {
        let [report, log, error] = resolve_channels(self.secret, selectors).await?;
        initialize_with(
            "Slack messenger",
            SlackMessenger::new(report, error, log, self.logger, self.secret),
        )
        .await
    }
}

/// Borrowing the secret for the scraper is explicit because its transport needs
/// access to that object for its full lifetime.
pub fn scraper<'a>(logger: &'a ProjectLogger, secret: &'a Secret<'a>) -> SourceScraper<'a> {
    SourceScraper::new(logger, secret)
}

/// Standalone factories for services that do not borrow a secret store.
pub fn file_io(logger: &ProjectLogger) -> FileIO<'_> {
    FileIO::new(logger)
}
pub fn compression(logger: &ProjectLogger) -> FileCompress<'_> {
    FileCompress::new(logger)
}
pub async fn aws_file_io(logger: &ProjectLogger) -> AWSFileIO<'_> {
    AWSFileIO::new(logger).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    struct FakeSecrets {
        requests: Mutex<Vec<String>>,
        fail_at: Option<usize>,
    }
    #[async_trait]
    impl SecretReader for FakeSecrets {
        type Error = std::io::Error;
        async fn read(&self, selector: SecretSelector<'_>) -> Result<String, Self::Error> {
            let mut requests = self.requests.lock().unwrap();
            requests.push(format!(
                "/{}/{}/{}",
                selector.project, selector.category, selector.name
            ));
            if self.fail_at == Some(requests.len()) {
                return Err(std::io::Error::other("fake failure"));
            }
            Ok(selector.name.to_string())
        }
    }
    #[tokio::test]
    async fn test_secret_order_and_initialization_failure() {
        for fail_at in [None, Some(1), Some(2), Some(3)] {
            let secrets = FakeSecrets {
                requests: Mutex::new(Vec::new()),
                fail_at,
            };
            let selectors = ["report", "log", "error"].map(|name| SecretSelector {
                project: "fixture",
                category: "notify",
                name,
            });
            let result = resolve_channels(&secrets, selectors).await;
            assert_eq!(secrets.requests.lock().unwrap().len(), fail_at.unwrap_or(3));
            if fail_at.is_some() {
                let error = result.unwrap_err();
                assert_eq!(error.service, "notification channel");
                assert_eq!(error.source.to_string(), "fake failure");
            } else {
                assert_eq!(result.unwrap(), ["report", "log", "error"]);
            }
        }
    }
}
