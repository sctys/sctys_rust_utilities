//! Retry transient MongoDB read failures, including errors while consuming a cursor.

use std::{future::Future, time::Duration};

use mongodb::error::{Error, ErrorKind};

const MAX_READ_ATTEMPTS: usize = 2;
const READ_RETRY_DELAY: Duration = Duration::from_secs(1);

/// A MongoDB read failure and the number of attempts made.
#[derive(Debug, thiserror::Error)]
#[error("MongoDB read failed after {attempts} attempt(s): {source}")]
pub struct MongoReadRetryError {
    pub attempts: usize,
    #[source]
    pub source: Error,
}

/// Whether the scraper's MongoDB read policy retries this error.
pub fn is_retryable_read_error(error: &Error) -> bool {
    matches!(
        error.kind.as_ref(),
        ErrorKind::Io(_)
            | ErrorKind::ConnectionPoolCleared { .. }
            | ErrorKind::ServerSelection { .. }
    )
}

/// Runs a complete read at most twice, retrying transient connection failures once.
///
/// The operation must consume its cursor before returning so that cursor errors
/// participate in the retry. The callback runs before the delay and receives the
/// failed attempt number and its error. Intended for reads that can be repeated.
///
/// # Errors
/// Returns the last MongoDB error and attempt count for non-retryable or exhausted failures.
pub async fn retry_read<T, F, Fut>(
    mut read: F,
    mut on_retry: impl FnMut(usize, &Error),
) -> Result<T, MongoReadRetryError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    for attempt in 1..MAX_READ_ATTEMPTS {
        match read().await {
            Ok(value) => return Ok(value),
            Err(error) if is_retryable_read_error(&error) => {
                on_retry(attempt, &error);
                tokio::time::sleep(READ_RETRY_DELAY).await;
            }
            Err(source) => {
                return Err(MongoReadRetryError {
                    attempts: attempt,
                    source,
                })
            }
        }
    }
    read().await.map_err(|source| MongoReadRetryError {
        attempts: MAX_READ_ATTEMPTS,
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[tokio::test(start_paused = true)]
    async fn test_transient_failure_retries_complete_read() {
        let attempts = Cell::new(0);
        let retries = Cell::new(0);
        let result = retry_read(
            || {
                attempts.set(attempts.get() + 1);
                let attempt = attempts.get();
                async move {
                    if attempt == 1 {
                        Err(Error::from(std::io::Error::other("connection lost")))
                    } else {
                        Ok(42)
                    }
                }
            },
            |attempt, _| retries.set(attempt),
        )
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.get(), 2);
        assert_eq!(retries.get(), 1);
    }

    #[tokio::test]
    async fn test_non_retryable_failure_stops_immediately() {
        let attempts = Cell::new(0);
        let error = retry_read(
            || {
                attempts.set(attempts.get() + 1);
                async { Err::<(), _>(Error::custom("invalid read")) }
            },
            |_, _| panic!("non-retryable error was retried"),
        )
        .await
        .unwrap_err();
        assert_eq!(error.attempts, 1);
        assert_eq!(attempts.get(), 1);
    }
}
