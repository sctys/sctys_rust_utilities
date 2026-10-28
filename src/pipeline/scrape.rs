//! One scraper execution engine with per-run mutable sessions and focused hooks.
use async_trait::async_trait;
use std::{
    collections::HashSet,
    fmt::{self, Display},
    hash::Hash,
};

pub fn response_status_message(status_code: impl Display, reason: impl Display) -> String {
    format!("Response with status code {status_code}. {reason}")
}

pub fn response_error_message(error: impl Display) -> String {
    format!("Unable to get response. {error}")
}

pub fn missing_element_message(element: impl Display) -> String {
    format!("Unable to find {element}")
}

pub fn missing_either_element_message(first: impl Display, second: impl Display) -> String {
    format!("Unable to find {first} or {second}")
}

#[derive(Clone, Copy, Debug)]
pub struct ScrapePolicy {
    pub max_attempts: usize,
    pub max_challenge_replays: usize,
    pub replay_consumes_attempt: bool,
    pub refresh_headers_on_retry: bool,
    pub refresh_headers_on_replay: bool,
    pub prepare_on_continuation: bool,
    pub filter_scraped: bool,
    pub filter_attempted: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestReason {
    First,
    Retry,
    Replay,
    Continuation,
}
pub enum Challenge<R> {
    Pass(R),
    Replay,
}
pub enum ResponseDecision<P, E> {
    Success(P),
    Retry(E),
    Reject(E),
    Fatal(E),
}
pub struct Transformed<R> {
    pub record: R,
    pub continue_input: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScrapeStage {
    Discover,
    Filter,
    OpenSession,
    Prepare,
    Request,
    Challenge,
    Transform,
    Persist,
    Log,
    CloseInput,
    Finalize,
}
#[derive(Debug, thiserror::Error)]
#[error("Scrape failed for input {input_index:?} at {stage:?}: {source}")]
pub struct ScrapeFailure<E> {
    pub input_index: Option<usize>,
    pub stage: ScrapeStage,
    #[source]
    pub source: E,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScrapeReport {
    pub requests: usize,
    pub attempted: usize,
    pub succeeded: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptFailureKind {
    ErrContinue,
    ErrTerminate,
}

impl Display for AttemptFailureKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ErrContinue => f.write_str("ErrContinue"),
            Self::ErrTerminate => f.write_str("ErrTerminate"),
        }
    }
}

#[derive(Debug)]
pub struct AttemptFailure<E> {
    pub input_index: usize,
    /// Number of failed attempts for this input, including the current failure.
    pub failed_attempts: usize,
    pub kind: AttemptFailureKind,
    pub will_retry: bool,
    pub source: E,
}

impl<E: Display> Display for AttemptFailure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let trial_suffix = if self.failed_attempts == 1 { "" } else { "s" };
        let outcome = match (self.kind, self.will_retry) {
            (AttemptFailureKind::ErrContinue, true) => "ErrContinue; retrying",
            (AttemptFailureKind::ErrContinue, false) => "ErrContinue; retries exhausted",
            (AttemptFailureKind::ErrTerminate, _) => "ErrTerminate; terminating",
        };
        write!(
            f,
            "Failed after {} trial{trial_suffix} ({outcome}): {}",
            self.failed_attempts, self.source
        )
    }
}

/// Session is created for one invocation and borrowed mutably by every hook.
/// `request` performs one physical request. Provider hooks interpret challenges;
/// this engine owns replay budgets, retry sequencing, persistence and counters.
#[async_trait]
pub trait ScrapeHooks: Send {
    type Input: Send + Sync;
    type Key: Clone + Ord + Hash + Send + Sync;
    type Session: Send;
    type Response: Send;
    type Payload: Send;
    type Record: Send + Sync;
    type Error: Send;
    async fn discover(&mut self) -> Result<Vec<Self::Input>, Self::Error>;
    fn key(&self, input: &Self::Input) -> Self::Key;
    async fn scraped_keys(&mut self) -> Result<HashSet<Self::Key>, Self::Error> {
        Ok(HashSet::new())
    }
    async fn attempted_keys(&mut self) -> Result<HashSet<Self::Key>, Self::Error> {
        Ok(HashSet::new())
    }
    async fn open_session(
        &mut self,
        first: Option<&Self::Input>,
    ) -> Result<Self::Session, Self::Error>;
    async fn prepare(
        &mut self,
        input: &Self::Input,
        session: &mut Self::Session,
        reason: RequestReason,
    ) -> Result<(), Self::Error>;
    async fn request(
        &mut self,
        input: &Self::Input,
        session: &mut Self::Session,
    ) -> Result<Self::Response, Self::Error>;
    async fn challenge(
        &mut self,
        response: Self::Response,
        _session: &mut Self::Session,
    ) -> Result<Challenge<Self::Response>, Self::Error> {
        Ok(Challenge::Pass(response))
    }
    fn classify(
        &mut self,
        response: Result<Self::Response, Self::Error>,
    ) -> ResponseDecision<Self::Payload, Self::Error>;
    async fn transform(
        &mut self,
        input: &Self::Input,
        payload: Self::Payload,
        session: &mut Self::Session,
    ) -> Result<Transformed<Self::Record>, Self::Error>;
    /// A navigation response may continue without representing a stored scrape.
    /// Some(false) records a terminal unsuccessful provider result.
    fn recorded_outcome(&self, _record: &Self::Record) -> Option<bool> {
        Some(true)
    }
    /// Current engine counters for hooks with provider-specific refresh policies.
    /// Some providers allow one challenge replay for each physical retry attempt.
    /// The default retains the existing per-input replay budget.
    fn reset_replays_on_retry(&self) -> bool {
        false
    }
    fn progress(&mut self, _report: ScrapeReport) {}
    async fn persist(&mut self, record: &Self::Record) -> Result<(), Self::Error>;
    async fn record_attempt(
        &mut self,
        input: &Self::Input,
        success: bool,
    ) -> Result<(), Self::Error>;
    async fn pause(&mut self, retry: bool);
    /// Reports a classified response failure. Formatting the event includes its trial count.
    async fn attempt_failed(&mut self, event: AttemptFailure<Self::Error>);
    async fn close_input(&mut self, session: &mut Self::Session) -> Result<(), Self::Error>;
    async fn finalize(&mut self, report: ScrapeReport) -> Result<(), Self::Error>;
}

#[derive(Debug, thiserror::Error)]
pub enum ScrapeRunError<E> {
    #[error("max_attempts must be greater than zero")]
    ZeroAttempts,
    #[error("challenge replay limit exceeded for input {input_index}")]
    ReplayLimit { input_index: usize },
    #[error(transparent)]
    Operation(#[from] ScrapeFailure<E>),
}

pub async fn run_scraper<H: ScrapeHooks>(
    hooks: &mut H,
    policy: ScrapePolicy,
) -> Result<ScrapeReport, ScrapeRunError<H::Error>> {
    if policy.max_attempts == 0 {
        return Err(ScrapeRunError::ZeroAttempts);
    }
    let fail = |stage, source| ScrapeFailure {
        stage,
        input_index: None,
        source,
    };
    let mut inputs = hooks
        .discover()
        .await
        .map_err(|source| fail(ScrapeStage::Discover, source))?;
    if policy.filter_scraped {
        let keys = hooks
            .scraped_keys()
            .await
            .map_err(|source| fail(ScrapeStage::Filter, source))?;
        inputs.retain(|input| !keys.contains(&hooks.key(input)));
    }
    if policy.filter_attempted {
        let keys = hooks
            .attempted_keys()
            .await
            .map_err(|source| fail(ScrapeStage::Filter, source))?;
        inputs.retain(|input| !keys.contains(&hooks.key(input)));
    }
    inputs.sort_by_key(|input| hooks.key(input));
    let mut session = hooks
        .open_session(inputs.first())
        .await
        .map_err(|source| fail(ScrapeStage::OpenSession, source))?;
    let mut report = ScrapeReport::default();
    for (index, input) in tqdm::tqdm(inputs.iter().enumerate()) {
        let fail = |stage, source| ScrapeFailure {
            stage,
            input_index: Some(index),
            source,
        };
        let mut failures = 0usize;
        let mut replays = 0usize;
        let mut reason = RequestReason::First;
        loop {
            if reason == RequestReason::First
                || (reason == RequestReason::Retry && policy.refresh_headers_on_retry)
                || (reason == RequestReason::Replay && policy.refresh_headers_on_replay)
                || (reason == RequestReason::Continuation && policy.prepare_on_continuation)
            {
                hooks
                    .prepare(input, &mut session, reason)
                    .await
                    .map_err(|source| fail(ScrapeStage::Prepare, source))?;
            }
            let response = hooks.request(input, &mut session).await;
            report.requests = report.requests.saturating_add(1);
            let response = match response {
                Ok(response) => match hooks
                    .challenge(response, &mut session)
                    .await
                    .map_err(|source| fail(ScrapeStage::Challenge, source))?
                {
                    Challenge::Pass(response) => Ok(response),
                    Challenge::Replay => {
                        if replays >= policy.max_challenge_replays {
                            return Err(ScrapeRunError::ReplayLimit { input_index: index });
                        }
                        replays += 1;
                        if policy.replay_consumes_attempt {
                            failures += 1;
                            if failures >= policy.max_attempts {
                                hooks
                                    .record_attempt(input, false)
                                    .await
                                    .map_err(|source| fail(ScrapeStage::Log, source))?;
                                report.attempted = report.attempted.saturating_add(1);
                                hooks.progress(report);
                                hooks.pause(false).await;
                                break;
                            }
                        }
                        reason = RequestReason::Replay;
                        continue;
                    }
                },
                Err(error) => Err(error),
            };
            match hooks.classify(response) {
                ResponseDecision::Success(payload) => {
                    let transformed = hooks
                        .transform(input, payload, &mut session)
                        .await
                        .map_err(|source| fail(ScrapeStage::Transform, source))?;
                    let outcome = hooks.recorded_outcome(&transformed.record);
                    hooks
                        .persist(&transformed.record)
                        .await
                        .map_err(|source| fail(ScrapeStage::Persist, source))?;
                    if let Some(success) = outcome {
                        hooks
                            .record_attempt(input, success)
                            .await
                            .map_err(|source| fail(ScrapeStage::Log, source))?;
                        report.succeeded = report.succeeded.saturating_add(usize::from(success));
                        report.attempted = report.attempted.saturating_add(1);
                    }
                    hooks.progress(report);
                    hooks.pause(false).await;
                    if !transformed.continue_input {
                        break;
                    }
                    failures = 0;
                    replays = 0;
                    reason = RequestReason::Continuation;
                }
                ResponseDecision::Retry(source) => {
                    failures += 1;
                    hooks
                        .attempt_failed(AttemptFailure {
                            input_index: index,
                            failed_attempts: failures,
                            kind: AttemptFailureKind::ErrContinue,
                            will_retry: failures < policy.max_attempts,
                            source,
                        })
                        .await;
                    if failures >= policy.max_attempts {
                        hooks
                            .record_attempt(input, false)
                            .await
                            .map_err(|source| fail(ScrapeStage::Log, source))?;
                        report.attempted = report.attempted.saturating_add(1);
                        hooks.progress(report);
                        hooks.pause(false).await;
                        break;
                    }
                    hooks.pause(true).await;
                    if hooks.reset_replays_on_retry() {
                        replays = 0;
                    }
                    reason = RequestReason::Retry;
                }
                ResponseDecision::Reject(source) => {
                    failures = failures.saturating_add(1);
                    hooks
                        .attempt_failed(AttemptFailure {
                            input_index: index,
                            failed_attempts: failures,
                            kind: AttemptFailureKind::ErrTerminate,
                            will_retry: false,
                            source,
                        })
                        .await;
                    hooks
                        .record_attempt(input, false)
                        .await
                        .map_err(|source| fail(ScrapeStage::Log, source))?;
                    report.attempted = report.attempted.saturating_add(1);
                    hooks.progress(report);
                    hooks.pause(false).await;
                    break;
                }
                ResponseDecision::Fatal(source) => {
                    return Err(fail(ScrapeStage::Request, source).into())
                }
            }
        }
        hooks
            .close_input(&mut session)
            .await
            .map_err(|source| fail(ScrapeStage::CloseInput, source))?;
    }
    hooks
        .finalize(report)
        .await
        .map_err(|source| fail(ScrapeStage::Finalize, source))?;
    Ok(report)
}

/// Compatibility filters and the scraper engine share stable retain semantics.
pub fn retain_unknown<I, K: Eq + Hash>(
    inputs: &mut Vec<I>,
    known: &HashSet<K>,
    key: impl Fn(&I) -> &K,
) {
    inputs.retain(|input| !known.contains(key(input)));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    struct Fake {
        inputs: Vec<usize>,
        responses: VecDeque<Result<&'static str, &'static str>>,
        events: Vec<String>,
        persist_failure: bool,
        outcome: Option<bool>,
        replay_per_attempt: bool,
    }
    impl Fake {
        fn new(responses: impl IntoIterator<Item = Result<&'static str, &'static str>>) -> Self {
            Self {
                inputs: vec![1],
                responses: responses.into_iter().collect(),
                events: Vec::new(),
                persist_failure: false,
                outcome: Some(true),
                replay_per_attempt: false,
            }
        }
    }
    fn policy() -> ScrapePolicy {
        ScrapePolicy {
            max_attempts: 2,
            max_challenge_replays: 1,
            replay_consumes_attempt: false,
            refresh_headers_on_retry: true,
            refresh_headers_on_replay: true,
            prepare_on_continuation: true,
            filter_scraped: false,
            filter_attempted: false,
        }
    }
    #[async_trait]
    impl ScrapeHooks for Fake {
        type Input = usize;
        type Key = usize;
        type Session = usize;
        type Response = &'static str;
        type Payload = &'static str;
        type Record = usize;
        type Error = &'static str;
        async fn discover(&mut self) -> Result<Vec<usize>, Self::Error> {
            self.events.push("discover".into());
            Ok(self.inputs.clone())
        }
        fn key(&self, input: &usize) -> usize {
            *input
        }
        async fn scraped_keys(&mut self) -> Result<HashSet<usize>, Self::Error> {
            Ok(HashSet::from([2]))
        }
        async fn attempted_keys(&mut self) -> Result<HashSet<usize>, Self::Error> {
            Ok(HashSet::from([3]))
        }
        async fn open_session(&mut self, _: Option<&usize>) -> Result<usize, Self::Error> {
            self.events.push("open".into());
            Ok(0)
        }
        async fn prepare(
            &mut self,
            input: &usize,
            session: &mut usize,
            reason: RequestReason,
        ) -> Result<(), Self::Error> {
            *session += 1;
            self.events.push(format!("prepare:{input}:{reason:?}"));
            Ok(())
        }
        async fn request(
            &mut self,
            _: &usize,
            _: &mut usize,
        ) -> Result<Self::Response, Self::Error> {
            self.events.push("request".into());
            self.responses.pop_front().expect("no unexpected requests")
        }
        async fn challenge(
            &mut self,
            response: Self::Response,
            _: &mut usize,
        ) -> Result<Challenge<Self::Response>, Self::Error> {
            self.events.push("challenge".into());
            Ok(if response == "challenge" {
                Challenge::Replay
            } else {
                Challenge::Pass(response)
            })
        }
        fn classify(
            &mut self,
            response: Result<Self::Response, Self::Error>,
        ) -> ResponseDecision<Self::Payload, Self::Error> {
            self.events.push("classify".into());
            match response {
                Ok("reject") => ResponseDecision::Reject("rejected"),
                Ok(data) => ResponseDecision::Success(data),
                Err("retry") => ResponseDecision::Retry("retry"),
                Err(error) => ResponseDecision::Fatal(error),
            }
        }
        async fn transform(
            &mut self,
            input: &usize,
            payload: Self::Payload,
            _: &mut usize,
        ) -> Result<Transformed<usize>, Self::Error> {
            self.events.push("transform".into());
            self.outcome = match payload {
                "navigate" => None,
                "unsaved" => Some(false),
                _ => Some(true),
            };
            Ok(Transformed {
                record: *input,
                continue_input: payload == "page" || payload == "navigate",
            })
        }
        fn reset_replays_on_retry(&self) -> bool {
            self.replay_per_attempt
        }
        fn recorded_outcome(&self, _: &usize) -> Option<bool> {
            self.outcome
        }
        async fn persist(&mut self, _: &usize) -> Result<(), Self::Error> {
            self.events.push("persist".into());
            if self.persist_failure {
                Err("persist")
            } else {
                Ok(())
            }
        }
        async fn record_attempt(&mut self, _: &usize, success: bool) -> Result<(), Self::Error> {
            self.events.push(format!("log:{success}"));
            Ok(())
        }
        async fn pause(&mut self, retry: bool) {
            self.events.push(format!("pause:{retry}"));
        }
        async fn close_input(&mut self, _: &mut usize) -> Result<(), Self::Error> {
            self.events.push("close".into());
            Ok(())
        }
        async fn finalize(&mut self, _: ScrapeReport) -> Result<(), Self::Error> {
            self.events.push("finalize".into());
            Ok(())
        }
        async fn attempt_failed(&mut self, event: AttemptFailure<Self::Error>) {
            self.events
                .push(format!("failure:{}:{}", event.source, event.will_retry));
        }
    }
    #[tokio::test]
    async fn test_success_order_and_mutable_session_hooks() {
        let mut hooks = Fake::new([Ok("ok")]);
        let report = run_scraper(&mut hooks, policy()).await.unwrap();
        assert_eq!(
            report,
            ScrapeReport {
                requests: 1,
                attempted: 1,
                succeeded: 1
            }
        );
        assert_eq!(
            hooks.events,
            [
                "discover",
                "open",
                "prepare:1:First",
                "request",
                "challenge",
                "classify",
                "transform",
                "persist",
                "log:true",
                "pause:false",
                "close",
                "finalize"
            ]
        );
    }
    #[tokio::test]
    async fn test_retry_hook_exhaustion_rejection_and_transport_failure() {
        let mut retry = Fake::new([Err("retry"), Ok("ok")]);
        assert_eq!(
            run_scraper(&mut retry, policy()).await.unwrap().succeeded,
            1
        );
        assert!(retry.events.contains(&"prepare:1:Retry".into()));
        let mut exhausted = Fake::new([Err("retry"), Err("retry")]);
        let report = run_scraper(&mut exhausted, policy()).await.unwrap();
        assert_eq!(
            report,
            ScrapeReport {
                requests: 2,
                attempted: 1,
                succeeded: 0
            }
        );
        assert_eq!(
            exhausted
                .events
                .iter()
                .filter(|event| event.as_str() == "log:false")
                .count(),
            1
        );
        let mut rejected = Fake::new([Ok("reject")]);
        assert_eq!(
            run_scraper(&mut rejected, policy())
                .await
                .unwrap()
                .attempted,
            1
        );
        assert!(!rejected.events.contains(&"persist".into()));
        let mut failed = Fake::new([Err("fatal")]);
        let error = run_scraper(&mut failed, policy()).await.unwrap_err();
        assert!(matches!(
            error,
            ScrapeRunError::Operation(ScrapeFailure {
                stage: ScrapeStage::Request,
                source: "fatal",
                ..
            })
        ));
        assert!(!failed.events.contains(&"finalize".into()));
    }
    #[tokio::test]
    async fn test_provider_replay_has_one_persist_and_explicit_budget() {
        let mut hooks = Fake::new([Ok("challenge"), Ok("ok")]);
        let report = run_scraper(&mut hooks, policy()).await.unwrap();
        assert_eq!(
            report,
            ScrapeReport {
                requests: 2,
                attempted: 1,
                succeeded: 1
            }
        );
        assert_eq!(
            hooks
                .events
                .iter()
                .filter(|event| event.as_str() == "persist")
                .count(),
            1
        );
        assert!(hooks.events.contains(&"prepare:1:Replay".into()));
        let mut hooks = Fake::new([Ok("challenge"), Ok("challenge")]);
        assert!(matches!(
            run_scraper(&mut hooks, policy()).await,
            Err(ScrapeRunError::ReplayLimit { input_index: 0 })
        ));
        let mut hooks = Fake::new([Ok("challenge")]);
        let mut config = policy();
        config.replay_consumes_attempt = true;
        config.max_attempts = 1;
        assert_eq!(run_scraper(&mut hooks, config).await.unwrap().succeeded, 0);
        assert!(!hooks.events.contains(&"persist".into()));
    }
    #[tokio::test]
    async fn test_filtering_order_pagination_and_persistence_failure() {
        let mut hooks = Fake::new([Ok("page"), Ok("ok"), Ok("ok")]);
        hooks.inputs = vec![4, 3, 2, 1];
        let mut config = policy();
        config.filter_scraped = true;
        config.filter_attempted = true;
        let report = run_scraper(&mut hooks, config).await.unwrap();
        assert_eq!(report.succeeded, 3);
        let preparations: Vec<_> = hooks
            .events
            .iter()
            .filter(|event| event.starts_with("prepare"))
            .map(String::as_str)
            .collect();
        assert_eq!(
            preparations,
            [
                "prepare:1:First",
                "prepare:1:Continuation",
                "prepare:4:First"
            ]
        );
        let mut failed = Fake::new([Ok("ok")]);
        failed.persist_failure = true;
        assert!(matches!(
            run_scraper(&mut failed, policy()).await,
            Err(ScrapeRunError::Operation(ScrapeFailure {
                stage: ScrapeStage::Persist,
                ..
            }))
        ));
        assert!(!failed.events.iter().any(|event| event.starts_with("log:")));
        assert!(!failed.events.contains(&"finalize".into()));
    }
    #[tokio::test]
    async fn test_empty_and_invalid_configuration() {
        let mut hooks = Fake::new([]);
        hooks.inputs.clear();
        assert_eq!(
            run_scraper(&mut hooks, policy()).await.unwrap(),
            ScrapeReport::default()
        );
        assert_eq!(hooks.events, ["discover", "open", "finalize"]);
        let mut invalid = policy();
        invalid.max_attempts = 0;
        let mut hooks = Fake::new([]);
        assert!(matches!(
            run_scraper(&mut hooks, invalid).await,
            Err(ScrapeRunError::ZeroAttempts)
        ));
        assert!(hooks.events.is_empty());
    }

    #[tokio::test]
    async fn test_header_preparation_policies_do_not_add_implicit_refreshes() {
        for responses in [
            vec![Err("retry"), Ok("ok")],
            vec![Ok("challenge"), Ok("ok")],
            vec![Ok("page"), Ok("ok")],
        ] {
            let mut hooks = Fake::new(responses);
            let mut config = policy();
            config.refresh_headers_on_retry = false;
            config.refresh_headers_on_replay = false;
            config.prepare_on_continuation = false;
            run_scraper(&mut hooks, config).await.unwrap();
            assert_eq!(
                hooks
                    .events
                    .iter()
                    .filter(|event| event.starts_with("prepare:"))
                    .count(),
                1
            );
        }
    }
    #[tokio::test]
    async fn test_navigation_is_not_counted_as_a_stored_scrape() {
        let mut hooks = Fake::new([Ok("navigate"), Ok("ok")]);
        let report = run_scraper(&mut hooks, policy()).await.unwrap();
        assert_eq!(
            report,
            ScrapeReport {
                requests: 2,
                attempted: 1,
                succeeded: 1
            }
        );
        assert_eq!(
            hooks
                .events
                .iter()
                .filter(|event| event.starts_with("log:"))
                .count(),
            1
        );
        let mut hooks = Fake::new([Ok("unsaved")]);
        assert_eq!(
            run_scraper(&mut hooks, policy()).await.unwrap(),
            ScrapeReport {
                requests: 1,
                attempted: 1,
                succeeded: 0
            }
        );
        assert!(hooks.events.contains(&"log:false".into()));
    }
    #[tokio::test]
    async fn test_provider_replay_budget_can_reset_for_each_retry_attempt() {
        let mut hooks = Fake::new([Ok("challenge"), Err("retry"), Ok("challenge"), Ok("ok")]);
        hooks.replay_per_attempt = true;
        assert_eq!(
            run_scraper(&mut hooks, policy()).await.unwrap(),
            ScrapeReport {
                requests: 4,
                attempted: 1,
                succeeded: 1
            }
        );
        assert_eq!(
            hooks
                .events
                .iter()
                .filter(|event| event.starts_with("log:"))
                .count(),
            1
        );
        let mut hooks = Fake::new([Ok("challenge"), Err("retry"), Ok("challenge"), Ok("ok")]);
        assert!(matches!(
            run_scraper(&mut hooks, policy()).await,
            Err(ScrapeRunError::ReplayLimit { .. })
        ));
    }
}
