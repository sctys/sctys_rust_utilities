//! Generic message construction and configured notification routing.
use super::{parse::ModuleParseReport, runner::FailureReporter, scrape::ScrapeReport};
use crate::slack_messenger::{Channel, SlackMessenger};
use async_trait::async_trait;
use std::fmt::Display;

pub fn processing_modules_summary(modules: &[String]) -> String {
    format!("Processing modules: {}", modules.join(", "))
}

pub fn final_export_datasets_summary(datasets: &[String]) -> String {
    format!(
        "Final export datasets (includes all active modules with --remain-modules): {}",
        datasets.join(", ")
    )
}

pub fn export_window_summary(start: impl Display, end: impl Display) -> String {
    format!("Export window: {start} to {end}")
}

pub fn current_month_run_date_error(selected: impl Display) -> String {
    format!("The run date is for this month. The run date is {selected}.")
}

pub fn current_or_last_month_run_date_error(selected: impl Display) -> String {
    format!("The run date is for this month or last month. The run date is {selected}.")
}

pub fn backup_missing_before_remove(module: &str) -> String {
    format!("Backup data not found in s3 for module {module}. It is not safe to remove the files.")
}

pub fn documents_removed_summary(module: &str, count: impl Display) -> String {
    format!("Number of documents removed from module {module}: {count}\n")
}

pub fn files_uploaded_summary(module: &str, count: impl Display) -> String {
    format!("Number of files uploaded to mongoDB for module {module}: {count}\n")
}

pub fn missing_scraped_dataset_message(module: &str) -> String {
    format!("Missing scraped dataset for {module}")
}

pub fn scrape_summary(module: &str, report: ScrapeReport) -> String {
    let rate = if report.attempted == 0 {
        "N/A".to_string()
    } else {
        format!(
            "{:.2}%",
            report.succeeded as f64 / report.attempted as f64 * 100.0
        )
    };
    format!(
        "Scrape completed for module {module}: {} / {}, {rate}",
        report.succeeded, report.attempted
    )
}
pub fn parse_summary<D>(
    module: &str,
    report: &ModuleParseReport<D>,
    name: impl Fn(&D) -> String,
) -> String {
    let mut message = format!(
        "Number of documents parsed from module {module}: {}\n",
        report.documents
    );
    for counts in &report.rows {
        let name = name(&counts.dataset);
        let change = counts.after as i128 - counts.before as i128;
        message.push_str(&format!("Number of rows before finalization in source_dataset {name}: {}\nChange in rows before finalization in source_dataset {name}: {change}\n", counts.after));
    }
    message
}

pub struct SlackFailureReporter<'a, 'logger> {
    pub messenger: &'a SlackMessenger<'logger>,
    pub caller: &'a str,
    pub channel: Channel,
}
#[async_trait]
impl<E: std::fmt::Display + Sync> FailureReporter<E> for SlackFailureReporter<'_, '_> {
    async fn failure(&mut self, error: &E) {
        self.messenger
            .retry_send_message(self.caller, &error.to_string(), self.channel)
            .await;
    }
}

/// Reports an error to Slack and terminates the current workflow on failure.
///
/// Use this at application boundaries that still expose a unit-returning runner
/// API. Fallible workflow code should return its error to a higher boundary
/// instead.
pub async fn terminate_on_error<T, E: Display + Sync>(
    result: Result<T, E>,
    caller: &str,
    messenger: &SlackMessenger<'_>,
) -> T {
    let mut reporter = SlackFailureReporter {
        messenger,
        caller,
        channel: Channel::Error,
    };
    super::runner::report_result(result, &mut reporter)
        .await
        .unwrap_or_else(|error| panic!("{error}"))
}

#[async_trait]
pub trait LogCounts<D: Sync>: Sync {
    type Error: Send;
    async fn count(
        &self,
        dataset: &D,
        cutoff: i64,
        successful_only: bool,
    ) -> Result<usize, Self::Error>;
}
pub struct ReportTarget<D> {
    pub module_name: String,
    pub dataset: D,
}
#[derive(Debug, thiserror::Error)]
#[error("Unable to read log counts for report target {target_index}, successful_only={successful_only}: {source}")]
pub struct ReportFailure<E> {
    pub target_index: usize,
    pub successful_only: bool,
    #[source]
    pub source: E,
}

/// Shared scheduled report query loop. The caller supplies the already-selected
/// schedule, cutoff policy and clock; empty schedules produce no notification.
pub async fn report_scrapes<D: Sync, B: LogCounts<D>>(
    targets: &[ReportTarget<D>],
    cutoff: chrono::DateTime<chrono::Utc>,
    now: chrono::DateTime<chrono::Utc>,
    backend: &B,
) -> Result<Option<String>, ReportFailure<B::Error>> {
    if targets.is_empty() {
        return Ok(None);
    }
    let mut message = format!(
        "Scraped results as of *{cutoff} ({} days ago)*:\n",
        (now - cutoff).num_days()
    );
    for (index, target) in targets.iter().enumerate() {
        let succeeded = backend
            .count(&target.dataset, cutoff.timestamp(), true)
            .await
            .map_err(|source| ReportFailure {
                target_index: index,
                successful_only: true,
                source,
            })?;
        let attempted = backend
            .count(&target.dataset, cutoff.timestamp(), false)
            .await
            .map_err(|source| ReportFailure {
                target_index: index,
                successful_only: false,
                source,
            })?;
        let rate = if attempted == 0 {
            "N/A".into()
        } else {
            format!("{:.2}%", succeeded as f64 / attempted as f64 * 100.0)
        };
        message.push_str(&format!(
            "{}: {succeeded}/{attempted} ({rate})\n",
            target.module_name
        ));
    }
    Ok(Some(message))
}

/// The legacy stock and sports query shapes are deliberately distinct settings.
#[derive(Clone, Copy)]
pub enum ScrapedPredicate {
    Truthy,
    EqualsTrue,
}
pub struct LogTable {
    pub table: String,
    pub insert_column: String,
    pub scraped_column: String,
    pub predicate: ScrapedPredicate,
}
impl LogTable {
    pub fn scraper_log(table: impl Into<String>, predicate: ScrapedPredicate) -> Self {
        Self {
            table: table.into(),
            insert_column: super::records::INSERT_TIME_FIELD.into(),
            scraped_column: super::records::SCRAPED_FIELD.into(),
            predicate,
        }
    }

    pub fn count_query(&self, cutoff: i64, successful_only: bool) -> String {
        use sea_query::{Alias, Asterisk, Expr, Func, MysqlQueryBuilder, Query};
        let mut query = Query::select();
        query
            .expr(Func::count(Expr::col(Asterisk)))
            .from(Alias::new(&self.table))
            .and_where(Expr::col(Alias::new(&self.insert_column)).gte(cutoff));
        if successful_only {
            let column = Expr::col(Alias::new(&self.scraped_column));
            query.and_where(match self.predicate {
                ScrapedPredicate::Truthy => column.into(),
                ScrapedPredicate::EqualsTrue => column.eq(true),
            });
        }
        query.to_string(MysqlQueryBuilder)
    }
}
pub struct ClickHouseLogCounts<'a> {
    pub client: &'a clickhouse::Client,
}
#[async_trait]
impl LogCounts<LogTable> for ClickHouseLogCounts<'_> {
    type Error = clickhouse::error::Error;
    async fn count(
        &self,
        dataset: &LogTable,
        cutoff: i64,
        successful_only: bool,
    ) -> Result<usize, Self::Error> {
        let count = self
            .client
            .query(&dataset.count_query(cutoff, successful_only))
            .fetch_one::<u64>()
            .await?;
        usize::try_from(count).map_err(|error| clickhouse::error::Error::Custom(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_report_construction_including_empty_and_negative_change() {
        assert_eq!(
            scrape_summary("fixture", ScrapeReport::default()),
            "Scrape completed for module fixture: 0 / 0, N/A"
        );
        assert_eq!(
            scrape_summary(
                "fixture",
                ScrapeReport {
                    requests: 3,
                    attempted: 2,
                    succeeded: 1
                }
            ),
            "Scrape completed for module fixture: 1 / 2, 50.00%"
        );
        let report = ModuleParseReport {
            documents: 1,
            rows: vec![super::super::parse::RowCounts {
                dataset: "fixture",
                before: 3,
                after: 1,
            }],
        };
        assert!(parse_summary("module", &report, |name| name.to_string())
            .contains("source_dataset fixture: -2\n"));
    }
}
