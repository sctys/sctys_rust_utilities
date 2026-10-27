//! Shared cursor accumulation and typed multi-output flushing.
//!
//! Decoders only transform an input batch and append their typed output. Sinks
//! only persist slices. This module owns input buffering, accounting, thresholds,
//! sequential output order and final flushes. Neither failures nor cancellation
//! undo earlier successful inserts.
//!
//! ```
//! # async fn example() -> Result<(), &'static str> {
//! use sctys_rust_utilities::{misc::batching::BatchLimits, pipeline::parse::*};
//! struct Sink;
//! #[async_trait::async_trait]
//! impl RowSink<u32> for Sink {
//!     type Error = &'static str;
//!     async fn insert(&mut self, _rows: &[u32]) -> Result<(), Self::Error> { Ok(()) }
//! }
//! let limits = BatchLimits::new(100, 4096).expect("positive limits");
//! let mut output = TypedOutput::new(
//!     |docs: &[u32], rows: &mut Vec<u32>| append_rows(rows, Ok(docs.to_vec()), |_| 4),
//!     Sink, |_: &u32| 4, limits,
//! );
//! let mut count = 0;
//! parse_stream(futures::stream::iter([Ok::<_, &'static str>(1u32)]),
//!     &mut count, &mut output, |result| result, |_| 4, |bytes| bytes > 4096).await?;
//! assert_eq!(count, 1);
//! assert!(output.rows.is_empty()); // final flush also runs below the threshold
//! # Ok(())
//! # }
//! ```
use crate::misc::batching::{BatchLimits, BatchRanges};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use std::{fmt::Display, future::Future};

pub fn selector_parse_error_message(tag: &str, error: impl Display) -> String {
    crate::netdata::html::selector_parse_error_message(tag, error)
}

pub fn parse_value_error_message(subject: &str, value: &str, error: impl Display) -> String {
    format!("Unable to parse {subject} {value}. {error}")
}

pub fn document_parse_error_message(query: impl Display, error: impl Display) -> String {
    format!("Unable to parse document for {query}. {error}")
}

pub fn module_dataset_parse_error_message(
    module: impl Display,
    dataset: impl Display,
    error: impl Display,
) -> String {
    format!("Unable to parse document for module {module} in source dataset {dataset}. {error}")
}

/// A typed persistence boundary. Implementations do not clear caller buffers.
#[async_trait]
pub trait RowSink<R: Sync>: Send {
    type Error: Send;
    async fn insert(&mut self, rows: &[R]) -> Result<(), Self::Error>;
}

/// Central clear-on-success operation used by owned buffers and legacy wrappers.
pub fn clear_after_success<R, E>(rows: &mut Vec<R>, result: Result<(), E>) -> Result<(), E> {
    result?;
    rows.clear();
    Ok(())
}

/// Inserts a borrowed slice in order without clearing or retrying it.
pub async fn insert_borrowed<R: Sync, S: RowSink<R>, F: FnMut(&R) -> usize>(
    rows: &[R],
    limits: BatchLimits,
    estimate: F,
    sink: &mut S,
) -> Result<(), S::Error> {
    for range in BatchRanges::new(rows, limits, estimate) {
        sink.insert(&rows[range]).await?;
    }
    Ok(())
}

/// Clears only after every batch succeeds, retaining allocation and full contents
/// on failure even when earlier batches have already reached the backend.
pub async fn flush_owned<R: Sync, S: RowSink<R>, F: FnMut(&R) -> usize>(
    rows: &mut Vec<R>,
    limits: BatchLimits,
    estimate: F,
    sink: &mut S,
) -> Result<(), S::Error> {
    let result = insert_borrowed(rows, limits, estimate, sink).await;
    clear_after_success(rows, result)
}

/// Appends one fallible decode result, counting only the newly appended rows.
pub fn append_rows<R, E>(
    rows: &mut Vec<R>,
    decoded: Result<Vec<R>, E>,
    estimate: impl Fn(&R) -> usize,
) -> Result<usize, E> {
    let decoded = decoded?;
    let bytes = decoded
        .iter()
        .map(estimate)
        .fold(0usize, usize::saturating_add);
    rows.extend(decoded);
    Ok(bytes)
}

/// Existing decoders differ in whether earlier decoded documents are appended
/// before a later document fails. Keep that choice explicit, with one loop here.
#[derive(Clone, Copy, Debug)]
pub enum DecodeCommit {
    AtomicBatch,
    PerDocument,
}

pub fn decode_append<D, R, E>(
    documents: &[D],
    rows: &mut Vec<R>,
    mut decode: impl FnMut(&D) -> Result<Vec<R>, E>,
    estimate: impl Fn(&R) -> usize,
    commit: DecodeCommit,
) -> Result<usize, E> {
    match commit {
        DecodeCommit::AtomicBatch => {
            let decoded = documents
                .iter()
                .map(decode)
                .collect::<Result<Vec<_>, _>>()?;
            append_rows(rows, Ok(decoded.into_iter().flatten().collect()), estimate)
        }
        DecodeCommit::PerDocument => {
            let mut bytes = 0usize;
            for document in documents {
                bytes = bytes.saturating_add(append_rows(rows, decode(document), &estimate)?);
            }
            Ok(bytes)
        }
    }
}

/// Stock's parallel atomic decoder retains declaration order and commits no
/// rows if any document fails, just like the sequential atomic contract.
pub fn decode_parallel_append<D: Sync, R: Send, E: Send>(
    documents: &[D],
    rows: &mut Vec<R>,
    decode: impl Fn(&D) -> Result<Vec<R>, E> + Sync + Send,
    estimate: impl Fn(&R) -> usize,
) -> Result<usize, E> {
    use rayon::prelude::*;
    let batches: Result<Vec<Vec<R>>, E> = documents.par_iter().map(decode).collect();
    append_rows(
        rows,
        batches.map(|batches| batches.into_iter().flatten().collect()),
        estimate,
    )
}

/// Shared behavior for one output or an ordered heterogeneous tuple of outputs.
#[async_trait]
pub trait OutputGroup<D: Sync>: Send {
    type Error: Send;
    fn append(&mut self, documents: &[D]) -> Result<(), Self::Error>;
    fn memory_used(&self) -> usize;
    async fn flush(&mut self) -> Result<(), Self::Error>;
}

/// Typed output declaration: row storage, focused decoder, estimator and sink.
/// The decoder returns the estimated bytes it appended to `rows`.
pub struct TypedOutput<R, F, S, Size> {
    pub rows: Vec<R>,
    decode: F,
    sink: S,
    estimate: Size,
    limits: BatchLimits,
    bytes: usize,
}

impl<R, F, S, Size> TypedOutput<R, F, S, Size> {
    pub fn new(decode: F, sink: S, estimate: Size, limits: BatchLimits) -> Self {
        Self {
            rows: Vec::new(),
            decode,
            sink,
            estimate,
            limits,
            bytes: 0,
        }
    }
}

#[async_trait]
impl<D, R, F, S, Size> OutputGroup<D> for TypedOutput<R, F, S, Size>
where
    D: Sync,
    R: Send + Sync,
    S: RowSink<R>,
    F: FnMut(&[D], &mut Vec<R>) -> Result<usize, S::Error> + Send,
    Size: FnMut(&R) -> usize + Send,
{
    type Error = S::Error;
    fn append(&mut self, documents: &[D]) -> Result<(), Self::Error> {
        self.bytes = self
            .bytes
            .saturating_add((self.decode)(documents, &mut self.rows)?);
        Ok(())
    }
    fn memory_used(&self) -> usize {
        self.bytes
    }
    async fn flush(&mut self) -> Result<(), Self::Error> {
        flush_owned(
            &mut self.rows,
            self.limits,
            &mut self.estimate,
            &mut self.sink,
        )
        .await?;
        self.bytes = 0;
        Ok(())
    }
}

macro_rules! output_tuple {
    ($first:ident:$first_index:tt $(, $name:ident:$index:tt)*) => {
        #[async_trait]
        impl<D, $first, $($name,)*> OutputGroup<D> for ($first, $($name,)*)
        where
            D: Sync,
            $first: OutputGroup<D>,
            $($name: OutputGroup<D, Error = $first::Error>,)*
        {
            type Error = $first::Error;
            fn append(&mut self, documents: &[D]) -> Result<(), Self::Error> {
                self.$first_index.append(documents)?;
                $(self.$index.append(documents)?;)*
                Ok(())
            }
            fn memory_used(&self) -> usize {
                self.$first_index.memory_used()$(.saturating_add(self.$index.memory_used()))*
            }
            async fn flush(&mut self) -> Result<(), Self::Error> {
                self.$first_index.flush().await?;
                $(self.$index.flush().await?;)*
                Ok(())
            }
        }
    };
}
output_tuple!(A:0);
output_tuple!(A:0, B:1);
output_tuple!(A:0, B:1, C:2);
output_tuple!(A:0, B:1, C:2, E:3);
output_tuple!(A:0, B:1, C:2, E:3, F:4);
output_tuple!(A:0, B:1, C:2, E:3, F:4, G:5);
output_tuple!(A:0, B:1, C:2, E:3, F:4, G:5, H:6);
output_tuple!(A:0, B:1, C:2, E:3, F:4, G:5, H:6, I:7);
output_tuple!(A:0, B:1, C:2, E:3, F:4, G:5, H:6, I:7, J:8);
output_tuple!(A:0, B:1, C:2, E:3, F:4, G:5, H:6, I:7, J:8, K:9);
output_tuple!(A:0, B:1, C:2, E:3, F:4, G:5, H:6, I:7, J:8, K:9, L:10);
output_tuple!(A:0, B:1, C:2, E:3, F:4, G:5, H:6, I:7, J:8, K:9, L:10, M:11);

/// Consumes a fallible stream. `extract` adds application context to cursor
/// errors; `estimate` and `exceeds` preserve caller-selected memory policies.
/// Counters advance when documents are obtained, before decoding, as in legacy
/// runners. Final input is decoded and all outputs flushed below the threshold.
pub async fn parse_stream<D, SE, St, O, Extract, Size, Threshold>(
    mut stream: St,
    document_counter: &mut usize,
    outputs: &mut O,
    mut extract: Extract,
    mut estimate: Size,
    exceeds: Threshold,
) -> Result<(), O::Error>
where
    D: Sync,
    St: Stream<Item = Result<D, SE>> + Unpin,
    O: OutputGroup<D>,
    Extract: FnMut(Result<D, SE>) -> Result<D, O::Error>,
    Size: FnMut(&D) -> usize,
    Threshold: Fn(usize) -> bool,
{
    let mut documents = Vec::new();
    let mut bytes = 0usize;
    while let Some(document) = stream.next().await {
        let document = extract(document)?;
        bytes = bytes.saturating_add(estimate(&document));
        documents.push(document);
        *document_counter = document_counter.saturating_add(1);
        if exceeds(bytes) {
            outputs.append(&documents)?;
            documents.clear();
            bytes = 0;
            if exceeds(outputs.memory_used()) {
                outputs.flush().await?;
            }
        }
    }
    if !documents.is_empty() {
        outputs.append(&documents)?;
    }
    outputs.flush().await
}

/// Parses and flushes a stream before running its caller-supplied finalizer.
/// Finalization never runs after a cursor, decode, or insert failure.
pub async fn parse_stream_and_then<D, SE, St, O, Extract, Size, Threshold, Finish, Fut>(
    stream: St,
    document_counter: &mut usize,
    outputs: &mut O,
    extract: Extract,
    estimate: Size,
    exceeds: Threshold,
    finish: Finish,
) -> Result<(), O::Error>
where
    D: Sync,
    St: Stream<Item = Result<D, SE>> + Unpin,
    O: OutputGroup<D>,
    Extract: FnMut(Result<D, SE>) -> Result<D, O::Error>,
    Size: FnMut(&D) -> usize,
    Threshold: Fn(usize) -> bool,
    Finish: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), O::Error>>,
{
    parse_stream(
        stream,
        document_counter,
        outputs,
        extract,
        estimate,
        exceeds,
    )
    .await?;
    finish().await
}

/// Table preparation, row accounting and cursor acquisition surrounding the
/// stream engine. Local adapters supply identities, schema and focused queries.
#[async_trait]
pub trait ModuleParseOperations: Send {
    type Dataset: Copy + Send + Sync;
    type Document: Send + Sync;
    type Error: Send;
    type Cursor: Stream<Item = Result<Self::Document, Self::Error>> + Unpin + Send;
    type Outputs: OutputGroup<Self::Document, Error = Self::Error>;
    /// Dataset and bookkeeping flag in declaration order.
    fn datasets(&self) -> Vec<(Self::Dataset, bool)>;
    fn outputs(&mut self) -> &mut Self::Outputs;
    fn estimate(document: &Self::Document) -> usize;
    fn exceeds(bytes: usize) -> bool;
    async fn prepare(&mut self, dataset: Self::Dataset) -> Result<(), Self::Error>;
    async fn row_count(&mut self, dataset: Self::Dataset) -> Result<usize, Self::Error>;
    async fn open_cursor(&mut self) -> Result<Self::Cursor, Self::Error>;
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseStage {
    Prepare,
    CountBefore,
    OpenCursor,
    Parse,
    CountAfter,
}
#[derive(Debug, thiserror::Error)]
#[error("Module parsing failed at {stage:?} for dataset {dataset:?}: {source}")]
pub struct ParseFailure<D, E> {
    pub stage: ParseStage,
    pub dataset: Option<D>,
    #[source]
    pub source: E,
}
#[derive(Debug)]
pub struct RowCounts<D> {
    pub dataset: D,
    pub before: usize,
    pub after: usize,
}
#[derive(Debug)]
pub struct ModuleParseReport<D> {
    pub documents: usize,
    pub rows: Vec<RowCounts<D>>,
}

pub async fn parse_module<B: ModuleParseOperations>(
    backend: &mut B,
) -> Result<ModuleParseReport<B::Dataset>, ParseFailure<B::Dataset, B::Error>> {
    let mut rows = Vec::new();
    for (dataset, bookkeeping) in backend.datasets() {
        if bookkeeping {
            continue;
        }
        backend
            .prepare(dataset)
            .await
            .map_err(|source| ParseFailure {
                stage: ParseStage::Prepare,
                dataset: Some(dataset),
                source,
            })?;
        let before = backend
            .row_count(dataset)
            .await
            .map_err(|source| ParseFailure {
                stage: ParseStage::CountBefore,
                dataset: Some(dataset),
                source,
            })?;
        rows.push(RowCounts {
            dataset,
            before,
            after: before,
        });
    }
    let cursor = backend.open_cursor().await.map_err(|source| ParseFailure {
        stage: ParseStage::OpenCursor,
        dataset: None,
        source,
    })?;
    let mut documents = 0;
    parse_stream(
        cursor,
        &mut documents,
        backend.outputs(),
        |result| result,
        B::estimate,
        B::exceeds,
    )
    .await
    .map_err(|source| ParseFailure {
        stage: ParseStage::Parse,
        dataset: None,
        source,
    })?;
    for counts in &mut rows {
        counts.after = backend
            .row_count(counts.dataset)
            .await
            .map_err(|source| ParseFailure {
                stage: ParseStage::CountAfter,
                dataset: Some(counts.dataset),
                source,
            })?;
    }
    Ok(ModuleParseReport { documents, rows })
}

/// A focused storage preparation operation, separate from decoding and insertion.
#[async_trait]
pub trait OutputPreparation: Send {
    type Error: Send;
    async fn prepare(&mut self) -> Result<(), Self::Error>;
}

/// Preserves workflows that must decode successfully before creating a table.
/// In final-only mode inputs are still decoded at their configured threshold,
/// while output insertion waits for the complete cursor to succeed.
pub struct PreparedOutput<G, P> {
    pub output: G,
    pub preparation: P,
    pub final_only: bool,
}
#[async_trait]
impl<D, G, P> OutputGroup<D> for PreparedOutput<G, P>
where
    D: Sync,
    G: OutputGroup<D>,
    P: OutputPreparation<Error = G::Error>,
{
    type Error = G::Error;
    fn append(&mut self, documents: &[D]) -> Result<(), Self::Error> {
        self.output.append(documents)
    }
    fn memory_used(&self) -> usize {
        if self.final_only {
            0
        } else {
            self.output.memory_used()
        }
    }
    async fn flush(&mut self) -> Result<(), Self::Error> {
        self.preparation.prepare().await?;
        self.output.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    type Events = Arc<Mutex<Vec<String>>>;
    struct Sink {
        events: Events,
        name: &'static str,
        fail_at: Option<usize>,
        calls: usize,
    }
    #[async_trait]
    impl<R: std::fmt::Debug + Sync> RowSink<R> for Sink {
        type Error = &'static str;
        async fn insert(&mut self, rows: &[R]) -> Result<(), Self::Error> {
            self.calls += 1;
            self.events
                .lock()
                .unwrap()
                .push(format!("{}:{rows:?}", self.name));
            if self.fail_at == Some(self.calls) {
                Err("insert")
            } else {
                Ok(())
            }
        }
    }
    fn sink(events: &Events, name: &'static str) -> Sink {
        Sink {
            events: events.clone(),
            name,
            fail_at: None,
            calls: 0,
        }
    }
    fn numbers(documents: &[usize], rows: &mut Vec<usize>) -> Result<usize, &'static str> {
        append_rows(rows, Ok(documents.to_vec()), |_| 1)
    }
    fn strings(documents: &[usize], rows: &mut Vec<String>) -> Result<usize, &'static str> {
        append_rows(
            rows,
            Ok(documents.iter().map(ToString::to_string).collect()),
            |_| 1,
        )
    }

    #[test]
    fn test_decode_commit_preserves_distinct_error_buffer_contracts() {
        for commit in [DecodeCommit::AtomicBatch, DecodeCommit::PerDocument] {
            let mut rows = vec![0];
            let mut decoded = Vec::new();
            let result = decode_append(
                &[1, 2, 3],
                &mut rows,
                |document| {
                    decoded.push(*document);
                    if *document == 2 {
                        Err("decode")
                    } else {
                        Ok(vec![*document])
                    }
                },
                |_| 1,
                commit,
            );
            assert_eq!(result, Err("decode"));
            assert_eq!(decoded, [1, 2]);
            assert_eq!(
                rows,
                match commit {
                    DecodeCommit::AtomicBatch => vec![0],
                    DecodeCommit::PerDocument => vec![0, 1],
                }
            );
        }
    }
    #[tokio::test]
    async fn test_typed_multi_output_threshold_and_final_flush() {
        let events = Events::default();
        let limits = BatchLimits::new(2, 10).unwrap();
        let mut outputs = (
            TypedOutput::new(numbers, sink(&events, "numbers"), |_: &usize| 1, limits),
            TypedOutput::new(strings, sink(&events, "strings"), |_: &String| 1, limits),
        );
        let mut counter = 0;
        parse_stream(
            futures::stream::iter((1..=5).map(Ok::<_, &'static str>)),
            &mut counter,
            &mut outputs,
            |result| result,
            |_| 1,
            |bytes| bytes > 2,
        )
        .await
        .unwrap();
        assert_eq!(counter, 5);
        assert_eq!(
            *events.lock().unwrap(),
            [
                "numbers:[1, 2]",
                "numbers:[3]",
                "strings:[\"1\", \"2\"]",
                "strings:[\"3\"]",
                "numbers:[4, 5]",
                "strings:[\"4\", \"5\"]"
            ]
        );
        assert!(outputs.0.rows.is_empty() && outputs.1.rows.is_empty());
        let capacity = outputs.0.rows.capacity();
        parse_stream(
            futures::stream::iter([Ok::<_, &'static str>(6)]),
            &mut counter,
            &mut outputs,
            |result| result,
            |_| 1,
            |_| false,
        )
        .await
        .unwrap();
        assert_eq!(counter, 6);
        assert_eq!(outputs.0.rows.capacity(), capacity);
    }

    #[tokio::test]
    async fn test_parse_stream_and_then_finalizes_only_after_successful_flush() {
        let events = Events::default();
        let mut output = TypedOutput::new(
            numbers,
            sink(&events, "rows"),
            |_: &usize| 1,
            BatchLimits::new(10, 10).unwrap(),
        );
        let mut counter = 0;
        let mut finalized = 0;
        parse_stream_and_then(
            futures::stream::iter([Ok::<_, &'static str>(1)]),
            &mut counter,
            &mut output,
            |result| result,
            |_| 1,
            |_| false,
            || {
                finalized += 1;
                std::future::ready(Ok::<(), &'static str>(()))
            },
        )
        .await
        .unwrap();
        assert_eq!(finalized, 1);
        assert_eq!(*events.lock().unwrap(), ["rows:[1]"]);

        let result = parse_stream_and_then(
            futures::stream::iter([Err::<usize, _>("cursor")]),
            &mut counter,
            &mut output,
            |result| result,
            |_| 1,
            |_| false,
            || {
                finalized += 1;
                std::future::ready(Ok::<(), &'static str>(()))
            },
        )
        .await;
        assert_eq!(result, Err("cursor"));
        assert_eq!(finalized, 1);
    }
    #[tokio::test]
    async fn test_owned_flush_retains_full_buffer_after_partial_insert_failure() {
        let events = Events::default();
        let mut backend = sink(&events, "rows");
        backend.fail_at = Some(2);
        let mut rows = Vec::with_capacity(10);
        rows.extend([1, 2, 3]);
        let capacity = rows.capacity();
        let result = flush_owned(
            &mut rows,
            BatchLimits::new(1, 1).unwrap(),
            |_| 1,
            &mut backend,
        )
        .await;
        assert_eq!(result, Err("insert"));
        assert_eq!(rows, [1, 2, 3]);
        assert_eq!(rows.capacity(), capacity);
        assert_eq!(events.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_later_output_failure_preserves_its_rows_after_prior_output_succeeded() {
        let events = Events::default();
        let limits = BatchLimits::new(2, 10).unwrap();
        let mut failing = sink(&events, "strings");
        failing.fail_at = Some(1);
        let mut outputs = (
            TypedOutput::new(numbers, sink(&events, "numbers"), |_: &usize| 1, limits),
            TypedOutput::new(strings, failing, |_: &String| 1, limits),
        );
        let result = parse_stream(
            futures::stream::iter([Ok::<_, &'static str>(1), Ok(2)]),
            &mut 0,
            &mut outputs,
            |result| result,
            |_| 1,
            |_| false,
        )
        .await;
        assert_eq!(result, Err("insert"));
        assert!(outputs.0.rows.is_empty());
        assert_eq!(outputs.1.rows, ["1", "2"]);
        assert_eq!(events.lock().unwrap().len(), 2);
        let mut rows = Vec::new();
        assert_eq!(
            append_rows(&mut rows, Ok::<_, ()>(vec![1, 2]), |_| usize::MAX),
            Ok(usize::MAX)
        );
    }
    #[tokio::test]
    async fn test_cursor_decode_and_insert_failures_stop_later_work() {
        for failure in ["cursor", "decode", "insert"] {
            let events = Events::default();
            let mut backend = sink(&events, "rows");
            if failure == "insert" {
                backend.fail_at = Some(1);
            }
            let mut output = TypedOutput::new(
                |docs: &[usize], rows: &mut Vec<usize>| {
                    if failure == "decode" {
                        return Err("decode");
                    }
                    numbers(docs, rows)
                },
                backend,
                |_: &usize| 1,
                BatchLimits::new(2, 10).unwrap(),
            );
            let mut counter = 0;
            let data = if failure == "cursor" {
                vec![Ok(1), Err("cursor"), Ok(3)]
            } else {
                vec![Ok(1), Ok(2), Ok(3)]
            };
            assert_eq!(
                parse_stream(
                    futures::stream::iter(data),
                    &mut counter,
                    &mut output,
                    |result| result,
                    |_| 1,
                    |_| true
                )
                .await,
                Err(failure)
            );
            assert_eq!(counter, 1);
            assert_eq!(
                events.lock().unwrap().len(),
                usize::from(failure != "decode")
            );
            assert_eq!(output.rows.len(), usize::from(failure == "insert"));
        }
    }
    #[tokio::test]
    async fn test_empty_stream_and_memory_saturation() {
        let events = Events::default();
        let mut output = TypedOutput::new(
            |docs: &[usize], rows: &mut Vec<usize>| {
                append_rows(rows, Ok::<_, &'static str>(docs.to_vec()), |_| usize::MAX)
            },
            sink(&events, "rows"),
            |_: &usize| usize::MAX,
            BatchLimits::new(2, usize::MAX).unwrap(),
        );
        let mut counter = 0;
        parse_stream(
            futures::stream::empty::<Result<usize, &'static str>>(),
            &mut counter,
            &mut output,
            |result| result,
            |_| 1,
            |_| false,
        )
        .await
        .unwrap();
        assert!(events.lock().unwrap().is_empty());
        parse_stream(
            futures::stream::iter([Ok::<_, &'static str>(1), Ok(2)]),
            &mut counter,
            &mut output,
            |result| result,
            |_| usize::MAX,
            |bytes| bytes == usize::MAX,
        )
        .await
        .unwrap();
        assert_eq!(counter, 2);
        assert_eq!(events.lock().unwrap().len(), 2);
    }
}

#[cfg(test)]
mod parallel_decode_tests {
    use super::*;
    #[test]
    fn preserves_order_and_atomic_failure_contract() {
        let mut rows = vec![9];
        assert_eq!(
            decode_parallel_append(
                &[3usize, 1, 2],
                &mut rows,
                |value| Ok::<_, ()>(vec![*value, *value]),
                |_| 1
            )
            .unwrap(),
            6
        );
        assert_eq!(rows, [9, 3, 3, 1, 1, 2, 2]);
        let before = rows.clone();
        assert_eq!(
            decode_parallel_append(
                &[1usize, 2],
                &mut rows,
                |value| if *value == 2 {
                    Err("decode")
                } else {
                    Ok(vec![*value])
                },
                |_| 1
            ),
            Err("decode")
        );
        assert_eq!(rows, before);
    }
}
