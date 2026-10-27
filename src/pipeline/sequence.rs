//! Sequential parse lifecycle with a fallible application callback.

use std::future::Future;

/// One operation in a parse workflow. Export applies to the caller's export scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseStep<M> {
    Parse(M),
    Backup(M),
    Remove(M),
    Export,
}

/// Parses each module, optionally backing up and removing it before continuing.
///
/// A full pipeline exports once at the end, even with an empty module list.
/// Parse-only mode performs no cleanup or export. The first callback error is
/// returned unchanged; panics propagate normally. Unit-returning application
/// operations can be wrapped with `Ok::<(), E>(())` after they finish.
///
/// ```
/// # async fn example() -> Result<(), std::convert::Infallible> {
/// use sctys_rust_utilities::pipeline::{run_parse_steps, ParseStep};
/// run_parse_steps(&['A', 'B'], true, |step| async move {
///     match step {
///         ParseStep::Parse(_module) => { /* parse module */ }
///         ParseStep::Backup(_module) => { /* back up module */ }
///         ParseStep::Remove(_module) => { /* remove module */ }
///         ParseStep::Export => { /* export the separately selected scope */ }
///     }
///     Ok(())
/// }).await
/// # }
/// ```
pub async fn run_parse_steps<M, F, Fut, E>(
    modules: &[M],
    full_pipeline: bool,
    mut action: F,
) -> Result<(), E>
where
    M: Copy,
    F: FnMut(ParseStep<M>) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    for step in parse_steps(modules, full_pipeline) {
        action(step).await?;
    }
    Ok(())
}

/// Lending callback variant for per-run mutable state. An async closure may
/// borrow its captured backend across an await without boxing or global state.
pub async fn run_parse_with<M: Copy, E>(
    modules: &[M],
    full_pipeline: bool,
    mut action: impl AsyncFnMut(ParseStep<M>) -> Result<(), E>,
) -> Result<(), E> {
    for step in parse_steps(modules, full_pipeline) {
        action(step).await?;
    }
    Ok(())
}

/// Declarative ordering shared by lending and Send callback executors.
pub fn parse_steps<M: Copy>(
    modules: &[M],
    full_pipeline: bool,
) -> impl Iterator<Item = ParseStep<M>> + '_ {
    modules
        .iter()
        .flat_map(move |module| {
            [
                Some(ParseStep::Parse(*module)),
                full_pipeline.then_some(ParseStep::Backup(*module)),
                full_pipeline.then_some(ParseStep::Remove(*module)),
            ]
            .into_iter()
            .flatten()
        })
        .chain(full_pipeline.then_some(ParseStep::Export))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::ready;

    const FULL: [ParseStep<char>; 7] = [
        ParseStep::Parse('A'),
        ParseStep::Backup('A'),
        ParseStep::Remove('A'),
        ParseStep::Parse('B'),
        ParseStep::Backup('B'),
        ParseStep::Remove('B'),
        ParseStep::Export,
    ];

    #[tokio::test]
    async fn test_full_order_and_parse_only() {
        for full in [false, true] {
            let mut steps = Vec::new();
            run_parse_steps(&['A', 'B'], full, |step| {
                steps.push(step);
                ready(Ok::<_, ()>(()))
            })
            .await
            .unwrap();
            let expected = if full {
                FULL.to_vec()
            } else {
                vec![FULL[0], FULL[3]]
            };
            assert_eq!(steps, expected);
        }
    }

    #[tokio::test]
    async fn test_empty_workflow() {
        for full in [false, true] {
            let mut steps = Vec::new();
            run_parse_steps::<char, _, _, ()>(&[], full, |step| {
                steps.push(step);
                ready(Ok(()))
            })
            .await
            .unwrap();
            assert_eq!(
                steps,
                if full {
                    vec![ParseStep::Export]
                } else {
                    vec![]
                }
            );
        }
    }

    #[tokio::test]
    async fn test_failure_at_every_step_stops_workflow() {
        for (index, failed) in FULL.iter().enumerate() {
            let mut steps = Vec::new();
            let result = run_parse_steps(&['A', 'B'], true, |step| {
                steps.push(step);
                ready(if step == *failed {
                    Err("injected failure")
                } else {
                    Ok(())
                })
            })
            .await;
            assert_eq!(result, Err("injected failure"));
            assert_eq!(steps, FULL[..=index]);
        }
    }

    #[tokio::test]
    #[should_panic(expected = "application panic")]
    async fn test_callback_panics_propagate() {
        run_parse_steps(&['A'], true, |_| -> std::future::Ready<Result<(), ()>> {
            panic!("application panic");
        })
        .await
        .unwrap();
    }
}
