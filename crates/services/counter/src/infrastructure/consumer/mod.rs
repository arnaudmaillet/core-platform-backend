//! The worker's ingestion path — the async command side.
//!
//! Each consumer runs on the shared at-least-once
//! [`run_consumer`](transport::kafka::consumer::run_consumer) runner (manual commit,
//! bounded retry + jitter, DLQ on poison/exhaustion). A decoded event is distilled
//! into [`Observation`]s and folded into the **shared** [`WindowAggregator`]; a
//! separate [`run_flush_loop`] drains closed windows on a ticker and fans them out
//! through the [`DeltaFlusher`], then publishes the coarse popularity signal for the
//! entities just touched.
//!
//! Durability note (the at-least-once windowing gap): events are committed once
//! folded into the in-memory aggregator, before the window is flushed. A worker
//! crash with un-flushed windows therefore loses a few seconds of aggregation —
//! acceptable by design (approximate metrics tolerate it; exact metrics are healed
//! by the Phase-7 reconciliation loop). Graceful shutdown drains everything first.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::Mutex;
use transport::kafka::consumer::{KafkaConsumerHandle, ProcessOutcome, RetryPolicy, run_consumer};
use transport::kafka::envelope::ConsumablePayload;
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::command::{DeltaFlusher, PopularityPublisher};
use crate::domain::{Observation, WindowAggregator};
use crate::error::CounterError;

/// Lets the at-least-once runner classify a failure: delegate to the error's own
/// retry verdict — transient store faults retry; decode / domain faults are poison
/// and dead-letter immediately.
impl transport::kafka::consumer::ClassifyError for CounterError {
    fn is_retryable(&self) -> bool {
        <Self as error::AppError>::is_retryable(self)
    }
}

/// Runs a fold consumer: decode each event into observations and fold them into the
/// shared aggregator. `map` is the per-topic distillation (`map_view`, `map_stake`,
/// …); `run_consumer` owns deserialization, so poison bytes dead-letter before they
/// reach `map`.
pub async fn run_fold_consumer<T, M>(
    label: &'static str,
    consumer: KafkaConsumerHandle,
    producer: KafkaProducerHandle,
    aggregator: Arc<Mutex<WindowAggregator>>,
    map: M,
) where
    T: ConsumablePayload + Clone,
    M: Fn(T) -> Result<Vec<Observation>, CounterError> + Copy + Send + Sync + 'static,
{
    tracing::info!(label, "counter consumer started");
    let policy = RetryPolicy::default();
    let result = run_consumer::<T, _>(&consumer, &producer, &policy, move |event| {
        let aggregator = Arc::clone(&aggregator);
        let event = event.clone();
        Box::pin(async move {
            let outcome = async {
                let observations = map(event)?;
                let mut agg = aggregator.lock().await;
                for obs in observations {
                    agg.fold(obs)?;
                }
                Ok::<(), CounterError>(())
            }
            .await;
            ProcessOutcome::from_result(outcome)
        })
    })
    .await;
    if let Err(e) = result {
        tracing::error!(label, error = %e, "counter consumer stopped");
    }
}

/// Drains closed windows on a ticker, flushes them across the three tiers, and
/// publishes the coarse popularity signal for the entities touched by the batch.
pub async fn run_flush_loop(
    aggregator: Arc<Mutex<WindowAggregator>>,
    flusher: Arc<DeltaFlusher>,
    popularity: Arc<PopularityPublisher>,
    flush_interval: Duration,
) {
    tracing::info!("counter flush loop started");
    let mut ticker = tokio::time::interval(flush_interval);
    loop {
        ticker.tick().await;
        let deltas = {
            let mut agg = aggregator.lock().await;
            agg.drain_closed(Utc::now())
        };
        if deltas.is_empty() {
            continue;
        }

        match flusher.flush(&deltas).await {
            Ok(report) => tracing::debug!(applied = report.applied, "counter windows flushed"),
            Err(error) => {
                // The drained windows are lost (see the module durability note);
                // exact metrics are healed by reconciliation, approximate tolerate it.
                tracing::error!(%error, "counter flush failed; dropped windows");
                continue;
            }
        }

        // Coarse popularity refresh for the distinct entities just touched.
        let mut seen = HashSet::new();
        for delta in &deltas {
            let entity = delta.entity();
            if seen.insert((entity.kind, entity.id.as_str().to_owned()))
                && let Err(error) = popularity.publish(entity).await
            {
                tracing::warn!(%error, "popularity publish failed");
            }
        }
    }
}
