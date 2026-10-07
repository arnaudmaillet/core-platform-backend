//! `topic-provisioner` — creates the fleet's Kafka topics ahead of the workloads.
//!
//! Brokers run with `auto.create.topics.enable=false` (MSK policy — auto-created
//! topics silently inherit broker defaults nobody chose, and MSK ships with
//! auto-creation off anyway, so an unprovisioned cluster can't be published to at
//! all). This binary derives the complete topic set from the `event-topology`
//! registry — every produced/consumed stream topic, plus the consumer runtime's
//! `<topic>.dlq` counterpart for each consumed topic — and creates them in one
//! idempotent admin call. `TOPIC_ALREADY_EXISTS` is success; anything else fails
//! the run (non-zero exit) so the deploy that depends on it does not proceed.
//!
//! Runs as an ArgoCD PreSync hook Job in each env's overlay: a new topic lands in
//! the registry and the very next sync provisions it before the fleet rolls.
//!
//! Env:
//! - `KAFKA_BROKERS` / `KAFKA_SECURITY_PROTOCOL` / `KAFKA_SASL_*` — the exact same
//!   client settings the fleet uses (`transport::kafka::KafkaClientConfig`).
//! - `TOPIC_REPLICATION_FACTOR` — REQUIRED. No default on purpose: silently
//!   defaulting to 1 on a multi-broker cluster would create unreplicated topics.
//! - `TOPIC_PARTITIONS` — partitions per topic (default 12, matching the KEDA
//!   workers' `maxReplicaCount` cap: a consumer group cannot parallelize beyond
//!   its partitions).
//!
//! A topic with a registry retention (`event_topology::RETENTION`, and its
//! `.dlq`) is created with `retention.ms`, and the setting is re-applied to it on
//! every run, so a topic that already existed (or a changed retention) follows.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use rdkafka::admin::{AdminClient, AdminOptions, AlterConfig, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::types::RDKafkaErrorCode;
use transport::kafka::config::KafkaClientConfig;
use transport::kafka::DLQ_SUFFIX;

#[tokio::main]
async fn main() -> Result<()> {
    let replication: i32 = std::env::var("TOPIC_REPLICATION_FACTOR")
        .context("TOPIC_REPLICATION_FACTOR is required (no default: silently creating unreplicated topics on a multi-broker cluster is worse than failing)")?
        .parse()
        .context("TOPIC_REPLICATION_FACTOR must be an integer")?;
    let partitions: i32 = match std::env::var("TOPIC_PARTITIONS") {
        Ok(v) => v.parse().context("TOPIC_PARTITIONS must be an integer")?,
        Err(_) => 12,
    };
    // Creating the full registry (53 topics × partitions × RF) on small managed
    // brokers takes real time; 30s timed out live on 2× kafka.t3.small.
    let admin_timeout: u64 = match std::env::var("TOPIC_ADMIN_TIMEOUT_SECS") {
        Ok(v) => v.parse().context("TOPIC_ADMIN_TIMEOUT_SECS must be an integer")?,
        Err(_) => 180,
    };

    let names = topic_names();
    println!(
        "provisioning {} topics (partitions={partitions}, rf={replication})",
        names.len()
    );

    let admin: AdminClient<DefaultClientContext> = KafkaClientConfig::from_env()
        .to_rdkafka()
        .create()
        .context("build Kafka admin client")?;

    let retentions: Vec<(String, String)> = names
        .iter()
        .filter_map(|name| event_topology::retention_ms(name).map(|ms| (name.clone(), ms.to_string())))
        .collect();
    let new_topics: Vec<NewTopic> = names
        .iter()
        .map(|name| {
            let topic = NewTopic::new(name, partitions, TopicReplication::Fixed(replication));
            match retentions.iter().find(|(t, _)| t == name) {
                Some((_, ms)) => topic.set("retention.ms", ms),
                None => topic,
            }
        })
        .collect();

    let results = admin
        .create_topics(
            new_topics.iter(),
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(admin_timeout))),
        )
        .await
        .context("create_topics admin call")?;

    let (mut created, mut existing, mut failed) = (0u32, 0u32, Vec::new());
    for result in results {
        match result {
            Ok(topic) => {
                created += 1;
                println!("  [created] {topic}");
            }
            Err((topic, RDKafkaErrorCode::TopicAlreadyExists)) => {
                existing += 1;
                println!("  [exists]  {topic}");
            }
            Err((topic, code)) => {
                println!("  [FAILED]  {topic}: {code}");
                failed.push((topic, code));
            }
        }
    }

    // Re-applied every run: a topic that predates its retention follows too.
    // rdkafka's `alter_configs` is Kafka's NON-incremental AlterConfigs: it
    // replaces the topic's whole set of dynamic configs, so any other override
    // on these topics (min.insync.replicas, cleanup.policy, … set by hand or by
    // infra) is reset to the cluster default on every run. Harmless while the
    // registry sets nothing else on them; before adding more per-topic settings,
    // pass the full intended set here (or move to IncrementalAlterConfigs).
    let alters: Vec<AlterConfig> = retentions
        .iter()
        .map(|(topic, ms)| AlterConfig::new(ResourceSpecifier::Topic(topic)).set("retention.ms", ms))
        .collect();
    if !alters.is_empty() {
        let results = admin
            .alter_configs(
                alters.iter(),
                &AdminOptions::new().operation_timeout(Some(Duration::from_secs(admin_timeout))),
            )
            .await
            .context("alter_configs admin call")?;
        for result in results {
            match result {
                Ok(resource) => println!("  [retention] {resource:?}"),
                Err((resource, code)) => {
                    println!("  [FAILED]  retention of {resource:?}: {code}");
                    failed.push((format!("{resource:?}"), code));
                }
            }
        }
    }

    println!("done: created={created} existing={existing} failed={}", failed.len());
    if !failed.is_empty() {
        bail!("{} topic(s) failed to provision: {failed:?}", failed.len());
    }
    Ok(())
}

/// The full broker topic set: every registry stream topic + a `.dlq` per
/// consumed topic. Sorted for stable, diffable logs.
fn topic_names() -> Vec<String> {
    let mut names: Vec<String> = event_topology::all_stream_topics()
        .into_iter()
        .map(str::to_owned)
        .chain(
            event_topology::consumed_stream_topics()
                .into_iter()
                .map(|topic| format!("{topic}{DLQ_SUFFIX}")),
        )
        .collect();
    names.sort_unstable();
    names.dedup();
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_set_covers_registry_and_dlqs_without_duplicates() {
        let names = topic_names();

        for topic in event_topology::all_stream_topics() {
            assert!(names.contains(&topic.to_owned()), "missing {topic}");
        }
        for topic in event_topology::consumed_stream_topics() {
            let dlq = format!("{topic}{DLQ_SUFFIX}");
            assert!(names.contains(&dlq), "missing {dlq}");
        }

        let mut deduped = names.clone();
        deduped.dedup();
        assert_eq!(names, deduped);
    }
}
