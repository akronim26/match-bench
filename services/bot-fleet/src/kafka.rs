//! This module implements kafka behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::RDKafkaErrorCode,
    message::Message,
    producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer},
    Offset, TopicPartitionList,
};

use iicpc_schemas_rust::{BarrierEvent, ReadySignal};

/// Replication factor and min.insync.replicas for topics this service creates.
///
/// Both default to 1, which is what a single-broker cluster can actually satisfy, and both
/// are overridable for a real multi-broker deployment.
///
/// They used to be hardcoded to 3 and 2. On one broker RF=3 cannot be satisfied, so every
/// topic creation failed -- and the failure was invisible, because create_topics() returns a
/// PER-TOPIC result vector that this function never inspected. Worse than the failure was
/// the shape of it: had the app path ever won the race against the topic-init Job and
/// created topics with min.insync.replicas=2 on RF=1, EVERY subsequent produce would fail
/// with NOT_ENOUGH_REPLICAS, because a single replica can never satisfy two in-sync ones.
/// The live cluster runs RF=1 / min.insync=1, so the code now says what the deployment is.
const DEFAULT_TOPIC_REPLICATION_FACTOR: i32 = 1;
const DEFAULT_MIN_INSYNC_REPLICAS: &str = "1";

fn topic_replication_factor() -> i32 {
    std::env::var("KAFKA_TOPIC_REPLICATION_FACTOR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_TOPIC_REPLICATION_FACTOR)
}

fn min_insync_replicas() -> String {
    std::env::var("KAFKA_MIN_INSYNC_REPLICAS")
        .unwrap_or_else(|_| DEFAULT_MIN_INSYNC_REPLICAS.to_string())
}
const DEFAULT_TOPIC_PARTITIONS: i32 = 3;
const HIGH_THROUGHPUT_TOPIC_PARTITIONS: i32 = 24;

#[derive(Clone)]
/// KafkaProducer stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct KafkaProducer {
    inner: FutureProducer,
}

/// KafkaConsumer stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct KafkaConsumer {
    inner: StreamConsumer,
}

/// KafkaMessage stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct KafkaMessage {
    pub payload: Option<Vec<u8>>,
    topic: String,
    partition: i32,
    offset: i64,
}

impl KafkaMessage {
    /// partition returns the Kafka partition this message arrived on. The worker keys
    /// its per-partition single-flight admission on this: at most one workload may
    /// execute per partition, so that each partition's offset commit stays independent
    /// of every other partition's.
    pub fn partition(&self) -> i32 {
        self.partition
    }
}

/// ensure_topics performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub async fn ensure_topics(brokers: &str, topics: &[&str]) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("create kafka admin client")?;

    let rf = topic_replication_factor();
    let insync = min_insync_replicas();
    let new_topics: Vec<NewTopic> = topics
        .iter()
        .map(|topic| {
            NewTopic::new(topic, topic_partitions(topic), TopicReplication::Fixed(rf))
                .set("min.insync.replicas", insync.as_str())
                .set("retention.ms", topic_retention_ms(topic))
                .set("max.message.bytes", "1048576")
        })
        .collect();

    let results = admin
        .create_topics(&new_topics, &AdminOptions::new())
        .await
        .context("create topics")?;

    // Inspect the PER-TOPIC results. The Vec returned here was previously discarded, so a
    // cluster that could not satisfy the requested replication reported success while
    // creating nothing -- the failure only surfaced later as a missing topic or as produce
    // errors, far from the cause. TopicAlreadyExists is the normal path (the topic-init Job
    // usually wins the race) and is not an error.
    for r in results {
        match r {
            Ok(_) => {}
            Err((topic, RDKafkaErrorCode::TopicAlreadyExists)) => {
                tracing::debug!(%topic, "topic already exists");
            }
            Err((topic, code)) => {
                return Err(anyhow::anyhow!(
                    "create topic {topic}: {code:?} (replication_factor={rf}, \
                     min.insync.replicas={insync}); a single-broker cluster cannot satisfy \
                     replication above 1"
                ));
            }
        }
    }
    Ok(())
}

/// topic_partitions performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn topic_partitions(topic: &str) -> i32 {
    match topic {
        iicpc_schemas_rust::TOPIC_ORDERS_ACKED
        | iicpc_schemas_rust::TOPIC_ORDERS_SENT
        | iicpc_schemas_rust::TOPIC_WORKLOAD_ASSIGNMENTS => HIGH_THROUGHPUT_TOPIC_PARTITIONS,
        _ => DEFAULT_TOPIC_PARTITIONS,
    }
}

/// topic_retention_ms performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn topic_retention_ms(topic: &str) -> &'static str {
    match topic {
        iicpc_schemas_rust::TOPIC_ORDERS_ACKED
        | iicpc_schemas_rust::TOPIC_ORDERS_SENT
        | iicpc_schemas_rust::TOPIC_WORKLOAD_ASSIGNMENTS
        | iicpc_schemas_rust::TOPIC_BARRIER
        | iicpc_schemas_rust::TOPIC_BOT_READY => "86400000",
        iicpc_schemas_rust::TOPIC_SCORES_CORRECTNESS => "2592000000",
        _ => "604800000",
    }
}

/// control_producer performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn control_producer(brokers: &str) -> Result<KafkaProducer> {
    let inner: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("acks", "all")
        // No idempotence: it requires the broker's transaction coordinator
        // (__transaction_state), which a fresh single-broker / RF=1 cluster may not
        // have ready ("Coordinator load in progress" stalls the producer indefinitely).
        // Control + telemetry are at-least-once (ingester/validator dedup by order_id),
        // so exactly-once is unnecessary; acks=all still gives leader durability.
        .set("enable.idempotence", "false")
        .set("linger.ms", "0")
        .set("retries", "2147483647")
        .set("retry.backoff.ms", "100")
        .set("delivery.timeout.ms", "10000")
        .create()
        .context("create kafka control producer")?;
    Ok(KafkaProducer { inner })
}

/// filter_compression_level keeps `n` only if it falls within librdkafka's
/// `compression.level` config-property range (-1..=12). Note this is narrower than
/// zstd's own native library range (roughly -131072..=22); values outside -1..=12
/// but inside zstd's native range still make `ClientConfig::create()` fail at
/// producer construction time, so they must be filtered here rather than passed
/// through to librdkafka.
fn filter_compression_level(n: i32) -> Option<i32> {
    (-1..=12).contains(&n).then_some(n)
}

/// telemetry_producer performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn telemetry_producer(brokers: &str) -> Result<KafkaProducer> {
    // High-throughput, lossless telemetry producer. The aggregator enqueues batches
    // WITHOUT awaiting delivery (enqueue_to_partition/send_result) and lets rdkafka's
    // background threads pipeline + batch them — so a deep internal queue is what
    // decouples the producer's drain rate from per-batch broker round-trips. linger
    // accumulates a few ms of batches; zstd shrinks the wire further than lz4 (now
    // that the vendored librdkafka is built with the `zstd` cargo feature, i.e.
    // libzstd IS compiled in); the large queue.buffering bounds in-flight memory and
    // provides backpressure (send_result returns QueueFull when saturated, which the
    // aggregator handles losslessly).
    let compression_level = std::env::var("KAFKA_TELEMETRY_COMPRESSION_LEVEL")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .and_then(filter_compression_level)
        .unwrap_or(3); // zstd level 3: fast, good ratio; matches librdkafka's own default
    let inner: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("acks", "1")
        // Drain the producer faster per broker round-trip: a longer linger accumulates many
        // app-batches into one large produce request (fewer requests = less broker per-request
        // CPU + fewer fsyncs), zstd shrinks the wire more than lz4 did, and the larger batch
        // ceilings let those big requests form.
        .set("linger.ms", "20")
        .set("compression.type", "zstd")
        .set("compression.level", compression_level.to_string())
        .set("batch.num.messages", "100000")
        .set("batch.size", "4194304") // 4 MiB produce-batch ceiling
        .set("queue.buffering.max.messages", "1000000")
        .set("queue.buffering.max.kbytes", "1048576") // 1 GiB in-flight ceiling
        .set("retries", "2147483647")
        .set("retry.backoff.ms", "25")
        .set("delivery.timeout.ms", "120000")
        .create()
        .context("create kafka telemetry producer")?;
    Ok(KafkaProducer { inner })
}

/// producer performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn producer(brokers: &str) -> Result<KafkaProducer> {
    telemetry_producer(brokers)
}

/// consumer performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn consumer(
    brokers: &str,
    group: &str,
    topics: &[&str],
    max_poll_interval: Duration,
) -> Result<KafkaConsumer> {
    let inner: StreamConsumer = consumer_client_config(brokers, group, max_poll_interval)
        .create()
        .context("create kafka consumer")?;
    inner.subscribe(topics).context("subscribe to topics")?;
    Ok(KafkaConsumer { inner })
}

/// consumer_client_config performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn consumer_client_config(brokers: &str, group: &str, max_poll_interval: Duration) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("fetch.min.bytes", "1")
        .set("fetch.wait.max.ms", "100")
        .set(
            "max.poll.interval.ms",
            max_poll_interval.as_millis().to_string(),
        )
        .set("session.timeout.ms", "10000")
        .set("partition.assignment.strategy", "roundrobin");
    config
}

/// publish_json performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub async fn publish_json<T: serde::Serialize>(
    producer: &KafkaProducer,
    topic: &str,
    key: &str,
    value: &T,
) -> Result<()> {
    let payload = serde_json::to_vec(value).context("serialize kafka json payload")?;
    publish_bytes(producer, topic, key, &payload).await
}

/// publish_bytes performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub async fn publish_bytes(
    producer: &KafkaProducer,
    topic: &str,
    key: &str,
    payload: &[u8],
) -> Result<()> {
    let record = FutureRecord::to(topic).key(key).payload(payload);
    producer
        .inner
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(err, _msg)| anyhow!("publish kafka message: {err}"))?;
    Ok(())
}

/// topic_partition_count reads the broker metadata partition count for a topic.
/// It returns None when metadata is unavailable or the topic cannot be found.
pub fn topic_partition_count(producer: &KafkaProducer, topic: &str) -> Option<i32> {
    let md = producer
        .inner
        .client()
        .fetch_metadata(Some(topic), Duration::from_secs(5))
        .ok()?;
    let n = md
        .topics()
        .iter()
        .find(|t| t.name() == topic)?
        .partitions()
        .len();
    (n > 0).then_some(n as i32)
}

/// publish_to_partition sends a keyed payload to an explicit Kafka partition.
/// It preserves co-partitioned telemetry ordering by bypassing producer-side
/// partition selection.
pub async fn publish_to_partition(
    producer: &KafkaProducer,
    topic: &str,
    partition: i32,
    key: &str,
    payload: &[u8],
) -> Result<()> {
    let record = FutureRecord::to(topic)
        .key(key)
        .payload(payload)
        .partition(partition);
    producer
        .inner
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(err, _msg)| anyhow!("publish kafka message to partition {partition}: {err}"))?;
    Ok(())
}

/// enqueue_to_partition queues a message for an explicit partition WITHOUT awaiting
/// delivery, returning the DeliveryFuture (resolves when the broker acks). This is
/// the high-throughput path: the caller pipelines many enqueues and accounts
/// deliveries asynchronously, instead of blocking one round-trip per message.
///
/// On a full internal queue rdkafka returns the record back; we surface that as
/// `Ok(None)` so the caller can poll() + retry (lossless backpressure) rather than
/// drop. A genuine enqueue error is returned as `Err`.
pub fn enqueue_to_partition(
    producer: &KafkaProducer,
    topic: &str,
    partition: i32,
    key: &str,
    payload: &[u8],
) -> Result<Option<DeliveryFuture>> {
    let record = FutureRecord::to(topic)
        .key(key)
        .payload(payload)
        .partition(partition);
    match producer.inner.send_result(record) {
        Ok(fut) => Ok(Some(fut)),
        Err((
            rdkafka::error::KafkaError::MessageProduction(
                rdkafka::types::RDKafkaErrorCode::QueueFull,
            ),
            _,
        )) => {
            Ok(None) // queue full — caller polls + retries
        }
        Err((err, _)) => Err(anyhow!(
            "enqueue kafka message to partition {partition}: {err}"
        )),
    }
}

/// flush_producer blocks until every queued message has been delivered (or the timeout
/// expires), returning the number still undelivered.
///
/// This is NOT optional at shutdown. enqueue_to_partition is fire-and-forget: it hands the
/// record to librdkafka's internal queue and drops the DeliveryFuture, so "flushed" in the
/// caller's accounting means ENQUEUED, not delivered. With linger.ms batching and a queue
/// sized in the hundreds of megabytes, a process that returns without flushing discards
/// whatever is still queued — silently, since nothing inspects delivery reports. In the
/// capture that showed up downstream as orders whose responses simply never existed.
pub fn flush_producer(producer: &KafkaProducer, timeout: Duration) -> Result<i32> {
    let _ = producer.inner.flush(timeout);
    Ok(producer.inner.in_flight_count())
}

/// in_flight_count reports messages queued in the producer but not yet acknowledged by
/// the broker. A rising value means the producer is being drained slower than it is fed.
pub fn in_flight_count(producer: &KafkaProducer) -> i32 {
    producer.inner.in_flight_count()
}

/// poll drives the producer's background delivery/callback queue. Call it when an
/// enqueue reports QueueFull to let in-flight messages drain before retrying.
pub fn poll_producer(producer: &KafkaProducer, timeout: Duration) {
    producer.inner.poll(timeout);
}

pub async fn wait_for_barrier(
    consumer: &KafkaConsumer,
    session_id: &str,
    wait_timeout: Duration,
) -> Result<BarrierEvent> {
    let deadline = tokio::time::Instant::now() + wait_timeout;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            anyhow::bail!("timed out waiting for barrier");
        }

        let message = match tokio::time::timeout(remaining, recv_message(consumer)).await {
            Ok(result) => result?,
            Err(_) => anyhow::bail!("timed out waiting for barrier"),
        };
        let Some(payload) = message.payload.as_deref() else {
            commit_message(consumer, &message).context("commit null barrier message")?;
            continue;
        };

        let event: BarrierEvent = match serde_json::from_slice(payload) {
            Ok(event) => event,
            Err(_) => {
                commit_message(consumer, &message).context("commit malformed barrier message")?;
                continue;
            }
        };
        if event.session_id == session_id {
            commit_message(consumer, &message).context("commit matching barrier message")?;
            return Ok(event);
        }
        commit_message(consumer, &message).context("commit unrelated barrier message")?;
    }
}

/// decode_workload performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn decode_workload(payload: &[u8]) -> Result<iicpc_schemas_rust::WorkloadSpec> {
    serde_json::from_slice(payload).context("decode workload assignment")
}

/// ready_key performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn ready_key(signal: &ReadySignal) -> String {
    format!("{}:{}", signal.session_id, signal.worker_id)
}

/// recv_message performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub async fn recv_message(consumer: &KafkaConsumer) -> Result<KafkaMessage> {
    let mut stream = consumer.inner.stream();
    let msg = stream
        .next()
        .await
        .ok_or_else(|| anyhow!("consumer stream ended unexpectedly"))?
        .context("read message")?;

    Ok(KafkaMessage {
        payload: msg.payload().map(|b| b.to_vec()),
        topic: msg.topic().to_string(),
        partition: msg.partition(),
        offset: msg.offset(),
    })
}

/// commit_message performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn commit_message(consumer: &KafkaConsumer, message: &KafkaMessage) -> Result<()> {
    let mut offsets = TopicPartitionList::new();
    offsets
        .add_partition_offset(
            &message.topic,
            message.partition,
            Offset::Offset(message.offset + 1),
        )
        .context("build kafka commit offset")?;
    consumer
        .inner
        .commit(&offsets, CommitMode::Sync)
        .context("commit kafka offset")?;
    Ok(())
}

/// recv_payload performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub async fn recv_payload(consumer: &KafkaConsumer) -> Result<Option<Vec<u8>>> {
    let message = recv_message(consumer).await?;
    let payload = message.payload.clone();
    commit_message(consumer, &message)?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// consumer_config_spreads_partitions_roundrobin performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn consumer_config_spreads_partitions_roundrobin() {
        let config = consumer_client_config(
            "localhost:9092",
            "bot-fleet",
            Duration::from_millis(1_800_000),
        );
        assert_eq!(
            config.get("partition.assignment.strategy"),
            Some("roundrobin")
        );
        assert_eq!(config.get("max.poll.interval.ms"), Some("1800000"));
        assert_eq!(config.get("enable.auto.commit"), Some("false"));
        assert_eq!(config.get("auto.offset.reset"), Some("earliest"));
    }

    #[test]
    /// filter_compression_level_matches_librdkafka_range verifies the filter narrows
    /// to librdkafka's compression.level range (-1..=12), not zstd's native library
    /// range (-131072..=22) — values in the latter but outside the former crash
    /// ClientConfig::create() at producer construction time.
    fn filter_compression_level_matches_librdkafka_range() {
        for n in [-1, 0, 12] {
            assert_eq!(filter_compression_level(n), Some(n), "expected {n} to pass");
        }
        for n in [15, 19, 22, -5, -131_072, 13, -2] {
            assert_eq!(
                filter_compression_level(n),
                None,
                "expected {n} to be rejected"
            );
        }
    }
}
