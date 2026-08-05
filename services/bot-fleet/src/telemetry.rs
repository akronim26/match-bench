//! This module implements telemetry behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::{
    sync::mpsc::{self, Sender},
    task::JoinHandle,
    time,
};
use tracing::error;

use iicpc_schemas_rust::{
    band_partition, partition_for, session_band_partition, OrderSentBatchV2Ref, OrderSentEvent,
    OrderSentEventFieldsRef, DEFAULT_PARTITION_BAND_WIDTH, ORDER_BAND_UNSET,
};

use crate::{
    kafka::{self, KafkaProducer},
    metrics,
};

/// Number of parallel drain shards the aggregator runs. Each shard owns its own
/// mpsc channel + PartitionBatcher + inflight-delivery set, draining independently.
/// A single shared `run_aggregator` task measured a ~445k/s durable ceiling (one
/// select! loop, one drain thread of control) — sharding by order_id lets N shards
/// drain concurrently while preserving the per-order co-partitioning invariant
/// (partition_for(order_id, M) keeps a given order's events on exactly one shard,
/// so per-order ordering/batching is unaffected by which shard handles it).
fn shard_count() -> usize {
    std::env::var("BOT_TELEMETRY_SHARDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(4)
}

/// Width (in partitions) of a session's partition band for `session_band_partition`.
fn partition_band_width() -> i32 {
    std::env::var("BOT_PARTITION_BAND_WIDTH")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_PARTITION_BAND_WIDTH)
}

#[derive(Clone)]
/// TelemetrySink stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct TelemetrySink {
    // One sender per drain shard. record() routes an event by
    // partition_for(order_id, shards) so a given order's events always land on
    // the same shard (preserving per-order batching/ordering invariants).
    tx: Vec<Sender<OrderSentEvent>>,
    handles: Arc<Mutex<Vec<JoinHandle<Result<()>>>>>,
    // When false (BOT_DISABLE_TELEMETRY=1), record() is a no-op and no aggregator is
    // spawned. Used to measure raw send capacity on a drain contestant, where there is
    // no validation and single-broker Kafka cannot absorb a telemetry event per order.
    enabled: bool,
}

impl TelemetrySink {
    /// new performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    ///
    /// The rdkafka producer is SHARED across shards (each shard gets a cheap
    /// `.clone()` of the same `KafkaProducer`), not one producer per shard.
    /// rdkafka's `FutureProducer` wraps an `Arc`-backed client internally and is
    /// explicitly designed for concurrent use from multiple tasks/threads — cloning
    /// it does not open a new connection or duplicate broker metadata/topic caches,
    /// it just hands out another handle to the same background I/O threads and
    /// internal queue. Per-shard producers would multiply connection/metadata
    /// overhead per broker for no batching benefit, since librdkafka already
    /// batches/pipelines concurrent produce calls from many callers internally.
    pub fn new(
        producer: KafkaProducer,
        topic: String,
        session_id: String,
        worker_id: String,
        capacity: usize,
        flush_interval: Duration,
        batch_size: usize,
        num_partitions: i32,
        order_band: u32,
    ) -> Self {
        let enabled = !matches!(
            std::env::var("BOT_DISABLE_TELEMETRY").as_deref(),
            Ok("1") | Ok("true")
        );
        let shards = shard_count();
        let band_width = partition_band_width();
        // Split the configured channel capacity across shards so total buffered
        // memory stays bounded the same way a single-channel sink was: N shards at
        // capacity/N each sum to ~capacity events in flight, not capacity*N.
        let per_shard_capacity = (capacity / shards).max(batch_size);

        if !enabled {
            tracing::warn!(
                "BOT_DISABLE_TELEMETRY set: telemetry recording disabled (send-capacity benchmark mode)"
            );
            return Self {
                tx: Vec::new(),
                handles: Arc::new(Mutex::new(Vec::new())),
                enabled: false,
            };
        }

        let mut tx = Vec::with_capacity(shards);
        let mut handles = Vec::with_capacity(shards);
        for shard_id in 0..shards {
            let (shard_tx, shard_rx) = mpsc::channel(per_shard_capacity);
            tx.push(shard_tx);
            handles.push(tokio::spawn(run_aggregator(
                shard_id,
                shard_rx,
                producer.clone(),
                topic.clone(),
                session_id.clone(),
                worker_id.clone(),
                flush_interval,
                num_partitions,
                band_width,
                order_band,
            )));
        }

        Self {
            tx,
            handles: Arc::new(Mutex::new(handles)),
            enabled,
        }
    }

    /// record performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    /// LOSSLESS: blocks on a full channel (backpressure) instead of dropping, so the
    /// generator self-paces to the sustainable telemetry rate rather than discarding
    /// measurement data. Only errors if the aggregator is gone (shutdown), which is
    /// the single case we still count as dropped.
    pub async fn record(&self, event: OrderSentEvent) {
        if !self.enabled {
            return;
        }
        let shard = partition_for(&event.order_id, self.tx.len() as i32) as usize;
        if self.tx[shard].send(event).await.is_err() {
            metrics::telemetry_dropped();
            error!("telemetry channel closed; dropping event");
        }
    }

    /// close performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub async fn close(self) -> Result<()> {
        drop(self.tx);

        let handles = self
            .handles
            .lock()
            .map_err(|err| {
                crate::errors::BotFleetError::TelemetryError(format!(
                    "telemetry join handle mutex poisoned: {err}"
                ))
            })?
            .drain(..)
            .collect::<Vec<_>>();

        // Await EVERY shard before reporting: an early-return `?` here dropped
        // the remaining JoinHandles, detaching (not cancelling) those shards and
        // leaving their shutdown flush unobserved — a run's telemetry tail could
        // silently vanish on an unlucky shutdown. Join all, then surface the
        // first error.
        let results = futures::future::join_all(handles).await;
        let mut first_err = None;
        for joined in results {
            match joined.context("join telemetry aggregator") {
                Ok(Ok(())) => {}
                Ok(Err(err)) if first_err.is_none() => first_err = Some(err),
                Err(err) if first_err.is_none() => first_err = Some(err),
                _ => {}
            }
        }
        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

/// Max events pulled from the channel per aggregator wake (recv_many), amortizing
/// the select!/wake overhead across a slice instead of paying it per event.
const RECV_BATCH: usize = 4096;

/// AckFuture is a pending delivery, tagged with the number of events in its batch
/// so completion can be accounted (flushed vs lost) without awaiting it inline.
type AckFuture = Pin<Box<dyn Future<Output = (usize, Result<(), String>)> + Send>>;

/// run_aggregator drains ONE shard's event channel and publishes per-partition
/// batches to Kafka WITHOUT blocking on each batch's delivery. The old design
/// awaited delivery per flush (one broker round-trip serialized the whole drain →
/// ~5.7k/s ceiling and 90% drops); a later fix decoupled drain from delivery via a
/// single select! loop but still ran ONE aggregator task per worker, ceiling at
/// ~445k/s. This is now one of `shard_count()` such loops, each independently
/// draining its own channel/batcher/inflight-set — sharded by
/// partition_for(order_id, shards) at record() time, so a given order's events
/// always land on the same shard. Within a shard: a single `select!` interleaves
/// draining the channel into a per-partition batcher, enqueuing full chunks
/// (rdkafka pipelines + batches them in the background), and accounting completed
/// deliveries from an `inflight` set. Backpressure is lossless: a full producer
/// queue makes `enqueue_chunk` poll+retry, which stalls the drain, fills the
/// channel, and blocks `record()` — the generator self-paces instead of dropping
/// evidence.
#[allow(clippy::too_many_arguments)]
async fn run_aggregator(
    shard_id: usize,
    mut rx: mpsc::Receiver<OrderSentEvent>,
    producer: KafkaProducer,
    topic: String,
    session_id: String,
    worker_id: String,
    flush_interval: Duration,
    num_partitions: i32,
    band_width: i32,
    order_band: u32,
) -> Result<()> {
    let _ = shard_id; // reserved for future per-shard metrics/logging
    let mut ticker = time::interval(flush_interval);
    let mut batcher = PartitionBatcher::new(num_partitions, band_width, order_band);
    let mut inflight: FuturesUnordered<AckFuture> = FuturesUnordered::new();
    // Drain the channel in bulk (recv_many) rather than one event per wake: at high
    // rates a single aggregator paid the select!/wake overhead per event, which was
    // the next drain ceiling after the await-per-flush fix. One wake now pulls up to
    // RECV_BATCH events and feeds them straight through the batcher.
    let mut buf: Vec<OrderSentEvent> = Vec::with_capacity(RECV_BATCH);

    loop {
        tokio::select! {
            biased;
            // Account finished deliveries first so `inflight` stays bounded.
            Some((n, res)) = inflight.next(), if !inflight.is_empty() => account_delivery(n, res),
            count = rx.recv_many(&mut buf, RECV_BATCH) => {
                if count == 0 {
                    break; // sink closed → drain to completion below
                }
                for event in buf.drain(..) {
                    if let Some((part, chunk)) = batcher.push(event) {
                        enqueue_chunk(&producer, &topic, &session_id, &worker_id, part, chunk, &mut inflight).await;
                    }
                }
            }
            _ = ticker.tick() => {
                for (part, chunk) in batcher.drain_ready() {
                    enqueue_chunk(&producer, &topic, &session_id, &worker_id, part, chunk, &mut inflight).await;
                }
            }
        }
    }

    // Shutdown: flush whatever is buffered, then wait for every in-flight delivery.
    for (part, chunk) in batcher.drain_ready() {
        enqueue_chunk(
            &producer,
            &topic,
            &session_id,
            &worker_id,
            part,
            chunk,
            &mut inflight,
        )
        .await;
    }
    while let Some((n, res)) = inflight.next().await {
        account_delivery(n, res);
    }
    Ok(())
}

fn account_delivery(n: usize, res: Result<(), String>) {
    match res {
        Ok(()) => metrics::telemetry_flushed(n),
        Err(err) => {
            metrics::telemetry_dropped_n(n);
            error!(error = %err, count = n, "telemetry batch failed delivery after retries");
        }
    }
}

/// enqueue_chunk encodes one partition's chunk and enqueues it without awaiting
/// delivery, retrying on a full producer queue (poll to drain, then retry) so events
/// are never silently dropped. rdkafka owns retry/ordering for the in-flight message;
/// the tagged DeliveryFuture is pushed to `inflight` for async accounting.
///
/// Encoding is positional msgpack (`rmp_serde::to_vec`, not `to_vec_named`) with
/// session_id/submission_id/worker_id hoisted into the batch envelope instead of
/// repeated per event — see `OrderSentBatchV2Ref` in schemas/rust. submission_id is
/// taken from the first event in the chunk: a TelemetrySink is scoped to one
/// session/run, so submission_id is constant across every event it ever batches.
async fn enqueue_chunk(
    producer: &KafkaProducer,
    topic: &str,
    session_id: &str,
    worker_id: &str,
    partition: i32,
    chunk: Vec<OrderSentEvent>,
    inflight: &mut FuturesUnordered<AckFuture>,
) {
    let n = chunk.len();
    let Some(submission_id) = chunk.first().map(|e| e.submission_id.as_str()) else {
        return;
    };
    let event_refs: Vec<OrderSentEventFieldsRef> =
        chunk.iter().map(OrderSentEventFieldsRef::from).collect();
    let payload = match rmp_serde::to_vec(&OrderSentBatchV2Ref {
        session_id,
        submission_id,
        worker_id,
        events: &event_refs,
    }) {
        Ok(p) => p,
        Err(err) => {
            error!(error = %err, "encode orders.sent messagepack; dropping batch");
            metrics::telemetry_dropped_n(n);
            return;
        }
    };
    loop {
        match kafka::enqueue_to_partition(producer, topic, partition, session_id, &payload) {
            Ok(Some(fut)) => {
                inflight.push(Box::pin(async move {
                    // DeliveryFuture resolves to Result<OwnedDeliveryResult, Canceled>;
                    // OwnedDeliveryResult is Result<(partition,offset), (KafkaError, msg)>.
                    match fut.await {
                        Ok(Ok(_)) => (n, Ok(())),
                        Ok(Err((err, _))) => (n, Err(err.to_string())),
                        Err(canceled) => (n, Err(canceled.to_string())),
                    }
                }));
                return;
            }
            Ok(None) => {
                // Producer queue full — drain it and retry (lossless backpressure).
                // poll_producer is a synchronous librdkafka FFI call that can block
                // for the full timeout; run it on the blocking pool so it never
                // stalls a tokio worker thread (KafkaProducer is Arc-backed/Clone).
                let producer = producer.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    kafka::poll_producer(&producer, Duration::from_millis(10));
                })
                .await;
                time::sleep(Duration::from_millis(1)).await;
            }
            Err(err) => {
                error!(error = %err, "enqueue orders.sent; dropping batch");
                metrics::telemetry_dropped_n(n);
                return;
            }
        }
    }
}

/// PartitionBatcher buffers events per destination partition — co-partitioned by
/// `order_band` via `band_partition` when the controller has leased one
/// (exclusive per-session band), falling back to the hash-derived
/// `session_band_partition` for band-unaware specs (`ORDER_BAND_UNSET`), same
/// contract as the ingester and the eBPF acked producer — and emits a chunk the
/// instant a partition reaches MAX_EVENTS_PER_BATCH, so the aggregator can
/// enqueue it without waiting for a timer. `drain_ready` returns the partial
/// remainder on tick/shutdown. No event is ever dropped here — everything
/// pushed is either emitted or drained.
struct PartitionBatcher {
    num_partitions: i32,
    band_width: i32,
    order_band: u32,
    by_part: BTreeMap<i32, Vec<OrderSentEvent>>,
}

impl PartitionBatcher {
    fn new(num_partitions: i32, band_width: i32, order_band: u32) -> Self {
        Self {
            num_partitions,
            band_width,
            order_band,
            by_part: BTreeMap::new(),
        }
    }

    /// Buffer one event; return a ready (partition, chunk) if it just filled one.
    fn push(&mut self, event: OrderSentEvent) -> Option<(i32, Vec<OrderSentEvent>)> {
        let part = if self.order_band != ORDER_BAND_UNSET {
            band_partition(
                self.order_band,
                &event.order_id,
                self.num_partitions,
                self.band_width,
            )
        } else {
            session_band_partition(
                &event.session_id,
                &event.order_id,
                self.num_partitions,
                self.band_width,
            )
        };
        let group = self.by_part.entry(part).or_default();
        group.push(event);
        if group.len() >= MAX_EVENTS_PER_BATCH {
            Some((part, std::mem::take(group)))
        } else {
            None
        }
    }

    /// Drain every buffered (partial) chunk — for the flush ticker and shutdown.
    fn drain_ready(&mut self) -> Vec<(i32, Vec<OrderSentEvent>)> {
        std::mem::take(&mut self.by_part)
            .into_iter()
            .filter(|(_, v)| !v.is_empty())
            .collect()
    }
}

pub(crate) const MAX_EVENTS_PER_BATCH: usize = 1000;

#[cfg(test)]
mod tests {
    use super::*;
    use iicpc_schemas_rust::{
        OrdType, OrderSentBatch, OrderSentBatchV2, PayloadType, Side, DEFAULT_PARTITION_BAND_WIDTH,
        ORDER_BAND_UNSET,
    };

    /// test_event performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn test_event(i: usize) -> OrderSentEvent {
        OrderSentEvent {
            session_id: "01890dd2-71f3-7abc-9def-0123456789ab".to_string(),
            submission_id: "01890dd2-71f3-7abc-9def-0123456789ac".to_string(),
            worker_id: "bot-fleet-7d9f8c6b5-x2k4j".to_string(),
            task_id: 42,
            order_id: format!("01890dd2-71f3-7abc-9def-0123456789ab_42_{i}_O"),
            target_send_ts_ns: 1_770_000_000_000_000_000 + i as u64,
            send_ts_ns: 1_770_000_000_000_000_100 + i as u64,
            recv_done_ts_ns: 1_770_000_000_000_000_900 + i as u64,
            timed_out: false,
            price: 10_000,
            qty: 25,
            side: Side::Buy,
            payload_type: PayloadType::New,
            ord_type: OrdType::Limit,
            orig_order_id: String::new(),
            barrier_epoch_ns: 1_770_000_000_000_000_000,
            smp_id: iicpc_schemas_rust::SMP_ID_NONE,
        }
    }

    // The batcher is the lossless core of the new aggregator: every pushed event is
    // either emitted in a full chunk or held for drain — nothing is dropped — and
    // emitted chunks are exactly MAX (so they stay under the broker message ceiling),
    // each event landing in its co-partition.
    #[test]
    fn partition_batcher_emits_and_drains_without_loss() {
        let n = 24;
        let total = 50_000usize;
        let mut b = PartitionBatcher::new(n, DEFAULT_PARTITION_BAND_WIDTH, ORDER_BAND_UNSET);
        let mut emitted: Vec<(i32, Vec<OrderSentEvent>)> = Vec::new();
        for i in 0..total {
            if let Some(chunk) = b.push(test_event(i)) {
                emitted.push(chunk);
            }
        }
        let drained = b.drain_ready();

        let count: usize = emitted
            .iter()
            .chain(drained.iter())
            .map(|(_, v)| v.len())
            .sum();
        assert_eq!(count, total, "no events lost across emit + drain");

        for (_, chunk) in &emitted {
            assert_eq!(
                chunk.len(),
                MAX_EVENTS_PER_BATCH,
                "push emits only full chunks"
            );
        }
        for (_, chunk) in &drained {
            assert!(!chunk.is_empty() && chunk.len() < MAX_EVENTS_PER_BATCH);
        }
        for (part, chunk) in emitted.iter().chain(drained.iter()) {
            for e in chunk {
                assert_eq!(
                    session_band_partition(
                        &e.session_id,
                        &e.order_id,
                        n,
                        DEFAULT_PARTITION_BAND_WIDTH
                    ),
                    *part,
                    "wrong partition"
                );
            }
        }
    }

    /// shard_events_spreads_a_single_session_across_partitions checks hash spread.
    /// It ensures a busy session still uses many partitions within its band.
    #[test]
    fn partition_batcher_spreads_a_session_across_its_band() {
        let n = 24;
        let band_width = DEFAULT_PARTITION_BAND_WIDTH;
        let mut b = PartitionBatcher::new(n, band_width, ORDER_BAND_UNSET);
        for i in 0..2_000 {
            let _ = b.push(test_event(i));
        }
        let used = b.drain_ready().len();
        assert!(
            used >= 2,
            "a busy session must use several partitions in its band, used {used}"
        );
        assert!(
            used as i32 <= band_width,
            "session must stay within its band, used {used}"
        );
    }

    /// When `order_band` is leased (set), the batcher must use `band_partition`
    /// and confine every event to that band's exclusive partition range,
    /// regardless of session_id (two different sessions sharing a band would be
    /// a controller bug, but the batcher itself must be band-pure).
    #[test]
    fn partition_batcher_uses_leased_band_when_set() {
        let n = 24;
        let band_width = 6i32;
        let band = 2u32; // partitions [12, 18)
        let mut b = PartitionBatcher::new(n, band_width, band);
        for i in 0..2_000 {
            let mut e = test_event(i);
            e.session_id = format!("session-{}", i % 5); // vary session_id
            if let Some((part, chunk)) = b.push(e) {
                for ev in &chunk {
                    let expected = band_partition(band, &ev.order_id, n, band_width);
                    assert_eq!(part, expected);
                }
                assert!(
                    (12..18).contains(&part),
                    "partition {part} outside leased band [12, 18)"
                );
            }
        }
        for (part, chunk) in b.drain_ready() {
            assert!(
                (12..18).contains(&part),
                "partition {part} outside leased band [12, 18)"
            );
            for ev in &chunk {
                assert_eq!(part, band_partition(band, &ev.order_id, n, band_width));
            }
        }
    }

    /// Back-compat regression: `ORDER_BAND_UNSET` must keep using the old
    /// hash-derived `session_band_partition` path, not `band_partition`.
    #[test]
    fn partition_batcher_falls_back_to_hash_derived_band_when_unset() {
        let n = 24;
        let band_width = DEFAULT_PARTITION_BAND_WIDTH;
        let mut b = PartitionBatcher::new(n, band_width, ORDER_BAND_UNSET);
        for i in 0..500 {
            if let Some((part, chunk)) = b.push(test_event(i)) {
                for ev in &chunk {
                    assert_eq!(
                        part,
                        session_band_partition(&ev.session_id, &ev.order_id, n, band_width),
                        "unset order_band must fall back to session_band_partition"
                    );
                }
            }
        }
        for (part, chunk) in b.drain_ready() {
            for ev in &chunk {
                assert_eq!(
                    part,
                    session_band_partition(&ev.session_id, &ev.order_id, n, band_width)
                );
            }
        }
    }

    #[test]
    /// full_chunk_stays_under_broker_message_ceiling performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn full_chunk_stays_under_broker_message_ceiling() {
        let events: Vec<OrderSentEvent> = (0..MAX_EVENTS_PER_BATCH).map(test_event).collect();
        let event_refs: Vec<OrderSentEventFieldsRef> =
            events.iter().map(OrderSentEventFieldsRef::from).collect();
        let payload = rmp_serde::to_vec(&OrderSentBatchV2Ref {
            session_id: "sess",
            submission_id: "sub",
            worker_id: "worker",
            events: &event_refs,
        })
        .expect("encode chunk");

        let decoded: OrderSentBatchV2 = rmp_serde::from_slice(&payload).expect("decode chunk");
        assert_eq!(decoded.events.len(), MAX_EVENTS_PER_BATCH);
        assert!(
            payload.len() < 1_048_576,
            "full chunk is {} bytes, must stay under 1 MiB",
            payload.len()
        );

        // Before/after comparison against the old named-fields, non-hoisted format
        // (audit's claimed 1.84x saving from names alone). The envelope-hoisting +
        // positional encoding here should beat that ratio.
        let old_payload = rmp_serde::to_vec_named(&OrderSentBatch {
            session_id: "sess".to_string(),
            worker_id: "worker".to_string(),
            events: events.clone(),
        })
        .expect("encode legacy chunk");
        let ratio = old_payload.len() as f64 / payload.len() as f64;
        println!(
            "orders.sent 1000-event batch: old(named+per-event envelope)={} bytes, new(positional+hoisted)={} bytes, ratio={:.3}x",
            old_payload.len(),
            payload.len(),
            ratio
        );
        assert!(
            payload.len() < old_payload.len(),
            "new positional+hoisted format must be smaller than the old named format"
        );
    }
}
