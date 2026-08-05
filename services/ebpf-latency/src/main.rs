//! This module starts the src service.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

mod capture;
mod cpu;
#[cfg(test)]
mod framing_property;
mod matcher;
mod mtu;
mod netns;
mod parse;
mod pipeline;
mod reassembly;

use std::fs::File;
use std::io::ErrorKind;
use std::os::fd::AsRawFd;
use std::{env, path::PathBuf, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use aya::{
    maps::{MapData, PerCpuArray, RingBuf},
    programs::{tc, SchedClassifier, TcAttachType, Xdp, XdpFlags},
    Ebpf,
};
use iicpc_bot_fleet::kafka::{self, KafkaProducer};
use iicpc_logger_rust::loki;
use iicpc_schemas_rust::{
    band_partition, session_band_partition, OrderAckedBatchRef, OrderAckedEventRef,
    DEFAULT_PARTITION_BAND_WIDTH, ORDER_BAND_UNSET, TOPIC_ORDERS_ACKED,
};
use std::collections::BTreeMap;
use tokio::signal::unix::{signal, Signal, SignalKind};
use tokio::time;
use tracing::{info, warn};

mod metrics;

use matcher::MatchedEvent;
use pipeline::Pipeline;

const DEFAULT_RINGBUF_MAP: &str = "EVENTS";
const DROPPED_EVENTS_MAP: &str = "DROPPED_EVENTS";
const TRUNCATED_CAPTURES_MAP: &str = "TRUNCATED_CAPTURES";
const XDP_PACKETS_MAP: &str = "XDP_PACKETS";
const TC_PACKETS_MAP: &str = "TC_PACKETS";
const XDP_LOAD_FAILED_MAP: &str = "XDP_LOAD_FAILED";
const TC_LOAD_FAILED_MAP: &str = "TC_LOAD_FAILED";
const SHORT_PAYLOAD_MAP: &str = "SHORT_PAYLOAD";
const DEFAULT_XDP_INGRESS_PROGRAM: &str = "iicpc_xdp_ingress";
const DEFAULT_TC_EGRESS_PROGRAM: &str = "iicpc_tc_egress";
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(5);
const DEFAULT_BATCH_SIZE: usize = 4096;
const EVICT_INTERVAL: Duration = Duration::from_secs(1);
const MAX_EVENTS_PER_BATCH: usize = 1000;
// 9001 (was 1500): with CAPTURE_CAP at 9029 a full jumbo frame fits one
// capture record, so the MTU clamp that de-optimised the whole grading path
// (~6x packet count, docs/grading-network-regime.md) is no longer needed to
// keep the capture complete. gso_max_size still clamps to this value —
// pre-GSO skbs at the tc hook must stay within one record — and clamp_to only
// ever lowers an interface MTU, so on a 1500-MTU local veth this is a no-op.
// CAPTURE_CLAMP_MTU=1500 remains the rollback lever.
const DEFAULT_CLAMP_MTU: usize = 9001;
/// How long to wait at shutdown for the producer queue to reach the broker. Must stay
/// below the pod's terminationGracePeriodSeconds or the SIGKILL lands mid-drain and the
/// flush achieves nothing.
const PRODUCER_FLUSH_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Debug, Clone)]
/// Config stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct Config {
    kafka_brokers: String,
    topic: String,
    session_id: String,
    contestant_id: String,
    iface: String,
    netns_path: Option<PathBuf>,
    object_path: PathBuf,
    xdp_ingress_program: String,
    tc_egress_program: String,
    ringbuf_map: String,
    flush_interval: Duration,
    batch_size: usize,
    clamp_mtu: usize,
    orders_partitions: i32,
    partition_band_width: i32,
    // order_band is the session's exclusively-leased orders.acked partition
    // band (set by sandbox-orchestrator's ORDER_BAND env var, itself sourced
    // from bot-fleet-controller's per-session lease). ORDER_BAND_UNSET
    // (u32::MAX) means unassigned — fall back to hash-derived
    // session_band_partition for back-compat with pre-leasing rollout.
    order_band: u32,
}

impl Config {
    /// from_env performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn from_env() -> Result<Self> {
        let object_path = env::var("EBPF_OBJECT_PATH")
            .or_else(|_| env::var("IICPC_EBPF_OBJECT"))
            .context("EBPF_OBJECT_PATH must point at the compiled eBPF object")?;

        let netns_path = match optional_path_env("EBPF_NETNS_PATH") {
            Some(p) => Some(p),
            None => match env::var("EBPF_ALGO_POD_UID")
                .ok()
                .filter(|v| !v.trim().is_empty())
            {
                Some(uid) => {
                    let cid = env::var("EBPF_ALGO_CONTAINER_ID").ok();
                    Some(
                        netns::resolve_netns_path(&uid, cid.as_deref())
                            .context("resolve algo pod network namespace from pod UID")?,
                    )
                }
                None => None,
            },
        };

        Ok(Self {
            kafka_brokers: env_or("KAFKA_BROKERS", "localhost:9092"),
            topic: env_or("ORDERS_ACKED_TOPIC", TOPIC_ORDERS_ACKED),
            session_id: required_env("SESSION_ID")?,
            contestant_id: required_env("CONTESTANT_ID")?,
            iface: required_env("EBPF_IFACE")?,
            netns_path,
            object_path: PathBuf::from(object_path),
            xdp_ingress_program: env_or("EBPF_XDP_INGRESS_PROGRAM", DEFAULT_XDP_INGRESS_PROGRAM),
            tc_egress_program: env_or("EBPF_TC_EGRESS_PROGRAM", DEFAULT_TC_EGRESS_PROGRAM),
            ringbuf_map: env_or("EBPF_RINGBUF_MAP", DEFAULT_RINGBUF_MAP),
            flush_interval: env_duration_ms("EBPF_FLUSH_INTERVAL_MS", DEFAULT_FLUSH_INTERVAL),
            batch_size: env_usize("EBPF_BATCH_SIZE", DEFAULT_BATCH_SIZE),
            clamp_mtu: env_usize("CAPTURE_CLAMP_MTU", DEFAULT_CLAMP_MTU),
            orders_partitions: env_usize("ORDERS_PARTITIONS", 24).max(1) as i32,
            partition_band_width: env_usize(
                "BOT_PARTITION_BAND_WIDTH",
                DEFAULT_PARTITION_BAND_WIDTH as usize,
            )
            .max(1) as i32,
            order_band: env::var("ORDER_BAND")
                .ok()
                .and_then(|v| v.trim().parse::<u32>().ok())
                .unwrap_or(ORDER_BAND_UNSET),
        })
    }

    /// validate performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn validate(&self) -> Result<()> {
        if self.batch_size == 0 {
            bail!("EBPF_BATCH_SIZE must be greater than zero");
        }
        if self.flush_interval.is_zero() {
            bail!("EBPF_FLUSH_INTERVAL_MS must be greater than zero");
        }
        if self.clamp_mtu != 0 && !(68..=65_535).contains(&self.clamp_mtu) {
            bail!("CAPTURE_CLAMP_MTU must be 0 (disabled) or between 68 and 65535");
        }
        Ok(())
    }
}

#[tokio::main(flavor = "multi_thread")]
/// main performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn main() -> Result<()> {
    let _loki_guard = loki::init("ebpf-latency");
    metrics::start_server();

    let mut config = Config::from_env()?;
    config.validate()?;

    let producer = kafka::telemetry_producer(&config.kafka_brokers)?;
    if let Some(n) = kafka::topic_partition_count(&producer, &config.topic) {
        config.orders_partitions = n;
    }
    run(config, producer).await
}

/// run performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn run(config: Config, producer: KafkaProducer) -> Result<()> {
    let mut bpf = Ebpf::load_file(&config.object_path)
        .with_context(|| format!("load eBPF object {}", config.object_path.display()))?;

    attach_programs(&mut bpf, &config)?;
    let mut ringbuf = RingBuf::try_from(
        bpf.take_map(&config.ringbuf_map)
            .ok_or_else(|| anyhow!("ringbuf map {} not found", config.ringbuf_map))?,
    )
    .with_context(|| format!("open ringbuf map {}", config.ringbuf_map))?;
    let dropped_events = take_counter(&mut bpf, DROPPED_EVENTS_MAP);
    let truncated_captures = take_counter(&mut bpf, TRUNCATED_CAPTURES_MAP);
    let xdp_packets = take_counter(&mut bpf, XDP_PACKETS_MAP);
    let tc_packets = take_counter(&mut bpf, TC_PACKETS_MAP);
    let xdp_load_failed = take_counter(&mut bpf, XDP_LOAD_FAILED_MAP);
    let tc_load_failed = take_counter(&mut bpf, TC_LOAD_FAILED_MAP);
    let short_payload = take_counter(&mut bpf, SHORT_PAYLOAD_MAP);
    let mut last_xdp_packets = 0u64;
    let mut last_tc_packets = 0u64;
    let mut last_xdp_load_failed = 0u64;
    let mut last_tc_load_failed = 0u64;
    let mut last_short_payload = 0u64;

    info!(
        iface = %config.iface,
        session_id = %config.session_id,
        contestant_id = %config.contestant_id,
        topic = %config.topic,
        netns_path = config.netns_path.as_ref().map(|p| p.display().to_string()).as_deref().unwrap_or("current"),
        "eBPF capture publisher started (userspace parse + match)"
    );

    let mut pipeline = Pipeline::new();
    let mut events: Vec<MatchedEvent> = Vec::with_capacity(config.batch_size);
    let mut ticker = time::interval(config.flush_interval);
    let mut evict_ticker = time::interval(EVICT_INTERVAL);
    let mut last_dropped = 0u64;
    let mut last_truncated = 0u64;
    let mut decoded_total = 0u64;
    let mut last_decoded = 0u64;
    let mut stats_tick = 0u64;
    let mut cpu_sampler = cpu::CpuSampler::new();
    let mut shutdown = ShutdownSignal::new()?;

    loop {
        tokio::select! {
            sig = shutdown.recv() => {
                info!(signal = sig, "shutdown signal received; flushing buffered orders.acked tail");
                // Drain the ring buffer one last time: records captured since the last
                // tick are still in it, and exiting here would discard them.
                decoded_total += drain_ringbuf(&mut ringbuf, &mut pipeline, &producer, &config, &mut events);
                flush(&producer, &config, &mut events);
                // flush() only ENQUEUES. Without the wait below, everything still in the
                // producer queue is discarded when this function returns — which is
                // invisible, because enqueue is fire-and-forget and nothing inspects
                // delivery reports.
                let queued = kafka::in_flight_count(&producer);
                info!(queued, decoded_total, "draining producer queue before exit");
                let left = kafka::flush_producer(&producer, PRODUCER_FLUSH_TIMEOUT).unwrap_or(queued);
                if left > 0 {
                    metrics::undelivered_at_exit(left as u64);
                    warn!(
                        undelivered = left,
                        timeout_s = PRODUCER_FLUSH_TIMEOUT.as_secs(),
                        "producer queue did NOT drain within the flush timeout; these orders.acked records are lost and their orders will look unanswered"
                    );
                } else {
                    info!("producer queue fully delivered before exit");
                }
                // FINAL totals, logged rather than left to a scrape.
                //
                // Every counter here is also exported to Prometheus, but the capture runs as
                // a per-run Job whose pod is reaped on completion, and a ~50s life scraped
                // every 15s loses whatever accumulated after the last scrape. That is not
                // hypothetical: one run's funnel reported 3.01M records emitted against
                // 4.11M events decoded — an impossibility that is purely an artefact of the
                // missing final scrape, and which made several cross-run comparisons
                // unreliable before anyone noticed. These lines are the authoritative
                // numbers for a session; the time series is only for shape.
                // Re-read the kernel counter rather than reusing the last reported value:
                // records dropped between the final tick and shutdown are exactly the ones
                // a scrape would miss, and they are the ones worth knowing about.
                let ringbuf_dropped_final = dropped_events
                    .as_ref()
                    .map(read_counter)
                    .unwrap_or(last_dropped);
                let (thr_periods, thr_usec) = cpu::read_throttling()
                    .map(|t| (t.nr_throttled, t.throttled_usec))
                    .unwrap_or((0, 0));
                info!(
                    decoded_total,
                    ringbuf_dropped = ringbuf_dropped_final,
                    cpu_throttled_periods = thr_periods,
                    cpu_throttled_ms = thr_usec / 1_000,
                    hold_overflows = pipeline.hold_overflows,
                    buffer_overflows = pipeline.buffer_overflows,
                    truncation_resets = pipeline.truncation_resets,
                    resync_skipped_bytes = pipeline.resync_skipped_bytes,
                    stream_gap_bytes = pipeline.stream_gap_bytes,
                    retransmitted_bytes = pipeline.retransmitted_bytes,
                    ws_compressed_frames = pipeline.ws_compressed_frames,
                    framed_requests = pipeline.framed_requests,
                    framed_responses = pipeline.framed_responses,
                    framed_no_clordid = pipeline.framed_no_clordid,
                    unmatched_responses = pipeline.unmatched_responses(),
                    evicted_idle = pipeline.evicted_idle(),
                    "eBPF capture FINAL counters"
                );
                return Ok(());
            }
            _ = ticker.tick() => {
                decoded_total += drain_ringbuf(&mut ringbuf, &mut pipeline, &producer, &config, &mut events);
                flush(&producer, &config, &mut events);
                // Periodic pipeline visibility: decoded = capture records read off the ringbuf
                // (both directions), unmatched_responses = responses seen with no prior request
                // capture to pair against, pending = matched events buffered for the next flush.
                stats_tick += 1;
                // Sample CPU on the same cadence as the stats log rather than every flush:
                // reading /proc/self/task is cheap but not free, and a profiler that
                // measurably slows the thing it measures answers the wrong question.
                if stats_tick % 10 == 0 {
                    let (threads, total) = cpu_sampler.sample();
                    if !threads.is_empty() {
                        metrics::process_cpu(total);
                        for t in &threads {
                            metrics::thread_cpu(&t.name, t.percent);
                        }
                        // Logged as well as exported: the capture Job is per-run and its
                        // pod is reaped on completion, so a scrape can miss the window
                        // entirely. The log survives in the Job's output.
                        let top: Vec<String> = threads
                            .iter()
                            .take(4)
                            .map(|t| format!("{}={:.0}%", t.name, t.percent))
                            .collect();
                        // Throttling alongside CPU, because the two answer different
                        // questions: high CPU says the capture is working hard, throttled
                        // periods say it was STOPPED while work was pending. The container
                        // is Burstable (requests 200m, limits 4), so on a busy node it can
                        // be squeezed toward its request — and a 2-core cap causing exactly
                        // these ringbuf drops is already on record in slot.go.
                        let (thr_periods, thr_usec) = match cpu::read_throttling() {
                            Some(t) => {
                                metrics::cpu_throttling(t.nr_throttled, t.throttled_usec);
                                (t.nr_throttled, t.throttled_usec)
                            }
                            None => (0, 0),
                        };
                        info!(
                            process_cpu_percent = format!("{total:.0}"),
                            top_threads = %top.join(" "),
                            throttled_periods = thr_periods,
                            throttled_ms = thr_usec / 1_000,
                            "eBPF capture CPU"
                        );
                    }
                }
                if stats_tick % 10 == 0 && decoded_total != last_decoded {
                    info!(
                        decoded = decoded_total,
                        unmatched_responses = pipeline.unmatched_responses(),
                        pending = events.len(),
                        "eBPF pipeline stats"
                    );
                    last_decoded = decoded_total;
                }
                {
                    // Attribute every path that can silently discard a FIX message. These
                    // are what remains between a healthy capture and capture_gaps == 0:
                    // an order whose records are lost here is absent from the reference
                    // book, so it damages not just its own grading but every later fill
                    // that trades against the liquidity it should have provided.
                    metrics::stream_loss("hold_overflow", pipeline.hold_overflows, &metrics::LAST_HOLD_OVERFLOW);
                    metrics::stream_loss("buffer_overflow", pipeline.buffer_overflows, &metrics::LAST_BUFFER_OVERFLOW);
                    metrics::stream_loss("truncation_reset", pipeline.truncation_resets, &metrics::LAST_TRUNCATION_RESET);
                    metrics::stream_loss("resync_skipped_bytes", pipeline.resync_skipped_bytes, &metrics::LAST_RESYNC_BYTES);
                    metrics::stream_loss("ws_compressed_frame", pipeline.ws_compressed_frames, &metrics::LAST_WS_COMPRESSED);
                    metrics::stream_loss("retransmitted_bytes", pipeline.retransmitted_bytes, &metrics::LAST_RETRANSMITTED_BYTES);
                    metrics::stream_loss("unmatched_response", pipeline.unmatched_responses(), &metrics::LAST_UNMATCHED);
                    metrics::stream_loss("matcher_evicted_unanswered", pipeline.evicted_idle(), &metrics::LAST_EVICTED_IDLE);
                    metrics::stream_loss("framed_no_clordid", pipeline.framed_no_clordid, &metrics::LAST_NO_CLORDID);
                    metrics::stream_loss("stream_gap_bytes", pipeline.stream_gap_bytes, &metrics::LAST_STREAM_GAP);
                    // Queue depth: the volume that would be lost if the pod were killed
                    // right now. This is the measurement that tells us whether the
                    // producer, not the capture, is where a session's records go missing.
                    metrics::producer_inflight(kafka::in_flight_count(&producer) as i64);
                    // The capture funnel, kernel first. A packet counted here that never
                    // becomes a framed message is a hole in the byte stream, and a hole on
                    // the REQUEST side costs the whole order: with no inflight entry, both
                    // of its responses arrive unmatchable and are discarded.
                    funnel(&xdp_packets, &mut last_xdp_packets, "xdp_packets");
                    funnel(&tc_packets, &mut last_tc_packets, "tc_packets");
                    funnel(&short_payload, &mut last_short_payload, "short_payload_skipped");
                    report_counter(&xdp_load_failed, &mut last_xdp_load_failed, "XDP load_bytes FAILED; request packet dropped with no record", metrics::xdp_load_failed);
                    report_counter(&tc_load_failed, &mut last_tc_load_failed, "tc load_bytes FAILED; response packet dropped with no record", metrics::tc_load_failed);
                    metrics::framed("request", pipeline.framed_requests, &metrics::LAST_FRAMED_REQ);
                    metrics::framed("response", pipeline.framed_responses, &metrics::LAST_FRAMED_RESP);
                    metrics::stream_loss("matcher_evicted_capacity", pipeline.evicted_capacity(), &metrics::LAST_EVICTED_CAPACITY);
                }
                report_counter(&dropped_events, &mut last_dropped, "eBPF ring buffer dropped events", metrics::ringbuf_dropped);
                // Not "check GSO/TSO off": ethtool feature flags govern on-wire framing,
                // while the tc egress hook runs before segmentation and sees the whole
                // pre-segmentation skb regardless. A non-zero total here means orders are
                // losing their responses wholesale — see clamp_gso.
                report_counter(&truncated_captures, &mut last_truncated, "eBPF truncated oversized captures; responses past the capture cap are LOST (check gso_max_size clamp)", metrics::truncated_captures);
            }
            _ = evict_ticker.tick() => {
                pipeline.evict_idle();
            }
        }
    }
}

/// ShutdownSignal stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct ShutdownSignal {
    sigterm: Signal,
    sigint: Signal,
}

impl ShutdownSignal {
    /// new performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn new() -> Result<Self> {
        Ok(Self {
            sigterm: signal(SignalKind::terminate()).context("install SIGTERM handler")?,
            sigint: signal(SignalKind::interrupt()).context("install SIGINT handler")?,
        })
    }

    /// recv performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.sigterm.recv() => "SIGTERM",
            _ = self.sigint.recv() => "SIGINT",
        }
    }
}

/// drain_ringbuf performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
// drain_ringbuf reads ALL currently-available capture records off the kernel ring
// buffer and processes them. It is SYNCHRONOUS and NON-BLOCKING: ringbuf.next()
// returns None when empty, and flush() never blocks (non-blocking enqueue), so the
// drain can never stall on Kafka. This is the fix for the capture freeze — the old
// flush did a blocking send().await that, on a backed-up broker, stalled the drain
// for up to 5s and overflowed the 64 MiB ring buffer.
fn drain_ringbuf(
    ringbuf: &mut RingBuf<MapData>,
    pipeline: &mut Pipeline,
    producer: &KafkaProducer,
    config: &Config,
    events: &mut Vec<MatchedEvent>,
) -> u64 {
    let mut decoded = 0u64;
    while let Some(item) = ringbuf.next() {
        match capture::decode(&item) {
            Ok(cap) => {
                decoded += 1;
                pipeline.process(&cap, events);
            }
            Err(err) => {
                metrics::decode_error();
                warn!(error = %err, "dropping malformed capture record");
                continue;
            }
        }
        if events.len() >= config.batch_size {
            flush(producer, config, events);
        }
    }
    decoded
}

// batch_by_partition drains `events` into per-partition chunks (co-partitioned by
// order_id so each order's acked event lands on the same partition the worker's
// sent event did), each chunk <= MAX_EVENTS_PER_BATCH. Pure + unit-tested.
//
// When `order_band` != ORDER_BAND_UNSET, the session holds an exclusive
// controller-leased band and partitioning goes through `band_partition`
// (deterministic within that band, no cross-session collisions possible).
// Otherwise it falls back to the old hash-derived `session_band_partition`
// path, kept for back-compat during rollout / unleased sessions.
fn batch_by_partition(
    events: &mut Vec<MatchedEvent>,
    session_id: &str,
    orders_partitions: i32,
    partition_band_width: i32,
    order_band: u32,
) -> Vec<(i32, Vec<MatchedEvent>)> {
    let mut by_part: BTreeMap<i32, Vec<MatchedEvent>> = BTreeMap::new();
    for e in events.drain(..) {
        let partition = if order_band != ORDER_BAND_UNSET {
            band_partition(
                order_band,
                &e.order_id,
                orders_partitions,
                partition_band_width,
            )
        } else {
            session_band_partition(
                session_id,
                &e.order_id,
                orders_partitions,
                partition_band_width,
            )
        };
        by_part.entry(partition).or_default().push(e);
    }
    let mut out = Vec::new();
    for (part, mut group) in by_part {
        while !group.is_empty() {
            let take = group.len().min(MAX_EVENTS_PER_BATCH);
            out.push((part, group.drain(..take).collect()));
        }
    }
    out
}

/// flush enqueues all buffered orders.acked events to Kafka WITHOUT blocking the
/// ring-buffer drain. Each per-partition batch is enqueued via the non-blocking
/// send_result path; on QueueFull we poll the producer once and retry, then DROP the
/// batch (orders.acked is loss-tolerant — graceful latency-coverage loss is far
/// better than stalling the drain and overflowing the 64 MiB kernel ring buffer,
/// which is what froze the capture at ~107k/s). Delivery is fire-and-forget:
/// rdkafka's FutureProducer background thread drives delivery of the returned futures.
fn flush(producer: &KafkaProducer, config: &Config, events: &mut Vec<MatchedEvent>) {
    if events.is_empty() {
        return;
    }
    for (part, chunk) in batch_by_partition(
        events,
        &config.session_id,
        config.orders_partitions,
        config.partition_band_width,
        config.order_band,
    ) {
        let event_refs = chunk
            .iter()
            .map(|e| OrderAckedEventRef {
                session_id: &config.session_id,
                contestant_id: &config.contestant_id,
                order_id: &e.order_id,
                src_ip: e.src_ip,
                src_port: e.src_port,
                tcp_seq: e.tcp_seq,
                t3_xdp_ingress_ns: e.t3_ns,
                t7_xdp_egress_ns: e.t7_ns,
                pod_service_time_ns: e.pod_service_time_ns,
                exec_type: &e.exec_type,
                fill_qty: e.fill_qty,
                fill_price: e.fill_price,
                orig_order_id: &e.orig_order_id,
                reordering_detected: e.reordering_detected,
                retransmission_count: e.retransmission_count,
                liquidity_ind: e.liquidity_ind,
            })
            .collect::<Vec<_>>();
        let batch = OrderAckedBatchRef {
            session_id: &config.session_id,
            contestant_id: &config.contestant_id,
            events: &event_refs,
        };
        let payload = match rmp_serde::to_vec_named(&batch) {
            Ok(p) => p,
            Err(err) => {
                warn!(error = %err, "encode orders.acked messagepack; dropping batch");
                metrics::acked_dropped(chunk.len());
                continue;
            }
        };
        // Non-blocking enqueue. One poll+retry on QueueFull, then drop (never block
        // the drain). Fire-and-forget the DeliveryFuture.
        let mut enqueued = false;
        for attempt in 0..2 {
            match kafka::enqueue_to_partition(
                producer,
                &config.topic,
                part,
                &config.contestant_id,
                &payload,
            ) {
                Ok(Some(_fut)) => {
                    enqueued = true;
                    break;
                }
                Ok(None) => {
                    if attempt == 0 {
                        kafka::poll_producer(producer, Duration::from_millis(0));
                    }
                }
                Err(err) => {
                    warn!(error = %err, "enqueue orders.acked failed; dropping batch");
                    break;
                }
            }
        }
        if enqueued {
            for event in &chunk {
                metrics::event_decoded(event.reordering_detected, event.retransmission_count);
            }
            metrics::flushed(chunk.len());
        } else {
            metrics::acked_dropped(chunk.len());
        }
    }
    kafka::poll_producer(producer, Duration::from_millis(0));
}

/// take_counter performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn take_counter(bpf: &mut Ebpf, name: &str) -> Option<PerCpuArray<MapData, u64>> {
    bpf.take_map(name)
        .and_then(|m| PerCpuArray::try_from(m).ok())
}

/// read_counter performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn read_counter(map: &PerCpuArray<MapData, u64>) -> u64 {
    map.get(&0, 0)
        .map(|vals| vals.iter().copied().sum())
        .unwrap_or(0)
}

/// report_counter mirrors one kernel-side PerCpuArray counter into its OWN Prometheus
/// counter and warns on each increase.
///
/// `sink` is a parameter because it used to be hardcoded to metrics::ringbuf_dropped for
/// every caller: truncated captures were published as ring-buffer drops, and since each
/// sink keeps a single "last total seen" cell, two different absolute totals swapping
/// through the same cell corrupted both. That is why a run with 894 truncations reported
/// 759 ring-buffer drops and no truncation metric at all.
fn report_counter(
    map: &Option<PerCpuArray<MapData, u64>>,
    last: &mut u64,
    msg: &str,
    sink: fn(u64),
) {
    if let Some(map) = map {
        let total = read_counter(map);
        if total > *last {
            sink(total);
            warn!(total, delta = total - *last, "{msg}");
        }
        *last = total;
    }
}

/// funnel mirrors a kernel-side absolute counter into the capture-funnel metric without
/// warning on it — these are totals, not losses.
fn funnel(map: &Option<PerCpuArray<MapData, u64>>, last: &mut u64, stage: &'static str) {
    if let Some(map) = map {
        let total = read_counter(map);
        if total > *last {
            metrics::capture_funnel(stage, total - *last);
        }
        *last = total;
    }
}

/// attach_programs performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn attach_programs(bpf: &mut Ebpf, config: &Config) -> Result<()> {
    let mut attach = || -> Result<()> {
        disable_offloads(&config.iface);
        clamp_gso(&config.iface, config.clamp_mtu);
        mtu::clamp_to(&config.iface, config.clamp_mtu);
        attach_xdp_ingress(bpf, &config.xdp_ingress_program, &config.iface)?;
        attach_tc_egress(bpf, &config.tc_egress_program, &config.iface)
    };
    if let Some(netns_path) = &config.netns_path {
        return with_network_namespace(netns_path, attach);
    }
    attach()
}

/// gso_clamp_args builds the `ip link` arguments that bound how large an skb the stack
/// may build for this interface.
///
/// This is the setting that actually governs what the capture sees, and it is NOT the
/// same as `ethtool -K gso off`. The tc egress hook runs in __dev_queue_xmit BEFORE GSO
/// segmentation, so it observes the pre-segmentation skb — up to 64KiB when the engine
/// coalesces responses — no matter what the device's offload feature flags say. The BPF
/// program can only copy COPY_CAP (1536) bytes of it, so every FIX message past the
/// leading ~11 was discarded: measured at 894 truncated packets costing 459,363 of
/// 1,791,744 orders (25.6%) their entire response record, against an engine that had in
/// fact answered 100% of them. gso_max_size/gso_max_segs are enforced where the skb is
/// built, upstream of the hook, so clamping them is what makes truncation impossible
/// rather than merely rarer.
///
/// gso_max_segs is pinned to 1 as well: gso_max_size alone still permits a multi-segment
/// skb whose total payload exceeds the cap.
fn gso_clamp_args(iface: &str, max_size: usize) -> Vec<String> {
    vec![
        "link".into(),
        "set".into(),
        "dev".into(),
        iface.into(),
        "gso_max_size".into(),
        max_size.to_string(),
        "gso_max_segs".into(),
        "1".into(),
    ]
}

/// clamp_gso bounds skb construction on the capture interface so the tc egress hook
/// never sees a payload larger than the BPF program can copy. See gso_clamp_args.
fn clamp_gso(iface: &str, clamp: usize) {
    if clamp == 0 {
        info!(iface, "capture GSO clamp disabled (CAPTURE_CLAMP_MTU=0)");
        return;
    }
    let args = gso_clamp_args(iface, clamp);
    match std::process::Command::new("ip").args(&args).output() {
        Ok(out) if out.status.success() => {
            info!(
                iface,
                gso_max_size = clamp,
                gso_max_segs = 1,
                "clamped GSO sizing on capture interface"
            );
        }
        Ok(out) => {
            warn!(
                iface,
                clamp,
                detail = %String::from_utf8_lossy(&out.stderr).trim(),
                "FAILED to clamp GSO sizing; coalesced responses will be truncated at the capture cap and their orders will look unanswered"
            );
        }
        Err(err) => {
            warn!(
                iface,
                clamp,
                error = %err,
                "failed to run ip link; GSO sizing unclamped, coalesced responses will be truncated"
            );
        }
    }
}

/// disable_offloads performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn disable_offloads(iface: &str) {
    for feature in ["tso", "gso", "gro", "lro"] {
        match std::process::Command::new("ethtool")
            .args(["-K", iface, feature, "off"])
            .output()
        {
            Ok(out) if out.status.success() => {
                info!(iface, feature, "disabled network offload");
            }
            Ok(out) => {
                warn!(
                    iface,
                    feature,
                    detail = %String::from_utf8_lossy(&out.stderr).trim(),
                    "could not disable offload (likely fixed on this device); continuing"
                );
            }
            Err(err) => {
                warn!(iface, feature, error = %err, "failed to run ethtool; continuing without offload disable");
            }
        }
    }
}

/// with_network_namespace performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn with_network_namespace<T>(netns_path: &PathBuf, f: impl FnOnce() -> Result<T>) -> Result<T> {
    let original = File::open("/proc/self/ns/net").context("open current network namespace")?;
    let target = File::open(netns_path)
        .with_context(|| format!("open target network namespace {}", netns_path.display()))?;

    set_network_namespace(target.as_raw_fd())
        .with_context(|| format!("enter target network namespace {}", netns_path.display()))?;

    let result = f();
    let restore =
        set_network_namespace(original.as_raw_fd()).context("restore original network namespace");

    match (result, restore) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(err), Ok(())) => Err(err),
        (Ok(_), Err(err)) => Err(err),
        (Err(err), Err(restore_err)) => Err(err).context(format!(
            "also failed to restore original network namespace: {restore_err}"
        )),
    }
}

/// set_network_namespace performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn set_network_namespace(fd: i32) -> Result<()> {
    let rc = unsafe { libc::setns(fd, libc::CLONE_NEWNET) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error()).context("setns(CLONE_NEWNET)")
    }
}

/// attach_xdp_ingress performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn attach_xdp_ingress(bpf: &mut Ebpf, program_name: &str, iface: &str) -> Result<()> {
    let program: &mut Xdp = bpf
        .program_mut(program_name)
        .ok_or_else(|| anyhow!("XDP program {program_name} not found"))?
        .try_into()
        .with_context(|| format!("{program_name} is not an XDP program"))?;
    program
        .load()
        .with_context(|| format!("load XDP program {program_name}"))?;
    match program.attach(iface, XdpFlags::DRV_MODE) {
        Ok(_) => {
            metrics::attach_ok();
            info!(
                iface,
                program = program_name,
                mode = "drv",
                "attached XDP program"
            );
            Ok(())
        }
        Err(err) => {
            warn!(iface, program = program_name, error = %err, "XDP driver mode attach failed; trying skb mode");
            program
                .attach(iface, XdpFlags::SKB_MODE)
                .with_context(|| format!("attach XDP program {program_name} to {iface}"))?;
            metrics::attach_ok();
            Ok(())
        }
    }
}

/// attach_tc_egress performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn attach_tc_egress(bpf: &mut Ebpf, program_name: &str, iface: &str) -> Result<()> {
    match tc::qdisc_add_clsact(iface) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::AlreadyExists => {}
        Err(err) => return Err(err).with_context(|| format!("add clsact qdisc to {iface}")),
    }
    let program: &mut SchedClassifier = bpf
        .program_mut(program_name)
        .ok_or_else(|| anyhow!("tc egress program {program_name} not found"))?
        .try_into()
        .with_context(|| format!("{program_name} is not a tc classifier program"))?;
    program
        .load()
        .with_context(|| format!("load tc egress program {program_name}"))?;
    program
        .attach(iface, TcAttachType::Egress)
        .with_context(|| format!("attach tc egress program {program_name} to {iface}"))?;
    metrics::attach_ok();
    info!(iface, program = program_name, "attached tc egress program");
    Ok(())
}

/// required_env performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn required_env(key: &str) -> Result<String> {
    env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow!("{key} must be set"))
}

/// env_or performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn env_or(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// optional_path_env performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn optional_path_env(key: &str) -> Option<PathBuf> {
    env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(PathBuf::from)
}

/// env_usize performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn env_usize(key: &str, default: usize) -> usize {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => match value.parse() {
            Ok(parsed) => parsed,
            Err(err) => {
                warn!(key, value, error = %err, default, "invalid numeric environment value; using default");
                default
            }
        },
        _ => default,
    }
}

/// env_duration_ms performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn env_duration_ms(key: &str, default: Duration) -> Duration {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => match value.parse::<u64>() {
            Ok(ms) => Duration::from_millis(ms),
            Err(err) => {
                warn!(key, value, error = %err, default_ms = default.as_millis(), "invalid duration environment value; using default");
                default
            }
        },
        _ => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iicpc_schemas_rust::OrderAckedBatch;
    use std::sync::{Mutex, OnceLock};

    /// gso_clamp_args_bounds_both_size_and_segs pins the clamp that keeps coalesced
    /// responses inside the BPF capture cap. Both knobs are required: gso_max_size alone
    /// still permits a multi-segment skb whose total payload exceeds the cap, and neither
    /// is implied by `ethtool -K gso off` (that governs on-wire framing, while the tc
    /// egress hook runs before segmentation).
    #[test]
    fn gso_clamp_args_bounds_both_size_and_segs() {
        let args = gso_clamp_args("eth0", 1500);
        assert_eq!(
            args,
            vec![
                "link",
                "set",
                "dev",
                "eth0",
                "gso_max_size",
                "1500",
                "gso_max_segs",
                "1"
            ]
        );
    }

    /// gso_clamp_target_tracks_the_capture_cap: the clamp must not exceed what the BPF
    /// program can copy in one record, or truncation returns.
    #[test]
    fn gso_clamp_target_tracks_the_capture_cap() {
        for mtu in [1500usize, 1400, 9000, DEFAULT_CLAMP_MTU] {
            let args = gso_clamp_args("eth0", mtu);
            let size: usize = args[5].parse().unwrap();
            if mtu <= capture::CAPTURE_CAP {
                assert!(
                    size <= capture::CAPTURE_CAP,
                    "clamp {size} exceeds the {}-byte capture cap",
                    capture::CAPTURE_CAP
                );
            }
            assert_eq!(args[7], "1", "gso_max_segs must pin to one segment");
        }
    }

    /// The DEFAULT clamp and the capture cap must stay compatible: a pre-GSO
    /// skb bounded by gso_max_size = DEFAULT_CLAMP_MTU carries at most
    /// MTU - 40 bytes of TCP payload, and every one of those bytes must fit a
    /// single capture record or truncation (25.6% response loss, 2026-07-31)
    /// silently returns. 9029 = 9001 + 28 covers a full jumbo frame plus the
    /// record header, which is what lets EKS keep MTU 9001 (the kernel-side
    /// constant in ebpf.rs and this mirror move in lockstep — capture.rs's
    /// decode rejects captured_len above the mirror, so a stale mirror would
    /// discard every complete jumbo capture as corrupt).
    #[test]
    fn default_clamp_fits_the_capture_cap() {
        assert_eq!(capture::CAPTURE_CAP, 9029);
        assert_eq!(DEFAULT_CLAMP_MTU, 9001);
        assert!(DEFAULT_CLAMP_MTU <= capture::CAPTURE_CAP);
        // Per-CPU scratch value: header + cap must fit the 32KB
        // PCPU_MIN_UNIT_SIZE bound on per-CPU map values.
        assert!(capture::CAPTURE_HEADER_LEN + capture::CAPTURE_CAP <= 32 * 1024);
    }

    fn mk_event(order_id: &str) -> MatchedEvent {
        MatchedEvent {
            order_id: order_id.to_string(),
            src_ip: 0,
            src_port: 0,
            tcp_seq: 0,
            t3_ns: 0,
            t7_ns: 0,
            pod_service_time_ns: 0,
            exec_type: "FILL".to_string(),
            fill_qty: 0,
            fill_price: 0,
            orig_order_id: String::new(),
            reordering_detected: false,
            retransmission_count: 0,
            liquidity_ind: 0,
        }
    }

    // The pure batching the non-blocking flush relies on: every event drained,
    // chunked to <= MAX_EVENTS_PER_BATCH, and co-partitioned by order_id so an
    // order's acked event lands on the same partition its sent event did.
    #[test]
    fn batch_by_partition_drains_all_events_chunked_and_co_partitioned() {
        let n = 8i32;
        let band_width = DEFAULT_PARTITION_BAND_WIDTH;
        let session_id = "sess-test";
        let mut events: Vec<MatchedEvent> =
            (0..2500).map(|i| mk_event(&format!("ord-{i}"))).collect();
        let batches = batch_by_partition(&mut events, session_id, n, band_width, ORDER_BAND_UNSET);

        assert!(events.is_empty(), "events must be fully drained");
        let total: usize = batches.iter().map(|(_, c)| c.len()).sum();
        assert_eq!(total, 2500, "no events lost");
        for (part, chunk) in &batches {
            assert!(*part >= 0 && *part < n, "partition in range");
            assert!(
                chunk.len() <= MAX_EVENTS_PER_BATCH,
                "chunk respects max batch size"
            );
            for e in chunk {
                assert_eq!(
                    session_band_partition(session_id, &e.order_id, n, band_width),
                    *part,
                    "co-partitioned by session_id+order_id"
                );
            }
        }
    }

    /// batch_by_partition_matches_bot_fleet_sender_partition pins the CRITICAL
    /// cross-topic invariant: an order's orders.acked partition (computed here)
    /// must equal its orders.sent partition (computed by bot-fleet's
    /// PartitionBatcher via the same `session_band_partition` function), so a
    /// consumer joining sent+acked by order_id can rely on both landing on the
    /// same partition.
    #[test]
    fn batch_by_partition_matches_bot_fleet_sender_partition() {
        let n = 24i32;
        let band_width = DEFAULT_PARTITION_BAND_WIDTH;
        let session_id = "01890dd2-71f3-7abc-9def-0123456789ab";
        let order_id = "01890dd2-71f3-7abc-9def-0123456789ab_42_7_O";

        let mut events = vec![mk_event(order_id)];
        let batches = batch_by_partition(&mut events, session_id, n, band_width, ORDER_BAND_UNSET);
        let (acked_partition, _) = &batches[0];

        let sent_partition = session_band_partition(session_id, order_id, n, band_width);
        assert_eq!(
            *acked_partition, sent_partition,
            "orders.acked and orders.sent must land on the same partition for the same order"
        );
    }

    #[test]
    fn batch_by_partition_empty_is_empty() {
        let mut events: Vec<MatchedEvent> = Vec::new();
        assert!(batch_by_partition(
            &mut events,
            "sess",
            24,
            DEFAULT_PARTITION_BAND_WIDTH,
            ORDER_BAND_UNSET
        )
        .is_empty());
    }

    // When a band is leased (order_band != ORDER_BAND_UNSET), partitioning must go
    // through band_partition rather than the hash-derived session_band_partition —
    // this is the whole point of exclusive per-session leasing.
    #[test]
    fn batch_by_partition_uses_band_partition_when_band_is_set() {
        let n = 24i32;
        let band_width = DEFAULT_PARTITION_BAND_WIDTH;
        let band = 2u32;
        let session_id = "sess-band-test";
        let mut events: Vec<MatchedEvent> =
            (0..50).map(|i| mk_event(&format!("ord-{i}"))).collect();
        let expected: Vec<(String, i32)> = events
            .iter()
            .map(|e| {
                (
                    e.order_id.clone(),
                    band_partition(band, &e.order_id, n, band_width),
                )
            })
            .collect();

        let batches = batch_by_partition(&mut events, session_id, n, band_width, band);

        assert!(events.is_empty(), "events must be fully drained");
        let base = band as i32 * band_width;
        for (part, chunk) in &batches {
            assert!(
                *part >= base && *part < base + band_width,
                "partition {part} must stay within the leased band [{base}, {})",
                base + band_width
            );
            for e in chunk {
                let want = expected.iter().find(|(id, _)| id == &e.order_id).unwrap().1;
                assert_eq!(
                    *part, want,
                    "must match band_partition, not the hash-derived path"
                );
            }
        }
    }

    // Sanity check that band_partition and session_band_partition actually diverge
    // for at least one order_id in this fixture — otherwise the two branches above
    // wouldn't be distinguishable and the "uses band_partition" assertion would be
    // vacuous.
    #[test]
    fn band_partition_and_session_band_partition_diverge_for_some_order() {
        let n = 24i32;
        let band_width = DEFAULT_PARTITION_BAND_WIDTH;
        let band = 2u32;
        // Any session_id whose hash-derived band != `band` will diverge from
        // band_partition for every order_id (different base offset); try a
        // handful so the test doesn't depend on one session_id's hash landing
        // in the "wrong" (coincidentally matching) band.
        let diverges = (0..20).any(|s| {
            let session_id = format!("sess-band-test-{s}");
            (0..10).any(|i| {
                let order_id = format!("ord-{i}");
                band_partition(band, &order_id, n, band_width)
                    != session_band_partition(&session_id, &order_id, n, band_width)
            })
        });
        assert!(
            diverges,
            "fixture must exercise a real difference between the two paths"
        );
    }

    const ENV_KEYS: &[&str] = &[
        "KAFKA_BROKERS",
        "ORDERS_ACKED_TOPIC",
        "SESSION_ID",
        "CONTESTANT_ID",
        "EBPF_IFACE",
        "EBPF_NETNS_PATH",
        "EBPF_OBJECT_PATH",
        "IICPC_EBPF_OBJECT",
        "EBPF_XDP_INGRESS_PROGRAM",
        "EBPF_TC_EGRESS_PROGRAM",
        "EBPF_RINGBUF_MAP",
        "EBPF_FLUSH_INTERVAL_MS",
        "EBPF_BATCH_SIZE",
        "CAPTURE_CLAMP_MTU",
        "ORDER_BAND",
    ];

    /// env_lock performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    /// clear_test_env performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn clear_test_env() {
        for key in ENV_KEYS {
            unsafe {
                env::remove_var(key);
            }
        }
    }

    /// set_env performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn set_env(key: &str, value: &str) {
        unsafe {
            env::set_var(key, value);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    /// kafka_integration_flush_publishes_real_orders_acked_batch performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn kafka_integration_flush_publishes_real_orders_acked_batch() {
        let Ok(brokers) = env::var("KAFKA_BROKERS") else {
            eprintln!("skipping real Kafka integration test: KAFKA_BROKERS is not set");
            return;
        };
        if brokers.trim().is_empty() {
            eprintln!("skipping real Kafka integration test: KAFKA_BROKERS is empty");
            return;
        }

        kafka::ensure_topics(&brokers, &[TOPIC_ORDERS_ACKED])
            .await
            .expect("ensure orders.acked topic");
        let suffix = integration_suffix();
        let session_id = format!("itest-ebpf-session-{suffix}");
        let contestant_id = format!("itest-contestant-{suffix}");
        let consumer_group = format!("itest-ebpf-acked-{suffix}");
        let consumer = kafka::consumer(
            &brokers,
            &consumer_group,
            &[TOPIC_ORDERS_ACKED],
            std::time::Duration::from_secs(300),
        )
        .expect("create orders.acked consumer");
        let producer = kafka::telemetry_producer(&brokers).expect("create telemetry producer");
        let config = Config {
            kafka_brokers: brokers,
            topic: TOPIC_ORDERS_ACKED.to_string(),
            session_id: session_id.clone(),
            contestant_id: contestant_id.clone(),
            iface: "eth0".to_string(),
            netns_path: None,
            object_path: PathBuf::from("/tmp/unused-ebpf-object.o"),
            xdp_ingress_program: DEFAULT_XDP_INGRESS_PROGRAM.to_string(),
            tc_egress_program: DEFAULT_TC_EGRESS_PROGRAM.to_string(),
            ringbuf_map: DEFAULT_RINGBUF_MAP.to_string(),
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            batch_size: DEFAULT_BATCH_SIZE,
            clamp_mtu: DEFAULT_CLAMP_MTU,
            orders_partitions: 24,
            partition_band_width: DEFAULT_PARTITION_BAND_WIDTH,
            order_band: ORDER_BAND_UNSET,
        };
        let mut events = vec![MatchedEvent {
            order_id: format!("order-{suffix}"),
            src_ip: 0x7f000001,
            src_port: 9898,
            tcp_seq: 42,
            t3_ns: 100,
            t7_ns: 175,
            pod_service_time_ns: 75,
            exec_type: "FILL".to_string(),
            fill_qty: 10,
            fill_price: 123_000_000_000,
            orig_order_id: format!("order-{suffix}"),
            reordering_detected: true,
            retransmission_count: 1,
            liquidity_ind: 2,
        }];

        flush(&producer, &config, &mut events);
        assert!(events.is_empty(), "flush should clear events after enqueue");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting for orders.acked batch"
            );
            let msg = tokio::time::timeout(remaining, kafka::recv_message(&consumer))
                .await
                .expect("receive timeout")
                .expect("receive Kafka message");
            let Some(payload) = msg.payload.as_deref() else {
                kafka::commit_message(&consumer, &msg).expect("commit tombstone");
                continue;
            };
            let batch = match rmp_serde::from_slice::<OrderAckedBatch>(payload) {
                Ok(batch) => batch,
                Err(_) => {
                    kafka::commit_message(&consumer, &msg).expect("commit unrelated message");
                    continue;
                }
            };
            kafka::commit_message(&consumer, &msg).expect("commit orders.acked message");
            if batch.session_id != session_id {
                continue;
            }
            assert_eq!(batch.contestant_id, contestant_id);
            assert_eq!(batch.events.len(), 1);
            assert_eq!(batch.events[0].pod_service_time_ns, 75);
            assert!(batch.events[0].reordering_detected);
            break;
        }
    }

    /// integration_suffix performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn integration_suffix() -> String {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_string()
    }

    #[test]
    /// config_from_env_uses_required_values_and_defaults performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn config_from_env_uses_required_values_and_defaults() {
        let _guard = env_lock();
        clear_test_env();
        set_env("SESSION_ID", "session-a");
        set_env("CONTESTANT_ID", "contestant-a");
        set_env("EBPF_IFACE", "eth0");
        set_env("EBPF_OBJECT_PATH", "/opt/iicpc/ebpf/iicpc_latency.bpf.o");

        let config = Config::from_env().unwrap();
        assert_eq!(config.kafka_brokers, "localhost:9092");
        assert_eq!(config.topic, TOPIC_ORDERS_ACKED);
        assert_eq!(config.session_id, "session-a");
        assert_eq!(config.contestant_id, "contestant-a");
        assert_eq!(config.iface, "eth0");
        assert_eq!(config.netns_path, None);
        assert_eq!(config.flush_interval, DEFAULT_FLUSH_INTERVAL);
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(config.clamp_mtu, DEFAULT_CLAMP_MTU);
        assert_eq!(
            config.order_band, ORDER_BAND_UNSET,
            "ORDER_BAND unset must default to the sentinel, not 0"
        );
        config.validate().unwrap();
        clear_test_env();
    }

    #[test]
    /// config_from_env_parses_order_band performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn config_from_env_parses_order_band() {
        let _guard = env_lock();
        let cases: &[(Option<&str>, u32)] = &[
            (None, ORDER_BAND_UNSET),
            (Some(""), ORDER_BAND_UNSET),
            (Some("not-a-number"), ORDER_BAND_UNSET),
            (Some("0"), 0),
            (Some("5"), 5),
        ];
        for &(value, want) in cases {
            clear_test_env();
            set_env("SESSION_ID", "session-a");
            set_env("CONTESTANT_ID", "contestant-a");
            set_env("EBPF_IFACE", "eth0");
            set_env("EBPF_OBJECT_PATH", "/tmp/latency.o");
            if let Some(v) = value {
                set_env("ORDER_BAND", v);
            }
            let config = Config::from_env().unwrap();
            assert_eq!(config.order_band, want, "ORDER_BAND={value:?}");
        }
        clear_test_env();
    }

    #[test]
    /// config_from_env_parses_capture_clamp_mtu performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn config_from_env_parses_capture_clamp_mtu() {
        let _guard = env_lock();
        let cases: &[(&str, usize)] = &[
            ("9001", 9001),
            ("0", 0),
            ("", DEFAULT_CLAMP_MTU),
            ("not-a-number", DEFAULT_CLAMP_MTU),
        ];
        for &(value, want) in cases {
            clear_test_env();
            set_env("SESSION_ID", "session-a");
            set_env("CONTESTANT_ID", "contestant-a");
            set_env("EBPF_IFACE", "eth0");
            set_env("EBPF_OBJECT_PATH", "/tmp/latency.o");
            set_env("CAPTURE_CLAMP_MTU", value);
            let config = Config::from_env().unwrap();
            assert_eq!(config.clamp_mtu, want, "CAPTURE_CLAMP_MTU={value:?}");
        }
        clear_test_env();
    }

    #[test]
    /// config_validate_rejects_out_of_range_clamp_mtu performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn config_validate_rejects_out_of_range_clamp_mtu() {
        let _guard = env_lock();
        let cases: &[(&str, bool)] = &[
            ("0", true),
            ("68", true),
            ("1500", true),
            ("65535", true),
            ("67", false),
            ("65536", false),
        ];
        for &(value, valid) in cases {
            clear_test_env();
            set_env("SESSION_ID", "session-a");
            set_env("CONTESTANT_ID", "contestant-a");
            set_env("EBPF_IFACE", "eth0");
            set_env("EBPF_OBJECT_PATH", "/tmp/latency.o");
            set_env("CAPTURE_CLAMP_MTU", value);
            let config = Config::from_env().unwrap();
            assert_eq!(
                config.validate().is_ok(),
                valid,
                "CAPTURE_CLAMP_MTU={value}"
            );
        }
        clear_test_env();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    /// shutdown_signal_resolves_on_sigterm_and_sigint performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn shutdown_signal_resolves_on_sigterm_and_sigint() {
        let mut shutdown = ShutdownSignal::new().expect("install signal handlers");

        unsafe { libc::raise(libc::SIGTERM) };
        let name = time::timeout(Duration::from_secs(5), shutdown.recv())
            .await
            .expect("SIGTERM not observed within 5s");
        assert_eq!(name, "SIGTERM");

        unsafe { libc::raise(libc::SIGINT) };
        let name = time::timeout(Duration::from_secs(5), shutdown.recv())
            .await
            .expect("SIGINT not observed within 5s");
        assert_eq!(name, "SIGINT");
    }

    #[test]
    /// config_from_env_rejects_missing_required_values performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn config_from_env_rejects_missing_required_values() {
        let _guard = env_lock();
        clear_test_env();
        set_env("SESSION_ID", "session-a");
        set_env("CONTESTANT_ID", "contestant-a");
        set_env("EBPF_OBJECT_PATH", "/tmp/latency.o");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("EBPF_IFACE must be set"));
        clear_test_env();
    }
}
