//! This module implements metrics behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::{
    env,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicU64, Ordering},
        LazyLock,
    },
    thread,
    time::Duration,
};

use prometheus_client::{
    encoding::text::encode,
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};

type ResultFamily = Family<[(&'static str, &'static str); 1], Counter>;
/// LossFamily labels each byte-stream/matcher loss by the reason it happened, so a
/// non-zero total can be attributed instead of merely noticed.
type LossFamily = Family<[(&'static str, &'static str); 1], Counter>;
/// Thread names are discovered at runtime, so this family carries an owned label rather
/// than the &'static str the others use.
type ThreadCpuFamily = Family<[(&'static str, String); 1], Gauge>;

/// Metrics stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct Metrics {
    registry: Registry,
    events_decoded: Counter,
    events_decode_errors: Counter,
    ringbuf_dropped: Counter,
    truncated_captures: Counter,
    stream_loss: LossFamily,
    producer_inflight: Gauge,
    /// Per-thread CPU as a percentage of ONE core, keyed by the kernel's comm. A single
    /// thread pinned near 100 identifies the bottleneck stage directly; the process total
    /// cannot, because everything after the ring buffer shares one tokio task.
    thread_cpu: ThreadCpuFamily,
    process_cpu: Gauge,
    cpu_throttled_periods: Gauge,
    cpu_throttled_usec: Gauge,
    undelivered_at_exit: Counter,
    capture_funnel: LossFamily,
    framed: LossFamily,
    flushes: Counter,
    events_flushed: Counter,
    acked_dropped: Counter,
    reordering: Counter,
    retransmissions: Counter,
    attach: ResultFamily,
}

static LAST_RINGBUF_DROPPED: AtomicU64 = AtomicU64::new(0);
static LAST_TRUNCATED: AtomicU64 = AtomicU64::new(0);

static METRICS: LazyLock<Metrics> = LazyLock::new(|| {
    let mut registry = Registry::default();

    let events_decoded = Counter::default();
    let events_decode_errors = Counter::default();
    let ringbuf_dropped = Counter::default();
    let truncated_captures = Counter::default();
    let stream_loss = LossFamily::default();
    let producer_inflight = Gauge::default();
    let thread_cpu = ThreadCpuFamily::default();
    let process_cpu = Gauge::default();
    let cpu_throttled_periods = Gauge::default();
    let cpu_throttled_usec = Gauge::default();
    let undelivered_at_exit = Counter::default();
    let capture_funnel = LossFamily::default();
    let framed = LossFamily::default();
    let flushes = Counter::default();
    let events_flushed = Counter::default();
    let acked_dropped = Counter::default();
    let reordering = Counter::default();
    let retransmissions = Counter::default();
    let attach = ResultFamily::default();

    registry.register(
        "iicpc_ebpf_events_decoded",
        "eBPF latency events decoded.",
        events_decoded.clone(),
    );
    registry.register(
        "iicpc_ebpf_events_decode_errors",
        "eBPF latency event decode errors.",
        events_decode_errors.clone(),
    );
    registry.register(
        "iicpc_ebpf_ringbuf_dropped",
        "eBPF ring-buffer events dropped.",
        ringbuf_dropped.clone(),
    );
    registry.register(
        "iicpc_ebpf_truncated_captures",
        "Packets whose payload exceeded the BPF capture cap. Every FIX message past the cap in such a packet is LOST, so its order looks unanswered downstream; a non-zero rate invalidates correctness scoring and biases latency toward uncoalesced responses.",
        truncated_captures.clone(),
    );
    registry.register(
        "iicpc_ebpf_stream_loss",
        "Byte-stream and matcher losses by reason. Every one of these discards data that may contain whole FIX messages; a lost REQUEST leaves its responses unmatchable, which is how an order ends up with no orders.acked record at all. Target is zero — a non-zero rate silently degrades correctness scoring, because the missing order is absent from the reference book that every later fill is graded against.",
        stream_loss.clone(),
    );
    registry.register(
        "iicpc_ebpf_capture_funnel",
        "Packets accepted by each capture hook, by stage. Compare against iicpc_ebpf_framed: a packet counted here that never becomes a framed FIX message is a hole in the byte stream.",
        capture_funnel.clone(),
    );
    registry.register(
        "iicpc_ebpf_framed",
        "FIX messages successfully framed out of the reassembled byte stream, by direction. A request-side shortfall against orders sent is what leaves an order with no records at all.",
        framed.clone(),
    );
    registry.register(
        "iicpc_ebpf_producer_inflight",
        "orders.acked messages enqueued in the Kafka producer but not yet acknowledged by the broker. Enqueue is fire-and-forget, so this is the volume that would be DISCARDED if the process exited now — a rising value means the producer is drained slower than it is fed.",
        producer_inflight.clone(),
    );
    registry.register(
        "iicpc_ebpf_thread_cpu_percent",
        "Per-thread CPU as a percentage of ONE core, by thread name. Everything after the ring buffer (decode, reassembly, framing, matching) runs in a single tokio task, so a value near 100 on one thread means that stage is saturated and is what drops ring-buffer records — a distinction the process total cannot make.",
        thread_cpu.clone(),
    );
    registry.register(
        "iicpc_ebpf_process_cpu_percent",
        "Total CPU across all threads, as a percentage of one core (200 = two cores busy).",
        process_cpu.clone(),
    );
    registry.register(
        "iicpc_ebpf_cpu_throttled_periods",
        "CFS periods in which this container was throttled (cgroup cpu.stat nr_throttled). Non-zero means the capture was held BELOW its CPU limit while work was pending — a different problem from needing more CPU, and one fixed by a limit/request change rather than by code. The container is Burstable (requests 200m, limits 4), so node pressure can squeeze it.",
        cpu_throttled_periods.clone(),
    );
    registry.register(
        "iicpc_ebpf_cpu_throttled_usec",
        "Total microseconds this container spent throttled by CFS (cgroup cpu.stat).",
        cpu_throttled_usec.clone(),
    );
    registry.register(
        "iicpc_ebpf_undelivered_at_exit",
        "orders.acked messages still queued after the shutdown flush timed out. These are lost, and each one can cost an order its entire response record.",
        undelivered_at_exit.clone(),
    );
    registry.register("iicpc_ebpf_flushes", "eBPF flushes.", flushes.clone());
    registry.register(
        "iicpc_ebpf_events_flushed",
        "eBPF events flushed to downstream sinks.",
        events_flushed.clone(),
    );
    registry.register(
        "iicpc_ebpf_acked_dropped",
        "orders.acked events dropped because the Kafka producer queue was full (non-blocking drain; graceful degradation).",
        acked_dropped.clone(),
    );
    registry.register(
        "iicpc_ebpf_reordering_detected",
        "eBPF observations where packet reordering was detected.",
        reordering.clone(),
    );
    registry.register(
        "iicpc_ebpf_retransmissions",
        "TCP retransmissions observed by the eBPF latency pipeline.",
        retransmissions.clone(),
    );
    registry.register(
        "iicpc_ebpf_attach",
        "eBPF attach attempts by result.",
        attach.clone(),
    );

    Metrics {
        registry,
        events_decoded,
        events_decode_errors,
        ringbuf_dropped,
        truncated_captures,
        stream_loss,
        producer_inflight,
        thread_cpu,
        process_cpu,
        cpu_throttled_periods,
        cpu_throttled_usec,
        undelivered_at_exit,
        capture_funnel,
        framed,
        flushes,
        events_flushed,
        acked_dropped,
        reordering,
        retransmissions,
        attach,
    }
});

/// start_server performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn start_server() {
    let port = env::var("METRICS_PORT").unwrap_or_else(|_| "9090".to_string());
    let addr = format!("0.0.0.0:{port}");

    thread::spawn(move || {
        let Ok(listener) = TcpListener::bind(&addr) else {
            eprintln!("metrics server bind failed on {addr}");
            return;
        };
        LazyLock::force(&METRICS);
        for stream in listener.incoming() {
            let Ok(stream) = stream else {
                continue;
            };
            handle_client(stream);
        }
    });
}

/// event_decoded performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn event_decoded(reordering: bool, retransmission_count: u32) {
    METRICS.events_decoded.inc();
    if reordering {
        METRICS.reordering.inc();
    }
    if retransmission_count > 0 {
        METRICS
            .retransmissions
            .inc_by(u64::from(retransmission_count));
    }
}

/// decode_error performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn decode_error() {
    METRICS.events_decode_errors.inc();
}

/// ringbuf_dropped performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn ringbuf_dropped(total: u64) {
    let previous = LAST_RINGBUF_DROPPED.swap(total, Ordering::Relaxed);
    if total > previous {
        METRICS.ringbuf_dropped.inc_by(total - previous);
    }
}

/// truncated_captures mirrors the kernel-side truncation counter (an absolute total) into
/// a monotonic Prometheus counter.
///
/// This was a log line only, which is why a defect that cost 25.6% of orders their entire
/// response record went unnoticed: nothing scraped it, nothing alerted on it, and the
/// downstream validator saw the result as contestants failing to answer.
pub fn truncated_captures(total: u64) {
    let previous = LAST_TRUNCATED.swap(total, Ordering::Relaxed);
    if total > previous {
        METRICS.truncated_captures.inc_by(total - previous);
    }
}

/// stream_loss mirrors the pipeline's absolute loss totals into labelled monotonic
/// counters. Each reason discards bytes that may hold whole FIX messages.
pub fn stream_loss(reason: &'static str, total: u64, last: &AtomicU64) {
    let previous = last.swap(total, Ordering::Relaxed);
    if total > previous {
        METRICS
            .stream_loss
            .get_or_create(&[("reason", reason)])
            .inc_by(total - previous);
    }
}

pub static LAST_HOLD_OVERFLOW: AtomicU64 = AtomicU64::new(0);
pub static LAST_BUFFER_OVERFLOW: AtomicU64 = AtomicU64::new(0);
pub static LAST_TRUNCATION_RESET: AtomicU64 = AtomicU64::new(0);
pub static LAST_RESYNC_BYTES: AtomicU64 = AtomicU64::new(0);
pub static LAST_WS_COMPRESSED: AtomicU64 = AtomicU64::new(0);
pub static LAST_RETRANSMITTED_BYTES: AtomicU64 = AtomicU64::new(0);
pub static LAST_UNMATCHED: AtomicU64 = AtomicU64::new(0);
pub static LAST_EVICTED_IDLE: AtomicU64 = AtomicU64::new(0);
pub static LAST_EVICTED_CAPACITY: AtomicU64 = AtomicU64::new(0);
pub static LAST_NO_CLORDID: AtomicU64 = AtomicU64::new(0);
pub static LAST_STREAM_GAP: AtomicU64 = AtomicU64::new(0);
pub static LAST_FRAMED_REQ: AtomicU64 = AtomicU64::new(0);
pub static LAST_FRAMED_RESP: AtomicU64 = AtomicU64::new(0);

/// capture_funnel records packets accepted at a capture stage.
pub fn capture_funnel(stage: &'static str, delta: u64) {
    METRICS
        .capture_funnel
        .get_or_create(&[("stage", stage)])
        .inc_by(delta);
}

/// framed records FIX messages framed out of the byte stream, by direction.
pub fn framed(direction: &'static str, total: u64, last: &AtomicU64) {
    let previous = last.swap(total, Ordering::Relaxed);
    if total > previous {
        METRICS
            .framed
            .get_or_create(&[("direction", direction)])
            .inc_by(total - previous);
    }
}

/// xdp_load_failed / tc_load_failed: the packet reached the hook and the copy failed, so
/// it is dropped with no record emitted. Previously a bare `return` with no counter.
pub fn xdp_load_failed(total: u64) {
    stream_loss("xdp_load_bytes_failed", total, &LAST_XDP_LOAD_FAILED);
}
pub fn tc_load_failed(total: u64) {
    stream_loss("tc_load_bytes_failed", total, &LAST_TC_LOAD_FAILED);
}
static LAST_XDP_LOAD_FAILED: AtomicU64 = AtomicU64::new(0);
static LAST_TC_LOAD_FAILED: AtomicU64 = AtomicU64::new(0);

/// thread_cpu publishes one thread's utilisation as a percentage of one core.
pub fn thread_cpu(name: &str, percent: f64) {
    METRICS
        .thread_cpu
        .get_or_create(&[("thread", name.to_string())])
        .set(percent.round() as i64);
}

/// process_cpu publishes total utilisation across all threads (200 = two full cores).
pub fn process_cpu(percent: f64) {
    METRICS.process_cpu.set(percent.round() as i64);
}

/// cpu_throttling publishes cgroup CFS throttling totals.
pub fn cpu_throttling(periods: u64, usec: u64) {
    METRICS.cpu_throttled_periods.set(periods as i64);
    METRICS.cpu_throttled_usec.set(usec as i64);
}

/// producer_inflight records the current producer queue depth.
pub fn producer_inflight(n: i64) {
    METRICS.producer_inflight.set(n);
}

/// undelivered_at_exit records messages abandoned in the producer queue at shutdown.
pub fn undelivered_at_exit(n: u64) {
    METRICS.undelivered_at_exit.inc_by(n);
}

/// flushed performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn flushed(events: usize) {
    METRICS.flushes.inc();
    METRICS.events_flushed.inc_by(events as u64);
}

/// acked_dropped records orders.acked events dropped because the Kafka producer
/// queue was full (non-blocking drain — graceful degradation under broker pressure).
pub fn acked_dropped(events: usize) {
    METRICS.acked_dropped.inc_by(events as u64);
}

/// attach_ok performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn attach_ok() {
    METRICS.attach.get_or_create(&[("result", "ok")]).inc();
}

/// render performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn render() -> String {
    let mut out = String::new();
    let _ = encode(&mut out, &METRICS.registry);
    out
}

/// handle_client performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn handle_client(mut stream: TcpStream) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut buf = [0_u8; 512];
    let Ok(n) = stream.read(&mut buf) else {
        return;
    };
    let request = String::from_utf8_lossy(&buf[..n]);
    let first_line = request.lines().next().unwrap_or("");
    if !first_line.starts_with("GET /metrics ") {
        let body = "not found\n";
        let response = format!(
            "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes());
        return;
    }

    let body = render();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
}
