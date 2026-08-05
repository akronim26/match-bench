//! This module implements bot worker fix roundtrip behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::{collections::HashSet, env, time::Duration};

use anyhow::{anyhow, Context, Result};
use iicpc_bot_fleet::{config::Config, fix, kafka as kafka_helper, worker};
use iicpc_schemas_rust::{
    BarrierEvent, BotProfile, OrderSentBatch, Protocol, TaskSpec, WorkloadSpec,
};
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    message::Message,
};
use serde::Deserialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::{sleep, timeout},
};

#[derive(Debug, Deserialize)]
/// OrderSentEventOwned stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct OrderSentEventOwned {
    #[allow(dead_code)]
    session_id: String,
    #[allow(dead_code)]
    submission_id: String,
    #[allow(dead_code)]
    worker_id: String,
    #[allow(dead_code)]
    task_id: u32,
    order_id: String,
    target_send_ts_ns: u64,
    send_ts_ns: u64,
    recv_done_ts_ns: u64,
    timed_out: bool,
    #[allow(dead_code)]
    price: u64,
    #[allow(dead_code)]
    qty: u64,
    #[allow(dead_code)]
    side: String,
}

#[derive(Debug, Deserialize)]
/// OrderSentBatchOwned stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct OrderSentBatchOwned {
    #[allow(dead_code)]
    session_id: String,
    #[allow(dead_code)]
    worker_id: String,
    events: Vec<OrderSentEventOwned>,
}

fn fix_bind() -> String {
    env::var("EXAMPLE_BIND").unwrap_or_else(|_| "127.0.0.1:9876".to_string())
}
const SESSION_ID: &str = "roundtrip-session";
const SUBMISSION_ID: &str = "roundtrip-submission";
// Defaults preserve the original correctness-roundtrip behavior; env overrides
// (EXAMPLE_TASKS / EXAMPLE_RPS / EXAMPLE_DURATION_S / EXAMPLE_ECHO_LATENCY_MS /
// EXAMPLE_DROP_EVERY) turn this into a load/profiling harness. EXAMPLE_BENCH=1
// skips telemetry collection + assertions (pair with BOT_DISABLE_TELEMETRY=1).
fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn target_rps() -> u32 {
    env_u64("EXAMPLE_RPS", 20) as u32
}
fn duration_secs() -> u64 {
    env_u64("EXAMPLE_DURATION_S", 3)
}
fn echo_latency_ms() -> u64 {
    env_u64("EXAMPLE_ECHO_LATENCY_MS", 5)
}
fn drop_every() -> u64 {
    env_u64("EXAMPLE_DROP_EVERY", 4)
}
fn task_count() -> u32 {
    env_u64("EXAMPLE_TASKS", 1) as u32
}
fn bench_mode() -> bool {
    env::var("EXAMPLE_BENCH").as_deref() == Ok("1")
}

// EXAMPLE_WORKER_THREADS sizes the tokio runtime (default 4) so the task-count
// sweep can compare 1 process x N threads vs N pinned single-thread processes.
fn main() -> Result<()> {
    let threads = env_u64("EXAMPLE_WORKER_THREADS", 4) as usize;
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?
        .block_on(async_main())
}

async fn async_main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init()
        .ok();

    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + std::process::id() as u64;
    let workload_topic = format!("test.workload.{suffix}");
    let barrier_topic = format!("test.barrier.{suffix}");
    let ready_topic = format!("test.ready.{suffix}");
    let workload_failed_topic = format!("test.workload.failed.{suffix}");
    let orders_sent_topic = format!("test.orders.sent.{suffix}");

    eprintln!("== bot_worker_fix_roundtrip ==");
    eprintln!("brokers           = {brokers}");
    eprintln!("workload_topic    = {workload_topic}");
    eprintln!("barrier_topic     = {barrier_topic}");
    eprintln!("orders_sent_topic = {orders_sent_topic}");

    create_topics(
        &brokers,
        &[
            &workload_topic,
            &barrier_topic,
            &ready_topic,
            &workload_failed_topic,
            &orders_sent_topic,
        ],
    )
    .await?;

    let echo = spawn_fix_echo_server(&fix_bind(), echo_latency_ms(), drop_every()).await?;
    eprintln!("fix echo server up on {}", fix_bind());

    let config = Config {
        worker_id: "roundtrip-worker".to_string(),
        kafka_brokers: brokers.clone(),
        consumer_group: format!("test-group-{suffix}"),
        workload_topic: workload_topic.clone(),
        barrier_topic: barrier_topic.clone(),
        ready_topic: ready_topic.clone(),
        workload_failed_topic: workload_failed_topic.clone(),
        orders_sent_topic: orders_sent_topic.clone(),
        telemetry_flush_interval: Duration::from_millis(5),
        telemetry_batch_size: 64,
        telemetry_channel_capacity: 4096,
        max_bots_per_worker: 1000,
        // from_env, not default: keeps BOT_MAX_INFLIGHT_PER_TASK / BOT_WRITE_BATCH
        // overrides working for load runs (explicit fields above still win).
        ..Config::from_env()
    };
    let worker_handle: JoinHandle<()> = tokio::spawn(async move {
        if let Err(err) = worker::run(config).await {
            eprintln!("worker exited: {err:?}");
        }
    });

    sleep(Duration::from_secs(1)).await;

    let barrier_epoch_ns = 0u64;

    let bind = fix_bind();
    let host = bind.split(':').next().unwrap().to_string();
    let port: u16 = bind.split(':').nth(1).unwrap().parse().unwrap();
    let spec = WorkloadSpec {
        session_id: SESSION_ID.to_string(),
        submission_id: SUBMISSION_ID.to_string(),
        contestant_id: "test".to_string(),
        target_host: host,
        target_port: port,
        protocol: Protocol::Fix,
        targets: Vec::new(),
        worker_index: 0,
        worker_count: 1,
        global_seed: 42,
        fix_version: "FIX.4.2".to_string(),
        connect_timeout_ms: 2000,
        write_timeout_ms: 500,
        barrier_epoch_ns,
        published_at_unix_ns: 0,
        order_band: iicpc_schemas_rust::ORDER_BAND_UNSET,
        tasks: (1..=task_count())
            .map(|i| TaskSpec {
                task_id: i,
                profile: BotProfile::Hft,
                target_rps: target_rps(),
                start_offset_ns: 0,
                duration_ns: Duration::from_secs(duration_secs()).as_nanos() as u64,
                market_pct: 0,
                cancel_pct: 0,
                replace_pct: 0,
                target_idx: 0,
                // 0 means this task emits NO self-match-prevention id — no FIX tag 7928 at
                // all — which keeps the roundtrip frames byte-identical to what this example
                // asserted before the field existed. Note this is NOT the same convention as
                // OrderSentEvent::smp_id, where 0 is a valid participant and the "absent"
                // sentinel is u32::MAX.
                smp_id_count: 0,
            })
            .collect(),
    };

    let producer = kafka_helper::producer(&brokers).context("producer")?;
    kafka_helper::publish_json(
        &producer,
        &workload_topic,
        &format!("{SESSION_ID}:0"),
        &spec,
    )
    .await
    .context("publish workload")?;
    eprintln!("workload published — waiting for ReadySignal before barrier...");

    let _ready_seen = wait_for_ready(&brokers, &ready_topic, SESSION_ID).await?;
    let barrier_epoch_ns = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        + Duration::from_millis(500))
    .as_nanos() as u64;

    let barrier = BarrierEvent {
        session_id: SESSION_ID.to_string(),
        target_epoch_unix_nanos: barrier_epoch_ns,
    };
    kafka_helper::publish_json(&producer, &barrier_topic, SESSION_ID, &barrier)
        .await
        .context("publish barrier")?;
    eprintln!("ready seen, barrier published (epoch_ns={barrier_epoch_ns})");

    if bench_mode() {
        // Load/profiling mode: no telemetry collection or assertions — just let
        // the run complete (worker logs per-second send snapshots), then exit.
        eprintln!(
            "bench mode: {} tasks x {} rps for {}s (pid {})",
            task_count(),
            target_rps(),
            duration_secs(),
            std::process::id()
        );
        sleep(Duration::from_secs(duration_secs() + 5)).await;
        echo.abort();
        worker_handle.abort();
        eprintln!("== BENCH RUN COMPLETE ==");
        return Ok(());
    }

    let collect_deadline = Duration::from_secs(2 + duration_secs() + 7);
    eprintln!(
        "collecting orders.sent for {}s ...",
        collect_deadline.as_secs()
    );
    let events = collect_events(&brokers, &orders_sent_topic, collect_deadline).await?;

    eprintln!("collected {} OrderSentEvent records", events.len());

    worker_handle.abort();
    echo.abort();
    let _ = worker_handle.await;
    let _ = echo.await;

    let total_expected = target_rps() as u64 * duration_secs() * task_count() as u64;
    if events.len() < (total_expected as usize) * 9 / 10 {
        return Err(anyhow!(
            "expected ~{} events, got {} (below 90% threshold)",
            total_expected,
            events.len()
        ));
    }

    let mut order_ids_seen = HashSet::new();
    let (mut matched, mut timed_out_count) = (0u64, 0u64);
    let mut latencies_ns: Vec<u64> = Vec::new();
    for ev in &events {
        if ev.target_send_ts_ns == 0 {
            return Err(anyhow!("event {} has target_send_ts_ns=0", ev.order_id));
        }
        if ev.send_ts_ns < ev.target_send_ts_ns {
            return Err(anyhow!(
                "event {} has send_ts_ns < target_send_ts_ns ({} < {})",
                ev.order_id,
                ev.send_ts_ns,
                ev.target_send_ts_ns
            ));
        }
        if !order_ids_seen.insert(ev.order_id.clone()) {
            return Err(anyhow!("duplicate order_id emitted: {}", ev.order_id));
        }

        if ev.timed_out {
            if ev.recv_done_ts_ns != 0 {
                return Err(anyhow!(
                    "event {} timed_out=true but recv_done_ts_ns={} (expected 0)",
                    ev.order_id,
                    ev.recv_done_ts_ns
                ));
            }
            timed_out_count += 1;
        } else {
            if ev.recv_done_ts_ns <= ev.send_ts_ns {
                return Err(anyhow!(
                    "event {} matched but recv_done_ts_ns ({}) <= send_ts_ns ({})",
                    ev.order_id,
                    ev.recv_done_ts_ns,
                    ev.send_ts_ns
                ));
            }
            latencies_ns.push(ev.recv_done_ts_ns - ev.send_ts_ns);
            matched += 1;
        }
    }
    eprintln!(
        "matched = {matched}, timed_out = {timed_out_count}, unique order_ids = {}",
        order_ids_seen.len()
    );

    let expected_dropped = events.len() as u64 / drop_every().max(1);
    let dropped_tolerance = expected_dropped / 4 + 2;
    let dropped_diff = timed_out_count.abs_diff(expected_dropped);
    if dropped_diff > dropped_tolerance {
        return Err(anyhow!(
            "drop count {timed_out_count} differs from expected ~{expected_dropped} by more than ±{dropped_tolerance}"
        ));
    }
    eprintln!("drop count within tolerance (expected ~{expected_dropped}, ±{dropped_tolerance})");

    if !latencies_ns.is_empty() {
        latencies_ns.sort_unstable();
        let p50 = latencies_ns[latencies_ns.len() / 2];
        let p99 = latencies_ns[latencies_ns.len() * 99 / 100];
        eprintln!(
            "matched latency p50 = {:.2} ms, p99 = {:.2} ms (echo latency target = {} ms)",
            p50 as f64 / 1e6,
            p99 as f64 / 1e6,
            echo_latency_ms()
        );
        let min_acceptable_ns = (echo_latency_ms() / 2) * 1_000_000;
        if p50 < min_acceptable_ns {
            return Err(anyhow!(
                "p50 latency {} ns is well below the echo server's artificial latency of {} ms — something is wrong",
                p50,
                echo_latency_ms()
            ));
        }
    }

    eprintln!("\n== PASS — t0 / t1 / r9 / watchdog all behave as designed ==");
    Ok(())
}

/// wait_for_ready performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn wait_for_ready(brokers: &str, topic: &str, session_id: &str) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("test-ready-collector-{topic}"))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;
    consumer.subscribe(&[topic])?;

    let deadline = Duration::from_secs(15);
    let started = std::time::Instant::now();
    while started.elapsed() < deadline {
        let remaining = deadline.saturating_sub(started.elapsed());
        match timeout(remaining.min(Duration::from_millis(500)), consumer.recv()).await {
            Ok(Ok(msg)) => {
                if let Some(payload) = msg.payload() {
                    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload) {
                        if value.get("session_id").and_then(|v| v.as_str()) == Some(session_id) {
                            return Ok(());
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Err(anyhow!("timed out waiting for ReadySignal"))
}

/// create_topics performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn create_topics(brokers: &str, topics: &[&str]) -> Result<()> {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()?;
    // RF=1 default so the example runs against a local single-broker Kafka;
    // set EXAMPLE_TOPIC_RF=3 (with min.insync.replicas=2) on a real cluster.
    let rf: i32 = env::var("EXAMPLE_TOPIC_RF")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let min_isr = if rf >= 3 { "2" } else { "1" };
    let news: Vec<NewTopic> = topics
        .iter()
        .map(|t| {
            NewTopic::new(t, 3, TopicReplication::Fixed(rf))
                .set("min.insync.replicas", min_isr)
                .set("retention.ms", "86400000")
                .set("max.message.bytes", "1048576")
        })
        .collect();
    let results = admin
        .create_topics(&news, &AdminOptions::new())
        .await
        .context("admin create_topics")?;
    for r in results {
        if let Err((topic, code)) = r {
            return Err(anyhow!("create topic {topic} failed: {code}"));
        }
    }
    // Creation is acked by the controller before metadata propagates to brokers;
    // a consumer subscribing in that window dies on UnknownTopicOrPartition.
    // Poll metadata until every topic is visible.
    let probe: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "topic-probe")
        .create()?;
    for _ in 0..50 {
        let md = probe.fetch_metadata(None, Duration::from_secs(2))?;
        let visible: HashSet<&str> = md.topics().iter().map(|t| t.name()).collect();
        if topics.iter().all(|t| visible.contains(t)) {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow!("topics not visible in metadata after 5s"))
}

/// collect_events performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn collect_events(
    brokers: &str,
    topic: &str,
    deadline: Duration,
) -> Result<Vec<OrderSentEventOwned>> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("test-collector-{}", topic))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;
    consumer.subscribe(&[topic])?;

    let mut events = Vec::new();
    let started = std::time::Instant::now();

    while started.elapsed() < deadline {
        let remaining = deadline.saturating_sub(started.elapsed());
        let recv = timeout(remaining.min(Duration::from_millis(500)), consumer.recv()).await;
        match recv {
            Ok(Ok(msg)) => {
                if let Some(payload) = msg.payload() {
                    let batch: OrderSentBatchOwned =
                        rmp_serde::from_slice(payload).with_context(|| "decode OrderSentBatch")?;
                    events.extend(batch.events);
                }
            }
            Ok(Err(err)) => {
                eprintln!("consumer recv error: {err}");
            }
            Err(_) => {}
        }
    }
    Ok(events)
}

/// spawn_fix_echo_server performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn spawn_fix_echo_server(
    bind: &str,
    latency_ms: u64,
    drop_every: u64,
) -> Result<JoinHandle<()>> {
    let listener = TcpListener::bind(bind).await?;
    let latency = Duration::from_millis(latency_ms);
    let handle = tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let _ = serve_one_fix_connection(stream, latency, drop_every).await;
            });
        }
    });
    Ok(handle)
}

/// serve_one_fix_connection performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn serve_one_fix_connection(
    mut stream: TcpStream,
    latency: Duration,
    drop_every: u64,
) -> Result<()> {
    stream.set_nodelay(true).ok();
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut chunk = [0u8; 4096];
    let mut response_seq: u64 = 1;
    let mut order_count: u64 = 0;

    // EXAMPLE_SINK=drain: pure read-and-discard (no FIX parse, no replies) —
    // matches the deploy-bench drain contestant used for raw send-capacity runs.
    if env::var("EXAMPLE_SINK").as_deref() == Ok("drain") {
        loop {
            if stream.read(&mut chunk).await? == 0 {
                return Ok(());
            }
        }
    }

    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        let (messages, consumed) = fix::parse_messages(&buf);
        for msg in messages {
            if msg.msg_type != b"D" {
                continue;
            }
            order_count += 1;
            if drop_every > 0 && order_count.is_multiple_of(drop_every) {
                continue;
            }
            let Some(clord_id_bytes) = msg.clord_id else {
                continue;
            };
            let clord_id = std::str::from_utf8(clord_id_bytes).unwrap_or("UNKNOWN");
            if !latency.is_zero() {
                sleep(latency).await;
            }
            let frame = fix::execution_report_frame("FIX.4.2", response_seq, clord_id);
            response_seq += 1;
            stream.write_all(&frame).await?;
        }
        if consumed > 0 {
            buf.drain(..consumed);
        }
    }
}

#[allow(dead_code)]
/// _orderbatch_marker performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn _orderbatch_marker() -> OrderSentBatch {
    OrderSentBatch {
        session_id: String::new(),
        worker_id: String::new(),
        events: Vec::new(),
    }
}
