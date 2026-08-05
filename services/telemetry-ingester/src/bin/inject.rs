//! inject — synthetic telemetry load generator for benchmarking the MEASUREMENT
//! pipeline (Kafka -> telemetry-ingester -> rollup -> TimescaleDB) DECOUPLED from
//! the contestant. Real runs cap delivered responses at the contestant's CPU
//! (~196k/pod), so the pipeline's own ceiling can't be reached through it. This
//! tool produces valid msgpack OrderSentBatch + matching OrderAckedBatch straight
//! to orders.sent / orders.acked, CO-PARTITIONED by order_id (partition_for) and
//! with realistic fields (barrier epoch for wave bucketing, t3/t7 -> service time)
//! so the ingester actually JOINS + finalizes them.
//!
//! Bypasses worker, contestant, and the eBPF capture entirely. Measure the ingester
//! (consumed/s, records_finalized/s, lag, TSDB writes) to find the downstream ceiling
//! and its horizontal-scaling curve (ingester replicas <= partition count).
//!
//! Env: KAFKA_BROKERS, RATE (target orders/s; 0 = max blast), DURATION_S, THREADS,
//!      PARTITIONS (orders topic partition count), BATCH (orders/iteration/thread),
//!      SESSION_ID.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use iicpc_schemas_rust::{
    session_band_partition, OrdType, OrderAckedBatch, OrderAckedEvent, OrderSentEvent, PayloadType,
    Side, DEFAULT_PARTITION_BAND_WIDTH, SMP_ID_NONE, TOPIC_ORDERS_ACKED, TOPIC_ORDERS_SENT,
};

fn env_or<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let brokers = std::env::var("KAFKA_BROKERS")
        .unwrap_or_else(|_| "kafka.data.svc.cluster.local:9092".into());
    let rate: u64 = env_or("RATE", 500_000); // target orders/s across all threads (0 = blast)
    let dur: u64 = env_or("DURATION_S", 60);
    let threads: u64 = env_or("THREADS", 8);
    let parts: i32 = env_or("PARTITIONS", 24);
    let batch: u64 = env_or("BATCH", 2000);
    let session = std::env::var("SESSION_ID").unwrap_or_else(|_| format!("inject-{:x}", now_ns()));
    let barrier = now_ns();
    eprintln!(
        "inject -> {brokers}  rate={rate}/s dur={dur}s threads={threads} parts={parts} batch={batch} session={session}"
    );

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("linger.ms", "5")
        .set("batch.size", "1048576")
        .set("compression.type", "lz4")
        .set("acks", "1")
        .set("queue.buffering.max.messages", "4000000")
        .set("queue.buffering.max.kbytes", "2097152")
        .create()?;
    let producer = Arc::new(producer);
    let total = Arc::new(AtomicU64::new(0));
    let per_thread_rate = if rate == 0 { 0 } else { rate / threads.max(1) };

    let mut handles = Vec::new();
    for tid in 0..threads {
        let producer = producer.clone();
        let total = total.clone();
        let session = session.clone();
        handles.push(tokio::spawn(async move {
            let start = Instant::now();
            let mut seq = tid;
            let mut produced: u64 = 0;
            while start.elapsed().as_secs() < dur {
                let ts = now_ns();
                let mut by_part: HashMap<i32, (Vec<OrderSentEvent>, Vec<OrderAckedEvent>)> =
                    HashMap::new();
                for _ in 0..batch {
                    seq += threads;
                    let oid = format!("{session}-{tid}-{seq}-O");
                    let p =
                        session_band_partition(&session, &oid, parts, DEFAULT_PARTITION_BAND_WIDTH);
                    let svc = 80_000u64; // 80us synthetic service time
                    let sent = OrderSentEvent {
                        session_id: session.clone(),
                        submission_id: "inject".into(),
                        worker_id: "inject".into(),
                        task_id: tid as u32,
                        order_id: oid.clone(),
                        target_send_ts_ns: ts,
                        send_ts_ns: ts,
                        recv_done_ts_ns: ts + 200_000,
                        timed_out: false,
                        price: 100,
                        qty: 10,
                        side: Side::Buy,
                        payload_type: PayloadType::New,
                        ord_type: OrdType::Limit,
                        orig_order_id: String::new(),
                        barrier_epoch_ns: barrier,
                        // Explicitly SMP_ID_NONE, never a bare 0: zero is a VALID participant
                        // id as well as Rust's default, so injected load that omits this reads
                        // as "participant 0" and self-crosses against every order carrying it.
                        smp_id: SMP_ID_NONE,
                    };
                    let acked = OrderAckedEvent {
                        session_id: session.clone(),
                        contestant_id: "inject".into(),
                        order_id: oid,
                        src_ip: 1,
                        src_port: 5,
                        tcp_seq: 1,
                        t3_xdp_ingress_ns: ts + 50_000,
                        t7_xdp_egress_ns: ts + 50_000 + svc,
                        pod_service_time_ns: svc,
                        exec_type: "0".into(),
                        fill_qty: 0,
                        fill_price: 0,
                        orig_order_id: String::new(),
                        reordering_detected: false,
                        retransmission_count: 0,
                        liquidity_ind: 0,
                    };
                    let e = by_part.entry(p).or_default();
                    e.0.push(sent);
                    e.1.push(acked);
                }
                // Serialize all partitions into OWNED buffers first (these outlive the
                // DeliveryFutures, which borrow the payload). Then fire all sends and
                // await delivery together — concurrent delivery instead of serial
                // per-message round-trips (the serial version capped ~56k/s).
                let mut bufs: Vec<(i32, Vec<u8>, Vec<u8>)> = Vec::with_capacity(by_part.len());
                for (p, (sents, ackeds)) in by_part {
                    let submission_id = sents
                        .first()
                        .map(|e| e.submission_id.as_str())
                        .unwrap_or("inject");
                    let event_refs: Vec<iicpc_schemas_rust::OrderSentEventFieldsRef> = sents
                        .iter()
                        .map(iicpc_schemas_rust::OrderSentEventFieldsRef::from)
                        .collect();
                    let sb = iicpc_schemas_rust::OrderSentBatchV2Ref {
                        session_id: &session,
                        submission_id,
                        worker_id: "inject",
                        events: &event_refs,
                    };
                    let ab = OrderAckedBatch {
                        session_id: session.clone(),
                        contestant_id: "inject".into(),
                        events: ackeds,
                    };
                    bufs.push((
                        p,
                        rmp_serde::to_vec(&sb).unwrap(),
                        rmp_serde::to_vec(&ab).unwrap(),
                    ));
                }
                let mut futs = Vec::with_capacity(bufs.len() * 2);
                for (p, sbuf, abuf) in &bufs {
                    futs.push(
                        producer.send(
                            FutureRecord::to(TOPIC_ORDERS_SENT)
                                .partition(*p)
                                .payload(sbuf)
                                .key(&session),
                            Timeout::Never,
                        ),
                    );
                    futs.push(
                        producer.send(
                            FutureRecord::to(TOPIC_ORDERS_ACKED)
                                .partition(*p)
                                .payload(abuf)
                                .key(&session),
                            Timeout::Never,
                        ),
                    );
                }
                for f in futs {
                    let _ = f.await;
                }
                produced += batch;
                total.fetch_add(batch, Ordering::Relaxed);
                // pace to per-thread target rate
                if per_thread_rate > 0 {
                    let want = produced as f64 / per_thread_rate as f64;
                    let have = start.elapsed().as_secs_f64();
                    if want > have {
                        tokio::time::sleep(Duration::from_secs_f64(want - have)).await;
                    }
                }
            }
        }));
    }

    // reporter
    let rep_total = total.clone();
    let reporter = tokio::spawn(async move {
        let start = Instant::now();
        let mut last = 0u64;
        while start.elapsed().as_secs() < dur {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let now = rep_total.load(Ordering::Relaxed);
            eprintln!(
                "   injected/s={:.0}  total={}",
                (now - last) as f64 / 2.0,
                now
            );
            last = now;
        }
    });

    for h in handles {
        let _ = h.await;
    }
    let _ = reporter.await;
    eprintln!("DONE injected_total={}", total.load(Ordering::Relaxed));
    Ok(())
}
