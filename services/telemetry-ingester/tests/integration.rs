//! This module defines tests for integration.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use iicpc_schemas_rust::{
    OrdType, OrderAckedBatch, OrderAckedEvent, OrderSentBatchV2, OrderSentBatchV2Ref,
    OrderSentEvent, OrderSentEventFieldsRef, PayloadType, Side, SMP_ID_NONE, TOPIC_ORDERS_ACKED,
    TOPIC_ORDERS_SENT,
};
use iicpc_telemetry_ingester::aggregate::{Aggregator, Snapshot, DEFAULT_WAVE_NS};
use iicpc_telemetry_ingester::redis_sink::RedisSink;
use iicpc_telemetry_ingester::store::Store;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

/// env performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.trim().is_empty())
}

/// now_ns performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// unique_suffix performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn unique_suffix() -> String {
    format!("{}-{}", std::process::id(), now_ns())
}

/// acked performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn acked(
    session: &str,
    contestant: &str,
    order: &str,
    t3: u64,
    t7: u64,
    exec: &str,
    fill_qty: u64,
) -> OrderAckedEvent {
    OrderAckedEvent {
        session_id: session.into(),
        contestant_id: contestant.into(),
        order_id: order.into(),
        src_ip: 0x0a00_0001,
        src_port: 51234,
        tcp_seq: 1,
        t3_xdp_ingress_ns: t3,
        t7_xdp_egress_ns: t7,
        pod_service_time_ns: t7.saturating_sub(t3),
        exec_type: exec.into(),
        fill_qty,
        fill_price: 42_500_000_000,
        orig_order_id: String::new(),
        reordering_detected: false,
        retransmission_count: 0,
        liquidity_ind: 0,
    }
}

/// sent performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn sent(session: &str, order: &str, t0: u64, t1: u64, r9: u64, timed_out: bool) -> OrderSentEvent {
    OrderSentEvent {
        session_id: session.into(),
        submission_id: "sub-1".into(),
        worker_id: "w-1".into(),
        task_id: 0,
        order_id: order.into(),
        target_send_ts_ns: t0,
        send_ts_ns: t1,
        recv_done_ts_ns: r9,
        timed_out,
        price: 100,
        qty: 10,
        side: Side::Buy,
        payload_type: PayloadType::New,
        ord_type: OrdType::Limit,
        orig_order_id: String::new(),
        barrier_epoch_ns: 0,
        // Explicitly SMP_ID_NONE, never a bare 0: zero is a VALID participant id as well as
        // Rust's default, so a synthetic event that omits this reads as "participant 0".
        smp_id: SMP_ID_NONE,
    }
}

/// Expected stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct Expected {
    responded: u64,
    error_rate: f64,
}

/// synthetic performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn synthetic(
    session: &str,
    contestant: &str,
) -> (Vec<OrderSentEvent>, Vec<OrderAckedEvent>, Expected) {
    let base = 1_000_000_000_000u64;
    let sents = vec![
        sent(session, "o1", base, base + 1_000, base + 50_000, false),
        sent(session, "o2", base, base + 1_000, base + 50_000, false),
        sent(session, "o3", base, base + 1_000, base + 50_000, false),
        sent(session, "o4", base, base + 1_000, 0, true),
    ];
    let ackeds = vec![
        acked(
            session,
            contestant,
            "o1",
            base + 10_000,
            base + 110_000,
            "0",
            0,
        ),
        acked(
            session,
            contestant,
            "o1",
            base + 10_000,
            base + 160_000,
            "2",
            10,
        ),
        acked(
            session,
            contestant,
            "o2",
            base + 10_000,
            base + 120_000,
            "0",
            0,
        ),
        acked(
            session,
            contestant,
            "o3",
            base + 10_000,
            base + 130_000,
            "8",
            0,
        ),
    ];
    (
        sents,
        ackeds,
        Expected {
            responded: 3,
            error_rate: 0.5,
        },
    )
}

/// aggregate_synthetic performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn aggregate_synthetic(session: &str, contestant: &str) -> (Vec<Snapshot>, Expected) {
    let (sents, ackeds, exp) = synthetic(session, contestant);
    let mut agg = Aggregator::new(DEFAULT_WAVE_NS);
    for s in &sents {
        agg.observe_sent(s);
    }
    for a in &ackeds {
        agg.observe_acked(a);
    }
    (agg.snapshot(now_ns(), 1.0), exp)
}

/// encode_sent_batch mirrors bot-fleet's telemetry.rs wire encode: positional
/// msgpack with session_id/submission_id/worker_id hoisted into the envelope
/// (`OrderSentBatchV2Ref`, `rmp_serde::to_vec`), not the legacy per-event named
/// format.
fn encode_sent_batch(session: &str, worker_id: &str, events: &[OrderSentEvent]) -> Vec<u8> {
    let submission_id = events
        .first()
        .map(|e| e.submission_id.as_str())
        .unwrap_or("sub-1");
    let event_refs: Vec<OrderSentEventFieldsRef> =
        events.iter().map(OrderSentEventFieldsRef::from).collect();
    rmp_serde::to_vec(&OrderSentBatchV2Ref {
        session_id: session,
        submission_id,
        worker_id,
        events: &event_refs,
    })
    .expect("encode orders.sent batch")
}

/// redis_snapshot_key performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn redis_snapshot_key(contestant: &str, session: &str, wave_index: u32) -> String {
    format!("contestant:{contestant}:{session}:{wave_index}")
}

#[tokio::test]
/// store_roundtrip_writes_and_reads_metrics performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn store_roundtrip_writes_and_reads_metrics() {
    let Some(url) = env("TIMESCALE_URL") else {
        eprintln!("skip store_roundtrip: TIMESCALE_URL unset");
        return;
    };
    let session = format!("itest-store-{}", unique_suffix());
    let contestant = format!("c-{}", unique_suffix());

    let store = Store::connect(&url, "test-shard-0".to_string())
        .await
        .expect("connect timescale");
    store.init_schema().await.expect("init schema");
    let (snaps, exp) = aggregate_synthetic(&session, &contestant);
    assert!(!snaps.is_empty(), "aggregator produced a snapshot");
    store.write(&snaps).await.expect("write metrics");

    let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("raw connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let row = client
        .query_one(
            "SELECT count(*)::bigint, max(tps_1s), max(p99_ns)::bigint FROM metrics_partial WHERE session_id = $1",
            &[&session],
        )
        .await
        .expect("query metrics");
    let count: i64 = row.get(0);
    let max_tps: f64 = row.get(1);
    let max_p99: i64 = row.get(2);
    assert_eq!(count, 1, "exactly one (session,wave) row");
    assert_eq!(max_tps, exp.responded as f64, "tps == responded count");
    assert!(max_p99 > 0, "p99 service time recorded");
    eprintln!("store_roundtrip OK: rows={count} tps={max_tps} p99={max_p99}");
}

#[tokio::test]
/// redis_roundtrip_writes_contestant_hash performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn redis_roundtrip_writes_contestant_hash() {
    let Some(url) = env("REDIS_URL") else {
        eprintln!("skip redis_roundtrip: REDIS_URL unset");
        return;
    };
    let session = format!("itest-redis-{}", unique_suffix());
    let contestant = format!("c-{}", unique_suffix());

    let sink = RedisSink::connect(&url).await.expect("connect redis");
    let (snaps, _exp) = aggregate_synthetic(&session, &contestant);
    sink.write(&snaps).await.expect("redis write");

    let client = redis::Client::open(url).expect("redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("redis conn");
    let map: std::collections::HashMap<String, String> = redis::cmd("HGETALL")
        .arg(redis_snapshot_key(&contestant, &session, 0))
        .query_async(&mut conn)
        .await
        .expect("HGETALL");
    assert_eq!(
        map.get("session_id").map(String::as_str),
        Some(session.as_str())
    );
    assert!(map.contains_key("p99_ns"), "p99_ns present");
    assert!(map.contains_key("tps_1s"), "tps_1s present");
    eprintln!("redis_roundtrip OK: {map:?}");
}

#[tokio::test]
/// kafka_produce_consume_aggregate performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn kafka_produce_consume_aggregate() {
    let Some(brokers) = env("KAFKA_BROKERS") else {
        eprintln!("skip kafka_produce_consume_aggregate: KAFKA_BROKERS unset");
        return;
    };
    let session = format!("itest-kafka-{}", unique_suffix());
    let contestant = format!("c-{}", unique_suffix());
    let (sents, ackeds, exp) = synthetic(&session, &contestant);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer");

    let acked_batch = OrderAckedBatch {
        session_id: session.clone(),
        contestant_id: contestant.clone(),
        events: ackeds,
    };
    let sent_bytes = encode_sent_batch(&session, "w-1", &sents);
    let acked_bytes = rmp_serde::to_vec_named(&acked_batch).unwrap();
    let want_sent = sents.len();
    producer
        .send(
            FutureRecord::to(TOPIC_ORDERS_SENT)
                .key(&session)
                .payload(&sent_bytes),
            Timeout::After(Duration::from_secs(5)),
        )
        .await
        .expect("produce orders.sent");
    producer
        .send(
            FutureRecord::to(TOPIC_ORDERS_ACKED)
                .key(&contestant)
                .payload(&acked_bytes),
            Timeout::After(Duration::from_secs(5)),
        )
        .await
        .expect("produce orders.acked");

    let group = format!("itest-{}", unique_suffix());
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("consumer");
    consumer
        .subscribe(&[TOPIC_ORDERS_SENT, TOPIC_ORDERS_ACKED])
        .expect("subscribe");

    let mut agg = Aggregator::new(DEFAULT_WAVE_NS);
    let mut seen_sent = 0usize;
    let mut seen_acked = 0usize;
    let want_acked = acked_batch.events.len();
    let deadline = Instant::now() + Duration::from_secs(20);
    while (seen_sent < want_sent || seen_acked < want_acked) && Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(m)) => {
                let Some(payload) = m.payload() else { continue };
                match m.topic() {
                    t if t == TOPIC_ORDERS_SENT => {
                        if let Ok(b) = rmp_serde::from_slice::<OrderSentBatchV2>(payload) {
                            if b.session_id == session {
                                let count = b.events.len();
                                for e in b.into_events() {
                                    agg.observe_sent(&e);
                                }
                                seen_sent += count;
                            }
                        }
                    }
                    t if t == TOPIC_ORDERS_ACKED => {
                        if let Ok(b) = rmp_serde::from_slice::<OrderAckedBatch>(payload) {
                            if b.session_id == session {
                                for e in &b.events {
                                    agg.observe_acked(e);
                                }
                                seen_acked += b.events.len();
                            }
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    assert_eq!(
        seen_sent, want_sent,
        "consumed all orders.sent for the session"
    );
    assert_eq!(
        seen_acked, want_acked,
        "consumed all orders.acked for the session"
    );

    let snaps = agg.snapshot(now_ns(), 1.0);
    let s = snaps
        .iter()
        .find(|s| s.session_id == session)
        .expect("snapshot for our session");
    assert_eq!(s.tps_1s, exp.responded as f64);
    assert!((s.error_rate - exp.error_rate).abs() < 1e-9);
    assert_eq!(s.contestant_id, contestant);
    assert!(s.p99_ns > 0);
    eprintln!(
        "kafka_produce_consume_aggregate OK: tps={} err={} p99={}",
        s.tps_1s, s.error_rate, s.p99_ns
    );
}

#[tokio::test]
/// full_pipeline_kafka_to_timescale_and_redis performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn full_pipeline_kafka_to_timescale_and_redis() {
    let (Some(brokers), Some(turl), Some(rurl)) =
        (env("KAFKA_BROKERS"), env("TIMESCALE_URL"), env("REDIS_URL"))
    else {
        eprintln!("skip full_pipeline: need KAFKA_BROKERS + TIMESCALE_URL + REDIS_URL");
        return;
    };
    let session = format!("itest-full-{}", unique_suffix());
    let contestant = format!("c-{}", unique_suffix());
    let (sents, ackeds, exp) = synthetic(&session, &contestant);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .unwrap();
    let sent_bytes = encode_sent_batch(&session, "w-1", &sents);
    let acked_bytes = rmp_serde::to_vec_named(&OrderAckedBatch {
        session_id: session.clone(),
        contestant_id: contestant.clone(),
        events: ackeds,
    })
    .unwrap();
    producer
        .send(
            FutureRecord::to(TOPIC_ORDERS_SENT)
                .key(&session)
                .payload(&sent_bytes),
            Timeout::After(Duration::from_secs(5)),
        )
        .await
        .unwrap();
    producer
        .send(
            FutureRecord::to(TOPIC_ORDERS_ACKED)
                .key(&contestant)
                .payload(&acked_bytes),
            Timeout::After(Duration::from_secs(5)),
        )
        .await
        .unwrap();

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", format!("itest-{}", unique_suffix()))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .unwrap();
    consumer
        .subscribe(&[TOPIC_ORDERS_SENT, TOPIC_ORDERS_ACKED])
        .unwrap();
    let mut agg = Aggregator::new(DEFAULT_WAVE_NS);
    let mut seen = 0usize;
    let want = 4 + 4;
    let deadline = Instant::now() + Duration::from_secs(20);
    while seen < want && Instant::now() < deadline {
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            let Some(p) = m.payload() else { continue };
            if m.topic() == TOPIC_ORDERS_SENT {
                if let Ok(b) = rmp_serde::from_slice::<OrderSentBatchV2>(p) {
                    if b.session_id == session {
                        let count = b.events.len();
                        b.into_events()
                            .into_iter()
                            .for_each(|e| agg.observe_sent(&e));
                        seen += count;
                    }
                }
            } else if m.topic() == TOPIC_ORDERS_ACKED {
                if let Ok(b) = rmp_serde::from_slice::<OrderAckedBatch>(p) {
                    if b.session_id == session {
                        b.events.iter().for_each(|e| agg.observe_acked(e));
                        seen += b.events.len();
                    }
                }
            }
        }
    }
    assert_eq!(seen, want, "consumed the full synthetic session");

    let snaps = agg.snapshot(now_ns(), 1.0);
    let store = Store::connect(&turl, "test-shard-0".to_string())
        .await
        .unwrap();
    store.init_schema().await.unwrap();
    store.write(&snaps).await.unwrap();
    let redis = RedisSink::connect(&rurl).await.unwrap();
    redis.write(&snaps).await.unwrap();

    let (client, conn) = tokio_postgres::connect(&turl, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let row = client
        .query_one(
            "SELECT max(tps_1s), max(error_rate), max(p99_ns)::bigint FROM metrics_partial WHERE session_id=$1",
            &[&session],
        )
        .await
        .unwrap();
    let tps: f64 = row.get(0);
    let err: f64 = row.get(1);
    let p99: i64 = row.get(2);
    assert_eq!(tps, exp.responded as f64);
    assert!((err - exp.error_rate).abs() < 1e-9);
    assert!(p99 > 0);

    let rclient = redis::Client::open(rurl).unwrap();
    let mut rconn = rclient.get_multiplexed_async_connection().await.unwrap();
    let map: std::collections::HashMap<String, String> = redis::cmd("HGETALL")
        .arg(redis_snapshot_key(&contestant, &session, 0))
        .query_async(&mut rconn)
        .await
        .unwrap();
    assert_eq!(
        map.get("session_id").map(String::as_str),
        Some(session.as_str())
    );
    assert!(map.get("p99_ns").is_some());
    eprintln!("full_pipeline OK: tps={tps} err={err} p99={p99} redis={map:?}");
}
