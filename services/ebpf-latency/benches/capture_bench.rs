//! Criterion benches for the capture's userspace pipeline.
//!
//! The binary crate keeps its modules private and the [lib] target is the BPF
//! object, so the modules are compiled into this bench via #[path] includes —
//! `crate::capture` etc. inside them resolve against this bench crate's root.
//!
//! Why these benches exist (docs/capture-ringbuf-drops.md §6.1): the local
//! cluster hits the veth packet-rate wall before the capture saturates, so the
//! pipeline's single-core ceiling is measured HERE, with no network in the
//! loop. The `pipeline_e2e` group is the ceiling number; the per-stage groups
//! attribute it. Fixtures are self-contained copies of the shapes in
//! `framing_property.rs` so the bench needs nothing from the test harness.
#![allow(dead_code)]

#[path = "../src/capture.rs"]
mod capture;
#[path = "../src/matcher.rs"]
mod matcher;
#[path = "../src/parse.rs"]
mod parse;
#[path = "../src/pipeline.rs"]
mod pipeline;
#[path = "../src/reassembly.rs"]
mod reassembly;

use capture::{Direction, Transport, CAPTURE_CAP, CAPTURE_HEADER_LEN};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use matcher::Matcher;
use pipeline::Pipeline;
use reassembly::Reassembler;

// ---------------------------------------------------------------- fixtures

fn clordid(seq: usize) -> String {
    // Real ids are `{session}_{task}_{seq}_{K}` with a ULID-ish session.
    format!("01890f2a3b4c5d6e7f8090a1_7_{seq}_O")
}

/// FIX NewOrderSingle, same shape as framing_property::fix_message (including
/// tag 38, whose "8=" byte pattern is the framer's historical trap).
fn fix_request(seq: usize) -> Vec<u8> {
    let id = clordid(seq);
    let body = format!(
        "35=D\u{1}49=IICPC-BOT\u{1}56=CONTESTANT\u{1}34={seq}\u{1}11={id}\u{1}\
         54=1\u{1}38=100\u{1}44=1000\u{1}40=2\u{1}7928={:03}\u{1}",
        seq % 8
    );
    let head = format!("8=FIX.4.2\u{1}9={}\u{1}", body.len());
    let mut msg = format!("{head}{body}").into_bytes();
    let sum: u32 = msg.iter().map(|&b| u32::from(b)).sum();
    msg.extend_from_slice(format!("10={:03}\u{1}", sum % 256).as_bytes());
    msg
}

/// FIX ExecutionReport ack for the same ClOrdID (shape from the bot's
/// contestant-side test template: 35=8, 150/39, 851 liquidity).
fn fix_response(seq: usize) -> Vec<u8> {
    let id = clordid(seq);
    let body = format!(
        "35=8\u{1}49=CONTESTANT\u{1}56=IICPC-BOT\u{1}34={seq}\u{1}37=EX{seq}\u{1}\
         11={id}\u{1}17=E{seq}\u{1}150=0\u{1}39=0\u{1}55=IICPC\u{1}54=1\u{1}\
         38=100\u{1}32=0\u{1}31=0\u{1}851=1\u{1}"
    );
    let head = format!("8=FIX.4.2\u{1}9={}\u{1}", body.len());
    let mut msg = format!("{head}{body}").into_bytes();
    let sum: u32 = msg.iter().map(|&b| u32::from(b)).sum();
    msg.extend_from_slice(format!("10={:03}\u{1}", sum % 256).as_bytes());
    msg
}

fn json_body(seq: usize) -> Vec<u8> {
    format!(
        "{{\"cl_ord_id\":\"{}\",\"side\":\"1\",\"qty\":100,\"price\":1000}}",
        clordid(seq)
    )
    .into_bytes()
}

fn http_request(seq: usize) -> Vec<u8> {
    let body = json_body(seq);
    let mut out = format!(
        "POST /orders HTTP/1.1\r\nHost: engine\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(&body);
    out
}

fn http_response(seq: usize) -> Vec<u8> {
    let body = json_body(seq);
    let mut out = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(&body);
    out
}

/// RFC 6455: requests masked, responses not — the framer enforces this.
fn ws_frame(direction: Direction, payload: &[u8]) -> Vec<u8> {
    let masked = direction == Direction::Request;
    let mut out = vec![0x81u8];
    let mask_bit = if masked { 0x80 } else { 0x00 };
    assert!(payload.len() < 126);
    out.push(mask_bit | payload.len() as u8);
    if masked {
        let mask = [0xA1u8, 0xB2, 0xC3, 0xD4];
        out.extend_from_slice(&mask);
        out.extend(payload.iter().enumerate().map(|(i, &b)| b ^ mask[i % 4]));
    } else {
        out.extend_from_slice(payload);
    }
    out
}

/// Encodes one kernel capture record, byte-identical to CaptureRecord's wire
/// layout (28-byte header + payload).
fn encode_record(
    ts: u64,
    client_ip: u32,
    tcp_seq: u32,
    client_port: u16,
    server_port: u16,
    direction: u8,
    payload: &[u8],
) -> Vec<u8> {
    let mut v = Vec::with_capacity(CAPTURE_HEADER_LEN + payload.len());
    v.extend_from_slice(&ts.to_le_bytes());
    v.extend_from_slice(&client_ip.to_le_bytes());
    v.extend_from_slice(&tcp_seq.to_le_bytes());
    v.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    v.extend_from_slice(&client_port.to_le_bytes());
    v.extend_from_slice(&server_port.to_le_bytes());
    v.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    v.push(direction);
    v.push(0);
    v.extend_from_slice(payload);
    v
}

struct FlowState {
    req_seq: u32,
    resp_seq: u32,
}

/// A realistic interleaved record stream: `orders` request/response pairs over
/// `flows` connections, one packet per message on the request side, and the
/// response side packed `batch` messages per record (capped by CAPTURE_CAP,
/// like the tc hook's clamp of coalesced egress skbs). `batch = 1` models an
/// engine that writes per-response; larger batches model coalescing.
fn record_stream(orders: usize, flows: usize, batch: usize, transport: Transport) -> Vec<Vec<u8>> {
    let server_port = match transport {
        Transport::Fix => 9898,
        Transport::HttpWs => 8080,
    };
    let mut per_flow: Vec<FlowState> = (0..flows)
        .map(|_| FlowState {
            req_seq: 1000,
            resp_seq: 5000,
        })
        .collect();
    let mut out = Vec::with_capacity(orders * 2);
    let mut ts = 1_000_000u64;
    let mut pending: Vec<(usize, Vec<u8>)> = Vec::new(); // (flow, response bytes)
    for seq in 1..=orders {
        let flow = seq % flows;
        let (req, resp) = match transport {
            Transport::Fix => (fix_request(seq), fix_response(seq)),
            Transport::HttpWs => (http_request(seq), http_response(seq)),
        };
        let st = &mut per_flow[flow];
        ts += 700;
        out.push(encode_record(
            ts,
            0x0a00_0000 + flow as u32,
            st.req_seq,
            40_000 + flow as u16,
            server_port,
            0,
            &req,
        ));
        st.req_seq = st.req_seq.wrapping_add(req.len() as u32);
        pending.push((flow, resp));
        // Flush a response record once `batch` messages for one flow are
        // queued, packing them into one payload the way a coalesced egress
        // skb would arrive, clamped at CAPTURE_CAP.
        if pending.iter().filter(|(f, _)| *f == flow).count() >= batch {
            flush_flow(
                flow,
                server_port,
                &mut pending,
                &mut per_flow,
                &mut ts,
                &mut out,
            );
        }
    }
    // End of stream: every flow's remaining responses go out, in cap-sized
    // records, so the bench's events==orders correctness assertion holds.
    while !pending.is_empty() {
        let flow = pending[0].0;
        flush_flow(
            flow,
            server_port,
            &mut pending,
            &mut per_flow,
            &mut ts,
            &mut out,
        );
    }
    out
}

/// Packs as many of `flow`'s pending responses as fit under CAPTURE_CAP into
/// one record; the remainder stays pending for a later flush.
fn flush_flow(
    flow: usize,
    server_port: u16,
    pending: &mut Vec<(usize, Vec<u8>)>,
    per_flow: &mut [FlowState],
    ts: &mut u64,
    out: &mut Vec<Vec<u8>>,
) {
    let mut payload = Vec::new();
    let mut rest = Vec::new();
    for (f, r) in pending.drain(..) {
        if f == flow && payload.len() + r.len() <= CAPTURE_CAP {
            payload.extend_from_slice(&r);
        } else {
            rest.push((f, r));
        }
    }
    *pending = rest;
    if !payload.is_empty() {
        let st = &mut per_flow[flow];
        *ts += 900;
        out.push(encode_record(
            *ts,
            0x0a00_0000 + flow as u32,
            st.resp_seq,
            40_000 + flow as u16,
            server_port,
            1,
            &payload,
        ));
        st.resp_seq = st.resp_seq.wrapping_add(payload.len() as u32);
    }
}

// ---------------------------------------------------------------- benches

fn bench_decode(c: &mut Criterion) {
    let rec = encode_record(1, 0x0a000001, 1000, 40001, 9898, 1, &fix_response(7));
    let mut g = c.benchmark_group("decode");
    g.throughput(Throughput::Elements(1));
    g.bench_function("fix_response_record", |b| {
        b.iter(|| capture::decode(std::hint::black_box(&rec)).unwrap().tcp_seq)
    });
    g.finish();
}

fn bench_reassembly(c: &mut Criterion) {
    // One stream of FIX requests cut into MTU-ish segments; per iteration the
    // whole stream goes through push+consume so the buffer stays bounded.
    let stream: Vec<u8> = (1..=2000).flat_map(fix_request).collect();
    let segs: Vec<(u32, &[u8])> = stream
        .chunks(1448)
        .scan(0u32, |seq, chunk| {
            let s = *seq;
            *seq += chunk.len() as u32;
            Some((s, chunk))
        })
        .collect();
    let mut g = c.benchmark_group("reassembly");
    g.throughput(Throughput::Bytes(stream.len() as u64));
    g.bench_function("contiguous_push_consume", |b| {
        b.iter(|| {
            let mut re = Reassembler::new();
            for (seq, data) in &segs {
                re.push(*seq, 1_000, data);
                let n = re.available().len();
                re.consume(n);
            }
            re.available().len()
        })
    });
    g.finish();
}

fn bench_framing(c: &mut Criterion) {
    let cases: Vec<(&str, Transport, Direction, Vec<u8>, usize)> = vec![
        (
            "fix_requests",
            Transport::Fix,
            Direction::Request,
            (1..=2000).flat_map(fix_request).collect(),
            2000,
        ),
        (
            "fix_responses",
            Transport::Fix,
            Direction::Response,
            (1..=2000).flat_map(fix_response).collect(),
            2000,
        ),
        (
            "http_requests",
            Transport::HttpWs,
            Direction::Request,
            (1..=2000).flat_map(http_request).collect(),
            2000,
        ),
        (
            "http_responses",
            Transport::HttpWs,
            Direction::Response,
            (1..=2000).flat_map(http_response).collect(),
            2000,
        ),
        (
            "ws_responses",
            Transport::HttpWs,
            Direction::Response,
            (1..=2000)
                .flat_map(|s| ws_frame(Direction::Response, &json_body(s)))
                .collect(),
            2000,
        ),
    ];
    let mut g = c.benchmark_group("framing");
    for (name, transport, direction, stream, msgs) in cases {
        g.throughput(Throughput::Elements(msgs as u64));
        // Fed in MTU-sized pushes with framing drained after each, like the
        // production drain loop — one giant push would make consume() memmove
        // the whole tail per message and benchmark an O(n²) that the real
        // pipeline never pays.
        let segs: Vec<(u32, &[u8])> = stream
            .chunks(1448)
            .scan(0u32, |seq, chunk| {
                let s = *seq;
                *seq += chunk.len() as u32;
                Some((s, chunk))
            })
            .collect();
        g.bench_function(name, |b| {
            b.iter(|| {
                let mut re = Reassembler::new();
                let mut framed = 0usize;
                for (seq, data) in &segs {
                    re.push(*seq, 1_000, data);
                    loop {
                        match parse::frame(transport, direction, re.available()) {
                            parse::Frame::Message(n) => {
                                let p = parse::parse(transport, direction, &re.available()[..n]);
                                assert!(!p.clordid.is_empty());
                                re.consume(n);
                                framed += 1;
                            }
                            parse::Frame::Incomplete => break,
                            parse::Frame::Resync(skip) => re.consume(skip.max(1)),
                        }
                    }
                }
                assert_eq!(framed, msgs);
                framed
            })
        });
    }
    g.finish();
}

fn bench_matcher(c: &mut Criterion) {
    // Insert+respond cycles over fresh ids against a 100k-entry base map: the
    // number that matters for the fixed-width-key A/B is ns per
    // request+response pair at realistic occupancy.
    const BASE: usize = 100_000;
    const OPS: usize = 10_000;
    let base_ids: Vec<String> = (0..BASE).map(clordid).collect();
    let op_ids: Vec<String> = (BASE..BASE + OPS).map(clordid).collect();
    let mut base = Matcher::new();
    for id in &base_ids {
        base.on_request(id, 100, 1, 5, 10, false);
    }
    let mut g = c.benchmark_group("matcher");
    g.throughput(Throughput::Elements(OPS as u64));
    g.sample_size(20);
    g.bench_function("request_response_pair_100k_occupancy", |b| {
        b.iter_batched_ref(
            || {
                let mut m = Matcher::new();
                for id in &base_ids {
                    m.on_request(id, 100, 1, 5, 10, false);
                }
                m
            },
            |m| {
                let mut matched = 0usize;
                for id in &op_ids {
                    m.on_request(id, 200, 1, 5, 11, false);
                    if m.on_response(id, 900, "0", 0, 0, "", false, 0).is_some() {
                        matched += 1;
                    }
                }
                assert_eq!(matched, OPS);
                matched
            },
            BatchSize::LargeInput,
        )
    });
    g.finish();
}

fn bench_pipeline(c: &mut Criterion) {
    const ORDERS: usize = 10_000;
    let cases: Vec<(&str, Vec<Vec<u8>>)> = vec![
        ("fix_batch1", record_stream(ORDERS, 4, 1, Transport::Fix)),
        ("fix_batch6", record_stream(ORDERS, 4, 6, Transport::Fix)),
        (
            "fix_batch40",
            record_stream(ORDERS, 4, 40, Transport::Fix),
        ),
        (
            "http_batch1",
            record_stream(ORDERS, 4, 1, Transport::HttpWs),
        ),
        (
            "http_batch6",
            record_stream(ORDERS, 4, 6, Transport::HttpWs),
        ),
    ];
    let mut g = c.benchmark_group("pipeline_e2e");
    g.sample_size(20);
    for (name, records) in &cases {
        // Records, not orders: the kernel emits records and the drain loop
        // pays per record, so the ceiling is stated in records/s.
        g.throughput(Throughput::Elements(records.len() as u64));
        g.bench_function(*name, |b| {
            b.iter(|| {
                let mut p = Pipeline::with_offset(0);
                let mut out = Vec::with_capacity(1024);
                let mut events = 0usize;
                for rec in records {
                    let cap = capture::decode(rec).unwrap();
                    p.process(&cap, &mut out);
                    events += out.len();
                    out.clear();
                }
                // Every order must produce exactly one matched ack event —
                // a throughput bench that silently drops correctness would
                // measure the wrong pipeline.
                assert_eq!(events, ORDERS);
                events
            })
        });
    }
    g.finish();
}

fn bench_encode(c: &mut Criterion) {
    // The msgpack encode in flush() runs on the DRAIN thread (main.rs), so it
    // belongs in the single-core budget alongside the pipeline. This bench
    // uses the exact production types (OrderAckedBatchRef over borrowed
    // events) at MAX_EVENTS_PER_BATCH size. Excluded, and therefore still
    // unmeasured: batch_by_partition's BTreeMap grouping and the rdkafka
    // enqueue — both live in main.rs, which a bench cannot include.
    use iicpc_schemas_rust::{OrderAckedBatchRef, OrderAckedEventRef};
    const BATCH: usize = 1000; // MAX_EVENTS_PER_BATCH in main.rs
    let events: Vec<matcher::MatchedEvent> = (0..BATCH)
        .map(|i| matcher::MatchedEvent {
            order_id: clordid(i),
            src_ip: 0x0a000001,
            src_port: 40001,
            tcp_seq: 1000 + i as u32,
            t3_ns: 1_000_000 + i as u64,
            t7_ns: 1_050_000 + i as u64,
            pod_service_time_ns: 50_000,
            exec_type: "0".to_string(),
            fill_qty: 0,
            fill_price: 0,
            orig_order_id: String::new(),
            reordering_detected: false,
            retransmission_count: 0,
            liquidity_ind: 0,
        })
        .collect();
    let session = "01890f2a3b4c5d6e7f8090a1";
    let contestant = "contestant-7";
    let mut g = c.benchmark_group("encode");
    g.throughput(Throughput::Elements(BATCH as u64));
    g.bench_function("orders_acked_msgpack_batch1000", |b| {
        b.iter(|| {
            let refs: Vec<OrderAckedEventRef> = events
                .iter()
                .map(|e| OrderAckedEventRef {
                    session_id: session,
                    contestant_id: contestant,
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
                .collect();
            let batch = OrderAckedBatchRef {
                session_id: session,
                contestant_id: contestant,
                events: &refs,
            };
            rmp_serde::to_vec_named(&batch).unwrap().len()
        })
    });
    g.finish();
}

criterion_group!(
    benches,
    bench_decode,
    bench_reassembly,
    bench_framing,
    bench_matcher,
    bench_pipeline,
    bench_encode
);
criterion_main!(benches);
