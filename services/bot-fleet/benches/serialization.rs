//! QoL-5: criterion promotion of the old `serialization_cost_breakdown` ignored test.
//! Compares the P2/P2' template-and-patch path (`TemplateCache`) against the
//! pre-P2 per-order `format!` builders (`order_frame`/`market_frame`), for all three
//! protocols. Run with:
//!   cargo bench -p iicpc-bot-fleet --bench serialization

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use iicpc_bot_fleet::fix::{market_frame, order_frame, TemplateCache};
use iicpc_schemas_rust::{Protocol, Side};

const FV: &str = "FIX.4.2";
const SID: &str = "01890dd2-71f3-7abc-9def-0123456789ab";
const BOT: u64 = 42;

fn bench_build_path(c: &mut Criterion, protocol: Protocol, host: &str, label: &str) {
    let mut group = c.benchmark_group(label);

    group.bench_function("build_per_order", |b| {
        let mut seq = 0u64;
        b.iter(|| {
            seq += 1;
            let f = order_frame(protocol, FV, SID, host, BOT, seq, 10_000, 25, Side::Buy);
            black_box(f.bytes.len())
        });
    });

    group.bench_function("template_patch", |b| {
        let mut cache = TemplateCache::new(protocol, FV, SID, host, BOT);
        let mut seq = 0u64;
        b.iter(|| {
            seq += 1;
            let f = cache.render_new(seq, 10_000, 25, Side::Buy);
            black_box(f.bytes.len())
        });
    });

    group.bench_function("template_patch_market", |b| {
        let mut cache = TemplateCache::new(protocol, FV, SID, host, BOT);
        let mut seq = 0u64;
        b.iter(|| {
            seq += 1;
            let f = cache.render_market(seq, 25, Side::Buy);
            black_box(f.bytes.len())
        });
    });

    group.bench_function("build_per_order_market", |b| {
        let mut seq = 0u64;
        b.iter(|| {
            seq += 1;
            let f = market_frame(protocol, FV, SID, host, BOT, seq, 25, Side::Buy);
            black_box(f.bytes.len())
        });
    });

    group.finish();
}

fn fix_bench(c: &mut Criterion) {
    bench_build_path(c, Protocol::Fix, "10.0.0.5:9898", "fix");
}

fn rest_bench(c: &mut Criterion) {
    bench_build_path(c, Protocol::Rest, "10.0.0.5:8080", "rest");
}

fn ws_bench(c: &mut Criterion) {
    bench_build_path(c, Protocol::Ws, "10.0.0.5:8080", "ws");
}

criterion_group!(benches, fix_bench, rest_bench, ws_bench);
criterion_main!(benches);
