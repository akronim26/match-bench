//! This module implements aggregate behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::collections::HashMap;

use hdrhistogram::serialization::{Serializer, V2DeflateSerializer};
use hdrhistogram::Histogram;
use iicpc_schemas_rust::{OrderAckedEvent, OrderSentEvent};

use crate::join::FirstResponseTracker;

/// SentEventRef is the borrowed view of the fields `observe_sent` actually reads
/// off an orders.sent event. It lets callers decoding the positional-msgpack V2
/// wire format (session_id/submission_id/worker_id hoisted into the batch
/// envelope) pass per-event data without cloning the envelope Strings for every
/// event in the batch — observe_sent never needed submission_id/worker_id at all.
pub struct SentEventRef<'a> {
    pub session_id: &'a str,
    pub order_id: &'a str,
    pub target_send_ts_ns: u64,
    pub send_ts_ns: u64,
    pub recv_done_ts_ns: u64,
    pub timed_out: bool,
    pub barrier_epoch_ns: u64,
}

impl<'a> From<&'a OrderSentEvent> for SentEventRef<'a> {
    fn from(e: &'a OrderSentEvent) -> Self {
        Self {
            session_id: &e.session_id,
            order_id: &e.order_id,
            target_send_ts_ns: e.target_send_ts_ns,
            send_ts_ns: e.send_ts_ns,
            recv_done_ts_ns: e.recv_done_ts_ns,
            timed_out: e.timed_out,
            barrier_epoch_ns: e.barrier_epoch_ns,
        }
    }
}

pub const DEFAULT_WAVE_NS: u64 = 20_000_000_000;
const HDR_MAX_NS: u64 = 60_000_000_000;
const HDR_SIGFIG: u8 = 3;
pub const FIRST_RESP_IDLE_NS: u64 = 5_000_000_000;
pub const WINDOW_IDLE_NS: u64 = 30_000_000_000;
/// How long an order_id stays marked as client-timed-out. The load gen abandons an
/// order at RESPONSE_TIMEOUT (5s), so a straggler's late ack almost always egresses
/// within a few seconds of that — 15s leaves a wide margin to exclude it. Kept short
/// on purpose: under overload (high timeout rate) this map grows as timeout_rate ×
/// this window, so a long window (e.g. the 60s HDR ceiling) would OOM the ingester
/// before the 5s join buffer does. The rare ack later than 15s slips into
/// service_time, but its pod_service_time is already >15s so it barely perturbs p99.
const TIMED_OUT_IDLE_NS: u64 = 15_000_000_000;

#[derive(Debug, Clone, PartialEq)]
/// Snapshot stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct Snapshot {
    pub time_ns: u64,
    pub session_id: String,
    pub contestant_id: String,
    pub wave_index: u32,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub rt_p50_ns: u64,
    pub rt_p90_ns: u64,
    pub rt_p99_ns: u64,
    pub tps_1s: f64,
    pub error_rate: f64,
    pub offered: u64,
    pub errors: u64,
    pub hdr_encoded: Vec<u8>,
    pub rt_hdr_encoded: Vec<u8>,
    pub slip_hdr_encoded: Vec<u8>,
    /// match_latency HDR (taker-fill t7-t3). Independent of hdr/rt/slip; its own chart.
    pub match_hdr_encoded: Vec<u8>,
}

/// Window stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct Window {
    contestant_id: String,
    service_time: Histogram<u64>,
    // match_latency: per-taker-fill matching cost (t7-t3) sampled ONLY on fills the
    // engine flags as taker (FIX LastLiquidityInd 851=2). Maker fills are market
    // wait, not engine time, so they're excluded. Independent of service_time.
    match_latency: Histogram<u64>,
    response_time: Histogram<u64>,
    schedule_slip: Histogram<u64>,
    last_update_ns: u64,
    offered: u64,
    responded: u64,
    accepted: u64,
    rejected: u64,
    timed_out: u64,
    fills: u64,
}

impl Window {
    /// new performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn new(contestant_id: String, now_ns: u64) -> Self {
        Self {
            contestant_id,
            service_time: new_hist(),
            match_latency: new_hist(),
            response_time: new_hist(),
            schedule_slip: new_hist(),
            last_update_ns: now_ns,
            offered: 0,
            responded: 0,
            accepted: 0,
            rejected: 0,
            timed_out: 0,
            fills: 0,
        }
    }

    /// interval_active performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn interval_active(&self) -> bool {
        self.offered > 0 || self.responded > 0 || self.fills > 0
    }
}

/// new_hist performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn new_hist() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, HDR_MAX_NS, HDR_SIGFIG)
        .expect("valid HDR bounds (1..=60s, 3 sig figs)")
}

/// record performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn record(hist: &mut Histogram<u64>, value_ns: u64) {
    let _ = hist.record(value_ns.max(1));
}

/// is_reject performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn is_reject(exec_type: &str) -> bool {
    exec_type == "8"
}

/// is_fill performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn is_fill(exec_type: &str, fill_qty: u64) -> bool {
    fill_qty > 0 && matches!(exec_type, "1" | "2" | "F")
}

/// is_taker reports whether a fill is the aggressor side per FIX LastLiquidityInd
/// (851): 2 = taker (removed liquidity). 1 = maker, 0 = unknown/absent → not counted.
fn is_taker(liquidity_ind: u8) -> bool {
    liquidity_ind == 2
}

/// Aggregator stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct Aggregator {
    windows: HashMap<(String, u32), Window>,
    session_start: HashMap<String, u64>,
    session_contestant: HashMap<String, String>,
    first_response: FirstResponseTracker,
    /// order_id -> last-seen ns for orders the load generator timed out (client
    /// abandoned). Their late acks are excluded from service_time/fill_latency so
    /// those histograms stay over the same population as response_time.
    timed_out_orders: HashMap<String, u64>,
    wave_ns: u64,
    last_evicted: usize,
    /// Orders finalized (matched -> first service_time sample recorded) since the
    /// last snapshot. Reported as iicpc_telemetry_records_finalized so the dashboard
    /// shows real recorded-latency throughput (NOT snapshot row count).
    finalized: usize,
}

impl Aggregator {
    /// new performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn new(wave_ns: u64) -> Self {
        Self {
            windows: HashMap::new(),
            session_start: HashMap::new(),
            session_contestant: HashMap::new(),
            first_response: FirstResponseTracker::new(),
            timed_out_orders: HashMap::new(),
            wave_ns: wave_ns.max(1),
            last_evicted: 0,
            finalized: 0,
        }
    }

    /// take_finalized returns the count of orders finalized since the last call and
    /// resets the running counter.
    pub fn take_finalized(&mut self) -> usize {
        std::mem::take(&mut self.finalized)
    }

    /// wave_of performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn wave_of(&mut self, session_id: &str, t_ns: u64) -> u32 {
        let start = self
            .session_start
            .entry(session_id.to_string())
            .or_insert(t_ns);
        if t_ns < *start {
            *start = t_ns;
        }
        ((t_ns.saturating_sub(*start)) / self.wave_ns) as u32
    }

    /// observe_sent performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn observe_sent<'a>(&mut self, e: impl Into<SentEventRef<'a>>) {
        let e = e.into();
        if e.barrier_epoch_ns > 0 {
            self.session_start
                .insert(e.session_id.to_string(), e.barrier_epoch_ns);
        }
        let t0 = e.target_send_ts_ns;
        let t1 = e.send_ts_ns;
        let r9 = e.recv_done_ts_ns;
        let wave = self.wave_of(e.session_id, t0);
        let contestant = self
            .session_contestant
            .get(e.session_id)
            .cloned()
            .unwrap_or_default();
        let w = self
            .windows
            .entry((e.session_id.to_string(), wave))
            .or_insert_with(|| Window::new(contestant, t1.max(t0)));

        w.offered += 1;
        if e.timed_out {
            w.timed_out += 1;
            // Mark the order so its (possibly much later) ack is excluded from
            // service_time — the client gave up, so there is no comparable
            // response_time sample. Keyed on the order's own time for eviction.
            let mark = self
                .timed_out_orders
                .entry(e.order_id.to_string())
                .or_insert(0);
            *mark = (*mark).max(t1.max(t0));
        }
        if t1 >= t0 {
            record(&mut w.schedule_slip, t1 - t0);
        }
        if !e.timed_out && r9 > t0 {
            record(&mut w.response_time, r9 - t0);
        }
        w.last_update_ns = w.last_update_ns.max(t1.max(t0));
    }

    /// observe_acked performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn observe_acked(&mut self, e: &OrderAckedEvent) {
        let t3 = e.t3_xdp_ingress_ns;
        self.session_contestant
            .insert(e.session_id.clone(), e.contestant_id.clone());
        // Drop late acks for orders the load generator already timed out: response_time
        // never saw them, so letting their pod_service_time into service_time/fill_latency
        // would make the two histograms cover different populations (service p99 could
        // then exceed response p99, which is impossible per order). Keep the marker alive
        // for any further late events (streaming fills) on the same order.
        if let Some(mark) = self.timed_out_orders.get_mut(&e.order_id) {
            *mark = (*mark).max(e.t7_xdp_egress_ns);
            return;
        }
        let wave = self.wave_of(&e.session_id, t3);
        let w = self
            .windows
            .entry((e.session_id.clone(), wave))
            .or_insert_with(|| Window::new(e.contestant_id.clone(), e.t7_xdp_egress_ns));
        if w.contestant_id.is_empty() {
            w.contestant_id = e.contestant_id.clone();
        }

        if self.first_response.observe(&e.order_id, e.t7_xdp_egress_ns) {
            record(&mut w.service_time, e.pod_service_time_ns);
            self.finalized += 1;
            w.responded += 1;
            if is_reject(&e.exec_type) {
                w.rejected += 1;
            } else {
                w.accepted += 1;
            }
        }
        if is_fill(&e.exec_type, e.fill_qty) {
            w.fills += 1;
        }
        // Matching latency: sample t7-t3 ONLY for taker fills (851=2). The taker is
        // the aggressor whose arrival did the matching, so this is engine cost; maker
        // fills (851=1) are market wait and excluded. Per-fill, no grouping — a deep
        // sweep contributes several samples, and its worst level shows in the tail.
        if is_taker(e.liquidity_ind) {
            record(&mut w.match_latency, e.pod_service_time_ns);
        }
        w.last_update_ns = w.last_update_ns.max(e.t7_xdp_egress_ns);
    }

    /// snapshot performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn snapshot(&mut self, now_ns: u64, interval_secs: f64) -> Vec<Snapshot> {
        let interval = if interval_secs > 0.0 {
            interval_secs
        } else {
            1.0
        };
        let mut out = Vec::new();
        for ((session_id, wave), w) in self.windows.iter_mut() {
            if !w.interval_active() {
                continue;
            }
            if w.contestant_id.is_empty() {
                if let Some(c) = self.session_contestant.get(session_id) {
                    w.contestant_id = c.clone();
                }
            }
            if !w.service_time.is_empty() && !w.contestant_id.is_empty() {
                let error_rate = if w.offered > 0 {
                    (w.timed_out + w.rejected) as f64 / w.offered as f64
                } else {
                    0.0
                };
                out.push(Snapshot {
                    time_ns: now_ns,
                    session_id: session_id.clone(),
                    contestant_id: w.contestant_id.clone(),
                    wave_index: *wave,
                    p50_ns: w.service_time.value_at_quantile(0.50),
                    p90_ns: w.service_time.value_at_quantile(0.90),
                    p99_ns: w.service_time.value_at_quantile(0.99),
                    p999_ns: w.service_time.value_at_quantile(0.999),
                    rt_p50_ns: w.response_time.value_at_quantile(0.50),
                    rt_p90_ns: w.response_time.value_at_quantile(0.90),
                    rt_p99_ns: w.response_time.value_at_quantile(0.99),
                    tps_1s: w.responded as f64 / interval,
                    error_rate,
                    offered: w.offered,
                    errors: w.timed_out + w.rejected,
                    hdr_encoded: serialize_hist(&w.service_time),
                    rt_hdr_encoded: serialize_hist(&w.response_time),
                    slip_hdr_encoded: serialize_hist(&w.schedule_slip),
                    match_hdr_encoded: serialize_hist(&w.match_latency),
                });
            }
            w.offered = 0;
            w.responded = 0;
            w.accepted = 0;
            w.rejected = 0;
            w.timed_out = 0;
            w.fills = 0;
        }

        self.windows
            .retain(|_, w| now_ns.saturating_sub(w.last_update_ns) < WINDOW_IDLE_NS);
        let live: std::collections::HashSet<&str> =
            self.windows.keys().map(|(s, _)| s.as_str()).collect();
        self.session_start.retain(|s, _| live.contains(s.as_str()));
        self.session_contestant
            .retain(|s, _| live.contains(s.as_str()));
        self.last_evicted = self.first_response.evict_idle(now_ns, FIRST_RESP_IDLE_NS);
        self.timed_out_orders
            .retain(|_, &mut seen| now_ns.saturating_sub(seen) < TIMED_OUT_IDLE_NS);
        out
    }

    /// window_count performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn window_count(&self) -> usize {
        self.windows.len()
    }

    /// join_buffer_size performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn join_buffer_size(&self) -> usize {
        self.first_response.len()
    }

    /// last_evicted performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn last_evicted(&self) -> usize {
        self.last_evicted
    }
}

/// serialize_hist performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn serialize_hist(hist: &Histogram<u64>) -> Vec<u8> {
    let mut buf = Vec::new();
    let _ = V2DeflateSerializer::new().serialize(hist, &mut buf);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use iicpc_schemas_rust::{OrdType, PayloadType, Side, SMP_ID_NONE};

    /// acked performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn acked(
        session: &str,
        order: &str,
        t3: u64,
        t7: u64,
        exec: &str,
        fill_qty: u64,
    ) -> OrderAckedEvent {
        OrderAckedEvent {
            session_id: session.into(),
            contestant_id: "c-1".into(),
            order_id: order.into(),
            src_ip: 1,
            src_port: 5,
            tcp_seq: 1,
            t3_xdp_ingress_ns: t3,
            t7_xdp_egress_ns: t7,
            pod_service_time_ns: t7.saturating_sub(t3),
            exec_type: exec.into(),
            fill_qty,
            fill_price: 0,
            orig_order_id: String::new(),
            reordering_detected: false,
            retransmission_count: 0,
            liquidity_ind: 0,
        }
    }

    /// acked_liq builds an acked event with an explicit FIX 851 liquidity indicator
    /// (1=maker, 2=taker), for exercising the match-latency sampling path.
    fn acked_liq(
        session: &str,
        order: &str,
        t3: u64,
        t7: u64,
        exec: &str,
        fill_qty: u64,
        liquidity_ind: u8,
    ) -> OrderAckedEvent {
        OrderAckedEvent {
            liquidity_ind,
            ..acked(session, order, t3, t7, exec, fill_qty)
        }
    }

    /// sent performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn sent(
        session: &str,
        order: &str,
        t0: u64,
        t1: u64,
        r9: u64,
        timed_out: bool,
    ) -> OrderSentEvent {
        OrderSentEvent {
            session_id: session.into(),
            submission_id: "s-1".into(),
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
            // Explicitly SMP_ID_NONE, never a bare 0: zero is a VALID participant id as well
            // as Rust's default, so a synthetic event that omits this reads as "participant
            // 0" and self-crosses against every other order carrying that id.
            smp_id: SMP_ID_NONE,
        }
    }

    /// barrier_epoch_makes_wave_index_consumer_independent checks deterministic waves.
    /// It ensures distributed shards compute the same wave from barrier_epoch_ns
    /// without depending on which event each consumer sees first.
    #[test]
    fn barrier_epoch_makes_wave_index_consumer_independent() {
        let barrier = 1_000_000_000_000u64;
        let wave_ns = DEFAULT_WAVE_NS;
        let t0 = barrier + wave_ns + wave_ns / 2;
        let mut ev = sent("sess", "sess_0_0_O", t0, t0, 0, false);
        ev.barrier_epoch_ns = barrier;

        let mut a = Aggregator::new(wave_ns);
        let mut earlier = sent("sess", "sess_0_1_O", barrier + 10, barrier + 10, 0, false);
        earlier.barrier_epoch_ns = barrier;
        a.observe_sent(&earlier);
        a.observe_sent(&ev);

        let mut b = Aggregator::new(wave_ns);
        b.observe_sent(&ev);

        assert_eq!(a.wave_of("sess", t0), 1);
        assert_eq!(b.wave_of("sess", t0), 1);
        assert_eq!(a.wave_of("sess", t0), b.wave_of("sess", t0));
    }

    /// decode_count deserializes a V2-deflate HDR blob and returns its sample count
    /// (0 for an empty/never-recorded histogram).
    fn decode_count(blob: &[u8]) -> u64 {
        use hdrhistogram::serialization::Deserializer;
        if blob.is_empty() {
            return 0;
        }
        Deserializer::new()
            .deserialize::<u64, _>(&mut std::io::Cursor::new(blob))
            .map(|h| h.len())
            .unwrap_or(0)
    }

    #[test]
    /// match_latency_samples_taker_fills_only verifies the dedicated match-latency
    /// histogram counts ONLY taker fills (851=2), excludes maker/ack, and that the
    /// service_time histogram is untouched by the new path.
    fn match_latency_samples_taker_fills_only() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        // o1: ack (no liquidity) then a taker fill -> 1 match sample.
        a.observe_acked(&acked_liq("S", "o1", 1_000, 101_000, "0", 0, 0));
        a.observe_acked(&acked_liq("S", "o1", 1_000, 201_000, "2", 12, 2));
        // o2: ack then a maker fill -> 0 match samples (market wait, not engine time).
        a.observe_acked(&acked_liq("S", "o2", 1_000, 101_000, "0", 0, 0));
        a.observe_acked(&acked_liq("S", "o2", 1_000, 901_000, "2", 5, 1));

        let snaps = a.snapshot(1_000_000_000, 1.0);
        assert_eq!(snaps.len(), 1);
        let s = &snaps[0];
        // exactly the one taker fill counted into match_latency.
        assert_eq!(decode_count(&s.match_hdr_encoded), 1, "only the taker fill");
        // service_time still records one first-response per order (o1, o2) = 2,
        // proving the new path didn't disturb the existing histogram.
        assert_eq!(decode_count(&s.hdr_encoded), 2, "service_time unchanged");
    }

    #[test]
    /// service_time_recorded_once_per_order_first_response performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn service_time_recorded_once_per_order_first_response() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        a.observe_acked(&acked("S", "o1", 1_000, 101_000, "0", 0));
        a.observe_acked(&acked("S", "o1", 1_000, 201_000, "2", 12));
        let snaps = a.snapshot(1_000_000_000, 1.0);
        assert_eq!(snaps.len(), 1);
        let s = &snaps[0];
        assert_eq!(s.p50_ns, hist_q(100_000));
        assert_eq!(s.tps_1s, 1.0, "one order responded");
        assert_eq!(s.contestant_id, "c-1");
        assert!(!s.hdr_encoded.is_empty());
    }

    #[test]
    /// late_streaming_fill_not_rescored performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn late_streaming_fill_not_rescored() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        a.observe_acked(&acked("S", "o1", 0, 1_000_000_000, "0", 0));
        let _ = a.snapshot(1_500_000_000, 1.0);
        a.observe_acked(&acked("S", "o1", 0, 4_000_000_000, "1", 5));
        let _ = a.snapshot(6_000_000_000, 1.0);
        a.observe_acked(&acked("S", "o1", 0, 6_500_000_000, "2", 5));
        let snaps = a.snapshot(7_000_000_000, 1.0);
        let s = snaps
            .iter()
            .find(|s| s.session_id == "S")
            .expect("window snapshot present");
        assert_eq!(
            s.tps_1s, 0.0,
            "H11: a trailing fill of an already-scored order must not count as a new response"
        );
    }

    #[test]
    /// sent_only_window_emits_no_row performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn sent_only_window_emits_no_row() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        a.observe_sent(&sent("S2", "o1", 0, 10, 0, true));
        let snaps = a.snapshot(1_000_000_000, 1.0);
        assert!(
            snaps.iter().all(|s| s.session_id != "S2"),
            "M33: sent-only window must not emit a latency row"
        );
    }

    #[test]
    /// session_state_pruned_after_window_eviction performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn session_state_pruned_after_window_eviction() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        a.observe_acked(&acked("S3", "o1", 0, 1_000, "0", 0));
        assert_eq!(a.session_start.len(), 1);
        let _ = a.snapshot(WINDOW_IDLE_NS + 2_000_000_000, 1.0);
        assert_eq!(a.window_count(), 0, "window evicted");
        assert_eq!(a.session_start.len(), 0, "M29: session_start pruned");
        assert_eq!(
            a.session_contestant.len(),
            0,
            "M29: session_contestant pruned"
        );
    }

    #[test]
    /// wave_bucketing_by_elapsed_since_session_start performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn wave_bucketing_by_elapsed_since_session_start() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        let start = 1_000_000_000;
        a.observe_acked(&acked("S", "o1", start, start + 1_000, "0", 0));
        a.observe_acked(&acked(
            "S",
            "o2",
            start + 19_000_000_000,
            start + 19_000_001_000,
            "0",
            0,
        ));
        a.observe_acked(&acked(
            "S",
            "o3",
            start + 21_000_000_000,
            start + 21_000_001_000,
            "0",
            0,
        ));
        let mut snaps = a.snapshot(start + 30_000_000_000, 1.0);
        snaps.sort_by_key(|s| s.wave_index);
        assert_eq!(snaps.len(), 2);
        assert_eq!(snaps[0].wave_index, 0);
        assert_eq!(snaps[0].tps_1s, 2.0);
        assert_eq!(snaps[1].wave_index, 1);
        assert_eq!(snaps[1].tps_1s, 1.0);
    }

    #[test]
    /// error_rate_from_timeouts_and_rejects performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn error_rate_from_timeouts_and_rejects() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        a.observe_sent(&sent("S", "o1", 1000, 1100, 0, true));
        a.observe_sent(&sent("S", "o2", 1000, 1100, 5000, false));
        a.observe_sent(&sent("S", "o3", 1000, 1100, 5000, false));
        a.observe_sent(&sent("S", "o4", 1000, 1100, 5000, false));
        a.observe_acked(&acked("S", "o2", 1000, 2000, "8", 0));
        a.observe_acked(&acked("S", "o3", 1000, 2000, "0", 0));
        a.observe_acked(&acked("S", "o4", 1000, 2000, "0", 0));
        let snaps = a.snapshot(1_000_000_000, 1.0);
        assert_eq!(snaps.len(), 1);
        assert!((snaps[0].error_rate - 0.5).abs() < 1e-9);
    }

    // An order the load generator abandoned (client-side RESPONSE_TIMEOUT) carries
    // no response_time sample (observe_sent excludes timed_out). The pod can still
    // egress a late response, so eBPF emits an acked with a huge pod_service_time.
    // Recording that into service_time while response_time omits it makes the two
    // histograms cover different populations, so service p99 can exceed response p99
    // — impossible per order. The aggregator must exclude a timed-out order's ack
    // from service_time so both metrics see the same "answered in time" population.
    #[test]
    fn timed_out_order_excluded_from_service_time() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        // In-time order: completed sent (response_time = 100us) + 50us service.
        a.observe_sent(&sent("S", "good", 1_000, 1_000, 101_000, false));
        a.observe_acked(&acked("S", "good", 1_000, 51_000, "0", 0));
        // Abandoned order: timed_out sent seen first (as in production: the 5s
        // watchdog fires hundreds of ms before the straggler egresses), then a 10s ack.
        a.observe_sent(&sent("S", "slow", 2_000, 2_000, 0, true));
        a.observe_acked(&acked("S", "slow", 2_000, 10_000_002_000, "0", 0));

        let snaps = a.snapshot(1_000_000_000, 1.0);
        assert_eq!(snaps.len(), 1);
        let s = &snaps[0];
        assert_eq!(
            s.p99_ns,
            hist_q(50_000),
            "service_time must reflect only the in-time order, never the 10s straggler"
        );
        assert!(
            s.p99_ns <= s.rt_p99_ns,
            "service p99 ({}) must not exceed response p99 ({})",
            s.p99_ns,
            s.rt_p99_ns
        );
    }

    // Regression guard: for a NORMAL (non-timed-out) order the eBPF ack (egress) is
    // typically consumed before the worker's completed sent event (recv). Such an
    // order is not in the timed-out set, so service_time must still be recorded.
    #[test]
    fn ack_before_completed_sent_still_records_service() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        a.observe_acked(&acked("S", "o1", 1_000, 51_000, "0", 0));
        a.observe_sent(&sent("S", "o1", 1_000, 1_000, 51_500, false));
        let snaps = a.snapshot(1_000_000_000, 1.0);
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].p50_ns, hist_q(50_000));
    }

    // The timed-out marker is reclaimed once the order is older than the longest
    // recordable service window, so the set cannot grow unbounded across a run.
    #[test]
    fn timed_out_markers_are_evicted() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        a.observe_sent(&sent("S", "slow", 1_000, 1_000, 0, true));
        assert_eq!(a.timed_out_orders.len(), 1);
        // Keep at least one live window so the session isn't pruned wholesale.
        a.observe_acked(&acked("S", "live", HDR_MAX_NS, HDR_MAX_NS + 1_000, "0", 0));
        let _ = a.snapshot(HDR_MAX_NS + 2_000_000_000, 1.0);
        assert_eq!(
            a.timed_out_orders.len(),
            0,
            "stale timed-out marker evicted"
        );
    }

    #[test]
    /// percentiles_are_cumulative_across_snapshots_counters_reset performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn percentiles_are_cumulative_across_snapshots_counters_reset() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        a.observe_acked(&acked("S", "o1", 0, 100_000, "0", 0));
        let s1 = a.snapshot(1_000_000_000, 1.0);
        assert_eq!(s1[0].tps_1s, 1.0);
        let p99_after_one = s1[0].p99_ns;
        a.observe_acked(&acked("S", "o2", 0, 100_000, "0", 0));
        let s2 = a.snapshot(2_000_000_000, 1.0);
        assert_eq!(
            s2[0].tps_1s, 1.0,
            "tps counts only this interval's responses"
        );
        assert_eq!(
            s2[0].p99_ns, p99_after_one,
            "histogram is cumulative — same value"
        );
        assert_eq!(s2[0].p50_ns, p99_after_one);
    }

    #[test]
    /// idle_windows_are_evicted performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn idle_windows_are_evicted() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        a.observe_acked(&acked("S", "o1", 1000, 2000, "0", 0));
        a.snapshot(3000, 1.0);
        assert_eq!(a.window_count(), 1);
        a.snapshot(3000 + WINDOW_IDLE_NS + 1, 1.0);
        assert_eq!(a.window_count(), 0);
    }

    /// hist_q performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn hist_q(v: u64) -> u64 {
        let mut h = new_hist();
        let _ = h.record(v);
        h.value_at_quantile(0.5)
    }
}
