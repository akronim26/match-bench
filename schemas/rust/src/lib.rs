//! This module defines shared schema contracts for lib.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use serde::{Deserialize, Serialize};

pub const TOPIC_SUBMISSION_BUILD_REQUESTED: &str = "submission.build.requested";
pub const TOPIC_SUBMISSION_STATUS_UPDATED: &str = "submission.status.updated";
pub const TOPIC_BENCHMARK_REQUESTED: &str = "benchmark.requested";
pub const TOPIC_BENCHMARK_STATUS_UPDATED: &str = "benchmark.status.updated";
pub const TOPIC_WORKLOAD_ASSIGNMENTS: &str = "workload.assignments";
pub const TOPIC_BARRIER: &str = "barrier";
pub const TOPIC_BOT_READY: &str = "bot.ready";
pub const TOPIC_WORKLOAD_FAILED: &str = "workload.failed";
pub const TOPIC_ORDERS_SENT: &str = "orders.sent";
pub const TOPIC_ORDERS_ACKED: &str = "orders.acked";
pub const TOPIC_SCORES_CORRECTNESS: &str = "scores.correctness";
pub const TOPIC_LEADERBOARD_UPDATES: &str = "leaderboard.updates";
pub const TELEMETRY_PRICE_SCALE: u64 = 1_000_000_000;

/// Platform-mandated port for FIX. Contestants do not choose this; the eBPF
/// capture filter hardcodes it (see docs/tps-improvement-plan.md §7.3 port policy).
pub const PORT_FIX: u16 = 9898;
/// Platform-mandated port shared by REST and WS (WS upgrades from HTTP on the
/// same connection). See docs/tps-improvement-plan.md §7.3 port policy.
pub const PORT_HTTP_WS: u16 = 8080;

/// port_for_protocol returns the platform-mandated port for a protocol.
pub fn port_for_protocol(protocol: Protocol) -> u16 {
    match protocol {
        Protocol::Fix => PORT_FIX,
        Protocol::Rest | Protocol::Ws => PORT_HTTP_WS,
    }
}

/// fnv1a_64 hashes bytes with FNV-1a 64-bit. Shared by every partition-selection
/// function in this module so producers stay deterministically consistent.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// partition_for maps an order id onto the Kafka partition contract.
/// It uses FNV-1a 64-bit hashing so sent and acked producers select the same
/// partition deterministically for a given order id.
pub fn partition_for(order_id: &str, num_partitions: i32) -> i32 {
    debug_assert!(num_partitions > 0, "num_partitions must be positive");
    if num_partitions <= 1 {
        return 0;
    }
    (fnv1a_64(order_id.as_bytes()) % num_partitions as u64) as i32
}

/// Default width (in partitions) of a session's partition band, used when
/// BOT_PARTITION_BAND_WIDTH / ORDERS_PARTITION_BAND_WIDTH is unset.
///
/// 6 divides the 24 orders.sent/orders.acked partitions into exactly 4 EXCLUSIVE
/// bands (0-5, 6-11, 12-17, 18-23), one per concurrently-running session at the
/// controller's MAX_CONCURRENT_SESSIONS=4.
///
/// The invariant that matters: `MAX_CONCURRENT_SESSIONS <=
/// floor(orders_partitions / band_width)`. It was violated when this was 8 —
/// floor(24/8) = 3 distinct bands against 4 leased sessions, so band 3's
/// `base = 3*8 = 24` wrapped modulo 24 straight back onto band 0's partitions.
/// Producer and consumer agreed on the wraparound, so traffic was consistent but
/// NOT exclusive: two sessions shared partitions and band-scoped validation
/// isolation silently stopped holding at the 4th session.
///
/// MUST stay in lockstep with the validator's VALIDATOR_ORDER_BAND_WIDTH default
/// (services/correctness-validator/main.go): the bot writes into
/// `[band*width, band*width+width)` and the validator reads exactly that range, so
/// a mismatch makes the validator read partitions the bot never wrote.
pub const DEFAULT_PARTITION_BAND_WIDTH: i32 = 6;

/// session_band_partition maps (session_id, order_id) onto a Kafka partition.
///
/// DEPRECATED fallback: exclusive per-session bands are now controller-leased
/// (see `band_partition`); this hash-derived variant remains only so
/// band-unaware messages (unset `WorkloadSpec::order_band`, i.e.
/// `ORDER_BAND_UNSET`) keep working during rollout / back-compat.
///
/// The partition space is divided into bands of `band_width` partitions. A
/// session is pinned to one band (hash(session_id) mod num_bands), and its
/// orders are hashed within that band (hash(order_id) mod band_width). This
/// keeps a session's traffic concentrated on a handful of partitions (better
/// consumer batching / cache locality per session) while still spreading a
/// single busy session across `band_width` partitions instead of one.
///
/// CRITICAL invariant: this is a pure function of (session_id, order_id,
/// num_partitions, band_width) — orders.sent and orders.acked producers calling
/// it with the same session_id, order_id, num_partitions, and band_width always
/// land on the same partition for a given order.
pub fn session_band_partition(
    session_id: &str,
    order_id: &str,
    num_partitions: i32,
    band_width: i32,
) -> i32 {
    debug_assert!(num_partitions > 0, "num_partitions must be positive");
    if num_partitions <= 1 {
        return 0;
    }
    let band_width = band_width.clamp(1, num_partitions);
    // Ceiling division so a trailing partial band (num_partitions % band_width != 0)
    // still gets a band index and its partitions remain reachable.
    let num_bands = (num_partitions as u32).div_ceil(band_width as u32).max(1) as i32;
    let band = (fnv1a_64(session_id.as_bytes()) % num_bands as u64) as i32;
    let base = band * band_width;
    let within = (fnv1a_64(order_id.as_bytes()) % band_width as u64) as i32;
    (base + within) % num_partitions
}

/// Sentinel for `WorkloadSpec::order_band` meaning "unassigned — fall back to
/// hash-derived banding via `session_band_partition`". Chosen as `u32::MAX`
/// (mirrored in Go as `math.MaxUint32`) since valid bands are always small
/// (< `num_partitions`), leaving the max value permanently free as a sentinel
/// in both languages without needing a signed type or an `Option`/pointer on
/// the wire.
pub const ORDER_BAND_UNSET: u32 = u32::MAX;

/// Sentinel for `OrderSentEvent::smp_id` meaning "this order carried NO self-match
/// prevention id on the wire" — no FIX tag 7928, no `smp_id` JSON key. Such an order
/// is UNCONSTRAINED: an engine may match it against anything, including another order
/// with no id.
///
/// `u32::MAX` for the same reason as `ORDER_BAND_UNSET`: real ids are small
/// (`< smp_id_count`), so the max value is permanently free as a sentinel in both
/// languages. Critically it is NOT 0 — 0 is a valid SMP id, and conflating "absent"
/// with "id 0" would make every id-less pass-2 order look like one participant, which
/// under skip-and-continue would stop the engine matching anything at all.
pub const SMP_ID_NONE: u32 = u32::MAX;

fn smp_id_none() -> u32 {
    SMP_ID_NONE
}

/// band_partition maps (band, order_id) onto a Kafka partition using an
/// EXCLUSIVELY leased band (see `bot-fleet-controller`'s band lease
/// allocator), rather than a hash-derived one. `base = band * band_width`;
/// `order_id` is hashed only within `[base, base + band_width)`. Unlike
/// `session_band_partition`, two different sessions never share a band here
/// because the controller leases bands exclusively per session — this
/// function only does the within-band placement.
///
/// CRITICAL invariant: pure function of (band, order_id, num_partitions,
/// band_width) — orders.sent and orders.acked producers/consumers calling it
/// with the same inputs land on the same partition for a given order.
pub fn band_partition(band: u32, order_id: &str, num_partitions: i32, band_width: i32) -> i32 {
    debug_assert!(num_partitions > 0, "num_partitions must be positive");
    if num_partitions <= 1 {
        return 0;
    }
    let band_width = band_width.clamp(1, num_partitions);
    let base = (band as i64 * band_width as i64) as i32;
    let within = (fnv1a_64(order_id.as_bytes()) % band_width as u64) as i32;
    (base + within) % num_partitions
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
/// Protocol enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum Protocol {
    Fix,
    Rest,
    Ws,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
/// PayloadType enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum PayloadType {
    New,
    Cancel,
    Replace,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
/// OrdType enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum OrdType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
/// BotProfile enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum BotProfile {
    Hft,
    Retail,
    Institutional,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
/// TargetSpec names one protocol+port a workload can dispatch tasks to.
/// Ports are platform-mandated (see `port_for_protocol`), not contestant-chosen.
pub struct TargetSpec {
    pub protocol: Protocol,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// WorkloadSpec stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct WorkloadSpec {
    pub session_id: String,
    pub submission_id: String,
    #[serde(default)]
    pub contestant_id: String,
    pub target_host: String,
    pub target_port: u16,
    pub protocol: Protocol,
    #[serde(default)]
    pub targets: Vec<TargetSpec>,
    pub worker_index: u32,
    pub worker_count: u32,
    pub global_seed: u64,
    #[serde(default = "default_fix_version")]
    pub fix_version: String,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_write_timeout_ms")]
    pub write_timeout_ms: u64,
    #[serde(default)]
    pub barrier_epoch_ns: u64,
    /// published_at_unix_ns is the unix-nanosecond timestamp the controller
    /// stamped at publish time. Zero means "unset" (older/other producers
    /// that don't set it) and must be treated as NOT stale by consumers.
    #[serde(default)]
    pub published_at_unix_ns: u64,
    /// order_band is the exclusively-leased partition band index for this
    /// session's orders.sent/orders.acked traffic (band N covers partitions
    /// `[N*band_width, (N+1)*band_width)`). `ORDER_BAND_UNSET` (`u32::MAX`,
    /// serde default) means unassigned — fall back to hash-derived
    /// `session_band_partition` for back-compat with band-unaware producers.
    #[serde(default = "default_order_band")]
    pub order_band: u32,
    pub tasks: Vec<TaskSpec>,
}

/// default_order_band is the serde default for `WorkloadSpec::order_band`:
/// `ORDER_BAND_UNSET`, meaning "fall back to hash-derived banding."
fn default_order_band() -> u32 {
    ORDER_BAND_UNSET
}

impl WorkloadSpec {
    /// resolved_targets returns `targets` if populated, otherwise a single-entry
    /// vec built from the legacy `protocol`/`target_port` fields, so callers never
    /// have to special-case old messages.
    pub fn resolved_targets(&self) -> Vec<TargetSpec> {
        if !self.targets.is_empty() {
            return self.targets.clone();
        }
        vec![TargetSpec {
            protocol: self.protocol,
            port: self.target_port,
        }]
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// TaskSpec stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct TaskSpec {
    pub task_id: u32,
    pub profile: BotProfile,
    pub target_rps: u32,
    pub start_offset_ns: u64,
    pub duration_ns: u64,
    #[serde(default)]
    pub market_pct: u8,
    #[serde(default)]
    pub cancel_pct: u8,
    #[serde(default)]
    pub replace_pct: u8,
    /// Index into the owning WorkloadSpec's `targets` (or its single resolved
    /// legacy target when `targets` is empty). Defaults to 0.
    #[serde(default)]
    pub target_idx: u8,
    /// smp_id_count is how many distinct self-match-prevention ids this task
    /// rotates through (`smp = seq % smp_id_count`). The correctness scenario sets
    /// 8 so a single-connection run still produces cross-participant matching;
    /// scale scenarios leave it 0.
    ///
    /// 0 (and 1) mean "no SMP id": the bot omits the field from the wire entirely —
    /// no FIX tag 7928, no `smp_id` JSON key — so pass-2 frames stay byte-identical
    /// to pre-SMP output, and a contestant can distinguish "no id" from "id 0". An
    /// order without an SMP id is unconstrained and matches normally.
    #[serde(default)]
    pub smp_id_count: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// BarrierEvent stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct BarrierEvent {
    pub session_id: String,
    pub target_epoch_unix_nanos: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// ReadySignal stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct ReadySignal {
    pub session_id: String,
    pub submission_id: String,
    pub worker_id: String,
    pub worker_index: u32,
    pub worker_count: u32,
    pub task_count: u32,
    pub connected_count: u32,
    pub ready_at_unix_nanos: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// OrderSentEvent stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct OrderSentEvent {
    pub session_id: String,
    pub submission_id: String,
    pub worker_id: String,
    pub task_id: u32,
    pub order_id: String,
    pub target_send_ts_ns: u64, // t0
    pub send_ts_ns: u64,
    pub recv_done_ts_ns: u64,
    pub timed_out: bool,
    pub price: u64,
    pub qty: u64,
    pub side: Side,
    pub payload_type: PayloadType,
    pub ord_type: OrdType,
    #[serde(default)]
    pub orig_order_id: String,
    #[serde(default)]
    pub barrier_epoch_ns: u64,
    /// smp_id is the self-match-prevention id this order was sent under, as
    /// assigned by the bot. `SMP_ID_NONE` means the order carried no SMP id on the
    /// wire and is unconstrained. The validator reads it from HERE rather than from
    /// the eBPF capture: acked events are joined to sent events by order id, and the
    /// bot is the authority on what it assigned, so the capture never needs to parse
    /// FIX tag 7928.
    #[serde(default = "smp_id_none")]
    pub smp_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// OrderSentBatch stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct OrderSentBatch {
    pub session_id: String,
    pub worker_id: String,
    pub events: Vec<OrderSentEvent>,
}

/// OrderSentEventFields is the per-order wire payload for the positional
/// orders.sent batch envelope: session_id/submission_id/worker_id are hoisted
/// out to `OrderSentBatchV2` (identical across every event in a batch), so this
/// struct only carries fields that vary per order. Field order here IS the wire
/// contract for `rmp_serde::to_vec`/`from_slice` (positional msgpack) — producer
/// (bot-fleet) and consumer (telemetry-ingester) must keep it identical.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderSentEventFields {
    pub task_id: u32,
    pub order_id: String,
    pub target_send_ts_ns: u64,
    pub send_ts_ns: u64,
    pub recv_done_ts_ns: u64,
    pub timed_out: bool,
    pub price: u64,
    pub qty: u64,
    pub side: Side,
    pub payload_type: PayloadType,
    pub ord_type: OrdType,
    pub orig_order_id: String,
    pub barrier_epoch_ns: u64,
    /// APPENDED LAST on purpose: this struct is POSITIONAL msgpack, so field order
    /// is the wire contract and a new field may only go at the end.
    pub smp_id: u32,
}

/// OrderSentEventFieldsRef is the zero-copy encode-side mirror of
/// `OrderSentEventFields`. Field order MUST match it exactly.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct OrderSentEventFieldsRef<'a> {
    pub task_id: u32,
    pub order_id: &'a str,
    pub target_send_ts_ns: u64,
    pub send_ts_ns: u64,
    pub recv_done_ts_ns: u64,
    pub timed_out: bool,
    pub price: u64,
    pub qty: u64,
    pub side: Side,
    pub payload_type: PayloadType,
    pub ord_type: OrdType,
    pub orig_order_id: &'a str,
    pub barrier_epoch_ns: u64,
    pub smp_id: u32,
}

impl<'a> From<&'a OrderSentEvent> for OrderSentEventFieldsRef<'a> {
    fn from(e: &'a OrderSentEvent) -> Self {
        Self {
            task_id: e.task_id,
            order_id: &e.order_id,
            target_send_ts_ns: e.target_send_ts_ns,
            send_ts_ns: e.send_ts_ns,
            recv_done_ts_ns: e.recv_done_ts_ns,
            timed_out: e.timed_out,
            price: e.price,
            qty: e.qty,
            side: e.side,
            payload_type: e.payload_type,
            ord_type: e.ord_type,
            orig_order_id: &e.orig_order_id,
            barrier_epoch_ns: e.barrier_epoch_ns,
            smp_id: e.smp_id,
        }
    }
}

impl OrderSentEventFields {
    /// into_event reconstitutes a full `OrderSentEvent` by re-attaching the
    /// batch-level envelope fields hoisted out of the wire format.
    pub fn into_event(
        self,
        session_id: String,
        submission_id: String,
        worker_id: String,
    ) -> OrderSentEvent {
        OrderSentEvent {
            session_id,
            submission_id,
            worker_id,
            task_id: self.task_id,
            order_id: self.order_id,
            target_send_ts_ns: self.target_send_ts_ns,
            send_ts_ns: self.send_ts_ns,
            recv_done_ts_ns: self.recv_done_ts_ns,
            timed_out: self.timed_out,
            price: self.price,
            qty: self.qty,
            side: self.side,
            payload_type: self.payload_type,
            ord_type: self.ord_type,
            orig_order_id: self.orig_order_id,
            barrier_epoch_ns: self.barrier_epoch_ns,
            smp_id: self.smp_id,
        }
    }
}

/// OrderSentBatchV2 is the positional-msgpack wire envelope for orders.sent
/// batches: session_id/submission_id/worker_id are hoisted to the envelope
/// (identical across every event in the batch) instead of repeated per event.
/// Field order IS the wire contract — see `OrderSentEventFields`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderSentBatchV2 {
    pub session_id: String,
    pub submission_id: String,
    pub worker_id: String,
    pub events: Vec<OrderSentEventFields>,
}

impl OrderSentBatchV2 {
    /// into_events reconstitutes full `OrderSentEvent`s from the hoisted
    /// envelope, for consumers (e.g. the aggregator) that operate on the
    /// per-event struct.
    pub fn into_events(self) -> Vec<OrderSentEvent> {
        let OrderSentBatchV2 {
            session_id,
            submission_id,
            worker_id,
            events,
        } = self;
        events
            .into_iter()
            .map(|f| f.into_event(session_id.clone(), submission_id.clone(), worker_id.clone()))
            .collect()
    }
}

/// OrderSentBatchV2Ref is the zero-copy encode-side mirror of
/// `OrderSentBatchV2`. Field order MUST match it exactly.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct OrderSentBatchV2Ref<'a> {
    pub session_id: &'a str,
    pub submission_id: &'a str,
    pub worker_id: &'a str,
    pub events: &'a [OrderSentEventFieldsRef<'a>],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// OrderAckedEvent stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct OrderAckedEvent {
    pub session_id: String,
    pub contestant_id: String,
    pub order_id: String,
    pub src_ip: u32,
    pub src_port: u16,
    pub tcp_seq: u32,
    pub t3_xdp_ingress_ns: u64,
    pub t7_xdp_egress_ns: u64,
    pub pod_service_time_ns: u64,
    pub exec_type: String,
    pub fill_qty: u64,
    pub fill_price: u64,
    pub orig_order_id: String,
    pub reordering_detected: bool,
    pub retransmission_count: u32,
    #[serde(default)]
    pub liquidity_ind: u8,
}

#[derive(Debug, Clone, Copy, Serialize)]
/// OrderAckedEventRef stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct OrderAckedEventRef<'a> {
    pub session_id: &'a str,
    pub contestant_id: &'a str,
    pub order_id: &'a str,
    pub src_ip: u32,
    pub src_port: u16,
    pub tcp_seq: u32,
    pub t3_xdp_ingress_ns: u64,
    pub t7_xdp_egress_ns: u64,
    pub pod_service_time_ns: u64,
    pub exec_type: &'a str,
    pub fill_qty: u64,
    pub fill_price: u64,
    pub orig_order_id: &'a str,
    pub reordering_detected: bool,
    pub retransmission_count: u32,
    pub liquidity_ind: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// OrderAckedBatch stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct OrderAckedBatch {
    pub session_id: String,
    pub contestant_id: String,
    pub events: Vec<OrderAckedEvent>,
}

#[derive(Debug, Clone, Copy, Serialize)]
/// OrderAckedBatchRef stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct OrderAckedBatchRef<'a> {
    pub session_id: &'a str,
    pub contestant_id: &'a str,
    pub events: &'a [OrderAckedEventRef<'a>],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// CorrectnessScoreEvent stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct CorrectnessScoreEvent {
    pub session_id: String,
    pub contestant_id: String,
    pub valid_fills: u64,
    pub total_fills: u64,
    pub correctness_score: f64,
    pub violation_count: u64,
    pub computed_at_ns: u64,
    #[serde(default)]
    pub sent_count: u64,
    #[serde(default)]
    pub acked_count: u64,
    #[serde(default)]
    pub matched_count: u64,
    #[serde(default)]
    pub jitter_p50_us: f64,
    #[serde(default)]
    pub jitter_p99_us: f64,
    #[serde(default)]
    pub jitter_p999_us: f64,
    #[serde(default)]
    pub jitter_max_us: f64,
    #[serde(default)]
    pub jitter_inversion_rate: f64,
    #[serde(default)]
    pub t7_reorder_late: u64,
    #[serde(default)]
    pub t7_anomalies: u64,
    #[serde(default)]
    pub result_tainted: bool,
    #[serde(default)]
    pub taint_reason: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
/// Side enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum Side {
    Buy,
    Sell,
}

/// default_fix_version performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn default_fix_version() -> String {
    "FIX.4.2".to_string()
}

/// default_connect_timeout_ms performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn default_connect_timeout_ms() -> u64 {
    1500
}

/// default_write_timeout_ms performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn default_write_timeout_ms() -> u64 {
    250
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// topic_constants_match_platform_contract performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn topic_constants_match_platform_contract() {
        let topics = [
            TOPIC_SUBMISSION_BUILD_REQUESTED,
            TOPIC_SUBMISSION_STATUS_UPDATED,
            TOPIC_BENCHMARK_REQUESTED,
            TOPIC_BENCHMARK_STATUS_UPDATED,
            TOPIC_WORKLOAD_ASSIGNMENTS,
            TOPIC_BARRIER,
            TOPIC_BOT_READY,
            TOPIC_WORKLOAD_FAILED,
            TOPIC_ORDERS_SENT,
            TOPIC_ORDERS_ACKED,
            TOPIC_SCORES_CORRECTNESS,
            TOPIC_LEADERBOARD_UPDATES,
        ];

        for topic in topics {
            assert!(!topic.trim().is_empty());
            assert!(
                topic.bytes().all(|b| b.is_ascii_lowercase()
                    || b.is_ascii_digit()
                    || matches!(b, b'.' | b'-')),
                "topic {topic} contains unsupported characters"
            );
        }
    }

    #[test]
    /// workload_spec_decodes_go_controller_payload performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn workload_spec_decodes_go_controller_payload() {
        let payload = br#"{
            "session_id":"sess-1",
            "submission_id":"sub-1",
            "contestant_id":"team-1",
            "target_host":"algo-sess-1.sandbox.svc.cluster.local",
            "target_port":8080,
            "protocol":"FIX",
            "worker_index":0,
            "worker_count":1,
            "global_seed":42,
            "fix_version":"FIX.4.2",
            "connect_timeout_ms":1500,
            "write_timeout_ms":250,
            "tasks":[
                {"task_id":1,"profile":"hft","target_rps":50,"start_offset_ns":0,"duration_ns":1000000000}
            ]
        }"#;

        let spec: WorkloadSpec = serde_json::from_slice(payload).expect("decode workload spec");
        assert_eq!(spec.session_id, "sess-1");
        assert_eq!(spec.submission_id, "sub-1");
        assert_eq!(spec.protocol, Protocol::Fix);
        assert_eq!(spec.tasks.len(), 1);
        assert_eq!(spec.tasks[0].profile, BotProfile::Hft);
        assert_eq!(spec.tasks[0].target_idx, 0);
        assert!(spec.targets.is_empty());
        assert_eq!(
            spec.resolved_targets(),
            vec![TargetSpec {
                protocol: Protocol::Fix,
                port: 8080
            }]
        );
    }

    #[test]
    /// workload_spec_decodes_multi_target_payload verifies the Shape A schema:
    /// a targets table plus per-task target_idx round-trip and resolve directly
    /// without falling back to the legacy single protocol/port fields.
    fn workload_spec_decodes_multi_target_payload() {
        let payload = br#"{
            "session_id":"sess-1",
            "submission_id":"sub-1",
            "target_host":"algo-sess-1.sandbox.svc.cluster.local",
            "target_port":9898,
            "protocol":"FIX",
            "targets":[
                {"protocol":"FIX","port":9898},
                {"protocol":"REST","port":8080},
                {"protocol":"WS","port":8080}
            ],
            "worker_index":0,
            "worker_count":1,
            "global_seed":42,
            "tasks":[
                {"task_id":1,"profile":"hft","target_rps":50,"start_offset_ns":0,"duration_ns":1000000000,"target_idx":2}
            ]
        }"#;

        let spec: WorkloadSpec = serde_json::from_slice(payload).expect("decode workload spec");
        assert_eq!(spec.targets.len(), 3);
        assert_eq!(spec.tasks[0].target_idx, 2);
        assert_eq!(spec.resolved_targets(), spec.targets);
        assert_eq!(
            spec.targets[spec.tasks[0].target_idx as usize].protocol,
            Protocol::Ws
        );
    }

    #[test]
    /// port_for_protocol_matches_platform_policy pins the mandated port table
    /// so worker/orchestrator/eBPF stay in sync per docs/tps-improvement-plan.md §7.3.
    fn port_for_protocol_matches_platform_policy() {
        assert_eq!(port_for_protocol(Protocol::Fix), PORT_FIX);
        assert_eq!(port_for_protocol(Protocol::Rest), PORT_HTTP_WS);
        assert_eq!(port_for_protocol(Protocol::Ws), PORT_HTTP_WS);
        assert_eq!(PORT_FIX, 9898);
        assert_eq!(PORT_HTTP_WS, 8080);
    }

    #[test]
    /// correctness_score_event_decodes_go_validator_payload performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn correctness_score_event_decodes_go_validator_payload() {
        let payload = br#"{
            "session_id":"sess-1",
            "contestant_id":"team-1",
            "valid_fills":2,
            "total_fills":4,
            "correctness_score":0.5,
            "violation_count":2,
            "computed_at_ns":123,
            "sent_count":1000,
            "acked_count":950,
            "matched_count":940
        }"#;

        let ev: CorrectnessScoreEvent =
            serde_json::from_slice(payload).expect("decode correctness score event");
        assert_eq!(ev.session_id, "sess-1");
        assert_eq!(ev.contestant_id, "team-1");
        assert_eq!(ev.valid_fills, 2);
        assert_eq!(ev.total_fills, 4);
        assert_eq!(ev.correctness_score, 0.5);
        assert_eq!(ev.violation_count, 2);
        assert_eq!(ev.computed_at_ns, 123);
        assert_eq!(ev.sent_count, 1000);
        assert_eq!(ev.acked_count, 950);
        assert_eq!(ev.matched_count, 940);

        let legacy = br#"{
            "session_id":"sess-1",
            "contestant_id":"team-1",
            "valid_fills":2,
            "total_fills":4,
            "correctness_score":0.5,
            "violation_count":2,
            "computed_at_ns":123
        }"#;
        let ev: CorrectnessScoreEvent =
            serde_json::from_slice(legacy).expect("decode legacy correctness score event");
        assert_eq!(ev.sent_count, 0);
        assert_eq!(ev.acked_count, 0);
        assert_eq!(ev.matched_count, 0);
        assert_eq!(ev.jitter_p50_us, 0.0);
        assert_eq!(ev.jitter_max_us, 0.0);
        assert_eq!(ev.jitter_inversion_rate, 0.0);

        let with_jitter = br#"{
            "session_id":"sess-1",
            "contestant_id":"team-1",
            "valid_fills":2,
            "total_fills":4,
            "correctness_score":0.5,
            "violation_count":2,
            "computed_at_ns":123,
            "jitter_p50_us":12.5,
            "jitter_p99_us":90.0,
            "jitter_p999_us":150.0,
            "jitter_max_us":800.0,
            "jitter_inversion_rate":0.02
        }"#;
        let ev: CorrectnessScoreEvent = serde_json::from_slice(with_jitter)
            .expect("decode correctness score event with jitter");
        assert_eq!(ev.jitter_p50_us, 12.5);
        assert_eq!(ev.jitter_p99_us, 90.0);
        assert_eq!(ev.jitter_p999_us, 150.0);
        assert_eq!(ev.jitter_max_us, 800.0);
        assert_eq!(ev.jitter_inversion_rate, 0.02);
    }

    #[test]
    /// ready_signal_encodes_go_controller_fields performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn ready_signal_encodes_go_controller_fields() {
        let signal = ReadySignal {
            session_id: "sess-1".into(),
            submission_id: "sub-1".into(),
            worker_id: "worker-1".into(),
            worker_index: 0,
            worker_count: 1,
            task_count: 10,
            connected_count: 10,
            ready_at_unix_nanos: 123,
        };

        let value = serde_json::to_value(signal).expect("encode ready signal");
        assert_eq!(value["session_id"], "sess-1");
        assert_eq!(value["submission_id"], "sub-1");
        assert_eq!(value["worker_id"], "worker-1");
        assert_eq!(value["task_count"], 10);
        assert_eq!(value["ready_at_unix_nanos"], 123);
    }

    /// partition_for_is_in_range_and_deterministic checks range and stability.
    /// It protects the producer co-partition contract used by sent and acked
    /// telemetry topics.
    #[test]
    fn partition_for_is_in_range_and_deterministic() {
        let n = 24;
        for i in 0..10_000u32 {
            let oid = format!("01890dd2-71f3-7abc-9def-0123456789ab_{}_{}_O", i % 200, i);
            let p = partition_for(&oid, n);
            assert!((0..n).contains(&p), "partition {p} out of range for {oid}");
            assert_eq!(p, partition_for(&oid, n));
        }
    }

    /// partition_for_same_order_id_same_partition checks repeated hash identity.
    /// It also verifies that a sample of ids spreads across multiple partitions.
    #[test]
    fn partition_for_same_order_id_same_partition() {
        let oid = "01890dd2-71f3-7abc-9def-0123456789ab_42_99_O";
        assert_eq!(partition_for(oid, 24), partition_for(oid, 24));
        let mut seen = std::collections::HashSet::new();
        for i in 0..1000 {
            seen.insert(partition_for(&format!("ord_{i}"), 24));
        }
        assert!(
            seen.len() > 10,
            "hash spreads poorly: only {} partitions used",
            seen.len()
        );
    }

    /// partition_for_handles_single_partition checks the degenerate partition case.
    /// It ensures single-partition topics always map to partition zero.
    #[test]
    fn partition_for_handles_single_partition() {
        assert_eq!(partition_for("anything", 1), 0);
    }

    /// session_band_partition_same_order_same_partition pins the CRITICAL cross-topic
    /// invariant: orders.sent and orders.acked producers computing the partition for
    /// the same (session_id, order_id) with the same (num_partitions, band_width)
    /// must land on the same partition, since it's the same pure function.
    #[test]
    fn session_band_partition_same_order_same_partition() {
        let session = "01890dd2-71f3-7abc-9def-0123456789ab";
        let order = "01890dd2-71f3-7abc-9def-0123456789ab_42_99_O";
        let sent_side = session_band_partition(session, order, 24, 8);
        let acked_side = session_band_partition(session, order, 24, 8);
        assert_eq!(sent_side, acked_side);
        assert!((0..24).contains(&sent_side));
    }

    /// session_band_partition_confines_session_to_its_band checks that every order
    /// belonging to a session lands within that session's band, not spread across
    /// the whole partition space.
    #[test]
    fn session_band_partition_confines_session_to_its_band() {
        let n = 24;
        let band_width = 8;
        let session = "sess-abc";
        let base = session_band_partition(session, "anchor_order", n, band_width);
        let band_start = (base / band_width) * band_width;
        let mut seen = std::collections::HashSet::new();
        for i in 0..500 {
            let p = session_band_partition(session, &format!("order_{i}"), n, band_width);
            assert!(
                (band_start..band_start + band_width).contains(&p),
                "partition {p} escaped session band [{band_start}, {})",
                band_start + band_width
            );
            seen.insert(p);
        }
        assert!(
            seen.len() > 1,
            "a busy session should still spread within its band"
        );
    }

    /// session_band_partition_spreads_sessions_across_bands checks different
    /// sessions land in different bands (not all funneled to one).
    #[test]
    fn session_band_partition_spreads_sessions_across_bands() {
        let n = 24;
        let band_width = 8;
        let mut seen = std::collections::HashSet::new();
        for i in 0..50 {
            let session = format!("sess-{i}");
            let base = session_band_partition(&session, "order_0", n, band_width);
            seen.insert(base / band_width);
        }
        assert!(
            seen.len() > 1,
            "sessions must spread across more than one band"
        );
    }

    /// session_band_partition_reaches_all_partitions_with_uneven_band_width is a
    /// property test pinning the div_ceil fix: when num_partitions is not a
    /// multiple of band_width, integer-division band counting silently drops the
    /// trailing `num_partitions % band_width` partitions (they're never selected
    /// for any session/order id). With ceiling division every partition in
    /// 0..num_partitions must be reachable across a large sample of session ids.
    #[test]
    fn session_band_partition_reaches_all_partitions_with_uneven_band_width() {
        // A small xorshift PRNG so this test has no external rand dependency.
        fn xorshift(mut x: u64) -> u64 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        }

        let cases: &[(i32, i32)] = &[(24, 5), (17, 4), (100, 7), (13, 3), (50, 6), (7, 4)];
        for &(num_partitions, band_width) in cases {
            assert_ne!(
                num_partitions % band_width,
                0,
                "test case {num_partitions}/{band_width} must be uneven to exercise the fix"
            );
            let mut seen = std::collections::HashSet::new();
            let mut seed: u64 =
                0x9e3779b97f4a7c15 ^ (num_partitions as u64) ^ ((band_width as u64) << 32);
            for _ in 0..5000 {
                seed = xorshift(seed);
                let session = format!("sess-{seed}");
                let order = format!("order-{}", xorshift(seed));
                let p = session_band_partition(&session, &order, num_partitions, band_width);
                assert!(
                    (0..num_partitions).contains(&p),
                    "partition {p} out of range for num_partitions={num_partitions}"
                );
                seen.insert(p);
            }
            assert_eq!(
                seen.len(),
                num_partitions as usize,
                "num_partitions={num_partitions} band_width={band_width}: expected all partitions \
                 reachable, only hit {seen:?}"
            );
        }
    }

    /// band_partition_same_order_same_partition pins the CRITICAL cross-topic
    /// invariant for leased bands: sent and acked producers computing the
    /// partition for the same (band, order_id, num_partitions, band_width)
    /// land on the same partition.
    #[test]
    fn band_partition_same_order_same_partition() {
        let order = "01890dd2-71f3-7abc-9def-0123456789ab_42_99_O";
        let sent_side = band_partition(2, order, 24, 6);
        let acked_side = band_partition(2, order, 24, 6);
        assert_eq!(sent_side, acked_side);
        assert!((0..24).contains(&sent_side));
    }

    /// band_partition_confines_band_to_its_range checks every order for a given
    /// band lands within that band's exact partition range, never touching a
    /// partition belonging to a different (concurrently leased) band.
    #[test]
    fn band_partition_confines_band_to_its_range() {
        let n = 24;
        let band_width = 6;
        for band in 0..4u32 {
            let band_start = band as i32 * band_width;
            for i in 0..500 {
                let p = band_partition(band, &format!("order_{i}"), n, band_width);
                assert!(
                    (band_start..band_start + band_width).contains(&p),
                    "partition {p} escaped band {band} range [{band_start}, {})",
                    band_start + band_width
                );
            }
        }
    }

    /// band_partition_exclusive_across_leased_bands checks that the four
    /// exclusive bands (0-5, 6-11, 12-17, 18-23) never overlap in partition
    /// range, which is the whole point of leasing bands instead of hashing
    /// sessions into them.
    #[test]
    fn band_partition_exclusive_across_leased_bands() {
        let n = 24;
        let band_width = 6;
        let mut ranges = Vec::new();
        for band in 0..4u32 {
            let mut seen = std::collections::HashSet::new();
            for i in 0..1000 {
                seen.insert(band_partition(
                    band,
                    &format!("o_{band}_{i}"),
                    n,
                    band_width,
                ));
            }
            ranges.push(seen);
        }
        for i in 0..ranges.len() {
            for j in (i + 1)..ranges.len() {
                assert!(
                    ranges[i].is_disjoint(&ranges[j]),
                    "band {i} and band {j} partitions overlap: {:?} vs {:?}",
                    ranges[i],
                    ranges[j]
                );
            }
        }
    }

    /// The band arithmetic must satisfy
    /// `MAX_CONCURRENT_SESSIONS <= floor(orders_partitions / DEFAULT_PARTITION_BAND_WIDTH)`,
    /// or leased bands alias onto each other and "exclusive band" stops being true.
    ///
    /// This asserts against the SHIPPED constant rather than a literal. The neighbouring
    /// `band_partition_exclusive_across_leased_bands` hardcodes `band_width = 6` and so
    /// kept passing while production ran width 8 — floor(24/8) = 3 bands against 4 leased
    /// sessions, wrapping band 3 back onto band 0. A test that restates the intended
    /// value cannot catch the constant drifting away from it.
    #[test]
    fn default_band_width_yields_enough_exclusive_bands_for_max_concurrent_sessions() {
        // Both values are contracts with services outside this crate: 24 is the
        // orders.sent/orders.acked partition count created by
        // k8s/data/kafka/topic-init-job.yaml and defaulted in bot-fleet's config.rs;
        // 4 is MAX_CONCURRENT_SESSIONS in bot-fleet-controller/main.go.
        const ORDERS_PARTITIONS: i32 = 24;
        const MAX_CONCURRENT_SESSIONS: i32 = 4;

        let distinct_bands = ORDERS_PARTITIONS / DEFAULT_PARTITION_BAND_WIDTH;
        assert!(
            distinct_bands >= MAX_CONCURRENT_SESSIONS,
            "DEFAULT_PARTITION_BAND_WIDTH={DEFAULT_PARTITION_BAND_WIDTH} over \
             {ORDERS_PARTITIONS} partitions yields only {distinct_bands} exclusive bands, \
             but the controller leases {MAX_CONCURRENT_SESSIONS} concurrent sessions — \
             band {distinct_bands} would wrap onto band 0's partitions"
        );

        // And prove it concretely: every leased band's partition set is disjoint from
        // every other's, using the real constant.
        let mut ranges = Vec::new();
        for band in 0..MAX_CONCURRENT_SESSIONS as u32 {
            let mut seen = std::collections::HashSet::new();
            for i in 0..500 {
                seen.insert(band_partition(
                    band,
                    &format!("o_{band}_{i}"),
                    ORDERS_PARTITIONS,
                    DEFAULT_PARTITION_BAND_WIDTH,
                ));
            }
            ranges.push(seen);
        }
        for i in 0..ranges.len() {
            for j in (i + 1)..ranges.len() {
                assert!(
                    ranges[i].is_disjoint(&ranges[j]),
                    "band {i} and band {j} overlap at the shipped default width: {:?} vs {:?}",
                    ranges[i],
                    ranges[j]
                );
            }
        }
    }

    /// order_band_unset_sentinel_defaults_and_round_trips checks the WorkloadSpec
    /// back-compat contract: an old payload without `order_band` decodes to the
    /// unset sentinel, and the sentinel round-trips through JSON.
    #[test]
    fn order_band_unset_sentinel_defaults_and_round_trips() {
        let payload = br#"{
            "session_id":"sess-1",
            "submission_id":"sub-1",
            "target_host":"algo-sess-1.sandbox.svc.cluster.local",
            "target_port":8080,
            "protocol":"FIX",
            "worker_index":0,
            "worker_count":1,
            "global_seed":42,
            "tasks":[]
        }"#;
        let spec: WorkloadSpec = serde_json::from_slice(payload).expect("decode workload spec");
        assert_eq!(spec.order_band, ORDER_BAND_UNSET);

        let value = serde_json::to_value(&spec).expect("encode");
        assert_eq!(value["order_band"], ORDER_BAND_UNSET);
    }

    /// order_sent_batch_v2_positional_round_trips_field_order pins the wire contract
    /// for the hoisted-envelope, positional-msgpack orders.sent format: bot-fleet's
    /// telemetry.rs encodes `OrderSentBatchV2Ref` with `rmp_serde::to_vec` (positional)
    /// and telemetry-ingester decodes it as `OrderSentBatchV2`. Field order here IS
    /// the cross-crate contract; a mismatch decodes garbage instead of erroring.
    #[test]
    fn order_sent_batch_v2_positional_round_trips_field_order() {
        let events = [OrderSentEvent {
            session_id: "sess-1".into(),
            submission_id: "sub-1".into(),
            worker_id: "worker-1".into(),
            task_id: 7,
            order_id: "sess-1_7_3_O".into(),
            target_send_ts_ns: 100,
            send_ts_ns: 110,
            recv_done_ts_ns: 900,
            timed_out: false,
            price: 10_000,
            qty: 25,
            side: Side::Buy,
            payload_type: PayloadType::New,
            ord_type: OrdType::Limit,
            orig_order_id: String::new(),
            barrier_epoch_ns: 1_770_000_000_000_000_000,
            smp_id: SMP_ID_NONE,
        }];
        let event_refs: Vec<OrderSentEventFieldsRef> =
            events.iter().map(OrderSentEventFieldsRef::from).collect();
        let batch_ref = OrderSentBatchV2Ref {
            session_id: "sess-1",
            submission_id: "sub-1",
            worker_id: "worker-1",
            events: &event_refs,
        };

        let bytes = rmp_serde::to_vec(&batch_ref).expect("positional encode");
        let decoded: OrderSentBatchV2 = rmp_serde::from_slice(&bytes).expect("positional decode");

        assert_eq!(decoded.session_id, "sess-1");
        assert_eq!(decoded.submission_id, "sub-1");
        assert_eq!(decoded.worker_id, "worker-1");
        assert_eq!(decoded.events.len(), 1);
        assert_eq!(decoded.events[0].order_id, "sess-1_7_3_O");
        assert_eq!(
            decoded.events[0].barrier_epoch_ns,
            1_770_000_000_000_000_000
        );

        let reconstituted = decoded.into_events();
        assert_eq!(reconstituted[0].session_id, events[0].session_id);
        assert_eq!(reconstituted[0].submission_id, events[0].submission_id);
        assert_eq!(reconstituted[0].worker_id, events[0].worker_id);
        assert_eq!(reconstituted[0].order_id, events[0].order_id);
    }

    /// order_sent_event_barrier_epoch_round_trips checks msgpack compatibility.
    /// It ensures barrier_epoch_ns survives the named-field wire format.
    #[test]
    fn order_sent_event_barrier_epoch_round_trips() {
        let ev = OrderSentEvent {
            session_id: "s".into(),
            submission_id: "sub".into(),
            worker_id: "w".into(),
            task_id: 1,
            order_id: "s_1_2_O".into(),
            target_send_ts_ns: 100,
            send_ts_ns: 110,
            recv_done_ts_ns: 0,
            timed_out: false,
            price: 1,
            qty: 1,
            side: Side::Buy,
            payload_type: PayloadType::New,
            ord_type: OrdType::Limit,
            orig_order_id: String::new(),
            barrier_epoch_ns: 1_770_000_000_000_000_000,
            smp_id: SMP_ID_NONE,
        };
        let bytes = rmp_serde::to_vec_named(&ev).expect("encode");
        let back: OrderSentEvent = rmp_serde::from_slice(&bytes).expect("decode");
        assert_eq!(back.barrier_epoch_ns, ev.barrier_epoch_ns);
        assert_eq!(back.order_id, ev.order_id);
    }

    /// order_sent_event_decodes_pre_field_message checks rolling upgrade safety.
    /// It decodes an old producer payload without barrier_epoch_ns and verifies
    /// the new field defaults cleanly.
    #[test]
    fn order_sent_event_decodes_pre_field_message() {
        #[derive(Serialize)]
        struct OldOrderSentEvent {
            session_id: String,
            submission_id: String,
            worker_id: String,
            task_id: u32,
            order_id: String,
            target_send_ts_ns: u64,
            send_ts_ns: u64,
            recv_done_ts_ns: u64,
            timed_out: bool,
            price: u64,
            qty: u64,
            side: Side,
            payload_type: PayloadType,
            ord_type: OrdType,
            orig_order_id: String,
        }
        let old = OldOrderSentEvent {
            session_id: "s".into(),
            submission_id: "sub".into(),
            worker_id: "w".into(),
            task_id: 1,
            order_id: "s_1_2_O".into(),
            target_send_ts_ns: 100,
            send_ts_ns: 110,
            recv_done_ts_ns: 0,
            timed_out: false,
            price: 1,
            qty: 1,
            side: Side::Buy,
            payload_type: PayloadType::New,
            ord_type: OrdType::Limit,
            orig_order_id: String::new(),
        };
        let bytes = rmp_serde::to_vec_named(&old).expect("encode old");
        let back: OrderSentEvent = rmp_serde::from_slice(&bytes).expect("decode into new");
        assert_eq!(back.barrier_epoch_ns, 0, "missing field must default to 0");
        assert_eq!(back.order_id, "s_1_2_O");
    }
}
