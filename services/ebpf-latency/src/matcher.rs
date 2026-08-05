//! This module implements matcher behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

// FxHashMap, not std's SipHash map: the only keys ever INSERTED are
// bot-generated ClOrdIDs (contestant-supplied ids on responses that match
// nothing are never stored), so hash-flooding is not a threat model here and
// the DoS-resistant hash is pure cost on the per-record hot path.
use rustc_hash::FxHashMap;

pub const DEFAULT_IDLE_NS: u64 = 5_000_000_000;
const MAX_INFLIGHT: usize = 1_000_000;
/// Upper bound on a ClOrdID the matcher will track. Bot-generated ids are
/// `{session}_{task}_{seq}_{K}` — ~40 bytes worst case — so 64 covers them
/// with margin. Anything longer is NOT a bot order (the request side of the
/// capture only carries bot traffic), so it is counted and dropped rather
/// than tracked.
const MAX_KEY_LEN: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Key is a ClOrdID inlined into a fixed-width array. The matcher previously
/// keyed `HashMap<String, _>`, which allocated on every insert and hashed
/// through a heap pointer on every lookup — on the hot path, once per capture
/// record. Zero-padded bytes plus an explicit length make Eq total without
/// any heap involvement.
struct Key {
    len: u8,
    bytes: [u8; MAX_KEY_LEN],
}

/// Hash covers only the filled prefix. A derived Hash would SipHash all 64
/// bytes — measurably slower than the String key it replaces (+18% on the
/// matcher bench). Equal keys have equal (len, prefix), so this agrees with
/// the derived Eq over the zero-padded array.
impl std::hash::Hash for Key {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(&self.bytes[..usize::from(self.len)]);
    }
}

impl Key {
    /// new returns None when the id exceeds MAX_KEY_LEN — the caller counts
    /// those instead of tracking them.
    fn new(clordid: &str) -> Option<Self> {
        let s = clordid.as_bytes();
        if s.len() > MAX_KEY_LEN {
            return None;
        }
        let mut bytes = [0u8; MAX_KEY_LEN];
        bytes[..s.len()].copy_from_slice(s);
        Some(Self {
            len: s.len() as u8,
            bytes,
        })
    }
}

#[derive(Debug, Clone)]
/// Inflight stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct Inflight {
    t3_ns: u64,
    /// responded: at least one response has been matched to this request. Eviction of a
    /// responded entry is routine cleanup; eviction of an UNRESPONDED one means the
    /// request is gone before its response arrived, and any response that shows up later
    /// is dropped as unmatched.
    responded: bool,
    client_ip: u32,
    client_port: u16,
    tcp_seq: u32,
    retransmission_count: u32,
    reordering_detected: bool,
    last_activity_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// MatchedEvent stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct MatchedEvent {
    pub order_id: String,
    pub src_ip: u32,
    pub src_port: u16,
    pub tcp_seq: u32,
    pub t3_ns: u64,
    pub t7_ns: u64,
    pub pod_service_time_ns: u64,
    pub exec_type: String,
    pub fill_qty: u64,
    pub fill_price: u64,
    pub orig_order_id: String,
    pub reordering_detected: bool,
    pub retransmission_count: u32,
    /// FIX LastLiquidityInd (851): 0 unknown, 1 maker, 2 taker. Pass-through from parse.
    pub liquidity_ind: u8,
}

#[derive(Debug, Default)]
/// Matcher stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct Matcher {
    inflight: FxHashMap<Key, Inflight>,
    pub unmatched_responses: u64,
    /// oversized_clordid: ids longer than MAX_KEY_LEN seen on either side.
    /// Requests carry only bot-generated ids (bounded ~40 bytes), so nonzero
    /// here means a fabricated or corrupted id, never a lost bot order.
    pub oversized_clordid: u64,
    /// evicted_idle_unanswered: requests dropped after DEFAULT_IDLE_NS that had never
    /// been responded to. A response arriving after this is unmatchable and discarded, so
    /// this is a real loss path. Evictions of ANSWERED entries are excluded — on_response
    /// leaves the entry in place, so idle eviction is also the matcher's only garbage
    /// collection, and counting those made routine cleanup look like data loss.
    pub evicted_idle_unanswered: u64,
    /// evicted_capacity: requests dropped because the inflight map hit MAX_INFLIGHT.
    pub evicted_capacity: u64,
}

impl Matcher {
    /// new performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn new() -> Self {
        Self::default()
    }

    /// on_request performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn on_request(
        &mut self,
        clordid: &str,
        t3_ns: u64,
        client_ip: u32,
        client_port: u16,
        tcp_seq: u32,
        reordered: bool,
    ) {
        if clordid.is_empty() {
            return;
        }
        let Some(key) = Key::new(clordid) else {
            self.oversized_clordid = self.oversized_clordid.saturating_add(1);
            return;
        };
        match self.inflight.get_mut(&key) {
            Some(existing) => {
                existing.retransmission_count = existing.retransmission_count.saturating_add(1);
                existing.reordering_detected |= reordered;
                existing.last_activity_ns = existing.last_activity_ns.max(t3_ns);
            }
            None => {
                if self.inflight.len() >= MAX_INFLIGHT {
                    self.evict_oldest();
                }
                self.inflight.insert(
                    key,
                    Inflight {
                        t3_ns,
                        responded: false,
                        client_ip,
                        client_port,
                        tcp_seq,
                        retransmission_count: 0,
                        reordering_detected: reordered,
                        last_activity_ns: t3_ns,
                    },
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    /// on_response performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn on_response(
        &mut self,
        clordid: &str,
        t7_ns: u64,
        exec_type: &str,
        fill_qty: u64,
        fill_price: u64,
        orig_order_id: &str,
        reordered: bool,
        liquidity_ind: u8,
    ) -> Option<MatchedEvent> {
        let Some(key) = Key::new(clordid) else {
            // Cannot have been tracked (on_request refuses the same length),
            // so this is an unmatched response like any other, plus the
            // oversized marker for diagnosis.
            self.oversized_clordid = self.oversized_clordid.saturating_add(1);
            self.unmatched_responses = self.unmatched_responses.saturating_add(1);
            return None;
        };
        let Some(inflight) = self.inflight.get_mut(&key) else {
            self.unmatched_responses = self.unmatched_responses.saturating_add(1);
            return None;
        };
        inflight.reordering_detected |= reordered;
        inflight.responded = true;
        inflight.last_activity_ns = inflight.last_activity_ns.max(t7_ns);
        let pod_service_time_ns = t7_ns.saturating_sub(inflight.t3_ns);
        Some(MatchedEvent {
            order_id: clordid.to_string(),
            src_ip: inflight.client_ip,
            src_port: inflight.client_port,
            tcp_seq: inflight.tcp_seq,
            t3_ns: inflight.t3_ns,
            t7_ns,
            pod_service_time_ns,
            exec_type: exec_type.to_string(),
            fill_qty,
            fill_price,
            orig_order_id: orig_order_id.to_string(),
            reordering_detected: inflight.reordering_detected,
            retransmission_count: inflight.retransmission_count,
            liquidity_ind,
        })
    }

    /// evict_idle performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn evict_idle(&mut self, now_ns: u64, idle_ns: u64) -> usize {
        let before = self.inflight.len();
        let mut unanswered = 0u64;
        self.inflight.retain(|_, v| {
            let keep = now_ns.saturating_sub(v.last_activity_ns) < idle_ns;
            if !keep && !v.responded {
                unanswered += 1;
            }
            keep
        });
        self.evicted_idle_unanswered = self.evicted_idle_unanswered.saturating_add(unanswered);
        before - self.inflight.len()
    }

    /// evict_oldest performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn evict_oldest(&mut self) {
        self.evicted_capacity = self.evicted_capacity.saturating_add(1);
        if let Some(key) = self
            .inflight
            .iter()
            .min_by_key(|(_, v)| v.last_activity_ns)
            .map(|(k, _)| *k)
        {
            self.inflight.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// two_responses_per_order_emit_two_events_sharing_t3 performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn two_responses_per_order_emit_two_events_sharing_t3() {
        let mut m = Matcher::new();
        m.on_request("o1", 100, 0x0a00_0001, 50000, 1, false);

        let ack = m.on_response("o1", 200, "0", 0, 0, "", false, 0).unwrap();
        let fill = m
            .on_response("o1", 350, "2", 12, 42_500_000_000, "", false, 2)
            .unwrap();

        assert_eq!(ack.t3_ns, 100);
        assert_eq!(fill.t3_ns, 100);
        assert_eq!(ack.t7_ns, 200);
        assert_eq!(fill.t7_ns, 350);
        assert_eq!(ack.exec_type, "0");
        assert_eq!(fill.exec_type, "2");
        assert_eq!(ack.pod_service_time_ns, 100);
        assert_eq!(fill.pod_service_time_ns, 250);
        assert_eq!(fill.fill_qty, 12);
    }

    #[test]
    /// liquidity_ind_passes_through verifies the FIX 851 liquidity indicator is
    /// carried verbatim onto the MatchedEvent (taker=2, maker=1, ack=0).
    fn liquidity_ind_passes_through() {
        let mut m = Matcher::new();
        m.on_request("t", 100, 1, 5, 10, false);
        m.on_request("k", 100, 1, 5, 11, false);
        let ack = m.on_response("t", 150, "0", 0, 0, "", false, 0).unwrap();
        let taker = m.on_response("t", 200, "2", 5, 0, "", false, 2).unwrap();
        let maker = m.on_response("k", 900, "2", 5, 0, "", false, 1).unwrap();
        assert_eq!(ack.liquidity_ind, 0);
        assert_eq!(taker.liquidity_ind, 2);
        assert_eq!(maker.liquidity_ind, 1);
    }

    #[test]
    /// per_clordid_isolation_across_pipelined_orders performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn per_clordid_isolation_across_pipelined_orders() {
        let mut m = Matcher::new();
        m.on_request("A", 100, 1, 5, 10, false);
        m.on_request("B", 130, 1, 5, 20, false);

        let rb = m.on_response("B", 300, "2", 1, 0, "", false, 2).unwrap();
        let ra = m.on_response("A", 320, "2", 1, 0, "", false, 2).unwrap();

        assert_eq!(rb.t3_ns, 130);
        assert_eq!(ra.t3_ns, 100);
        assert_eq!(rb.pod_service_time_ns, 170);
        assert_eq!(ra.pod_service_time_ns, 220);
    }

    #[test]
    /// response_without_request_is_unmatched performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn response_without_request_is_unmatched() {
        let mut m = Matcher::new();
        assert!(m
            .on_response("ghost", 200, "0", 0, 0, "", false, 0)
            .is_none());
        assert_eq!(m.unmatched_responses, 1);
    }

    #[test]
    /// duplicate_request_bumps_retransmission_and_keeps_first_t3 performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn duplicate_request_bumps_retransmission_and_keeps_first_t3() {
        let mut m = Matcher::new();
        m.on_request("o1", 100, 1, 5, 10, false);
        m.on_request("o1", 175, 1, 5, 10, false);
        let e = m.on_response("o1", 200, "0", 0, 0, "", false, 0).unwrap();
        assert_eq!(e.t3_ns, 100);
        assert_eq!(e.retransmission_count, 1);
    }

    #[test]
    /// A 64-byte id (exactly MAX_KEY_LEN) is tracked and matched like any
    /// other; the fixed-width key must not truncate or reject the boundary.
    fn max_len_clordid_still_matches() {
        let id = "x".repeat(64);
        let mut m = Matcher::new();
        m.on_request(&id, 100, 1, 5, 10, false);
        let e = m.on_response(&id, 250, "0", 0, 0, "", false, 0).unwrap();
        assert_eq!(e.order_id, id);
        assert_eq!(e.pod_service_time_ns, 150);
        assert_eq!(m.oversized_clordid, 0);
    }

    #[test]
    /// Ids longer than MAX_KEY_LEN are never bot orders: the request is
    /// counted and dropped rather than tracked, and a response carrying one
    /// is unmatched. Distinct ids sharing a 64-byte prefix must not collide.
    fn oversized_clordid_is_counted_not_tracked() {
        let long_a = format!("{}A", "y".repeat(64));
        let long_b = format!("{}B", "y".repeat(64));
        let mut m = Matcher::new();
        m.on_request(&long_a, 100, 1, 5, 10, false);
        assert_eq!(m.oversized_clordid, 1);
        // Same 64-byte prefix, different id — must not match anything.
        assert!(m.on_response(&long_b, 200, "0", 0, 0, "", false, 0).is_none());
        assert_eq!(m.oversized_clordid, 2);
        assert_eq!(m.unmatched_responses, 1);
        // And the prefix itself was never inserted either.
        let prefix = "y".repeat(64);
        assert!(m.on_response(&prefix, 300, "0", 0, 0, "", false, 0).is_none());
        assert_eq!(m.unmatched_responses, 2);
    }

    #[test]
    /// idle_orders_are_evicted performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn idle_orders_are_evicted() {
        let mut m = Matcher::new();
        m.on_request("old", 100, 1, 5, 10, false);
        m.on_request("new", 9_000_000_000, 1, 5, 11, false);
        let evicted = m.evict_idle(10_000_000_000, DEFAULT_IDLE_NS);
        assert_eq!(evicted, 1);
        assert!(m
            .on_response("old", 10_000_000_100, "0", 0, 0, "", false, 0)
            .is_none());
        assert!(m
            .on_response("new", 10_000_000_100, "0", 0, 0, "", false, 0)
            .is_some());
    }
}
