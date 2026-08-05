//! This module implements pipeline behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::collections::HashMap;

/// Cap on how many resync events get their bytes logged per session.
const MAX_RESYNC_SAMPLES: u32 = 24;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::capture::{Capture, Direction, FlowKey, Transport};
use crate::matcher::{MatchedEvent, Matcher, DEFAULT_IDLE_NS};
use crate::parse::{self, Classified, Frame};
use crate::reassembly::Reassembler;

/// Pipeline stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct Pipeline {
    reassemblers: HashMap<(FlowKey, Direction), Reassembler>,
    matcher: Matcher,
    clock_offset_ns: u64,
    /// Byte-stream losses. Every one of these discards bytes that may contain whole FIX
    /// messages, and a request lost here leaves its responses unmatchable — which is how
    /// an order ends up with no orders.acked record at all. They were previously computed
    /// (PushStats.reset) and thrown away at the call site, so the only visible symptom was
    /// orders that inexplicably looked unanswered.
    pub hold_overflows: u64,
    pub buffer_overflows: u64,
    pub truncation_resets: u64,
    pub resync_skipped_bytes: u64,
    /// Resync points whose head byte looks like a `permessage-deflate` WebSocket frame
    /// header (RSV1 set, otherwise legal). Compression is unsupported — the payload carries
    /// no readable cl_ord_id — and without this counter such a submission simply goes
    /// silent, which is indistinguishable from an engine that answered nothing.
    ///
    /// Counts resync ATTEMPTS, not frames: a compressed stream resyncs byte by byte, so
    /// treat any nonzero value as "a contestant is sending compressed frames", not as a
    /// message count.
    pub ws_compressed_frames: u64,
    /// Bytes written off because a hole in the TCP stream was declared permanently lost.
    /// This is honest loss — the capture never saw those bytes — but bounded to the hole
    /// itself rather than stalling the flow behind it.
    pub stream_gap_bytes: u64,
    pub retransmitted_bytes: u64,
    /// Complete the funnel: FIX messages successfully FRAMED out of the byte stream, per
    /// direction. Compared against orders sent, a shortfall on the request side is the
    /// signature of requests never reaching the parser — which is what leaves an order
    /// with no records at all, since both its responses then arrive unmatchable.
    pub framed_requests: u64,
    pub framed_responses: u64,
    /// Framed but unusable: no ClOrdID could be extracted, so the message cannot join.
    pub framed_no_clordid: u64,
    /// How many resync events have already been sampled into the log. Bounded so a
    /// pathological stream cannot turn diagnosis into a log flood.
    resync_samples: u32,
}

impl Pipeline {
    /// new performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn new() -> Self {
        Self::with_offset(realtime_minus_monotonic_ns())
    }

    /// with_offset performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn with_offset(clock_offset_ns: u64) -> Self {
        Self {
            reassemblers: HashMap::new(),
            matcher: Matcher::new(),
            clock_offset_ns,
            hold_overflows: 0,
            buffer_overflows: 0,
            truncation_resets: 0,
            resync_skipped_bytes: 0,
            ws_compressed_frames: 0,
            stream_gap_bytes: 0,
            retransmitted_bytes: 0,
            framed_requests: 0,
            framed_responses: 0,
            framed_no_clordid: 0,
            resync_samples: 0,
        }
    }

    /// to_realtime performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn to_realtime(&self, monotonic_ns: u64) -> u64 {
        monotonic_ns.wrapping_add(self.clock_offset_ns)
    }

    #[allow(dead_code)] // used for diagnostics; not exercised by every consumer
    /// unmatched_responses performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn unmatched_responses(&self) -> u64 {
        self.matcher.unmatched_responses
    }

    /// evicted_idle counts requests dropped after the idle window that never got a
    /// response — a real loss, unlike the routine eviction of completed orders.
    pub fn evicted_idle(&self) -> u64 {
        self.matcher.evicted_idle_unanswered
    }

    /// evicted_capacity counts requests dropped because the inflight map was full.
    pub fn evicted_capacity(&self) -> u64 {
        self.matcher.evicted_capacity
    }

    /// process performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn process(&mut self, cap: &Capture, out: &mut Vec<MatchedEvent>) {
        let ts = self.to_realtime(cap.timestamp_ns);

        let mut framed: Vec<(u64, u32, bool, parse::ParsedMessage)> = Vec::new();
        let (mut framed_requests, mut framed_responses, mut framed_no_clordid) = (0u64, 0u64, 0u64);
        let truncated = cap.payload_len as usize > cap.payload.len();
        {
            let re = self
                .reassemblers
                .entry((cap.flow, cap.direction))
                .or_default();
            if truncated {
                self.truncation_resets = self.truncation_resets.saturating_add(1);
                re.reset_for_truncation();
            } else {
                let stats = re.push(cap.tcp_seq, ts, cap.payload);
                let reordered = stats.reordered;
                if stats.hold_overflow {
                    self.hold_overflows = self.hold_overflows.saturating_add(1);
                }
                if stats.buffer_overflow {
                    self.buffer_overflows = self.buffer_overflows.saturating_add(1);
                }
                if stats.gap_skipped_bytes > 0 {
                    self.stream_gap_bytes = self
                        .stream_gap_bytes
                        .saturating_add(stats.gap_skipped_bytes as u64);
                }
                self.retransmitted_bytes = self
                    .retransmitted_bytes
                    .saturating_add(stats.retransmitted_bytes as u64);
                loop {
                    match parse::frame(cap.transport, cap.direction, re.available()) {
                        Frame::Message(n) => {
                            let msg_ts = re.timestamp_at(0);
                            let msg_seq = re.seq_at(0);
                            let parsed =
                                parse::parse(cap.transport, cap.direction, &re.available()[..n]);
                            re.consume(n);
                            if cap.direction == Direction::Request {
                                framed_requests += 1;
                            } else {
                                framed_responses += 1;
                            }
                            if parsed.clordid.is_empty() {
                                framed_no_clordid += 1;
                            }
                            framed.push((msg_ts, msg_seq, reordered, parsed));
                        }
                        Frame::Incomplete => break,
                        Frame::Resync(skip) => {
                            // Unframeable bytes skipped to regain sync. Whatever was in
                            // them is gone, so this is a loss path like the overflows.
                            let n = skip.max(1);
                            self.resync_skipped_bytes =
                                self.resync_skipped_bytes.saturating_add(n as u64);
                            if cap.transport == Transport::HttpWs
                                && parse::ws_looks_compressed(cap.direction, re.available())
                            {
                                self.ws_compressed_frames =
                                    self.ws_compressed_frames.saturating_add(1);
                            }
                            // Sample what is being thrown away. A clean mid-message
                            // fragment means bytes went missing upstream; a malformed
                            // frame means the renderer or framer has a shape bug. Reading
                            // the code cannot distinguish those, and guessing has been
                            // wrong repeatedly.
                            if self.resync_samples < MAX_RESYNC_SAMPLES {
                                self.resync_samples += 1;
                                let avail = re.available();
                                let head = &avail[..avail.len().min(96)];
                                tracing::warn!(
                                    direction = ?cap.direction,
                                    skipped = n,
                                    buffered = avail.len(),
                                    sample = %String::from_utf8_lossy(head).replace('\u{1}', "|"),
                                    "framer resync: discarding unframeable bytes"
                                );
                            }
                            re.consume(n)
                        }
                    }
                }
            }
        }

        self.framed_requests += framed_requests;
        self.framed_responses += framed_responses;
        self.framed_no_clordid += framed_no_clordid;
        for (msg_ts, msg_seq, reordered, p) in framed {
            match p.class {
                Classified::Request => self.matcher.on_request(
                    &p.clordid,
                    msg_ts,
                    cap.flow.client_ip,
                    cap.flow.client_port,
                    msg_seq,
                    reordered,
                ),
                Classified::Response => {
                    if let Some(ev) = self.matcher.on_response(
                        &p.clordid,
                        msg_ts,
                        &p.exec_type,
                        p.fill_qty,
                        p.fill_price,
                        &p.orig_clordid,
                        reordered,
                        p.liquidity,
                    ) {
                        out.push(ev);
                    }
                }
                Classified::Ignore => {}
            }
        }
    }

    #[allow(dead_code)] // driven by main's evict ticker; not by the integration test
    /// evict_idle performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn evict_idle(&mut self) {
        let now = self.to_realtime(monotonic_now_ns());
        self.matcher.evict_idle(now, DEFAULT_IDLE_NS);
        self.reassemblers
            .retain(|_, r| r.idle_ns(now) < DEFAULT_IDLE_NS);
    }
}

impl Default for Pipeline {
    /// default performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn default() -> Self {
        Self::with_offset(0)
    }
}

/// monotonic_now_ns performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn monotonic_now_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    (ts.tv_sec as u64) * 1_000_000_000 + ts.tv_nsec as u64
}

/// realtime_minus_monotonic_ns performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn realtime_minus_monotonic_ns() -> u64 {
    let real = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    real.wrapping_sub(monotonic_now_ns())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capture::{Capture, FlowKey, Transport};

    /// fix performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn fix(body: &str) -> Vec<u8> {
        let body = body.replace('|', "\x01");
        let head = format!("8=FIX.4.2\x019={}\x01", body.len());
        let mut bytes = format!("{head}{body}").into_bytes();
        let sum: u32 = bytes.iter().map(|&b| b as u32).sum::<u32>() % 256;
        bytes.extend_from_slice(format!("10={sum:03}\x01").as_bytes());
        bytes
    }

    /// cap performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn cap(dir: Direction, seq: u32, ts: u64, payload: &[u8]) -> Capture<'_> {
        Capture {
            timestamp_ns: ts,
            flow: FlowKey {
                client_ip: 1,
                client_port: 5,
            },
            server_port: 9898,
            tcp_seq: seq,
            payload_len: payload.len() as u32,
            direction: dir,
            transport: Transport::Fix,
            payload,
        }
    }

    #[test]
    /// request_then_two_responses_emit_two_events_sharing_t3 performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn request_then_two_responses_emit_two_events_sharing_t3() {
        let mut p = Pipeline::with_offset(0);
        let mut out = Vec::new();

        let req = fix("35=D|49=IICPC-BOT|56=CONTESTANT|34=1|11=o-1|38=12|40=2|44=42.5|");
        p.process(&cap(Direction::Request, 1, 100, &req), &mut out);
        assert!(out.is_empty());

        let ack = fix("35=8|49=CONTESTANT|56=IICPC-BOT|34=1|11=o-1|150=0|39=0|32=0|31=0|");
        let fill = fix("35=8|49=CONTESTANT|56=IICPC-BOT|34=2|11=o-1|150=2|39=2|32=12|31=42.5|");
        p.process(&cap(Direction::Response, 1, 200, &ack), &mut out);
        p.process(
            &cap(Direction::Response, 1 + ack.len() as u32, 350, &fill),
            &mut out,
        );

        assert_eq!(out.len(), 2);
        assert_eq!(out[0].t3_ns, 100);
        assert_eq!(out[1].t3_ns, 100);
        assert_eq!(out[1].fill_qty, 12);
    }

    #[test]
    /// two_pipelined_orders_match_per_clordid performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn two_pipelined_orders_match_per_clordid() {
        let mut p = Pipeline::with_offset(0);
        let mut out = Vec::new();
        let a = fix("35=D|49=IICPC-BOT|34=1|11=A|38=1|40=2|44=1.0|");
        let b = fix("35=D|49=IICPC-BOT|34=2|11=B|38=1|40=2|44=2.0|");
        let mut both = a.clone();
        both.extend_from_slice(&b);
        p.process(&cap(Direction::Request, 1, 100, &both), &mut out);

        let rb = fix("35=8|49=CONTESTANT|34=1|11=B|150=2|39=2|32=1|31=2.0|");
        let ra = fix("35=8|49=CONTESTANT|34=2|11=A|150=2|39=2|32=1|31=1.0|");
        p.process(&cap(Direction::Response, 1, 300, &rb), &mut out);
        p.process(
            &cap(Direction::Response, 1 + rb.len() as u32, 320, &ra),
            &mut out,
        );

        assert_eq!(out.len(), 2);
        let by_id = |id: &str| out.iter().find(|e| e.order_id == id).unwrap();
        assert_eq!(by_id("B").t3_ns, 100);
        assert_eq!(by_id("A").t3_ns, 100);
        assert_eq!(by_id("B").t7_ns, 300);
        assert_eq!(by_id("A").t7_ns, 320);
    }
}
