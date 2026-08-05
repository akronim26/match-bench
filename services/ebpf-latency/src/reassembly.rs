//! This module implements reassembly behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::collections::BTreeMap;

const MAX_BUFFERED: usize = 1 << 20; // 1 MiB
const MAX_HOLD_SEGMENTS: usize = 64;
/// Pushes with data held but no forward progress before the hole ahead of `next_seq` is
/// declared lost. Small enough that a stall costs a handful of segments, large enough that
/// ordinary reordering (which resolves within a segment or two) is never mistaken for a
/// hole.
const GAP_SKIP_AFTER_STALLED_PUSHES: u32 = 4;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
/// PushStats stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct PushStats {
    pub reordered: bool,
    pub retransmitted_bytes: usize,
    pub reset: bool,
    /// hold_overflow: more than MAX_HOLD_SEGMENTS out-of-order segments were waiting on a
    /// predecessor that never arrived, so the whole hold set was discarded. Every FIX
    /// message in those bytes is lost — a request lost this way leaves its responses
    /// unmatchable, and an order that loses all of its records becomes a capture gap.
    pub hold_overflow: bool,
    /// gap_skipped_bytes: a hole was declared permanently lost and `next_seq` jumped over
    /// it. Only these bytes are gone; everything after them is recovered.
    pub gap_skipped_bytes: usize,
    /// buffer_overflow: the contiguous stream buffer passed MAX_BUFFERED and was reset.
    /// Same consequence as hold_overflow, different trigger.
    pub buffer_overflow: bool,
}

#[derive(Debug)]
/// Reassembler stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct Reassembler {
    initialized: bool,
    next_seq: u32,
    front_abs: u64,
    buf: Vec<u8>,
    marks: Vec<(u64, u64, u32)>,
    hold: BTreeMap<u32, (u64, Vec<u8>)>,
    last_activity_ns: u64,
    /// Consecutive pushes that produced no forward progress while data sat in `hold`.
    /// The capture never sees a retransmission for a segment the kernel already delivered
    /// to the application, so a hole here can be permanent — and `next_seq` advances only
    /// through contiguous data. Without this, one missing segment stalls the flow for the
    /// rest of the session: measured live as a 0.2% loss becoming 93.8%.
    stalled_pushes: u32,
}

impl Default for Reassembler {
    /// default performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn default() -> Self {
        Self::new()
    }
}

impl Reassembler {
    /// new performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn new() -> Self {
        Self {
            initialized: false,
            next_seq: 0,
            front_abs: 0,
            buf: Vec::new(),
            marks: Vec::new(),
            hold: BTreeMap::new(),
            last_activity_ns: 0,
            stalled_pushes: 0,
        }
    }

    /// delivered_abs performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn delivered_abs(&self) -> u64 {
        self.front_abs + self.buf.len() as u64
    }

    /// append_contiguous performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn append_contiguous(&mut self, ts: u64, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let abs = self.delivered_abs();
        self.marks.push((abs, ts, self.next_seq));
        self.buf.extend_from_slice(data);
        self.next_seq = self.next_seq.wrapping_add(data.len() as u32);
    }

    /// drain_hold performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn drain_hold(&mut self) {
        loop {
            let key = self.hold.iter().find_map(|(&seq, (_, d))| {
                let end = seq.wrapping_add(d.len() as u32);
                if seq_le(seq, self.next_seq) && seq_lt(self.next_seq, end) {
                    Some(seq)
                } else {
                    None
                }
            });
            match key {
                Some(seq) => {
                    let (ts, data) = self.hold.remove(&seq).expect("key just found");
                    let skip = self.next_seq.wrapping_sub(seq) as usize;
                    self.append_contiguous(ts, &data[skip..]);
                }
                None => break,
            }
        }
        let next = self.next_seq;
        self.hold
            .retain(|&seq, (_, d)| seq_ge(seq.wrapping_add(d.len() as u32), next));
    }

    /// push performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn push(&mut self, seq: u32, ts: u64, data: &[u8]) -> PushStats {
        let mut stats = PushStats::default();
        if data.is_empty() {
            return stats;
        }
        self.last_activity_ns = self.last_activity_ns.max(ts);

        if !self.initialized {
            self.initialized = true;
            self.next_seq = seq;
            self.append_contiguous(ts, data);
            self.drain_hold();
            return stats;
        }

        let had_hold = !self.hold.is_empty();
        let seq_before = self.next_seq;
        let gap = seq.wrapping_sub(self.next_seq) as i32;
        if gap == 0 {
            self.append_contiguous(ts, data);
            self.drain_hold();
            if had_hold {
                stats.reordered = true;
            }
        } else if gap < 0 {
            let overlap = (-(gap as i64)) as usize;
            if overlap >= data.len() {
                stats.retransmitted_bytes = data.len();
            } else {
                stats.retransmitted_bytes = overlap;
                self.append_contiguous(ts, &data[overlap..]);
                self.drain_hold();
                if had_hold {
                    stats.reordered = true;
                }
            }
        } else {
            stats.reordered = true;
            self.hold.insert(seq, (ts, data.to_vec()));
            if self.hold.len() > MAX_HOLD_SEGMENTS {
                // Previously this cleared the hold outright, discarding every segment
                // waiting behind the hole AND leaving next_seq pinned to it, so the flow
                // stalled and the same overflow repeated forever. Skipping the hole keeps
                // the held data and costs only the missing bytes.
                stats.hold_overflow = true;
                stats.gap_skipped_bytes += self.skip_gap();
            }
        }

        // Forward progress check. `next_seq` only moves through contiguous data, so if it
        // has not moved while segments are queued, the bytes in between are not coming.
        if self.next_seq == seq_before && !self.hold.is_empty() {
            self.stalled_pushes += 1;
            if self.stalled_pushes >= GAP_SKIP_AFTER_STALLED_PUSHES {
                stats.gap_skipped_bytes += self.skip_gap();
            }
        } else {
            self.stalled_pushes = 0;
        }

        if self.buf.len() > MAX_BUFFERED {
            self.reset_buffer();
            stats.reset = true;
            stats.buffer_overflow = true;
        }
        stats
    }

    /// skip_gap declares the hole ahead of `next_seq` permanently lost and jumps to the
    /// earliest held segment, returning the bytes written off.
    ///
    /// This is what bounds the cost of a missing segment to the missing segment. The
    /// framer then resyncs from a real message boundary and the stream continues; without
    /// it the flow is dead from the first hole onward.
    fn skip_gap(&mut self) -> usize {
        let Some(&target) = self.hold.keys().next() else {
            return 0;
        };
        let lost = target.wrapping_sub(self.next_seq) as usize;
        self.next_seq = target;
        self.stalled_pushes = 0;
        self.drain_hold();
        lost
    }

    /// reset_buffer performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn reset_buffer(&mut self) {
        self.front_abs = self.delivered_abs();
        self.buf.clear();
        self.marks.clear();
        self.hold.clear();
    }

    /// reset_for_truncation performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn reset_for_truncation(&mut self) {
        self.initialized = false;
        self.front_abs = 0;
        self.buf.clear();
        self.marks.clear();
        self.hold.clear();
    }

    /// available performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn available(&self) -> &[u8] {
        &self.buf
    }

    /// timestamp_at performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn timestamp_at(&self, offset: usize) -> u64 {
        let abs = self.front_abs + offset as u64;
        let mut ts = 0;
        for &(start, mark_ts, _) in &self.marks {
            if start <= abs {
                ts = mark_ts;
            } else {
                break;
            }
        }
        ts
    }

    /// seq_at performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn seq_at(&self, offset: usize) -> u32 {
        let abs = self.front_abs + offset as u64;
        let mut base: Option<(u64, u32)> = None;
        for &(start, _, seq) in &self.marks {
            if start <= abs {
                base = Some((start, seq));
            } else {
                break;
            }
        }
        match base {
            Some((start, seq)) => seq.wrapping_add((abs - start) as u32),
            None => 0,
        }
    }

    /// consume performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn consume(&mut self, n: usize) {
        let n = n.min(self.buf.len());
        if n == 0 {
            return;
        }
        self.buf.drain(0..n);
        self.front_abs += n as u64;
        while self.marks.len() >= 2 && self.marks[1].0 <= self.front_abs {
            self.marks.remove(0);
        }
    }

    /// idle_ns performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn idle_ns(&self, now_ns: u64) -> u64 {
        now_ns.saturating_sub(self.last_activity_ns)
    }
}

/// seq_ge performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn seq_ge(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) >= 0
}

/// seq_le performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn seq_le(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) <= 0
}

/// seq_lt performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn seq_lt(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) < 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// in_order_single_segment performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn in_order_single_segment() {
        let mut r = Reassembler::new();
        r.push(1000, 50, b"hello");
        assert_eq!(r.available(), b"hello");
        assert_eq!(r.timestamp_at(0), 50);
        r.consume(5);
        assert_eq!(r.available(), b"");
    }

    #[test]
    /// coalesced_multiple_messages_in_one_segment performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn coalesced_multiple_messages_in_one_segment() {
        let mut r = Reassembler::new();
        r.push(0, 10, b"AAABBBCCC");
        assert_eq!(r.available(), b"AAABBBCCC");
        assert_eq!(r.timestamp_at(0), 10);
        assert_eq!(r.timestamp_at(3), 10);
        assert_eq!(r.timestamp_at(6), 10);
        r.consume(3);
        assert_eq!(r.available(), b"BBBCCC");
        assert_eq!(r.timestamp_at(0), 10);
    }

    #[test]
    /// straddled_message_across_two_segments_keeps_first_byte_timestamp performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn straddled_message_across_two_segments_keeps_first_byte_timestamp() {
        let mut r = Reassembler::new();
        r.push(0, 100, b"MES");
        r.push(3, 200, b"SAGE");
        assert_eq!(r.available(), b"MESSAGE");
        assert_eq!(r.timestamp_at(0), 100);
        assert_eq!(r.timestamp_at(4), 200);
    }

    #[test]
    /// out_of_order_segments_are_reordered performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn out_of_order_segments_are_reordered() {
        let mut r = Reassembler::new();
        r.push(0, 10, b"AAA");
        let s = r.push(6, 30, b"CCC");
        assert!(s.reordered);
        assert_eq!(r.available(), b"AAA");
        r.push(3, 20, b"BBB");
        assert_eq!(r.available(), b"AAABBBCCC");
        assert_eq!(r.timestamp_at(0), 10);
        assert_eq!(r.timestamp_at(3), 20);
        assert_eq!(r.timestamp_at(6), 30);
    }

    #[test]
    /// overlapping_held_segment_tail_is_drained performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn overlapping_held_segment_tail_is_drained() {
        let mut r = Reassembler::new();
        r.push(0, 10, b"AAA");
        r.push(5, 30, b"XYZZ");
        assert_eq!(r.available(), b"AAA");
        r.push(3, 20, b"BBCC");
        assert_eq!(
            r.available(),
            b"AAABBCCZZ",
            "H10: overlapping held segment's tail must be delivered, not stranded"
        );
    }

    #[test]
    /// gap_fill_push_reports_reorder performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn gap_fill_push_reports_reorder() {
        let mut r = Reassembler::new();
        r.push(0, 10, b"AAA");
        let held = r.push(6, 30, b"CCC");
        assert!(held.reordered, "the holding push flags reorder");
        let fill = r.push(3, 20, b"BBB");
        assert!(
            fill.reordered,
            "M31: the gap-filling push that delivers held bytes must flag reorder"
        );
        assert_eq!(r.available(), b"AAABBBCCC");
    }

    #[test]
    /// truncation_reset_resyncs_without_stall performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn truncation_reset_resyncs_without_stall() {
        let mut r = Reassembler::new();
        r.push(0, 10, b"AAA");
        r.reset_for_truncation();
        r.push(100, 20, b"BBB");
        assert_eq!(r.available(), b"BBB");
        assert_eq!(r.timestamp_at(0), 20);
    }

    #[test]
    /// duplicate_retransmit_is_detected_and_ignored performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn duplicate_retransmit_is_detected_and_ignored() {
        let mut r = Reassembler::new();
        r.push(0, 10, b"AAA");
        let s = r.push(0, 11, b"AAA");
        assert_eq!(s.retransmitted_bytes, 3);
        assert_eq!(r.available(), b"AAA");
    }

    #[test]
    /// partial_overlap_retransmit_appends_only_new_tail performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn partial_overlap_retransmit_appends_only_new_tail() {
        let mut r = Reassembler::new();
        r.push(0, 10, b"AAA");
        let s = r.push(2, 12, b"AXYZ");
        assert_eq!(s.retransmitted_bytes, 1);
        assert_eq!(r.available(), b"AAAXYZ");
    }
}
