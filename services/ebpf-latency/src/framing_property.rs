//! Property tests for the byte-stream half of the capture: Reassembler + frame_fix.
//!
//! Two silent data-loss bugs have now shipped in this pair, and neither was visible to the
//! existing tests, because those feed well-formed whole messages — the one shape the real
//! world never guarantees. What the real world does is split a TCP stream at arbitrary
//! byte offsets, deliver segments out of order, retransmit them, and occasionally never
//! show one to the capture at all.
//!
//! The property is total and cheap to check:
//!
//!   Given a FIX byte stream and ANY segmentation of it, every message in the stream must
//!   be framed exactly once — except messages overlapping a segment that was never
//!   delivered, which may be lost, and which must not prevent the rest from being framed.
//!
//! That last clause is the one that matters: a hole must cost its own bytes, not the
//! remainder of the session. Both shipped bugs were violations of this property, and both
//! reproduce here in milliseconds rather than in a ten-minute cluster run.

use crate::capture::{Direction, Transport};
use crate::parse::{frame, parse, Frame};
use crate::reassembly::Reassembler;

/// Deterministic LCG. Tests must fail identically on every run — a flaky property test
/// gets muted, and a muted test is worse than none.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, bound: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % bound.max(1)
    }
}

/// A realistic FIX NewOrderSingle, including the fields whose byte patterns previously
/// confused the framer: tag 38 (contains "8=") and a rotating self-match-prevention id.
fn fix_message(seq: usize) -> Vec<u8> {
    let clordid = format!("sess_7_{seq}_O");
    let body = format!(
        "35=D\u{1}49=IICPC-BOT\u{1}56=CONTESTANT\u{1}34={seq}\u{1}11={clordid}\u{1}\
         54=1\u{1}38=100\u{1}44=1000\u{1}40=2\u{1}7928={:03}\u{1}",
        seq % 8
    );
    let head = format!("8=FIX.4.2\u{1}9={}\u{1}", body.len());
    let mut msg = format!("{head}{body}").into_bytes();
    let sum: u32 = msg.iter().map(|&b| u32::from(b)).sum();
    msg.extend_from_slice(format!("10={:03}\u{1}", sum % 256).as_bytes());
    msg
}

/// Builds a stream of `n` messages and the ClOrdIDs it should yield.
fn build_stream(n: usize) -> (Vec<u8>, Vec<String>) {
    let mut bytes = Vec::new();
    let mut ids = Vec::new();
    for seq in 1..=n {
        bytes.extend_from_slice(&fix_message(seq));
        ids.push(format!("sess_7_{seq}_O"));
    }
    (bytes, ids)
}

/// Cuts a stream into (seq, data) segments of the given sizes, cycling through them.
fn segment(bytes: &[u8], sizes: &[usize]) -> Vec<(u32, Vec<u8>)> {
    let mut out = Vec::new();
    let mut off = 0usize;
    let mut i = 0usize;
    while off < bytes.len() {
        let take = sizes[i % sizes.len()].max(1).min(bytes.len() - off);
        out.push((off as u32, bytes[off..off + take].to_vec()));
        off += take;
        i += 1;
    }
    out
}

/// Feeds segments through the reassembler and frames everything available, returning the
/// ClOrdID of every message recovered.
///
/// `guard` bounds the framing loop: a stalled framer would otherwise spin forever, and a
/// hanging test reports as an infrastructure problem rather than as the defect it is.
fn replay(segments: &[(u32, Vec<u8>)]) -> Vec<String> {
    let mut re = Reassembler::new();
    let mut ids = Vec::new();
    for (seq, data) in segments {
        re.push(*seq, 1_000, data);
        let mut guard = 0;
        loop {
            guard += 1;
            assert!(guard < 100_000, "framing loop failed to terminate");
            match frame(Transport::Fix, Direction::Request, re.available()) {
                Frame::Message(n) => {
                    let msg = &re.available()[..n];
                    if let Some(id) = clordid_of(msg) {
                        ids.push(id);
                    }
                    re.consume(n);
                }
                Frame::Incomplete => break,
                Frame::Resync(skip) => re.consume(skip.max(1)),
            }
        }
    }
    ids
}

fn clordid_of(msg: &[u8]) -> Option<String> {
    msg.split(|&b| b == 0x01)
        .find(|f| f.starts_with(b"11="))
        .map(|f| String::from_utf8_lossy(&f[3..]).into_owned())
}

/// Every segmentation must recover every message. Segment sizes are swept across the
/// awkward range — smaller than a header, straddling BodyLength, straddling the checksum —
/// because the shipped bug fired only when a boundary landed within a couple of bytes of a
/// BeginString.
#[test]
fn every_segmentation_recovers_every_message() {
    let (bytes, expected) = build_stream(200);
    for size in [1usize, 2, 3, 5, 7, 11, 13, 64, 97, 128, 200, 512, 1448] {
        let segs = segment(&bytes, &[size]);
        let got = replay(&segs);
        assert_eq!(
            got,
            expected,
            "segment size {size}: recovered {} of {} messages",
            got.len(),
            expected.len()
        );
    }
}

/// Irregular segment sizes, deterministically randomised — the real pattern, where a
/// write's tail and the next write's head share a segment.
#[test]
fn randomised_segmentation_recovers_every_message() {
    let (bytes, expected) = build_stream(300);
    let mut rng = Lcg(0x5eed);
    for round in 0..25 {
        let sizes: Vec<usize> = (0..16).map(|_| 1 + rng.next(300)).collect();
        let segs = segment(&bytes, &sizes);
        let got = replay(&segs);
        assert_eq!(got, expected, "round {round} with sizes {sizes:?}");
    }
}

/// Out-of-order delivery: adjacent segments swapped. TCP reorders, and the reassembler
/// exists precisely to absorb that without losing anything.
#[test]
fn out_of_order_segments_recover_every_message() {
    let (bytes, expected) = build_stream(200);
    let mut segs = segment(&bytes, &[137]);
    // Interior pairs only. The reassembler initialises next_seq from the FIRST segment it
    // sees, so a reordered stream start makes it adopt the wrong origin and silently drop
    // the true beginning — a separate pre-existing defect, recorded in
    // reordering_at_stream_start_loses_the_head rather than conflated with this property.
    for i in (1..segs.len().saturating_sub(1)).step_by(2) {
        segs.swap(i, i + 1);
    }
    let got = replay(&segs);
    assert_eq!(
        got,
        expected,
        "recovered {} of {}",
        got.len(),
        expected.len()
    );
}

/// Retransmissions: every segment delivered twice. Duplicates must not produce duplicate
/// messages — a doubled execution report would read downstream as an overfill.
#[test]
fn duplicate_segments_do_not_duplicate_messages() {
    let (bytes, expected) = build_stream(150);
    let segs = segment(&bytes, &[211]);
    let mut doubled = Vec::new();
    for s in &segs {
        doubled.push(s.clone());
        doubled.push(s.clone());
    }
    let got = replay(&doubled);
    assert_eq!(
        got,
        expected,
        "recovered {} of {}",
        got.len(),
        expected.len()
    );
}

/// THE regression test: one segment is never delivered.
///
/// A hole must cost only the messages that overlap it. The capture never sees a
/// retransmission for a segment the kernel delivered to the application, so this hole is
/// permanent — and the reassembler advances `next_seq` only through contiguous data, so
/// without explicit gap recovery the stream stalls here forever. That stall is what turned
/// a 0.2% loss into 93.8% on a live run.
#[test]
fn a_permanently_missing_segment_costs_only_its_own_messages() {
    let (bytes, expected) = build_stream(200);
    let segs = segment(&bytes, &[300]);
    let drop_at = segs.len() / 2;
    let (hole_start, hole_len) = (segs[drop_at].0 as usize, segs[drop_at].1.len());

    let kept: Vec<(u32, Vec<u8>)> = segs
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != drop_at)
        .map(|(_, s)| s.clone())
        .collect();

    let got = replay(&kept);

    // Which messages genuinely overlap the hole? Only those may be missing.
    let mut off = 0usize;
    let mut unaffected = Vec::new();
    for (i, id) in expected.iter().enumerate() {
        let len = fix_message(i + 1).len();
        let overlaps = off < hole_start + hole_len && hole_start < off + len;
        if !overlaps {
            unaffected.push(id.clone());
        }
        off += len;
    }

    let recovered_after: Vec<&String> = got
        .iter()
        .filter(|id| unaffected.iter().any(|u| u == *id))
        .collect();
    assert_eq!(
        recovered_after.len(),
        unaffected.len(),
        "a single missing segment must not cost the rest of the stream: recovered {} of {} \
         unaffected messages (total recovered {})",
        recovered_after.len(),
        unaffected.len(),
        got.len()
    );
}

// ─── HTTP / WebSocket ────────────────────────────────────────────────────────
//
// Everything above this line covers Transport::Fix. The HTTP/WS framer has strictly MORE
// state than the FIX one — request framing by Content-Length, chunked bodies, WebSocket
// masking, three different length encodings, and an HTTP-to-WS transition mid-stream — and
// until now had no coverage against arbitrary segmentation at all. `ProtocolAll`
// submissions listen on both transports simultaneously, so this path carries the same
// grading weight as FIX.
//
// The property is identical: given a byte stream and ANY segmentation of it, every message
// must be framed exactly once.

/// Generic replay: reassemble under the given segmentation and return the ClOrdID of every
/// message recovered, extracted through the REAL parser rather than a test-local shim, so a
/// framing bug that yields a technically-complete-but-wrong message is still caught.
fn replay_transport(
    transport: Transport,
    direction: Direction,
    segments: &[(u32, Vec<u8>)],
) -> Vec<String> {
    let mut re = Reassembler::new();
    let mut ids = Vec::new();
    for (seq, data) in segments {
        re.push(*seq, 1_000, data);
        let mut guard = 0;
        loop {
            guard += 1;
            assert!(guard < 100_000, "framing loop failed to terminate");
            match frame(transport, direction, re.available()) {
                Frame::Message(n) => {
                    let msg = &re.available()[..n];
                    let id = parse(transport, direction, msg).clordid;
                    if !id.is_empty() {
                        ids.push(id);
                    }
                    re.consume(n);
                }
                Frame::Incomplete => break,
                Frame::Resync(skip) => re.consume(skip.max(1)),
            }
        }
    }
    ids
}

/// Like `replay_transport`, but also returns the absolute stream offset at which each
/// message was framed.
///
/// Offsets are what actually distinguish "recovered the message" from "got lucky": a
/// misaligned framer can still emit a REAL ClOrdID, because parse scans the body for the
/// key rather than requiring the slice to be a genuine message. Checking IDs alone would
/// pass that. Checking boundaries catches it.
fn replay_with_offsets(
    transport: Transport,
    direction: Direction,
    segments: &[(u32, Vec<u8>)],
) -> Vec<(usize, String)> {
    let mut re = Reassembler::new();
    let mut out = Vec::new();
    for (seq, data) in segments {
        re.push(*seq, 1_000, data);
        let mut guard = 0;
        loop {
            guard += 1;
            assert!(guard < 100_000, "framing loop failed to terminate");
            match frame(transport, direction, re.available()) {
                Frame::Message(n) => {
                    let offset = re.seq_at(0) as usize;
                    let msg = &re.available()[..n];
                    let id = parse(transport, direction, msg).clordid;
                    re.consume(n);
                    if !id.is_empty() {
                        out.push((offset, id));
                    }
                }
                Frame::Incomplete => break,
                Frame::Resync(skip) => re.consume(skip.max(1)),
            }
        }
    }
    out
}

/// The true byte offset at which each WebSocket frame starts.
fn ws_frame_offsets(direction: Direction, n: usize, form: LenForm, pad_to: usize) -> Vec<usize> {
    let mut offsets = Vec::new();
    let mut off = 0usize;
    for seq in 1..=n {
        offsets.push(off);
        off += ws_frame(direction, &json_body(seq, pad_to), form).len();
    }
    offsets
}

/// After a hole, EVERY message the framer emits must sit on a real frame boundary, and no
/// message may be emitted twice.
///
/// This is the property that distinguishes a framer which re-synchronised from one which
/// merely wandered into a slice that happened to contain a readable cl_ord_id. The second
/// case is worse than loss: it attributes a real order to the wrong boundary, so instead of
/// a visible capture gap it silently corrupts the latency sample that feeds the score.
#[test]
fn ws_after_a_hole_every_framed_message_sits_on_a_real_boundary() {
    let (bytes, expected) = build_ws_stream(Direction::Response, 120, LenForm::Short, 0);
    let truth = ws_frame_offsets(Direction::Response, 120, LenForm::Short, 0);
    let segs = segment(&bytes, &[300]);
    let drop_at = segs.len() / 2;

    let kept: Vec<(u32, Vec<u8>)> = segs
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != drop_at)
        .map(|(_, s)| s.clone())
        .collect();

    let got = replay_with_offsets(Transport::HttpWs, Direction::Response, &kept);

    let misaligned: Vec<&(usize, String)> =
        got.iter().filter(|(off, _)| !truth.contains(off)).collect();
    assert!(
        misaligned.is_empty(),
        "framer emitted {} message(s) at offsets that are not frame boundaries — a real \
         ClOrdID attributed to the wrong message: {:?}",
        misaligned.len(),
        misaligned.iter().take(5).collect::<Vec<_>>()
    );

    let mut seen = std::collections::HashSet::new();
    let dupes: Vec<&String> = got
        .iter()
        .filter(|(_, id)| !seen.insert(id.clone()))
        .map(|(_, id)| id)
        .collect();
    assert!(
        dupes.is_empty(),
        "framer emitted duplicate ClOrdIDs, which read downstream as overfills: {dupes:?}"
    );

    for (_, id) in &got {
        assert!(
            expected.contains(id),
            "framer invented a ClOrdID not present in the stream: {id}"
        );
    }
}

/// How to encode a WebSocket payload length. All three are legal on the wire and an engine
/// picks between them purely by payload size, so the capture must handle all three.
#[derive(Clone, Copy, PartialEq)]
enum LenForm {
    /// len7 < 126 — the 7-bit inline form.
    Short,
    /// len7 == 126 — 16-bit extended length.
    Extended16,
    /// len7 == 127 — 64-bit extended length, used once a payload exceeds 65535 bytes.
    Extended64,
}

fn json_body(seq: usize, pad_to: usize) -> Vec<u8> {
    let mut s =
        format!("{{\"cl_ord_id\":\"sess_7_{seq}_O\",\"side\":\"1\",\"qty\":100,\"price\":1000");
    while s.len() + 2 < pad_to {
        s.push_str(",\"p\":\"xxxxxxxx\"");
    }
    s.push('}');
    s.into_bytes()
}

/// Builds one WebSocket text frame. Requests are masked and responses are not, per RFC 6455
/// — the framer checks this, so getting it wrong here would test the wrong thing.
fn ws_frame(direction: Direction, payload: &[u8], form: LenForm) -> Vec<u8> {
    let masked = direction == Direction::Request;
    let mut out = vec![0x81u8]; // FIN + text opcode
    let mask_bit = if masked { 0x80 } else { 0x00 };
    match form {
        LenForm::Short => {
            assert!(payload.len() < 126);
            out.push(mask_bit | payload.len() as u8);
        }
        LenForm::Extended16 => {
            out.push(mask_bit | 126);
            out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        LenForm::Extended64 => {
            out.push(mask_bit | 127);
            out.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
    }
    if masked {
        let mask = [0xA1u8, 0xB2, 0xC3, 0xD4];
        out.extend_from_slice(&mask);
        out.extend(payload.iter().enumerate().map(|(i, &b)| b ^ mask[i % 4]));
    } else {
        out.extend_from_slice(payload);
    }
    out
}

fn build_ws_stream(
    direction: Direction,
    n: usize,
    form: LenForm,
    pad_to: usize,
) -> (Vec<u8>, Vec<String>) {
    let mut bytes = Vec::new();
    let mut ids = Vec::new();
    for seq in 1..=n {
        bytes.extend_from_slice(&ws_frame(direction, &json_body(seq, pad_to), form));
        ids.push(format!("sess_7_{seq}_O"));
    }
    (bytes, ids)
}

fn http_request(seq: usize) -> Vec<u8> {
    let body = json_body(seq, 0);
    let mut out = format!(
        "POST /orders HTTP/1.1\r\nHost: engine\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(&body);
    out
}

fn http_chunked_response(seq: usize) -> Vec<u8> {
    let body = json_body(seq, 0);
    let mut out = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
         Transfer-Encoding: chunked\r\n\r\n"
        .to_string()
        .into_bytes();
    out.extend_from_slice(format!("{:x}\r\n", body.len()).as_bytes());
    out.extend_from_slice(&body);
    out.extend_from_slice(b"\r\n0\r\n\r\n");
    out
}

fn build_http_stream(n: usize, chunked: bool) -> (Vec<u8>, Vec<String>) {
    let mut bytes = Vec::new();
    let mut ids = Vec::new();
    for seq in 1..=n {
        let msg = if chunked {
            http_chunked_response(seq)
        } else {
            http_request(seq)
        };
        bytes.extend_from_slice(&msg);
        ids.push(format!("sess_7_{seq}_O"));
    }
    (bytes, ids)
}

/// The HTTP request stream under every awkward segment size. Sizes below 4 are the
/// interesting ones: `looks_like_http` needs 4 bytes to recognise "POST", and a buffer
/// holding fewer is the HEAD of a good request, not a malformed one.
#[test]
fn http_every_segmentation_recovers_every_message() {
    let (bytes, expected) = build_http_stream(120, false);
    // Report EVERY failing size, not just the first. Which sizes fail is what separates a
    // pathological-only defect from one a real TCP stream will hit, and aborting on size 1
    // hides that distinction entirely.
    let mut failures = Vec::new();
    for size in [1usize, 2, 3, 4, 5, 7, 11, 13, 64, 97, 128, 512, 1000, 1448] {
        let segs = segment(&bytes, &[size]);
        let got = replay_transport(Transport::HttpWs, Direction::Request, &segs);
        if got != expected {
            failures.push((size, got.len()));
        }
    }
    assert!(
        failures.is_empty(),
        "sizes that lost messages (size, recovered of {}): {failures:?}",
        expected.len()
    );
}

/// Chunked responses. The body terminator "0\r\n\r\n" can itself straddle a segment
/// boundary, which is the chunked analogue of the FIX checksum split.
#[test]
fn http_chunked_every_segmentation_recovers_every_message() {
    let (bytes, expected) = build_http_stream(120, true);
    for size in [1usize, 2, 3, 5, 7, 13, 64, 97, 128, 512, 1448] {
        let segs = segment(&bytes, &[size]);
        let got = replay_transport(Transport::HttpWs, Direction::Response, &segs);
        assert_eq!(
            got,
            expected,
            "segment size {size}: recovered {} of {}",
            got.len(),
            expected.len()
        );
    }
}

/// The adversarial placement, with otherwise entirely realistic segment sizes: a boundary
/// landing 1-3 bytes AFTER a message start, leaving the framer holding a prefix too short
/// for `looks_like_http` to recognise.
///
/// This is the exact shape that made the FIX version of this defect a production loss
/// ("a TCP segment boundary landing within ~2 bytes of a BeginString"). A sweep over fixed
/// sizes cannot find it — the leftover after each consumed message follows modular
/// arithmetic that may never land in the window — so its absence there is a sampling
/// artifact, not evidence of safety.
#[test]
fn http_boundary_just_after_message_start_recovers_every_message() {
    let (bytes, expected) = build_http_stream(60, false);
    let first_len = http_request(1).len();
    let mut failures = Vec::new();
    for k in 1..=3usize {
        // One realistic-sized segment carrying message 1 plus k bytes of message 2, then the
        // remainder in MTU-sized segments.
        let cut = first_len + k;
        let mut segs = vec![(0u32, bytes[..cut].to_vec())];
        let mut off = cut;
        while off < bytes.len() {
            let take = 1448.min(bytes.len() - off);
            segs.push((off as u32, bytes[off..off + take].to_vec()));
            off += take;
        }
        let got = replay_transport(Transport::HttpWs, Direction::Request, &segs);
        if got != expected {
            failures.push((k, got.len()));
        }
    }
    assert!(
        failures.is_empty(),
        "a boundary k bytes past a message start lost messages (k, recovered of {}): {failures:?}",
        expected.len()
    );
}

/// Irregular, realistic segment sizes. `http_every_segmentation_recovers_every_message`
/// sweeps single sizes including pathological ones; this asks the question that decides
/// severity — does the defect fire at sizes a real TCP stream actually produces?
#[test]
fn http_randomised_segmentation_recovers_every_message() {
    let (bytes, expected) = build_http_stream(150, false);
    let mut rng = Lcg(0x5eed);
    for round in 0..25 {
        let sizes: Vec<usize> = (0..16).map(|_| 1 + rng.next(1448)).collect();
        let segs = segment(&bytes, &sizes);
        let got = replay_transport(Transport::HttpWs, Direction::Request, &segs);
        assert_eq!(
            got,
            expected,
            "round {round}: recovered {} of {} with sizes {sizes:?}",
            got.len(),
            expected.len()
        );
    }
}

/// The WebSocket equivalent of the above.
#[test]
fn ws_randomised_segmentation_recovers_every_message() {
    let (bytes, expected) = build_ws_stream(Direction::Response, 200, LenForm::Short, 0);
    let mut rng = Lcg(0xd00d);
    for round in 0..25 {
        let sizes: Vec<usize> = (0..16).map(|_| 1 + rng.next(1448)).collect();
        let segs = segment(&bytes, &sizes);
        let got = replay_transport(Transport::HttpWs, Direction::Response, &segs);
        assert_eq!(
            got,
            expected,
            "round {round}: recovered {} of {}",
            got.len(),
            expected.len()
        );
    }
}

/// Masked client-to-server frames, every segmentation. The 4-byte mask key sits between the
/// header and the payload, so a boundary inside it is a distinct failure mode from a
/// boundary inside the payload.
#[test]
fn ws_request_every_segmentation_recovers_every_message() {
    let (bytes, expected) = build_ws_stream(Direction::Request, 150, LenForm::Short, 0);
    for size in [1usize, 2, 3, 5, 7, 11, 13, 64, 97, 128, 512, 1448] {
        let segs = segment(&bytes, &[size]);
        let got = replay_transport(Transport::HttpWs, Direction::Request, &segs);
        assert_eq!(
            got,
            expected,
            "segment size {size}: recovered {} of {}",
            got.len(),
            expected.len()
        );
    }
}

/// Unmasked server-to-client frames — the direction that carries execution reports, and so
/// the direction whose loss shows up as an engine that "never answered".
#[test]
fn ws_response_every_segmentation_recovers_every_message() {
    let (bytes, expected) = build_ws_stream(Direction::Response, 150, LenForm::Short, 0);
    for size in [1usize, 2, 3, 5, 7, 11, 13, 64, 97, 128, 512, 1448] {
        let segs = segment(&bytes, &[size]);
        let got = replay_transport(Transport::HttpWs, Direction::Response, &segs);
        assert_eq!(
            got,
            expected,
            "segment size {size}: recovered {} of {}",
            got.len(),
            expected.len()
        );
    }
}

/// 16-bit extended length: any batched response over 125 bytes uses this form, so it is the
/// COMMON case for an engine that answers with more than a trivial payload.
#[test]
fn ws_extended16_length_recovers_every_message() {
    let (bytes, expected) = build_ws_stream(Direction::Response, 60, LenForm::Extended16, 300);
    for size in [1usize, 3, 7, 64, 128, 512, 1448] {
        let segs = segment(&bytes, &[size]);
        let got = replay_transport(Transport::HttpWs, Direction::Response, &segs);
        assert_eq!(
            got,
            expected,
            "segment size {size}: recovered {} of {}",
            got.len(),
            expected.len()
        );
    }
}

/// 64-bit extended length. Legal RFC 6455, and what any payload past 65535 bytes must use —
/// reachable by an engine that batches execution reports aggressively.
#[test]
fn ws_extended64_length_is_framed() {
    let payload = json_body(1, 70_000);
    let bytes = ws_frame(Direction::Response, &payload, LenForm::Extended64);
    match frame(Transport::HttpWs, Direction::Response, &bytes) {
        Frame::Message(n) => assert_eq!(n, bytes.len(), "framed the wrong length"),
        Frame::Incomplete => panic!("a complete 64-bit-length frame was reported Incomplete"),
        Frame::Resync(skip) => panic!(
            "a legal 64-bit-length WebSocket frame was rejected as malformed (Resync({skip})); \
             every response inside it is lost and the resync walks into the NEXT frame"
        ),
    }
}

/// The FIX framer learned this the expensive way: "not enough bytes yet" is NOT "malformed".
/// `looks_like_http` needs 4 bytes to match "POST"/"GET "/"HTTP", so a 1-3 byte buffer is
/// the head of a request that has not finished arriving. Routing it to the WebSocket framer
/// instead reads byte 1 as a WS length header and discards a perfectly good request.
#[test]
fn short_http_prefix_is_incomplete_not_malformed() {
    for prefix in [
        &b"P"[..],
        &b"PO"[..],
        &b"POS"[..],
        &b"G"[..],
        &b"GE"[..],
        &b"GET"[..],
        &b"H"[..],
        &b"HTT"[..],
    ] {
        match frame(Transport::HttpWs, Direction::Request, prefix) {
            Frame::Incomplete => {}
            other => panic!(
                "{:?} is the head of a good HTTP request but was framed as {}; the request is \
                 discarded and the stream resyncs mid-message",
                String::from_utf8_lossy(prefix),
                match other {
                    Frame::Message(n) => format!("Message({n})"),
                    Frame::Resync(n) => format!("Resync({n})"),
                    Frame::Incomplete => unreachable!(),
                }
            ),
        }
    }
}

/// The real `ProtocolAll` shape: an HTTP upgrade handshake, then WebSocket frames on the
/// same connection. The framer must switch transports mid-stream without losing the first
/// frame after the transition.
#[test]
fn http_upgrade_then_ws_frames_recovers_every_message() {
    let mut bytes = b"GET /ws HTTP/1.1\r\nHost: engine\r\nUpgrade: websocket\r\n\
        Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\r\n"
        .to_vec();
    let mut expected = Vec::new();
    for seq in 1..=80 {
        bytes.extend_from_slice(&ws_frame(
            Direction::Request,
            &json_body(seq, 0),
            LenForm::Short,
        ));
        expected.push(format!("sess_7_{seq}_O"));
    }
    for size in [1usize, 3, 7, 64, 128, 512] {
        let segs = segment(&bytes, &[size]);
        let got = replay_transport(Transport::HttpWs, Direction::Request, &segs);
        assert_eq!(
            got,
            expected,
            "segment size {size}: recovered {} of {}",
            got.len(),
            expected.len()
        );
    }
}

/// Retransmissions must not duplicate messages — a doubled execution report reads downstream
/// as an overfill, which is a grading violation against an innocent engine.
#[test]
fn ws_duplicate_segments_do_not_duplicate_messages() {
    let (bytes, expected) = build_ws_stream(Direction::Response, 100, LenForm::Short, 0);
    let segs = segment(&bytes, &[211]);
    let mut doubled = Vec::new();
    for s in &segs {
        doubled.push(s.clone());
        doubled.push(s.clone());
    }
    let got = replay_transport(Transport::HttpWs, Direction::Response, &doubled);
    assert_eq!(
        got,
        expected,
        "recovered {} of {}",
        got.len(),
        expected.len()
    );
}

/// Out-of-order delivery, interior pairs only (see the FIX equivalent for why the stream
/// start is excluded).
#[test]
fn ws_out_of_order_segments_recover_every_message() {
    let (bytes, expected) = build_ws_stream(Direction::Response, 100, LenForm::Short, 0);
    let mut segs = segment(&bytes, &[137]);
    for i in (1..segs.len().saturating_sub(1)).step_by(2) {
        segs.swap(i, i + 1);
    }
    let got = replay_transport(Transport::HttpWs, Direction::Response, &segs);
    assert_eq!(
        got,
        expected,
        "recovered {} of {}",
        got.len(),
        expected.len()
    );
}

/// A permanently missing segment must cost only the messages overlapping it. This is the
/// property whose FIX violation turned a 0.2% loss into 93.8% on a live run.
#[test]
fn ws_permanently_missing_segment_costs_only_its_own_messages() {
    let (bytes, expected) = build_ws_stream(Direction::Response, 120, LenForm::Short, 0);
    let segs = segment(&bytes, &[300]);
    let drop_at = segs.len() / 2;
    let (hole_start, hole_len) = (segs[drop_at].0 as usize, segs[drop_at].1.len());

    let kept: Vec<(u32, Vec<u8>)> = segs
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != drop_at)
        .map(|(_, s)| s.clone())
        .collect();

    let got = replay_transport(Transport::HttpWs, Direction::Response, &kept);

    let mut off = 0usize;
    let mut unaffected = Vec::new();
    for (i, id) in expected.iter().enumerate() {
        let len = ws_frame(Direction::Response, &json_body(i + 1, 0), LenForm::Short).len();
        let overlaps = off < hole_start + hole_len && hole_start < off + len;
        if !overlaps {
            unaffected.push(id.clone());
        }
        off += len;
    }

    let recovered: Vec<&String> = got
        .iter()
        .filter(|id| unaffected.iter().any(|u| u == *id))
        .collect();
    assert_eq!(
        recovered.len(),
        unaffected.len(),
        "a single missing segment must not cost the rest of the stream: recovered {} of {} \
         unaffected messages (total recovered {})",
        recovered.len(),
        unaffected.len(),
        got.len()
    );
}

/// Documents the stream-start behaviour rather than asserting it is correct: the
/// reassembler adopts the first segment it sees as the stream origin, so if the very first
/// segments arrive out of order the head of the stream is lost. Harmless for a long-lived
/// FIX session (the capture attaches before the connection opens), but it is a real edge
/// and should not be discovered a third time by a production run.
#[test]
fn reordering_at_stream_start_loses_the_head() {
    let (bytes, expected) = build_stream(50);
    let mut segs = segment(&bytes, &[137]);
    segs.swap(0, 1);
    let got = replay(&segs);
    assert!(
        got.len() < expected.len(),
        "if this now recovers everything, the origin handling was fixed — update this test"
    );
    assert!(
        !got.is_empty(),
        "a reordered start must not cost the whole stream, only its head"
    );
}
