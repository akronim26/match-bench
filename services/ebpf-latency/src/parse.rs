//! This module implements parse behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use crate::capture::{Direction, Transport};

const SOH: u8 = 0x01;
const PRICE_SCALE: u64 = 1_000_000_000;
const MAX_FIX_MESSAGE: usize = 64 * 1024;
/// FIX_BEGIN is the real start-of-message marker: tag 8 (BeginString) always carries a
/// value starting "FIX". Resync MUST search for this, not for a bare "8=".
///
/// "8=" is not a message boundary — it occurs inside almost every message, because tag 38
/// (OrderQty) renders as "38=", as do 58, 108, 118. Searching for it made recovery
/// pathological: after any desync the framer landed on a false start inside the NEXT
/// message's 38=, passed the two-byte check, failed the "9=" check one field later,
/// resynced again, and walked forward in small steps — burning a whole message or more per
/// desync instead of jumping to the next real boundary. Measured at ~284 bytes skipped per
/// lost request on a pass-1 run.
const FIX_BEGIN: &[u8] = b"8=FIX";
const MAX_HTTP_MESSAGE: usize = 64 * 1024;
/// Upper bound on a WebSocket frame the capture will wait for. Matches the reassembler's
/// MAX_BUFFERED: a frame larger than the buffer can never be completed, so waiting for one
/// would stall the flow permanently.
const MAX_WS_MESSAGE: usize = 1 << 20;

#[derive(Debug, PartialEq, Eq)]
/// Frame enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum Frame {
    Message(usize),
    Incomplete,
    Resync(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Classified enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum Classified {
    Request,
    Response,
    Ignore,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// ParsedMessage stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct ParsedMessage {
    pub class: Classified,
    pub clordid: String,
    pub orig_clordid: String,
    pub exec_type: String,
    pub fill_qty: u64,
    pub fill_price: u64,
    /// FIX LastLiquidityInd (tag 851) / JSON "liquidity": 0 unknown, 1 maker, 2 taker.
    pub liquidity: u8,
}

impl Default for Classified {
    /// default performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn default() -> Self {
        Classified::Ignore
    }
}

/// frame performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn frame(transport: Transport, direction: Direction, buf: &[u8]) -> Frame {
    match transport {
        Transport::Fix => frame_fix(buf),
        Transport::HttpWs => frame_http_ws(direction, buf),
    }
}

/// parse performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn parse(transport: Transport, direction: Direction, msg: &[u8]) -> ParsedMessage {
    match transport {
        Transport::Fix => parse_fix(direction, msg),
        Transport::HttpWs => parse_http_ws(direction, msg),
    }
}

/// frame_fix performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn frame_fix(buf: &[u8]) -> Frame {
    if buf.len() < FIX_BEGIN.len() {
        // Too short to tell a real BeginString from a prefix of one. Waiting is correct:
        // skipping here would discard the head of a message that is merely incomplete.
        return Frame::Incomplete;
    }
    if !buf.starts_with(FIX_BEGIN) {
        return match find(buf, FIX_BEGIN) {
            Some(i) => Frame::Resync(i),
            // No boundary anywhere in the buffer. Retain the last FIX_BEGIN.len()-1 bytes:
            // a genuine "8=FIX" may straddle the end of what has been reassembled so far,
            // and skipping it would turn one desync into a second.
            None => Frame::Resync(buf.len().saturating_sub(FIX_BEGIN.len() - 1)),
        };
    }
    let Some(soh1) = find_byte(buf, SOH, 0) else {
        return Frame::Incomplete;
    };
    // "Not enough bytes yet" is NOT "malformed", and conflating them was a data-loss
    // bug: a buffer holding exactly "8=FIX.4.2\x01" plus a byte or two is the head of a
    // perfectly good message that has not finished arriving, and resyncing here discarded
    // it. The remainder then began mid-message, so the next call skipped forward to the
    // following BeginString and the whole order was lost — no packet loss required.
    //
    // It fires whenever a TCP segment boundary lands within a couple of bytes of a
    // message's BeginString: about one boundary per segment over ~7 messages, which
    // matches the ~0.2% of requests that went missing per pass-1 run.
    if buf.len() < soh1 + 3 {
        return Frame::Incomplete;
    }
    if &buf[soh1 + 1..soh1 + 3] != b"9=" {
        return Frame::Resync(soh1 + 1);
    }
    let Some(soh2) = find_byte(buf, SOH, soh1 + 3) else {
        return Frame::Incomplete;
    };
    let Some(body_len) = parse_uint(&buf[soh1 + 3..soh2]) else {
        return Frame::Resync(soh2 + 1);
    };
    let body_len = body_len as usize;
    let total = (soh2 + 1) + body_len + 7;
    if body_len > MAX_FIX_MESSAGE {
        return Frame::Resync(soh1 + 1);
    }
    if buf.len() < total {
        return Frame::Incomplete;
    }
    Frame::Message(total)
}

/// parse_fix performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn parse_fix(direction: Direction, msg: &[u8]) -> ParsedMessage {
    let mut msg_type: &[u8] = b"";
    let mut clordid = String::new();
    let mut orig = String::new();
    let mut exec150 = String::new();
    let mut ordstatus39 = String::new();
    let mut fill_qty = 0u64;
    let mut fill_price = 0u64;
    let mut liquidity = 0u8;

    for field in msg.split(|&b| b == SOH) {
        let Some(eq) = field.iter().position(|&b| b == b'=') else {
            continue;
        };
        let (tag, val) = (&field[..eq], &field[eq + 1..]);
        match tag {
            b"35" => msg_type = val,
            b"11" => clordid = string(val),
            b"41" => orig = string(val),
            b"150" => exec150 = string(val),
            b"39" => ordstatus39 = string(val),
            b"32" => fill_qty = parse_uint(val).unwrap_or(0),
            b"31" => fill_price = parse_decimal_scaled(val),
            b"851" => liquidity = parse_uint(val).unwrap_or(0) as u8,
            _ => {}
        }
    }

    let class = match (direction, msg_type) {
        (Direction::Request, b"D") | (Direction::Request, b"F") | (Direction::Request, b"G") => {
            Classified::Request
        }
        (Direction::Response, b"8") => Classified::Response,
        _ => Classified::Ignore,
    };

    let exec_type = if !exec150.is_empty() {
        exec150
    } else {
        ordstatus39
    };

    ParsedMessage {
        class,
        clordid,
        orig_clordid: orig,
        exec_type,
        fill_qty,
        fill_price,
        liquidity,
    }
}

/// frame_http_ws performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn frame_http_ws(direction: Direction, buf: &[u8]) -> Frame {
    if buf.is_empty() {
        return Frame::Incomplete;
    }
    if looks_like_http(buf) {
        return frame_http(buf);
    }
    // "Not enough bytes yet" is NOT "malformed" — the same conflation that cost FIX ~0.2% of
    // its requests (see frame_fix). looks_like_http needs four bytes to recognise a method,
    // so a buffer holding "P", "PO" or "POS" is the head of a good request that has not
    // finished arriving. Falling through to frame_ws here reads byte 1 as a WebSocket length
    // header, finds the mask bit inconsistent with the direction, and resyncs into the middle
    // of the request.
    //
    // Only STRICT prefixes of a method token wait. A two-byte WebSocket frame is not a prefix
    // of any of them, so genuine short frames are unaffected.
    if is_http_method_prefix(buf) {
        return Frame::Incomplete;
    }
    frame_ws(direction, buf)
}

/// The tokens that open an HTTP message. Shared by looks_like_http and
/// is_http_method_prefix so the two can never disagree about what counts as HTTP.
const HTTP_PREFIXES: [&[u8]; 5] = [b"POST", b"GET ", b"PUT ", b"DELE", b"HTTP"];

/// True when `buf` is a strict prefix of an HTTP method token — too short to classify, but
/// consistent with a request that is still arriving.
fn is_http_method_prefix(buf: &[u8]) -> bool {
    HTTP_PREFIXES
        .iter()
        .any(|p| buf.len() < p.len() && p.starts_with(buf))
}

/// looks_like_http performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn looks_like_http(buf: &[u8]) -> bool {
    HTTP_PREFIXES.iter().any(|p| buf.starts_with(p))
}

/// frame_http performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn frame_http(buf: &[u8]) -> Frame {
    let Some(hdr_end) = find(buf, b"\r\n\r\n") else {
        if buf.len() > MAX_HTTP_MESSAGE {
            return Frame::Resync(buf.len());
        }
        return Frame::Incomplete;
    };
    let body_start = hdr_end + 4;
    if header_is_chunked(&buf[..hdr_end]) {
        return match find(&buf[body_start..], b"0\r\n\r\n") {
            Some(rel) => {
                let total = body_start + rel + 5;
                if total > MAX_HTTP_MESSAGE {
                    Frame::Resync(body_start)
                } else {
                    Frame::Message(total)
                }
            }
            None if buf.len() > MAX_HTTP_MESSAGE => Frame::Resync(body_start),
            None => Frame::Incomplete,
        };
    }
    let content_len = header_content_length(&buf[..hdr_end]).unwrap_or(0);
    let total = body_start + content_len;
    if total > MAX_HTTP_MESSAGE {
        return Frame::Resync(body_start);
    }
    if buf.len() < total {
        return Frame::Incomplete;
    }
    Frame::Message(total)
}

/// header_is_chunked performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn header_is_chunked(headers: &[u8]) -> bool {
    let lower: Vec<u8> = headers.iter().map(|b| b.to_ascii_lowercase()).collect();
    match find(&lower, b"transfer-encoding:") {
        Some(i) => {
            let rest = &lower[i + b"transfer-encoding:".len()..];
            let end = rest.iter().position(|&b| b == b'\r').unwrap_or(rest.len());
            find(&rest[..end], b"chunked").is_some()
        }
        None => false,
    }
}

/// frame_ws performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn frame_ws(direction: Direction, buf: &[u8]) -> Frame {
    if buf.len() < 2 {
        return Frame::Incomplete;
    }
    // Byte 0 is FIN | RSV1 | RSV2 | RSV3 | opcode(4). Validating it is the only structure a
    // WebSocket stream offers for re-synchronisation: unlike FIX ("8=FIX.4.2") and HTTP
    // ("POST"), a binary frame header has no reserved marker, so after a gap the framer was
    // reading JSON payload bytes as headers and wandering through the stream — a hole cost
    // far more than its own bytes.
    //
    // Only 12 of 256 byte values are legal here, and every printable ASCII byte (0x20-0x7E)
    // sets a bit in the RSV mask, so a JSON payload cannot masquerade as a header.
    if !is_ws_frame_header(buf[0]) {
        return Frame::Resync(1);
    }
    let masked = buf[1] & 0x80 != 0;
    let len7 = (buf[1] & 0x7f) as usize;
    let (mut header, payload_len) = if len7 < 126 {
        (2usize, len7)
    } else if len7 == 126 {
        if buf.len() < 4 {
            return Frame::Incomplete;
        }
        (4usize, ((buf[2] as usize) << 8) | buf[3] as usize)
    } else {
        // len7 == 127: the 64-bit extended length, which RFC 6455 requires for any payload
        // past 65535 bytes — reachable by an engine that batches execution reports. Treating
        // it as malformed desynchronised the stream one byte at a time, losing the frame and
        // walking into the next.
        if buf.len() < 10 {
            return Frame::Incomplete;
        }
        let len = u64::from_be_bytes([
            buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9],
        ]);
        // A frame larger than the reassembler can ever hold would stall this flow forever
        // waiting for bytes that cannot be buffered, so treat it as a desync instead.
        if len > MAX_WS_MESSAGE as u64 {
            return Frame::Resync(1);
        }
        (10usize, len as usize)
    };
    if masked {
        header += 4;
    }
    let expect_masked = direction == Direction::Request;
    if masked != expect_masked {
        return Frame::Resync(1);
    }
    let total = header + payload_len;
    if buf.len() < total {
        return Frame::Incomplete;
    }
    Frame::Message(total)
}

/// True when `b` can legally open a WebSocket frame: reserved bits clear and a defined
/// opcode (continuation, text, binary, close, ping, pong).
///
/// RSV1 is treated as illegal rather than tolerated. It signals `permessage-deflate`, and a
/// compressed payload carries no readable `cl_ord_id` no matter how it is framed — so the
/// submission is unscoreable either way. Tolerating RSV1 would double the accepted byte
/// space and, worse, admit 0x40-0x4F ("@" through "O"), which appear throughout ordinary
/// JSON — measurably weakening the very marker this function exists to provide.
fn is_ws_frame_header(b: u8) -> bool {
    b & 0x70 == 0 && matches!(b & 0x0f, 0x0 | 0x1 | 0x2 | 0x8 | 0x9 | 0xA)
}

/// True when `buf` opens what would be a legal WebSocket frame except that RSV1 is set —
/// i.e. a `permessage-deflate` compressed frame.
///
/// Compression is not supported: the pipeline reads `cl_ord_id` out of the payload as plain
/// JSON. Without this, such a submission desynchronises and goes silent, which is
/// indistinguishable from an engine that answered nothing. Counting it turns that into a
/// named signal instead of a mystery zero.
///
/// Checking byte 0 ALONE is not enough, and shipping that was a mistake: roughly 9% of
/// arbitrary bytes satisfy it, so a REST session that had been holed by ring-buffer drops
/// reported 925 "compressed frames" on a stream carrying no WebSocket at all. Port 8080
/// serves REST and WS both, so the transport cannot disambiguate it either. The frame must
/// therefore be plausible as a WHOLE — correct mask direction and a length that actually
/// fits — before this claims anything.
pub fn ws_looks_compressed(direction: Direction, buf: &[u8]) -> bool {
    if buf.len() < 2 {
        return false;
    }
    // RSV1 set, RSV2/RSV3 clear, defined opcode.
    if buf[0] & 0x40 == 0 || buf[0] & 0x30 != 0 {
        return false;
    }
    if !matches!(buf[0] & 0x0f, 0x0 | 0x1 | 0x2 | 0x8 | 0x9 | 0xA) {
        return false;
    }
    // Masking is mandatory client-to-server and forbidden server-to-client; a mismatch
    // means these bytes are not a frame header at all.
    let masked = buf[1] & 0x80 != 0;
    if masked != (direction == Direction::Request) {
        return false;
    }
    // And the frame must actually fit what has been reassembled.
    let len7 = (buf[1] & 0x7f) as usize;
    if len7 >= 126 {
        return false;
    }
    let header = 2 + if masked { 4 } else { 0 };
    header + len7 <= buf.len()
}

/// parse_http_ws performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn parse_http_ws(direction: Direction, msg: &[u8]) -> ParsedMessage {
    let body: Vec<u8> = if looks_like_http(msg) {
        match find(msg, b"\r\n\r\n") {
            Some(i) => msg[i + 4..].to_vec(),
            None => Vec::new(),
        }
    } else {
        ws_unmasked_payload(direction, msg)
    };
    parse_json(direction, &body)
}

/// ws_unmasked_payload performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn ws_unmasked_payload(direction: Direction, frame: &[u8]) -> Vec<u8> {
    if frame.len() < 2 {
        return Vec::new();
    }
    let masked = frame[1] & 0x80 != 0;
    let len7 = (frame[1] & 0x7f) as usize;
    let (mut off, payload_len) = if len7 < 126 {
        (2usize, len7)
    } else if len7 == 126 && frame.len() >= 4 {
        (4usize, ((frame[2] as usize) << 8) | frame[3] as usize)
    } else if len7 == 127 && frame.len() >= 10 {
        // Must mirror frame_ws: framing a 64-bit-length frame correctly but then reading an
        // empty body here would move the loss downstream rather than fix it.
        let len = u64::from_be_bytes([
            frame[2], frame[3], frame[4], frame[5], frame[6], frame[7], frame[8], frame[9],
        ]);
        (10usize, len as usize)
    } else {
        return Vec::new();
    };
    let _ = direction;
    if masked {
        if frame.len() < off + 4 {
            return Vec::new();
        }
        let mask = [frame[off], frame[off + 1], frame[off + 2], frame[off + 3]];
        off += 4;
        let end = (off + payload_len).min(frame.len());
        frame[off..end]
            .iter()
            .enumerate()
            .map(|(i, &b)| b ^ mask[i % 4])
            .collect()
    } else {
        let end = (off + payload_len).min(frame.len());
        frame[off..end].to_vec()
    }
}

/// parse_json performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn parse_json(direction: Direction, body: &[u8]) -> ParsedMessage {
    let clordid = json_string(body, "cl_ord_id").unwrap_or_default();
    let orig = json_string(body, "orig_cl_ord_id").unwrap_or_default();
    let exec_type = json_string(body, "exec_type").unwrap_or_default();
    let fill_qty = json_uint(body, "fill_qty").unwrap_or(0);
    let fill_price = json_decimal_scaled(body, "fill_price").unwrap_or(0);
    let liquidity = json_uint(body, "liquidity").unwrap_or(0) as u8;

    let class = if clordid.is_empty() {
        Classified::Ignore
    } else if direction == Direction::Request {
        Classified::Request
    } else {
        Classified::Response
    };

    ParsedMessage {
        class,
        clordid,
        orig_clordid: orig,
        exec_type,
        fill_qty,
        fill_price,
        liquidity,
    }
}

/// find performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    // memmem, not a positional scan: the naive version compiled to a memcmp
    // call per byte offset and was 63% of the HTTP pipeline profile
    // (docs/http-pipeline-flamegraph.svg, 2026-08-01).
    if needle.is_empty() {
        return None;
    }
    memchr::memmem::find(hay, needle)
}

/// find_byte performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn find_byte(hay: &[u8], b: u8, from: usize) -> Option<usize> {
    hay.get(from..)?
        .iter()
        .position(|&x| x == b)
        .map(|i| i + from)
}

/// string performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn string(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// parse_uint performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn parse_uint(b: &[u8]) -> Option<u64> {
    if b.is_empty() {
        return None;
    }
    let mut v = 0u64;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.saturating_mul(10).saturating_add((c - b'0') as u64);
    }
    Some(v)
}

/// parse_decimal_scaled performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn parse_decimal_scaled(b: &[u8]) -> u64 {
    let mut whole = 0u64;
    let mut frac = 0u64;
    let mut frac_digits = 0u32;
    let mut seen_dot = false;
    for &c in b {
        match c {
            b'.' => seen_dot = true,
            b'0'..=b'9' => {
                if seen_dot {
                    if frac_digits < 9 {
                        frac = frac * 10 + (c - b'0') as u64;
                        frac_digits += 1;
                    }
                } else {
                    whole = whole.saturating_mul(10).saturating_add((c - b'0') as u64);
                }
            }
            _ => break,
        }
    }
    while frac_digits < 9 {
        frac *= 10;
        frac_digits += 1;
    }
    whole.saturating_mul(PRICE_SCALE).saturating_add(frac)
}

/// header_content_length performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn header_content_length(headers: &[u8]) -> Option<usize> {
    let lower: Vec<u8> = headers.iter().map(|b| b.to_ascii_lowercase()).collect();
    let i = find(&lower, b"content-length:")?;
    let rest = &headers[i + b"content-length:".len()..];
    let end = rest.iter().position(|&b| b == b'\r').unwrap_or(rest.len());
    let val: Vec<u8> = rest[..end]
        .iter()
        .copied()
        .filter(|b| !b.is_ascii_whitespace())
        .collect();
    parse_uint(&val).map(|v| v as usize)
}

/// Longest JSON key the extractors look up; patterns are built on the stack
/// (`format!` here allocated per field per message — ~15% of the HTTP
/// pipeline profile between malloc/free/format_inner).
const MAX_JSON_KEY: usize = 30;

/// key_pattern writes `"key"` into `buf` and returns the filled slice.
fn key_pattern<'a>(buf: &'a mut [u8; MAX_JSON_KEY + 2], key: &str) -> &'a [u8] {
    let k = key.as_bytes();
    debug_assert!(k.len() <= MAX_JSON_KEY, "raise MAX_JSON_KEY for {key}");
    buf[0] = b'"';
    buf[1..1 + k.len()].copy_from_slice(k);
    buf[1 + k.len()] = b'"';
    &buf[..k.len() + 2]
}

/// json_string performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn json_string(body: &[u8], key: &str) -> Option<String> {
    let mut pbuf = [0u8; MAX_JSON_KEY + 2];
    let pat = key_pattern(&mut pbuf, key);
    let i = find(body, pat)?;
    let rest = &body[i + pat.len()..];
    let colon = rest.iter().position(|&b| b == b':')?;
    let after = &rest[colon + 1..];
    let q1 = after.iter().position(|&b| b == b'"')?;
    let after_q = &after[q1 + 1..];
    let q2 = after_q.iter().position(|&b| b == b'"')?;
    Some(string(&after_q[..q2]))
}

/// json_value_slice performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn json_value_slice<'a>(body: &'a [u8], key: &str) -> Option<&'a [u8]> {
    let mut pbuf = [0u8; MAX_JSON_KEY + 2];
    let pat = key_pattern(&mut pbuf, key);
    let i = find(body, pat)?;
    let rest = &body[i + pat.len()..];
    let colon = rest.iter().position(|&b| b == b':')?;
    let after = &rest[colon + 1..];
    let start = after.iter().position(|&b| !b.is_ascii_whitespace())?;
    let val = &after[start..];
    let end = val
        .iter()
        .position(|&b| b == b',' || b == b'}' || b == b'"' || b == b' ')
        .unwrap_or(val.len());
    Some(&val[..end])
}

/// json_uint performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn json_uint(body: &[u8], key: &str) -> Option<u64> {
    parse_uint(json_value_slice(body, key)?)
}

/// json_decimal_scaled performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn json_decimal_scaled(body: &[u8], key: &str) -> Option<u64> {
    Some(parse_decimal_scaled(json_value_slice(body, key)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A valid message head that has not finished arriving must WAIT, never resync.
    ///
    /// This is the bug that produced the entire capture-gap population. Sampled live:
    /// buffered=11, sample="8=FIX.4.2|9" — a good message whose head was discarded because
    /// the length check shared a branch with the malformed check. The remainder then
    /// started mid-message and the next resync skipped 229 bytes to the next BeginString,
    /// taking the order with it.
    #[test]
    fn fix_frame_waits_when_begin_string_arrives_without_the_length_field() {
        for partial in [
            &b"8=FIX.4.2\x01"[..],
            &b"8=FIX.4.2\x019"[..],
            &b"8=FIX.4.4\x01"[..],
        ] {
            assert!(
                matches!(frame_fix(partial), Frame::Incomplete),
                "a message head must be awaited, not discarded: {:?}",
                String::from_utf8_lossy(partial)
            );
        }
    }

    /// Once enough bytes exist, a genuinely wrong second tag still resyncs.
    #[test]
    fn fix_frame_resyncs_when_the_second_tag_is_not_body_length() {
        let buf = b"8=FIX.4.2\x0134=1\x0110=000\x01";
        assert!(
            matches!(frame_fix(buf), Frame::Resync(_)),
            "a real malformed frame must still resync"
        );
    }

    /// A desync must land on the NEXT REAL message, not on "8=" inside tag 38.
    ///
    /// This is the bug that made a 0.2% request loss self-amplifying: mid-message bytes
    /// followed by a complete order. Searching for a bare "8=" matched the "8=" inside
    /// "38=100", so the framer resynced into the middle of the very message it was trying
    /// to recover, then walked forward field by field and consumed it entirely.
    #[test]
    fn fix_resync_skips_to_begin_string_not_to_tag_38() {
        let garbage = b"45=x\x0138=100\x01";
        let msg = b"8=FIX.4.4\x019=5\x0135=D\x0110=000\x01";
        let mut buf = Vec::new();
        buf.extend_from_slice(garbage);
        buf.extend_from_slice(msg);

        match frame_fix(&buf) {
            Frame::Resync(skip) => {
                assert_eq!(
                    skip,
                    garbage.len(),
                    "resync must skip exactly the garbage and land on 8=FIX, not inside 38="
                );
                assert!(
                    buf[skip..].starts_with(FIX_BEGIN),
                    "post-resync buffer must start at a real message boundary"
                );
            }
            other => panic!("expected Resync, got {other:?}"),
        }
    }

    /// A buffer holding only mid-message bytes keeps the last few, so a BeginString
    /// straddling the reassembly boundary is not chopped in half.
    #[test]
    fn fix_resync_without_a_boundary_retains_a_partial_begin_string() {
        let buf = b"38=100\x0144=9\x018=FI";
        match frame_fix(buf) {
            Frame::Resync(skip) => {
                assert_eq!(skip, buf.len() - (FIX_BEGIN.len() - 1));
                assert_eq!(
                    &buf[skip..],
                    b"8=FI",
                    "the partial BeginString must survive"
                );
            }
            other => panic!("expected Resync, got {other:?}"),
        }
    }

    /// "8=" appearing inside a field must never be treated as a message start.
    #[test]
    fn fix_frame_does_not_start_on_tag_38() {
        let buf = b"38=100\x0110=000\x01";
        match frame_fix(buf) {
            Frame::Resync(_) => {}
            other => panic!("tag 38 must not frame as a message, got {other:?}"),
        }
    }

    /// Fewer bytes than "8=FIX" is incomplete, not a desync: discarding here would eat
    /// the head of a message that has merely not fully arrived.
    #[test]
    fn fix_frame_waits_for_enough_bytes_to_identify_a_boundary() {
        assert!(matches!(frame_fix(b"8=F"), Frame::Incomplete));
        assert!(matches!(frame_fix(b""), Frame::Incomplete));
    }

    use super::*;

    #[test]
    /// chunked_http_framed_through_terminator performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn chunked_http_framed_through_terminator() {
        let resp = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
        match frame_http(resp) {
            Frame::Message(n) => assert_eq!(n, resp.len(), "frame whole chunked message"),
            other => panic!("expected Message, got {:?}", other),
        }
        let partial = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhel";
        assert_eq!(frame_http(partial), Frame::Incomplete);
    }

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

    #[test]
    /// frames_one_fix_message_by_body_length performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn frames_one_fix_message_by_body_length() {
        let m = fix("35=D|49=IICPC-BOT|56=CONTESTANT|34=7|11=sess_1_7_O|55=IICPC|54=1|38=12|40=2|44=42.5|59=0|");
        assert_eq!(
            frame(Transport::Fix, Direction::Request, &m),
            Frame::Message(m.len())
        );
    }

    #[test]
    /// frames_coalesced_then_consumes performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn frames_coalesced_then_consumes() {
        let a = fix("35=D|49=IICPC-BOT|56=CONTESTANT|34=8|11=sess_1_8_O|38=1|40=2|44=1.0|");
        let b = fix("35=D|49=IICPC-BOT|56=CONTESTANT|34=9|11=sess_1_9_O|38=1|40=2|44=2.0|");
        let mut stream = a.clone();
        stream.extend_from_slice(&b);
        let Frame::Message(n) = frame(Transport::Fix, Direction::Request, &stream) else {
            panic!("expected first message");
        };
        assert_eq!(n, a.len());
        let Frame::Message(n2) = frame(Transport::Fix, Direction::Request, &stream[n..]) else {
            panic!("expected second message");
        };
        assert_eq!(n2, b.len());
    }

    #[test]
    /// incomplete_fix_waits_for_more performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn incomplete_fix_waits_for_more() {
        let m = fix("35=D|49=IICPC-BOT|34=7|11=sess_1_7_O|38=1|40=2|44=42.5|");
        assert_eq!(
            frame(Transport::Fix, Direction::Request, &m[..m.len() - 3]),
            Frame::Incomplete
        );
    }

    #[test]
    /// extracts_clordid_regardless_of_offset performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn extracts_clordid_regardless_of_offset() {
        let m = fix("35=D|49=IICPC-BOT|56=CONTESTANT|34=123|52=19700101-00:00:00.000|11=sess_42_123_O|21=1|55=IICPC|54=1|38=12|40=2|44=42.5|59=0|");
        let p = parse(Transport::Fix, Direction::Request, &m);
        assert_eq!(p.class, Classified::Request);
        assert_eq!(p.clordid, "sess_42_123_O");
    }

    #[test]
    /// cancel_request_carries_orig_clordid performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn cancel_request_carries_orig_clordid() {
        let m = fix(
            "35=F|49=IICPC-BOT|56=CONTESTANT|34=5|11=sess_1_5_C|41=sess_1_2_O|55=IICPC|54=1|38=3|",
        );
        let p = parse(Transport::Fix, Direction::Request, &m);
        assert_eq!(p.class, Classified::Request);
        assert_eq!(p.clordid, "sess_1_5_C");
        assert_eq!(p.orig_clordid, "sess_1_2_O");
    }

    #[test]
    /// execution_report_is_a_response_with_fill performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn execution_report_is_a_response_with_fill() {
        let m = fix("35=8|49=CONTESTANT|56=IICPC-BOT|34=2|37=EXEC_2|11=order-real-1|17=E2|150=F|39=2|32=12|31=42.5|");
        let p = parse(Transport::Fix, Direction::Response, &m);
        assert_eq!(p.class, Classified::Response);
        assert_eq!(p.clordid, "order-real-1");
        assert_eq!(p.exec_type, "F");
        assert_eq!(p.fill_qty, 12);
        assert_eq!(p.fill_price, 42_500_000_000);
    }

    #[test]
    /// logon_is_ignored performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn logon_is_ignored() {
        let m = fix("35=A|49=IICPC-BOT|56=CONTESTANT|34=1|98=0|108=30|");
        let p = parse(Transport::Fix, Direction::Request, &m);
        assert_eq!(p.class, Classified::Ignore);
    }

    #[test]
    /// http_response_framed_by_content_length_and_parsed performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn http_response_framed_by_content_length_and_parsed() {
        let body =
            br#"{"cl_ord_id":"order-rest-1","exec_type":"F","fill_qty":7,"fill_price":99.25}"#;
        let mut msg = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        msg.extend_from_slice(body);
        assert_eq!(
            frame(Transport::HttpWs, Direction::Response, &msg),
            Frame::Message(msg.len())
        );
        let p = parse(Transport::HttpWs, Direction::Response, &msg);
        assert_eq!(p.class, Classified::Response);
        assert_eq!(p.clordid, "order-rest-1");
        assert_eq!(p.exec_type, "F");
        assert_eq!(p.fill_qty, 7);
        assert_eq!(p.fill_price, 99_250_000_000);
    }

    #[test]
    /// masked_ws_request_is_unmasked_and_parsed performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn masked_ws_request_is_unmasked_and_parsed() {
        let body = br#"{"cl_ord_id":"order-ws-1","qty":3,"price":11.75}"#;
        let mask = [0x13u8, 0x37, 0xc0, 0xde];
        let mut frame_bytes = vec![0x81u8, 0x80 | body.len() as u8];
        frame_bytes.extend_from_slice(&mask);
        for (i, &b) in body.iter().enumerate() {
            frame_bytes.push(b ^ mask[i % 4]);
        }
        assert_eq!(
            frame(Transport::HttpWs, Direction::Request, &frame_bytes),
            Frame::Message(frame_bytes.len())
        );
        let p = parse(Transport::HttpWs, Direction::Request, &frame_bytes);
        assert_eq!(p.class, Classified::Request);
        assert_eq!(p.clordid, "order-ws-1");
    }
}
