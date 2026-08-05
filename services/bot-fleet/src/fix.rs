//! FIX/JSON frame construction and template-and-patch rendering for bot-fleet.

use iicpc_schemas_rust::{OrdType, PayloadType, Protocol, Side};

use crate::time;

pub const FIX_TIMESTAMP_LEN: usize = 21;
const FIX_TIMESTAMP_PLACEHOLDER: &[u8; FIX_TIMESTAMP_LEN] = b"19700101-00:00:00.000";

const SOH: u8 = 0x01;

/// split_order_id extracts the task-local (seq, kind letter) from a
/// `{session}_{task}_{seq}_{K}` order id, so hot-path bookkeeping (the expiry
/// queue) can store 5 bytes instead of the whole heap string; `join_order_id`
/// rebuilds the exact id on the rare eviction path.
pub fn split_order_id(order_id: &str) -> Option<(u32, u8)> {
    let mut it = order_id.rsplitn(3, '_');
    let kind = it.next()?.as_bytes();
    let seq: u32 = it.next()?.parse().ok()?;
    if kind.len() != 1 {
        return None;
    }
    Some((seq, kind[0]))
}

/// join_order_id is split_order_id's inverse given the task's identity.
pub fn join_order_id(session_id: &str, bot_id: u64, seq: u32, kind: u8) -> String {
    format!("{session_id}_{bot_id}_{seq}_{}", kind as char)
}

pub fn new_limit_order_id(session_id: &str, bot_id: u64, seq: u64) -> String {
    format!("{session_id}_{bot_id}_{seq}_O")
}

pub fn replace_order_id(session_id: &str, bot_id: u64, seq: u64) -> String {
    format!("{session_id}_{bot_id}_{seq}_R")
}

#[derive(Debug, Clone)]
/// OrderFrame carries exactly one wire representation — the one selected by the
/// task's protocol at render time. `tag52_offset` is only ever `Some` for FIX
/// frames (there is no tag 52 to patch in REST/WS payloads).
pub struct OrderFrame {
    pub order_id: String,
    pub orig_order_id: String,
    pub price: u64,
    pub qty: u64,
    pub side: Side,
    pub bytes: Vec<u8>,
    pub tag52_offset: Option<usize>,
    pub payload_type: PayloadType,
    pub ord_type: OrdType,
    /// Self-match-prevention id encoded into `bytes` (FIX tag 7928 / JSON `smp_id`),
    /// or `SMP_ID_NONE` when the task carries no SMP id and the field is OMITTED from
    /// the wire entirely. Carried alongside the frame so telemetry can report what was
    /// actually sent without re-parsing the payload.
    pub smp_id: u32,
}

impl OrderFrame {
    pub fn patch_timestamp(&mut self, now_ns: u64) {
        let Some(offset) = self.tag52_offset else {
            return;
        };

        let new_ts = time::format_fix_timestamp(now_ns);

        let mut old_sum = 0u32;
        let mut new_sum = 0u32;
        for i in 0..FIX_TIMESTAMP_LEN {
            old_sum += u32::from(self.bytes[offset + i]);
            new_sum += u32::from(new_ts[i]);
            self.bytes[offset + i] = new_ts[i];
        }

        let chk_offset = self.bytes.len() - 4;
        let old_chk_digit1 = self.bytes[chk_offset] - b'0';
        let old_chk_digit2 = self.bytes[chk_offset + 1] - b'0';
        let old_chk_digit3 = self.bytes[chk_offset + 2] - b'0';
        let old_checksum = u32::from(old_chk_digit1) * 100
            + u32::from(old_chk_digit2) * 10
            + u32::from(old_chk_digit3);

        let new_checksum = (old_checksum + 256 + (new_sum % 256) - (old_sum % 256)) % 256;

        self.bytes[chk_offset] = b'0' + (new_checksum / 100) as u8;
        self.bytes[chk_offset + 1] = b'0' + ((new_checksum / 10) % 10) as u8;
        self.bytes[chk_offset + 2] = b'0' + (new_checksum % 10) as u8;
    }
}

pub fn logon_frame(fix_version: &str, seq: u64) -> Vec<u8> {
    let body = format!("35=A\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={seq}\x0198=0\x01108=30\x01");
    finalize_fix(fix_version, &body)
}

fn find_tag52_offset(fix: &[u8]) -> Option<usize> {
    fix.windows(3 + FIX_TIMESTAMP_LEN)
        .position(|window| window.starts_with(b"52=") && &window[3..] == FIX_TIMESTAMP_PLACEHOLDER)
        .map(|pos| pos + 3)
}

#[derive(Clone, Copy)]
/// FrameKind enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
enum FrameKind {
    New,
    Market,
    Cancel,
    Replace,
}

impl FrameKind {
    fn payload_type(self) -> PayloadType {
        match self {
            Self::New | Self::Market => PayloadType::New,
            Self::Cancel => PayloadType::Cancel,
            Self::Replace => PayloadType::Replace,
        }
    }

    fn ord_type(self) -> OrdType {
        match self {
            Self::Market => OrdType::Market,
            Self::New | Self::Cancel | Self::Replace => OrdType::Limit,
        }
    }

    fn order_id(self, session_id: &str, bot_id: u64, seq: u64) -> String {
        match self {
            Self::New => new_limit_order_id(session_id, bot_id, seq),
            Self::Market => format!("{session_id}_{bot_id}_{seq}_M"),
            Self::Cancel => format!("{session_id}_{bot_id}_{seq}_C"),
            Self::Replace => replace_order_id(session_id, bot_id, seq),
        }
    }

    fn rest_method(self) -> &'static str {
        match self {
            Self::New | Self::Market => "POST",
            Self::Cancel => "DELETE",
            Self::Replace => "PUT",
        }
    }

    fn rest_path(self, orig_order_id: &str) -> String {
        match self {
            Self::New | Self::Market => "/orders".to_string(),
            Self::Cancel | Self::Replace => format!("/orders/{orig_order_id}"),
        }
    }
}

fn build_fix_body(
    kind: FrameKind,
    seq: u64,
    order_id: &str,
    orig_order_id: Option<&str>,
    price: u64,
    qty: u64,
    side: Side,
    smp_id: u32,
) -> String {
    let side_tag = match side {
        Side::Buy => "1",
        Side::Sell => "2",
    };

    let msg_seq_num = seq + 1;
    // Empty when the order carries no SMP id, so the tag is absent from the wire.
    let smp = fix_smp_tag(smp_id);

    match kind {
        FrameKind::New => format!(
            "35=D\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={msg_seq_num}\x0152=19700101-00:00:00.000\x0111={order_id}\x0121=1\x0155=IICPC\x0154={side_tag}\x0138={qty}\x0140=2\x0144={price}\x01{smp}59=0\x01"
        ),
        FrameKind::Market => format!(
            "35=D\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={msg_seq_num}\x0152=19700101-00:00:00.000\x0111={order_id}\x0121=1\x0155=IICPC\x0154={side_tag}\x0138={qty}\x0140=1\x01{smp}59=0\x01"
        ),
        FrameKind::Cancel => {
            let orig_order_id = orig_order_id.expect("cancel requires orig_order_id");
            format!(
                "35=F\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={msg_seq_num}\x0152=19700101-00:00:00.000\x0111={order_id}\x0141={orig_order_id}\x0155=IICPC\x0154={side_tag}\x0138={qty}\x01{smp}"
            )
        }
        FrameKind::Replace => {
            let orig_order_id = orig_order_id.expect("replace requires orig_order_id");
            format!(
                "35=G\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={msg_seq_num}\x0152=19700101-00:00:00.000\x0111={order_id}\x0141={orig_order_id}\x0121=1\x0155=IICPC\x0154={side_tag}\x0138={qty}\x0140=2\x0144={price}\x01{smp}"
            )
        }
    }
}

fn build_json_payload(
    kind: FrameKind,
    order_id: &str,
    orig_order_id: Option<&str>,
    price: u64,
    qty: u64,
    side: Side,
) -> String {
    match kind {
        FrameKind::New => {
            let side_name = match side {
                Side::Buy => "BUY",
                Side::Sell => "SELL",
            };
            let encoded_order_id =
                serde_json::to_string(order_id).expect("serializing String cannot fail");
            format!("{{\"cl_ord_id\":{encoded_order_id},\"symbol\":\"IICPC\",\"side\":\"{side_name}\",\"qty\":{qty},\"price\":{price}}}")
        }
        FrameKind::Market => {
            let side_name = match side {
                Side::Buy => "BUY",
                Side::Sell => "SELL",
            };
            let encoded_order_id =
                serde_json::to_string(order_id).expect("serializing String cannot fail");
            format!("{{\"cl_ord_id\":{encoded_order_id},\"symbol\":\"IICPC\",\"side\":\"{side_name}\",\"qty\":{qty},\"ord_type\":\"MARKET\"}}")
        }
        FrameKind::Cancel => {
            let orig_order_id = orig_order_id.expect("cancel requires orig_order_id");
            format!("{{\"action\":\"CANCEL\",\"cl_ord_id\":\"{order_id}\",\"orig_cl_ord_id\":\"{orig_order_id}\",\"symbol\":\"IICPC\"}}")
        }
        FrameKind::Replace => {
            let orig_order_id = orig_order_id.expect("replace requires orig_order_id");
            let side_name = match side {
                Side::Buy => "BUY",
                Side::Sell => "SELL",
            };
            format!("{{\"action\":\"REPLACE\",\"cl_ord_id\":\"{order_id}\",\"orig_cl_ord_id\":\"{orig_order_id}\",\"symbol\":\"IICPC\",\"side\":\"{side_name}\",\"qty\":{qty},\"price\":{price}}}")
        }
    }
}

fn build_rest_request(method: &str, target_host: &str, path: &str, json: &str) -> Vec<u8> {
    let mut rest = String::with_capacity(96 + json.len());
    use std::fmt::Write;
    write!(
        &mut rest,
        "{method} {path} HTTP/1.1\r\nHost: {target_host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
        json.len(),
        json
    )
    .expect("writing to String cannot fail");
    rest.into_bytes()
}

#[allow(clippy::too_many_arguments)]
fn build_frame(
    protocol: Protocol,
    fix_version: &str,
    session_id: &str,
    target_host: &str,
    bot_id: u64,
    seq: u64,
    orig_order_id: Option<&str>,
    price: u64,
    qty: u64,
    side: Side,
    kind: FrameKind,
    smp_id: u32,
) -> OrderFrame {
    let order_id = kind.order_id(session_id, bot_id, seq);

    let (bytes, tag52_offset) = match protocol {
        Protocol::Fix => {
            let body = build_fix_body(
                kind,
                seq,
                &order_id,
                orig_order_id,
                price,
                qty,
                side,
                smp_id,
            );
            let fix = finalize_fix(fix_version, &body);
            let tag52_offset = find_tag52_offset(&fix);
            (fix, tag52_offset)
        }
        Protocol::Rest => {
            let json = build_json_payload(kind, &order_id, orig_order_id, price, qty, side);
            let rest = build_rest_request(
                kind.rest_method(),
                target_host,
                &kind.rest_path(orig_order_id.unwrap_or("")),
                &json,
            );
            (rest, None)
        }
        Protocol::Ws => {
            let json = build_json_payload(kind, &order_id, orig_order_id, price, qty, side);
            (json.into_bytes(), None)
        }
    };

    OrderFrame {
        order_id,
        orig_order_id: orig_order_id.unwrap_or("").to_string(),
        price,
        qty,
        side,
        bytes,
        tag52_offset,
        payload_type: kind.payload_type(),
        ord_type: kind.ord_type(),
        smp_id,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn order_frame(
    protocol: Protocol,
    fix_version: &str,
    session_id: &str,
    target_host: &str,
    bot_id: u64,
    seq: u64,
    price: u64,
    qty: u64,
    side: Side,
) -> OrderFrame {
    build_frame(
        protocol,
        fix_version,
        session_id,
        target_host,
        bot_id,
        seq,
        None,
        price,
        qty,
        side,
        FrameKind::New,
        iicpc_schemas_rust::SMP_ID_NONE,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn market_frame(
    protocol: Protocol,
    fix_version: &str,
    session_id: &str,
    target_host: &str,
    bot_id: u64,
    seq: u64,
    qty: u64,
    side: Side,
) -> OrderFrame {
    build_frame(
        protocol,
        fix_version,
        session_id,
        target_host,
        bot_id,
        seq,
        None,
        0,
        qty,
        side,
        FrameKind::Market,
        iicpc_schemas_rust::SMP_ID_NONE,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn cancel_frame(
    protocol: Protocol,
    fix_version: &str,
    session_id: &str,
    target_host: &str,
    bot_id: u64,
    seq: u64,
    orig_order_id: &str,
    price: u64,
    qty: u64,
    side: Side,
) -> OrderFrame {
    build_frame(
        protocol,
        fix_version,
        session_id,
        target_host,
        bot_id,
        seq,
        Some(orig_order_id),
        price,
        qty,
        side,
        FrameKind::Cancel,
        iicpc_schemas_rust::SMP_ID_NONE,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn replace_frame(
    protocol: Protocol,
    fix_version: &str,
    session_id: &str,
    target_host: &str,
    bot_id: u64,
    seq: u64,
    orig_order_id: &str,
    price: u64,
    qty: u64,
    side: Side,
) -> OrderFrame {
    build_frame(
        protocol,
        fix_version,
        session_id,
        target_host,
        bot_id,
        seq,
        Some(orig_order_id),
        price,
        qty,
        side,
        FrameKind::Replace,
        iicpc_schemas_rust::SMP_ID_NONE,
    )
}

// ---------------------------------------------------------------------------
// P2 / P2': template-and-patch rendering.
//
// The builders above (`build_frame` and friends) are kept as the reference
// implementation: they are the correctness oracle the property tests check
// the template path against, and `order_frame`/`market_frame`/etc. remain the
// public per-call API used by Cancel/Replace (see below) and by the criterion
// bench baseline.
//
// FIX numeric fields (34=/38=/44=) are ASCII tag=value, so they zero-pad safely to
// `FIX_NUM_WIDTH` (wide enough for any u64) and BodyLength never moves because of a
// qty/price value change there. ClOrdID (tag 11) stays unpadded per the join-key
// contract, so its digit count (and therefore BodyLength) still moves at each
// power-of-ten seq boundary; that is the one condition that forces a FIX template
// re-render. JSON numbers cannot be zero-padded at all (see `JsonTemplate` below),
// so the REST/WS path re-renders on qty/price digit-width change too.

const FIX_NUM_WIDTH: usize = 20;

fn digit_width(mut v: u64) -> usize {
    let mut w = 1;
    v /= 10;
    while v > 0 {
        w += 1;
        v /= 10;
    }
    w
}

/// SMP_ID_WIDTH is the fixed width of FIX tag 7928's value. Zero-padded ASCII, which
/// FIX's tag=value model allows, so the field never changes length and the in-place
/// byte patcher keeps working (unlike JSON, where leading zeros are illegal in
/// numbers — see build_json_payload_natural).
const SMP_ID_WIDTH: usize = 3;

/// fix_smp_tag renders `7928=<padded>\x01`, or the EMPTY STRING when the order
/// carries no SMP id. Omitting the tag entirely (rather than sending 7928=000 or an
/// empty value) is the contract: an order with no id is UNCONSTRAINED, and a
/// contestant must be able to tell that apart from id 0.
fn fix_smp_tag(smp_id: u32) -> String {
    if smp_id == iicpc_schemas_rust::SMP_ID_NONE {
        return String::new();
    }
    format!("7928={}\x01", pad_u64(smp_id as u64, SMP_ID_WIDTH))
}

fn pad_u64(v: u64, width: usize) -> String {
    format!("{v:0width$}")
}

/// Overwrites `width` zero-padded decimal digits of `value` at `buf[off..]`,
/// returning the (old, new) byte-sum deltas so the caller can fold them into
/// a single checksum update instead of rescanning the whole frame.
fn write_padded_u64(buf: &mut [u8], off: usize, width: usize, value: u64) -> (u32, u32) {
    let mut old_sum = 0u32;
    let mut new_sum = 0u32;
    let mut v = value;
    for i in (0..width).rev() {
        let digit = (v % 10) as u8;
        v /= 10;
        let b = b'0' + digit;
        old_sum += u32::from(buf[off + i]);
        new_sum += u32::from(b);
        buf[off + i] = b;
    }
    (old_sum, new_sum)
}

/// Applies a byte-sum delta to the embedded 3-digit FIX checksum (tag 10),
/// the same delta trick `OrderFrame::patch_timestamp` already used for tag 52.
fn apply_checksum_delta(buf: &mut [u8], checksum_off: usize, old_sum: u32, new_sum: u32) {
    let d1 = u32::from(buf[checksum_off] - b'0');
    let d2 = u32::from(buf[checksum_off + 1] - b'0');
    let d3 = u32::from(buf[checksum_off + 2] - b'0');
    let old_checksum = d1 * 100 + d2 * 10 + d3;
    let new_checksum = (old_checksum + 256 + (new_sum % 256) - (old_sum % 256)) % 256;
    buf[checksum_off] = b'0' + (new_checksum / 100) as u8;
    buf[checksum_off + 1] = b'0' + ((new_checksum / 10) % 10) as u8;
    buf[checksum_off + 2] = b'0' + (new_checksum % 10) as u8;
}

fn build_fix_body_padded(
    kind: FrameKind,
    seq: u64,
    order_id: &str,
    side: Side,
    qty: u64,
    price: u64,
    smp_id: u32,
) -> String {
    let side_tag = match side {
        Side::Buy => "1",
        Side::Sell => "2",
    };
    let msg_seq_num = pad_u64(seq + 1, FIX_NUM_WIDTH);
    let qty_s = pad_u64(qty, FIX_NUM_WIDTH);
    // Fixed width, so the field never shifts the offsets the patcher relies on.
    let smp = fix_smp_tag(smp_id);
    match kind {
        FrameKind::New => {
            let price_s = pad_u64(price, FIX_NUM_WIDTH);
            format!(
                "35=D\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={msg_seq_num}\x0152=19700101-00:00:00.000\x0111={order_id}\x0121=1\x0155=IICPC\x0154={side_tag}\x0138={qty_s}\x0140=2\x0144={price_s}\x01{smp}59=0\x01"
            )
        }
        FrameKind::Market => format!(
            "35=D\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={msg_seq_num}\x0152=19700101-00:00:00.000\x0111={order_id}\x0121=1\x0155=IICPC\x0154={side_tag}\x0138={qty_s}\x0140=1\x01{smp}59=0\x01"
        ),
        FrameKind::Cancel | FrameKind::Replace => {
            unreachable!("build_fix_body_padded only supports New/Market; Cancel/Replace use the reference builder")
        }
    }
}

/// A reusable FIX buffer for one `FrameKind` on one FIX-protocol task. New/Market
/// orders patch in place; Cancel/Replace (whose `orig_order_id` references another
/// order's unpadded ClOrdID, defeating fixed offsets) fall back to the reference
/// builder every call — see the module doc above.
struct FixTemplate {
    kind: FrameKind,
    fix_version: String,
    session_id: String,
    bot_id: u64,
    buf: Vec<u8>,
    clordid_seq_width: usize,
    clordid_seq_off: usize,
    tag34_off: usize,
    side_off: usize,
    qty_off: usize,
    price_off: Option<usize>,
    tag52_off: usize,
    checksum_off: usize,
    /// Byte offset of FIX tag 7928's fixed-width value, or None when this template
    /// was built for an order with no SMP id (the tag is then absent entirely).
    /// Fixed width means `patch` can rewrite the id in place, so the rotating id does
    /// not force a re-render per order.
    smp_off: Option<usize>,
    /// The SMP id currently baked into `buf`. When `patch` is called with a different
    /// id it is written at `smp_off`; when it is called with SMP_ID_NONE against a
    /// template that HAS the tag (or vice versa) the buffer must be rebuilt, since
    /// presence/absence changes the byte length.
    smp_id: u32,
}

impl FixTemplate {
    #[allow(clippy::too_many_arguments)]
    fn build(
        kind: FrameKind,
        fix_version: &str,
        session_id: &str,
        bot_id: u64,
        seq: u64,
        qty: u64,
        price: u64,
        side: Side,
        smp_id: u32,
    ) -> Self {
        let order_id = kind.order_id(session_id, bot_id, seq);
        let body = build_fix_body_padded(kind, seq, &order_id, side, qty, price, smp_id);
        let buf = finalize_fix(fix_version, &body);

        let tag34_off = find_subslice(&buf, b"\x0134=").expect("tag 34 present") + 4;
        let side_off = find_subslice(&buf, b"\x0154=").expect("tag 54 present") + 4;
        let qty_off = find_subslice(&buf, b"\x0138=").expect("tag 38 present") + 4;
        let price_off = matches!(kind, FrameKind::New)
            .then(|| find_subslice(&buf, b"\x0144=").expect("tag 44 present") + 4);
        let clordid_prefix = format!("\x0111={session_id}_{bot_id}_");
        let clordid_seq_off = find_subslice(&buf, clordid_prefix.as_bytes())
            .expect("ClOrdID present")
            + clordid_prefix.len();
        let tag52_off = find_tag52_offset(&buf).expect("tag 52 placeholder present");
        let checksum_off = buf.len() - 4;
        // None when this order carries no SMP id: the tag is absent from the buffer,
        // so there is nothing to patch and presence/absence changes byte length.
        let smp_off = (smp_id != iicpc_schemas_rust::SMP_ID_NONE)
            .then(|| find_subslice(&buf, b"\x017928=").expect("tag 7928 present") + 6);

        Self {
            kind,
            fix_version: fix_version.to_string(),
            session_id: session_id.to_string(),
            bot_id,
            buf,
            clordid_seq_width: digit_width(seq),
            clordid_seq_off,
            tag34_off,
            side_off,
            qty_off,
            price_off,
            tag52_off,
            checksum_off,
            smp_off,
            smp_id,
        }
    }

    fn patch(&mut self, seq: u64, qty: u64, price: u64, side: Side, smp_id: u32) -> OrderFrame {
        let needed_width = digit_width(seq);
        // A template baked WITH the tag cannot serve an order without it (or vice
        // versa): presence changes the buffer length, so every offset after it moves.
        // Within a session smp_id_count is constant, so this only triggers on the
        // pathological mixed case and costs nothing in the normal path.
        let smp_presence_changed =
            (smp_id == iicpc_schemas_rust::SMP_ID_NONE) != self.smp_off.is_none();
        if needed_width != self.clordid_seq_width || smp_presence_changed {
            *self = Self::build(
                self.kind,
                &self.fix_version,
                &self.session_id,
                self.bot_id,
                seq,
                qty,
                price,
                side,
                smp_id,
            );
        } else {
            let mut old_sum = 0u32;
            let mut new_sum = 0u32;

            let (o, n) = write_padded_u64(&mut self.buf, self.tag34_off, FIX_NUM_WIDTH, seq + 1);
            old_sum += o;
            new_sum += n;

            let (o, n) = write_padded_u64(
                &mut self.buf,
                self.clordid_seq_off,
                self.clordid_seq_width,
                seq,
            );
            old_sum += o;
            new_sum += n;

            // Rotating SMP id: fixed width, so it patches in place like tag 34/38/44
            // and never forces a re-render.
            if let Some(off) = self.smp_off {
                if smp_id != self.smp_id {
                    let (o, n) = write_padded_u64(&mut self.buf, off, SMP_ID_WIDTH, smp_id as u64);
                    old_sum += o;
                    new_sum += n;
                    self.smp_id = smp_id;
                }
            }

            let side_byte = match side {
                Side::Buy => b'1',
                Side::Sell => b'2',
            };
            old_sum += u32::from(self.buf[self.side_off]);
            new_sum += u32::from(side_byte);
            self.buf[self.side_off] = side_byte;

            let (o, n) = write_padded_u64(&mut self.buf, self.qty_off, FIX_NUM_WIDTH, qty);
            old_sum += o;
            new_sum += n;

            if let Some(price_off) = self.price_off {
                let (o, n) = write_padded_u64(&mut self.buf, price_off, FIX_NUM_WIDTH, price);
                old_sum += o;
                new_sum += n;
            }

            apply_checksum_delta(&mut self.buf, self.checksum_off, old_sum, new_sum);
        }

        OrderFrame {
            order_id: self.kind.order_id(&self.session_id, self.bot_id, seq),
            orig_order_id: String::new(),
            price,
            qty,
            side,
            bytes: self.buf.clone(),
            tag52_offset: Some(self.tag52_off),
            payload_type: self.kind.payload_type(),
            ord_type: self.kind.ord_type(),
            smp_id,
        }
    }
}

// JSON numbers cannot carry leading zeros (RFC 8259) — unlike FIX's ASCII tag=value
// fields, `"qty":00025` is invalid JSON. So JSON qty/price are NOT zero-padded to a
// fixed width; instead their *natural* digit width is tracked and re-rendered on
// change, exactly like the ClOrdID rollover. Content-Length is therefore constant
// whenever qty/price/seq all keep their digit counts — not unconditionally constant
// as it is for FIX's tag=value fields. This is a deliberate deviation from a literal
// reading of "fixed-width zero-padded ... JSON qty/price" in the plan, made to avoid
// emitting invalid JSON.
/// json_smp_field renders `,"smp_id":"007"` or the EMPTY STRING when the order carries
/// no id. A fixed-width STRING, not a number: JSON forbids leading zeros in numbers
/// (RFC 8259), but a zero-padded string keeps a constant byte length so the in-place
/// patcher and REST Content-Length both stay stable across the rotation.
fn json_smp_field(smp_id: u32) -> String {
    if smp_id == iicpc_schemas_rust::SMP_ID_NONE {
        return String::new();
    }
    format!(",\"smp_id\":\"{:0width$}\"", smp_id, width = SMP_ID_WIDTH)
}

fn build_json_payload_natural(
    kind: FrameKind,
    order_id: &str,
    side: Side,
    qty: u64,
    price: u64,
    smp_id: u32,
) -> String {
    let side_name = match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };
    let encoded_order_id = serde_json::to_string(order_id).expect("serializing String cannot fail");
    let smp = json_smp_field(smp_id);
    match kind {
        FrameKind::New => {
            format!("{{\"cl_ord_id\":{encoded_order_id},\"symbol\":\"IICPC\",\"side\":\"{side_name}\",\"qty\":{qty},\"price\":{price}{smp}}}")
        }
        FrameKind::Market => {
            format!("{{\"cl_ord_id\":{encoded_order_id},\"symbol\":\"IICPC\",\"side\":\"{side_name}\",\"qty\":{qty},\"ord_type\":\"MARKET\"{smp}}}")
        }
        FrameKind::Cancel | FrameKind::Replace => {
            unreachable!("build_json_payload_natural only supports New/Market; Cancel/Replace use the reference builder")
        }
    }
}

/// A reusable REST/WS JSON buffer for one `FrameKind`. Two fields defeat naive
/// fixed-offset patching and fall back to a full re-render when they change: `side`
/// (`"BUY"`/`"SELL"` differ in byte length; padding a JSON string value would change
/// it) and qty/price digit width (JSON forbids zero-padded numbers). ClOrdID-width
/// rollover triggers a re-render too, same as `FixTemplate`. Whenever none of the
/// three changed since the last order, the fast in-place path applies.
struct JsonTemplate {
    kind: FrameKind,
    protocol: Protocol,
    session_id: String,
    bot_id: u64,
    target_host: String,
    buf: Vec<u8>,
    clordid_seq_width: usize,
    clordid_seq_off: usize,
    side_len: usize,
    side_off: Option<usize>,
    qty_width: usize,
    qty_off: Option<usize>,
    price_width: usize,
    price_off: Option<usize>,
    /// Offset of the fixed-width `smp_id` STRING value, or None when the order carries
    /// no id (the field is then absent from the body entirely).
    smp_off: Option<usize>,
    /// The id currently rendered in `buf`; a different id is patched in place.
    smp_id: u32,
}

impl JsonTemplate {
    #[allow(clippy::too_many_arguments)]
    fn build(
        kind: FrameKind,
        protocol: Protocol,
        session_id: &str,
        bot_id: u64,
        target_host: &str,
        seq: u64,
        qty: u64,
        price: u64,
        side: Side,
        smp_id: u32,
    ) -> Self {
        let order_id = kind.order_id(session_id, bot_id, seq);
        let json = build_json_payload_natural(kind, &order_id, side, qty, price, smp_id);
        let (buf, json_start) = match protocol {
            Protocol::Rest => {
                let rest =
                    build_rest_request(kind.rest_method(), target_host, &kind.rest_path(""), &json);
                let start = rest.len() - json.len();
                (rest, start)
            }
            Protocol::Ws => (json.clone().into_bytes(), 0),
            Protocol::Fix => unreachable!("JsonTemplate only renders Rest/Ws"),
        };

        let side_name = match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        };
        let side_off =
            find_subslice(&buf[json_start..], b"\"side\":\"").map(|p| json_start + p + 8);
        let qty_off = find_subslice(&buf[json_start..], b"\"qty\":").map(|p| json_start + p + 6);
        let price_off =
            find_subslice(&buf[json_start..], b"\"price\":").map(|p| json_start + p + 8);
        let clordid_prefix = format!("\"cl_ord_id\":\"{session_id}_{bot_id}_");
        let clordid_seq_off = find_subslice(&buf[json_start..], clordid_prefix.as_bytes())
            .map(|p| json_start + p + clordid_prefix.len())
            .expect("ClOrdID present in JSON body");
        let smp_off = (smp_id != iicpc_schemas_rust::SMP_ID_NONE)
            .then(|| {
                find_subslice(&buf[json_start..], b"\"smp_id\":\"").map(|p| json_start + p + 10)
            })
            .flatten();

        Self {
            kind,
            protocol,
            session_id: session_id.to_string(),
            bot_id,
            target_host: target_host.to_string(),
            buf,
            clordid_seq_width: digit_width(seq),
            clordid_seq_off,
            side_len: side_name.len(),
            side_off,
            qty_width: digit_width(qty),
            qty_off,
            price_width: digit_width(price),
            price_off,
            smp_off,
            smp_id,
        }
    }

    fn patch(&mut self, seq: u64, qty: u64, price: u64, side: Side, smp_id: u32) -> OrderFrame {
        let side_name = match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        };
        // Presence of the smp_id field changes the body length, so a template baked
        // with it cannot serve an order without it (or vice versa). Within a session
        // smp_id_count is constant, so this only fires on the pathological mixed case.
        let smp_presence_changed =
            (smp_id == iicpc_schemas_rust::SMP_ID_NONE) != self.smp_off.is_none();
        let needs_rebuild = digit_width(seq) != self.clordid_seq_width
            || side_name.len() != self.side_len
            || digit_width(qty) != self.qty_width
            || (self.price_off.is_some() && digit_width(price) != self.price_width)
            || smp_presence_changed;

        if needs_rebuild {
            *self = Self::build(
                self.kind,
                self.protocol,
                &self.session_id,
                self.bot_id,
                &self.target_host,
                seq,
                qty,
                price,
                side,
                smp_id,
            );
        } else {
            write_padded_u64(
                &mut self.buf,
                self.clordid_seq_off,
                self.clordid_seq_width,
                seq,
            );
            // Fixed-width string value: patches in place, no re-render, and the REST
            // Content-Length is unaffected.
            if let Some(off) = self.smp_off {
                if smp_id != self.smp_id {
                    write_padded_u64(&mut self.buf, off, SMP_ID_WIDTH, smp_id as u64);
                    self.smp_id = smp_id;
                }
            }
            if let Some(off) = self.qty_off {
                write_padded_u64(&mut self.buf, off, self.qty_width, qty);
            }
            if let Some(off) = self.price_off {
                write_padded_u64(&mut self.buf, off, self.price_width, price);
            }
            if let Some(off) = self.side_off {
                self.buf[off..off + self.side_len].copy_from_slice(side_name.as_bytes());
            }
        }

        OrderFrame {
            order_id: self.kind.order_id(&self.session_id, self.bot_id, seq),
            orig_order_id: String::new(),
            price,
            qty,
            side,
            bytes: self.buf.clone(),
            tag52_offset: None,
            payload_type: self.kind.payload_type(),
            ord_type: self.kind.ord_type(),
            smp_id,
        }
    }
}

fn idx(kind: FrameKind) -> usize {
    match kind {
        FrameKind::New => 0,
        FrameKind::Market => 1,
        FrameKind::Cancel => 2,
        FrameKind::Replace => 3,
    }
}

/// Per-task cache of one template per `FrameKind`, holding whichever protocol the
/// task was assigned (P1: a task speaks exactly one protocol). Cancel/Replace never
/// get the in-place fast path (see `FixTemplate`/`JsonTemplate` docs above) but are
/// still routed through here so callers have one entry point.
pub struct TemplateCache {
    protocol: Protocol,
    fix_version: String,
    session_id: String,
    target_host: String,
    bot_id: u64,
    /// How many distinct self-match-prevention ids this task rotates through. 0 or 1
    /// mean "no SMP id": the field is OMITTED from the wire entirely, which is what
    /// pass-2 scale scenarios use and keeps their bytes identical to pre-SMP output.
    smp_id_count: u32,
    fix: [Option<FixTemplate>; 4],
    json: [Option<JsonTemplate>; 4],
}

impl TemplateCache {
    pub fn new(
        protocol: Protocol,
        fix_version: &str,
        session_id: &str,
        target_host: &str,
        bot_id: u64,
    ) -> Self {
        Self::with_smp(protocol, fix_version, session_id, target_host, bot_id, 0)
    }

    /// with_smp builds a cache whose orders carry a rotating self-match-prevention id.
    /// `smp_id_count <= 1` means no SMP id at all (field omitted from the wire).
    pub fn with_smp(
        protocol: Protocol,
        fix_version: &str,
        session_id: &str,
        target_host: &str,
        bot_id: u64,
        smp_id_count: u32,
    ) -> Self {
        Self {
            protocol,
            fix_version: fix_version.to_string(),
            session_id: session_id.to_string(),
            target_host: target_host.to_string(),
            bot_id,
            smp_id_count,
            fix: [None, None, None, None],
            json: [None, None, None, None],
        }
    }

    /// smp_for maps a sequence number onto this task's SMP id, or `SMP_ID_NONE` when
    /// the task carries none. Round-robin so a single-connection correctness run still
    /// produces cross-participant matching; deterministic from `seq`, so a given
    /// global_seed reproduces the same assignment with no extra bot state.
    fn smp_for(&self, seq: u64) -> u32 {
        if self.smp_id_count <= 1 {
            return iicpc_schemas_rust::SMP_ID_NONE;
        }
        (seq % self.smp_id_count as u64) as u32
    }

    fn render_fast(
        &mut self,
        kind: FrameKind,
        seq: u64,
        qty: u64,
        price: u64,
        side: Side,
    ) -> OrderFrame {
        let smp_id = self.smp_for(seq);
        match self.protocol {
            Protocol::Fix => {
                let slot = &mut self.fix[idx(kind)];
                match slot {
                    Some(t) => t.patch(seq, qty, price, side, smp_id),
                    None => {
                        let mut t = FixTemplate::build(
                            kind,
                            &self.fix_version,
                            &self.session_id,
                            self.bot_id,
                            seq,
                            qty,
                            price,
                            side,
                            smp_id,
                        );
                        let frame = t.patch(seq, qty, price, side, smp_id);
                        *slot = Some(t);
                        frame
                    }
                }
            }
            Protocol::Rest | Protocol::Ws => {
                let slot = &mut self.json[idx(kind)];
                match slot {
                    Some(t) => t.patch(seq, qty, price, side, smp_id),
                    None => {
                        let mut t = JsonTemplate::build(
                            kind,
                            self.protocol,
                            &self.session_id,
                            self.bot_id,
                            &self.target_host,
                            seq,
                            qty,
                            price,
                            side,
                            smp_id,
                        );
                        let frame = t.patch(seq, qty, price, side, smp_id);
                        *slot = Some(t);
                        frame
                    }
                }
            }
        }
    }

    pub fn render_new(&mut self, seq: u64, price: u64, qty: u64, side: Side) -> OrderFrame {
        self.render_fast(FrameKind::New, seq, qty, price, side)
    }

    pub fn render_market(&mut self, seq: u64, qty: u64, side: Side) -> OrderFrame {
        self.render_fast(FrameKind::Market, seq, qty, 0, side)
    }

    pub fn render_cancel(
        &mut self,
        seq: u64,
        orig_order_id: &str,
        price: u64,
        qty: u64,
        side: Side,
    ) -> OrderFrame {
        build_frame(
            self.protocol,
            &self.fix_version,
            &self.session_id,
            &self.target_host,
            self.bot_id,
            seq,
            Some(orig_order_id),
            price,
            qty,
            side,
            FrameKind::Cancel,
            self.smp_for(seq),
        )
    }

    pub fn render_replace(
        &mut self,
        seq: u64,
        orig_order_id: &str,
        price: u64,
        qty: u64,
        side: Side,
    ) -> OrderFrame {
        build_frame(
            self.protocol,
            &self.fix_version,
            &self.session_id,
            &self.target_host,
            self.bot_id,
            seq,
            Some(orig_order_id),
            price,
            qty,
            side,
            FrameKind::Replace,
            self.smp_for(seq),
        )
    }
}

/// Builds the RFC6455 client-masked wire frame (2-byte header + optional 2-byte
/// extended length + 4-byte mask key + masked payload) that `ebpf-latency`'s
/// `frame_ws`/`ws_unmasked_payload` (`parse.rs`) expect from a client. Text opcode
/// (0x81), matching the JSON payloads bot-fleet sends. Not wired into the worker
/// write path yet — see docs/tps-improvement-plan.md P2' notes in the PR report;
/// tungstenite still owns the live socket there. Kept here, tested, as the
/// ready-to-wire template for that follow-up.
pub fn build_ws_frame(payload: &[u8], mask_key: [u8; 4]) -> Vec<u8> {
    let len = payload.len();
    let mut out = Vec::with_capacity(2 + 4 + 4 + len);
    out.push(0x81);
    if len < 126 {
        out.push(0x80 | len as u8);
    } else if len < 65536 {
        out.push(0x80 | 126);
        out.push((len >> 8) as u8);
        out.push((len & 0xff) as u8);
    } else {
        panic!(
            "build_ws_frame: payload too large for the len16 template (bot orders are small JSON)"
        );
    }
    out.extend_from_slice(&mask_key);
    let start = out.len();
    out.extend_from_slice(payload);
    for (i, b) in out[start..].iter_mut().enumerate() {
        *b ^= mask_key[i % 4];
    }
    out
}

/// Reference-only unmask, mirroring `ws_unmasked_payload` in
/// `services/ebpf-latency/src/parse.rs`, used by property tests to check
/// `build_ws_frame` round-trips.
#[cfg(test)]
fn unmask_ws_frame(frame: &[u8]) -> Vec<u8> {
    let masked = frame[1] & 0x80 != 0;
    let len7 = (frame[1] & 0x7f) as usize;
    let (mut off, payload_len) = if len7 < 126 {
        (2usize, len7)
    } else {
        (4usize, ((frame[2] as usize) << 8) | frame[3] as usize)
    };
    assert!(masked, "client frames must be masked");
    let mask = [frame[off], frame[off + 1], frame[off + 2], frame[off + 3]];
    off += 4;
    frame[off..off + payload_len]
        .iter()
        .enumerate()
        .map(|(i, b)| b ^ mask[i % 4])
        .collect()
}

/// Wraps a `JsonTemplate` (Ws protocol) and re-masks the full payload on each patch
/// with a mask key constant for the connection's lifetime (RFC6455 requires masking
/// but not per-message key rotation against a non-intermediary peer). This is a full
/// re-XOR per order (O(payload), no allocation beyond the owned output buffer), not
/// an XOR-only-the-patched-bytes optimization — see module notes.
pub struct WsFrameTemplate {
    json: JsonTemplate,
    mask_key: [u8; 4],
}

impl WsFrameTemplate {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        kind_is_new: bool,
        session_id: &str,
        bot_id: u64,
        seq: u64,
        qty: u64,
        price: u64,
        side: Side,
        mask_key: [u8; 4],
    ) -> Self {
        let kind = if kind_is_new {
            FrameKind::New
        } else {
            FrameKind::Market
        };
        let json = JsonTemplate::build(
            kind,
            Protocol::Ws,
            session_id,
            bot_id,
            "",
            seq,
            qty,
            price,
            side,
            iicpc_schemas_rust::SMP_ID_NONE,
        );
        Self { json, mask_key }
    }

    pub fn patch(&mut self, seq: u64, qty: u64, price: u64, side: Side) -> Vec<u8> {
        // No SMP id: this template is the standalone WS masking helper used by the
        // roundtrip example, not the TemplateCache path that carries SMP ids.
        let frame = self
            .json
            .patch(seq, qty, price, side, iicpc_schemas_rust::SMP_ID_NONE);
        build_ws_frame(&frame.bytes, self.mask_key)
    }
}

pub fn finalize_fix(fix_version: &str, body: &str) -> Vec<u8> {
    let mut frame = format!("8={fix_version}\x019={}\x01{body}", body.len()).into_bytes();
    let checksum = frame
        .iter()
        .fold(0u32, |sum, b| sum.wrapping_add(u32::from(*b)))
        % 256;
    frame.extend_from_slice(format!("10={checksum:03}\x01").as_bytes());
    frame
}

#[derive(Debug, Clone, Copy)]
/// MessageRef stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct MessageRef<'a> {
    pub msg_type: &'a [u8],
    pub clord_id: Option<&'a [u8]>,
}

pub fn parse_messages(buf: &[u8]) -> (Vec<MessageRef<'_>>, usize) {
    let mut out = Vec::new();
    let mut cursor = 0;

    while cursor < buf.len() {
        let Some(start_off) = find_subslice(&buf[cursor..], b"8=FIX") else {
            cursor += partial_marker_start(&buf[cursor..]);
            break;
        };
        let abs_start = cursor + start_off;

        let Some(end) = find_message_end(&buf[abs_start..]) else {
            cursor = abs_start;
            break;
        };
        let abs_end = abs_start + end;
        let msg = &buf[abs_start..abs_end];

        out.push(parse_single_message(msg));
        cursor = abs_end;
    }

    (out, cursor)
}

fn find_message_end(buf: &[u8]) -> Option<usize> {
    let needle = b"\x0110=";
    let i = find_subslice(buf, needle)?;
    let after_eq = i + needle.len();
    let soh = buf[after_eq..].iter().position(|&b| b == SOH)?;
    Some(after_eq + soh + 1)
}

fn parse_single_message(msg: &[u8]) -> MessageRef<'_> {
    MessageRef {
        msg_type: extract_tag(msg, b"35").unwrap_or(b""),
        clord_id: extract_tag(msg, b"11"),
    }
}

fn extract_tag<'a>(msg: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    let mut needle = Vec::with_capacity(tag.len() + 2);
    needle.push(SOH);
    needle.extend_from_slice(tag);
    needle.push(b'=');

    let pos = find_subslice(msg, &needle)?;
    let value_start = pos + needle.len();
    let value_end = msg[value_start..].iter().position(|&b| b == SOH)?;
    Some(&msg[value_start..value_start + value_end])
}

fn partial_marker_start(buf: &[u8]) -> usize {
    let marker = b"8=FIX";
    let lo = buf.len().saturating_sub(marker.len() - 1);
    for p in lo..buf.len() {
        if marker.starts_with(&buf[p..]) {
            return p;
        }
    }
    buf.len()
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

pub fn execution_report_frame(fix_version: &str, seq: u64, clord_id: &str) -> Vec<u8> {
    let body = format!(
        "35=8\x0149=CONTESTANT\x0156=IICPC-BOT\x0134={seq}\x0152=19700101-00:00:00.000\x0137=EXEC_{seq}\x0111={clord_id}\x0117=EXECID_{seq}\x01150=0\x0139=0\x0155=IICPC\x0154=1\x0138=0\x0114=0\x016=0\x01"
    );
    finalize_fix(fix_version, &body)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Offline serialization-cost breakdown for the raw-send bottleneck. Post-P1,
    // build_frame renders only the protocol the caller asks for — this measures each
    // protocol's render cost in isolation via the real order_frame entry point (no
    // more all3/manual-path split, since there is no all3 path left to measure). Run
    // with: cargo test -p iicpc-bot-fleet --lib serialization_cost -- --ignored --nocapture
    #[test]
    #[ignore]
    fn serialization_cost_breakdown() {
        use std::time::Instant;
        let n = 1_000_000u64;
        let (fv, sid, host) = (
            "FIX.4.2",
            "01890dd2-71f3-7abc-9def-0123456789ab",
            "10.0.0.5:9898",
        );

        let mut sink = 0usize;

        let t = Instant::now();
        for seq in 0..n {
            let f = order_frame(Protocol::Fix, fv, sid, host, 42, seq, 10_000, 25, Side::Buy);
            sink += f.bytes.len();
        }
        let fix_only = t.elapsed().as_nanos() / n as u128;

        let t = Instant::now();
        for seq in 0..n {
            let f = order_frame(
                Protocol::Rest,
                fv,
                sid,
                host,
                42,
                seq,
                10_000,
                25,
                Side::Buy,
            );
            sink += f.bytes.len();
        }
        let rest_only = t.elapsed().as_nanos() / n as u128;

        let t = Instant::now();
        for seq in 0..n {
            let f = order_frame(Protocol::Ws, fv, sid, host, 42, seq, 10_000, 25, Side::Buy);
            sink += f.bytes.len();
        }
        let ws_only = t.elapsed().as_nanos() / n as u128;

        eprintln!(
            "SERTIME ns/order  fix_only={fix_only}  rest_only={rest_only}  ws_only={ws_only}  (sink={sink})"
        );
        eprintln!(
            "SERTIME implied max ser-only throughput/core: fix={}/s rest={}/s ws={}/s",
            1_000_000_000 / fix_only.max(1),
            1_000_000_000 / rest_only.max(1),
            1_000_000_000 / ws_only.max(1)
        );
    }

    #[test]
    fn logon_and_first_orders_have_monotonic_seq_nums() {
        let logon = logon_frame("FIX.4.2", 1);
        assert_eq!(extract_tag(&logon, b"34"), Some(b"1".as_ref()));

        let first = order_frame(
            Protocol::Fix,
            "FIX.4.2",
            "sess1",
            "host",
            7,
            1,
            10_000,
            5,
            Side::Buy,
        );
        let second = order_frame(
            Protocol::Fix,
            "FIX.4.2",
            "sess1",
            "host",
            7,
            2,
            10_000,
            5,
            Side::Buy,
        );

        assert_eq!(extract_tag(&first.bytes, b"34"), Some(b"2".as_ref()));
        assert_eq!(extract_tag(&second.bytes, b"34"), Some(b"3".as_ref()));

        assert_eq!(first.order_id, "sess1_7_1_O");
        assert_eq!(
            extract_tag(&first.bytes, b"11"),
            Some(b"sess1_7_1_O".as_ref())
        );
        assert_eq!(second.order_id, "sess1_7_2_O");
    }

    #[test]
    fn parses_one_complete_message() {
        let msg = execution_report_frame("FIX.4.2", 1, "ORD_42");
        let (msgs, consumed) = parse_messages(&msg);
        assert_eq!(consumed, msg.len(), "should consume the full buffer");
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].msg_type, b"8");
        assert_eq!(msgs[0].clord_id, Some(b"ORD_42".as_ref()));
    }

    #[test]
    fn parses_back_to_back_messages() {
        let mut buf = execution_report_frame("FIX.4.2", 1, "A");
        buf.extend(execution_report_frame("FIX.4.2", 2, "B"));
        buf.extend(execution_report_frame("FIX.4.2", 3, "C"));
        let (msgs, consumed) = parse_messages(&buf);
        assert_eq!(consumed, buf.len());
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0].clord_id, Some(b"A".as_ref()));
        assert_eq!(msgs[1].clord_id, Some(b"B".as_ref()));
        assert_eq!(msgs[2].clord_id, Some(b"C".as_ref()));
    }

    #[test]
    fn truncated_tail_is_carry_over() {
        let m1 = execution_report_frame("FIX.4.2", 1, "FIRST");
        let m2 = execution_report_frame("FIX.4.2", 2, "SECOND");
        let half_second = &m2[..m2.len() / 2];
        let mut buf = m1.clone();
        buf.extend_from_slice(half_second);

        let (msgs, consumed) = parse_messages(&buf);
        assert_eq!(msgs.len(), 1, "only the complete first message is parsed");
        assert_eq!(msgs[0].clord_id, Some(b"FIRST".as_ref()));
        assert_eq!(
            consumed,
            m1.len(),
            "consumed = end of first message; rest is carry-over"
        );
    }

    #[test]
    fn ignores_non_execution_reports() {
        let body = "35=D\x0149=X\x0156=Y\x0134=1\x0111=ORDER_X\x01";
        let frame = finalize_fix("FIX.4.2", body);
        let (msgs, _) = parse_messages(&frame);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].msg_type, b"D");
        assert_eq!(msgs[0].clord_id, Some(b"ORDER_X".as_ref()));
    }

    fn embedded_checksum_is_valid(fix: &[u8]) -> bool {
        let len = fix.len();
        let computed: u32 = fix[..len - 7].iter().map(|&b| u32::from(b)).sum::<u32>() % 256;
        let embedded = u32::from(fix[len - 4] - b'0') * 100
            + u32::from(fix[len - 3] - b'0') * 10
            + u32::from(fix[len - 2] - b'0');
        computed == embedded
    }

    #[test]
    fn patch_timestamp_sets_sending_time_and_keeps_checksum_valid() {
        let mut frame = order_frame(
            Protocol::Fix,
            "FIX.4.2",
            "sess1",
            "host",
            7,
            42,
            10_000,
            5,
            Side::Buy,
        );
        let off = frame.tag52_offset.expect("tag 52 offset must be located");

        assert_eq!(
            &frame.bytes[off..off + FIX_TIMESTAMP_LEN],
            &FIX_TIMESTAMP_PLACEHOLDER[..]
        );
        assert!(embedded_checksum_is_valid(&frame.bytes));

        let ns = 1_716_023_400_123_000_000_u64;
        frame.patch_timestamp(ns);

        let expected = time::format_fix_timestamp(ns);
        assert_eq!(&frame.bytes[off..off + FIX_TIMESTAMP_LEN], &expected[..]);
        assert_ne!(
            &frame.bytes[off..off + FIX_TIMESTAMP_LEN],
            &FIX_TIMESTAMP_PLACEHOLDER[..]
        );

        assert!(embedded_checksum_is_valid(&frame.bytes));
    }

    #[test]
    /// rest_and_ws_frames_have_no_tag52_offset_and_carry_no_fix_bytes ensures the
    /// single-protocol render (P1) really only builds the requested representation.
    fn rest_and_ws_frames_have_no_tag52_offset_and_carry_no_fix_bytes() {
        let rest = order_frame(
            Protocol::Rest,
            "FIX.4.2",
            "sess1",
            "host",
            7,
            1,
            10_000,
            5,
            Side::Buy,
        );
        assert!(rest.tag52_offset.is_none());
        assert!(rest.bytes.starts_with(b"POST /orders HTTP/1.1"));

        let ws = order_frame(
            Protocol::Ws,
            "FIX.4.2",
            "sess1",
            "host",
            7,
            1,
            10_000,
            5,
            Side::Buy,
        );
        assert!(ws.tag52_offset.is_none());
        assert!(ws.bytes.starts_with(b"{\"cl_ord_id\""));
    }

    #[test]
    fn echo_read_loop_answers_every_order_under_arbitrary_chunking() {
        const N: u64 = 200;
        let mut wire: Vec<u8> = Vec::new();
        let mut expected: Vec<String> = Vec::new();
        for seq in 1..=N {
            let f = order_frame(
                Protocol::Fix,
                "FIX.4.2",
                "sess1",
                "host",
                7,
                seq,
                10_000 + seq,
                5,
                Side::Buy,
            );
            expected.push(f.order_id.clone());
            wire.extend_from_slice(&f.bytes);
        }

        for &chunk in &[1usize, 64, 256, 512, 1024, 4096, wire.len()] {
            let mut buf: Vec<u8> = Vec::new();
            let mut answered: Vec<String> = Vec::new();
            for piece in wire.chunks(chunk) {
                buf.extend_from_slice(piece);
                let (messages, consumed) = parse_messages(&buf);
                for msg in &messages {
                    if msg.msg_type == b"D" {
                        if let Some(c) = msg.clord_id {
                            answered.push(String::from_utf8(c.to_vec()).unwrap());
                        }
                    }
                }
                if consumed > 0 {
                    buf.drain(..consumed);
                }
            }
            assert_eq!(
                answered, expected,
                "chunk={chunk}: every order must be answered exactly once (BF-PARSE-1)"
            );
        }
    }

    // --- P2/P2' correctness oracle: template-patched output vs the reference
    // format!-built frame, across randomized seq/qty/price/kind and width rollovers.

    fn small_lcg(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *state >> 33
    }

    #[test]
    fn fix_template_byte_identical_to_reference_across_random_orders() {
        let (fv, sid, host, bot) = ("FIX.4.2", "sess-tpl", "10.0.0.5:9898", 7u64);
        let mut cache = TemplateCache::new(Protocol::Fix, fv, sid, host, bot);
        let mut state = 42u64;

        for i in 0..5000u64 {
            let seq = i; // monotonic increasing, drives ClOrdID width rollovers at 1,10,100,1000
            let qty = small_lcg(&mut state) % 100_000;
            let price = small_lcg(&mut state) % 1_000_000;
            let side = if small_lcg(&mut state).is_multiple_of(2) {
                Side::Buy
            } else {
                Side::Sell
            };
            let is_market = small_lcg(&mut state).is_multiple_of(5);

            let got = if is_market {
                cache.render_market(seq, qty, side)
            } else {
                cache.render_new(seq, price, qty, side)
            };
            let want = if is_market {
                market_frame(Protocol::Fix, fv, sid, host, bot, seq, qty, side)
            } else {
                order_frame(Protocol::Fix, fv, sid, host, bot, seq, price, qty, side)
            };

            assert_eq!(got.order_id, want.order_id, "seq={seq}");
            assert_eq!(
                extract_tag(&got.bytes, b"11"),
                extract_tag(&want.bytes, b"11"),
                "ClOrdID must match exactly (unpadded join key), seq={seq}"
            );
            // Numeric fields compare equal as integers (padded vs unpadded) — the wire
            // bytes themselves legitimately differ in width now, that is the point of P2.
            assert_eq!(
                extract_tag(&got.bytes, b"38").map(parse_ascii_u64),
                extract_tag(&want.bytes, b"38").map(parse_ascii_u64),
                "qty mismatch seq={seq}"
            );
            if !is_market {
                assert_eq!(
                    extract_tag(&got.bytes, b"44").map(parse_ascii_u64),
                    extract_tag(&want.bytes, b"44").map(parse_ascii_u64),
                    "price mismatch seq={seq}"
                );
            }
            assert!(
                embedded_checksum_is_valid(&got.bytes),
                "seq={seq}: checksum must stay valid after patch"
            );
            assert_eq!(
                extract_tag(&got.bytes, b"52"),
                extract_tag(&want.bytes, b"52"),
                "timestamp placeholder must match pre-patch, seq={seq}"
            );
        }
    }

    fn parse_ascii_u64(digits: &[u8]) -> u64 {
        digits
            .iter()
            .fold(0u64, |acc, &b| acc * 10 + u64::from(b - b'0'))
    }

    #[test]
    fn fix_template_survives_seq_width_rollovers_9_to_10_and_99_to_100() {
        let (fv, sid, host, bot) = ("FIX.4.2", "sess-roll", "host", 3u64);
        let mut cache = TemplateCache::new(Protocol::Fix, fv, sid, host, bot);

        for seq in [8u64, 9, 10, 11, 98, 99, 100, 101, 998, 999, 1000, 1001] {
            let got = cache.render_new(seq, 10_000, 25, Side::Buy);
            let want = order_frame(
                Protocol::Fix,
                fv,
                sid,
                host,
                bot,
                seq,
                10_000,
                25,
                Side::Buy,
            );
            assert_eq!(got.order_id, want.order_id, "seq={seq}");
            assert_eq!(
                extract_tag(&got.bytes, b"11"),
                extract_tag(&want.bytes, b"11"),
                "seq={seq}"
            );
            assert!(embedded_checksum_is_valid(&got.bytes), "seq={seq}");
        }
    }

    #[test]
    fn fix_template_cancel_replace_route_through_reference_builder() {
        let (fv, sid, host, bot) = ("FIX.4.2", "sess-cr", "host", 9u64);
        let mut cache = TemplateCache::new(Protocol::Fix, fv, sid, host, bot);

        let cancel = cache.render_cancel(5, "sess-cr_9_1_O", 10_000, 25, Side::Buy);
        let want = cancel_frame(
            Protocol::Fix,
            fv,
            sid,
            host,
            bot,
            5,
            "sess-cr_9_1_O",
            10_000,
            25,
            Side::Buy,
        );
        assert_eq!(cancel.bytes, want.bytes);

        let replace = cache.render_replace(6, "sess-cr_9_1_O", 11_000, 30, Side::Sell);
        let want = replace_frame(
            Protocol::Fix,
            fv,
            sid,
            host,
            bot,
            6,
            "sess-cr_9_1_O",
            11_000,
            30,
            Side::Sell,
        );
        assert_eq!(replace.bytes, want.bytes);
    }

    #[test]
    fn json_template_byte_equivalent_to_reference_for_rest_and_ws() {
        for protocol in [Protocol::Rest, Protocol::Ws] {
            let (fv, sid, host, bot) = ("FIX.4.2", "sess-json", "10.0.0.5:8080", 4u64);
            let mut cache = TemplateCache::new(protocol, fv, sid, host, bot);
            let mut state = 7u64;

            for i in 0..2000u64 {
                let seq = i;
                let qty = small_lcg(&mut state) % 50_000;
                let price = small_lcg(&mut state) % 500_000;
                let side = if small_lcg(&mut state).is_multiple_of(2) {
                    Side::Buy
                } else {
                    Side::Sell
                };

                let got = cache.render_new(seq, price, qty, side);
                let want = order_frame(protocol, fv, sid, host, bot, seq, price, qty, side);

                assert_eq!(
                    got.order_id, want.order_id,
                    "protocol={protocol:?} seq={seq}"
                );

                let got_json = json_body_of(protocol, &got.bytes);
                let want_json = json_body_of(protocol, &want.bytes);
                let got_v: serde_json::Value =
                    serde_json::from_slice(got_json).expect("got json parses");
                let want_v: serde_json::Value =
                    serde_json::from_slice(want_json).expect("want json parses");
                assert_eq!(
                    got_v["cl_ord_id"], want_v["cl_ord_id"],
                    "protocol={protocol:?} seq={seq}"
                );
                assert_eq!(
                    got_v["side"], want_v["side"],
                    "protocol={protocol:?} seq={seq}"
                );
                assert_eq!(
                    got_v["qty"].as_u64().unwrap(),
                    want_v["qty"].as_u64().unwrap(),
                    "protocol={protocol:?} seq={seq}"
                );
                assert_eq!(
                    got_v["price"].as_u64().unwrap(),
                    want_v["price"].as_u64().unwrap(),
                    "protocol={protocol:?} seq={seq}"
                );
            }
        }
    }

    fn json_body_of(protocol: Protocol, bytes: &[u8]) -> &[u8] {
        match protocol {
            Protocol::Ws => bytes,
            Protocol::Rest => {
                let sep = b"\r\n\r\n";
                let pos = bytes
                    .windows(sep.len())
                    .position(|w| w == sep)
                    .expect("REST request has a header/body separator");
                &bytes[pos + sep.len()..]
            }
            Protocol::Fix => unreachable!(),
        }
    }

    #[test]
    fn rest_content_length_is_constant_while_digit_widths_are_stable() {
        let (fv, sid, host, bot) = ("FIX.4.2", "sess-cl", "10.0.0.5:8080", 4u64);
        let mut cache = TemplateCache::new(Protocol::Rest, fv, sid, host, bot);

        let header = |bytes: &[u8]| -> usize {
            let s = std::str::from_utf8(bytes).unwrap();
            let line = s
                .lines()
                .find(|l| l.starts_with("Content-Length:"))
                .unwrap();
            line.trim_start_matches("Content-Length:")
                .trim()
                .parse()
                .unwrap()
        };

        // Same digit widths (seq/qty/price all 1-digit vs 6-digit-but-fixed across both
        // calls): Content-Length is untouched by the fast in-place patch path.
        let f1 = cache.render_new(1, 100_000, 100_000, Side::Buy);
        let f2 = cache.render_new(2, 999_999, 999_999, Side::Buy);
        assert_eq!(f1.bytes.len(), f2.bytes.len());
        assert_eq!(header(&f1.bytes), header(&f2.bytes));

        // A qty digit-width change forces a re-render (JSON forbids zero-padding), which
        // legitimately moves Content-Length — this is the documented deviation from FIX's
        // unconditionally-fixed-width numeric fields.
        let f3 = cache.render_new(3, 1, 1, Side::Buy);
        assert_ne!(header(&f2.bytes), header(&f3.bytes));
    }

    #[test]
    fn ws_frame_template_masks_and_unmasks_to_the_reference_json() {
        let (sid, bot) = ("sess-ws", 2u64);
        let mask_key = [0xDE, 0xAD, 0xBE, 0xEF];
        let mut tpl = WsFrameTemplate::new(true, sid, bot, 1, 25, 10_000, Side::Buy, mask_key);
        let mut state = 99u64;

        for i in 1..500u64 {
            let qty = small_lcg(&mut state) % 10_000;
            let price = small_lcg(&mut state) % 100_000;
            let side = if i % 3 == 0 { Side::Sell } else { Side::Buy };

            let wire = tpl.patch(i, qty, price, side);
            let unmasked = unmask_ws_frame(&wire);

            let reference = order_frame(Protocol::Ws, "FIX.4.2", sid, "", bot, i, price, qty, side);
            let ref_v: serde_json::Value = serde_json::from_slice(&reference.bytes).unwrap();
            let got_v: serde_json::Value = serde_json::from_slice(&unmasked).unwrap();

            assert_eq!(got_v["cl_ord_id"], ref_v["cl_ord_id"], "i={i}");
            assert_eq!(got_v["side"], ref_v["side"], "i={i}");
            assert_eq!(
                got_v["qty"].as_u64().unwrap(),
                ref_v["qty"].as_u64().unwrap(),
                "i={i}"
            );

            assert!(wire[1] & 0x80 != 0, "client frames must set the mask bit");
        }
    }
}

#[cfg(test)]
mod smp_tests {
    use super::*;
    use iicpc_schemas_rust::SMP_ID_NONE;

    fn tag_value(bytes: &[u8], tag: &str) -> Option<String> {
        let needle = format!("\x01{tag}=");
        let start = find_subslice(bytes, needle.as_bytes())? + needle.len();
        let end = bytes[start..].iter().position(|&b| b == 0x01)? + start;
        Some(String::from_utf8_lossy(&bytes[start..end]).to_string())
    }

    #[test]
    /// With no SMP id the tag must be ABSENT, not 7928=000 and not an empty value.
    /// A contestant has to distinguish "unconstrained" from "id 0", and pass-2 frames
    /// must stay byte-identical to pre-SMP output.
    fn fix_omits_tag_7928_when_no_smp_id() {
        let mut c = TemplateCache::new(Protocol::Fix, "FIX.4.2", "sess", "host", 7);
        let f = c.render_new(1, 10_000, 5, Side::Buy);
        assert!(
            find_subslice(&f.bytes, b"\x017928=").is_none(),
            "tag 7928 must be absent without an SMP id, got: {}",
            String::from_utf8_lossy(&f.bytes)
        );
        assert_eq!(f.smp_id, SMP_ID_NONE);
    }

    #[test]
    /// The id must ROTATE across orders and be patched in place, not baked once.
    fn fix_rotates_smp_id_round_robin() {
        let mut c = TemplateCache::with_smp(Protocol::Fix, "FIX.4.2", "sess", "host", 7, 8);
        for seq in 0..24u64 {
            let f = c.render_new(seq, 10_000, 5, Side::Buy);
            let want = (seq % 8) as u32;
            assert_eq!(f.smp_id, want, "seq {seq} frame smp_id");
            assert_eq!(
                tag_value(&f.bytes, "7928").as_deref(),
                Some(format!("{want:03}").as_str()),
                "seq {seq} wire tag 7928"
            );
        }
    }

    #[test]
    /// FIX BodyLength(9) and CheckSum(10) must stay correct as the id changes —
    /// the in-place patch updates the checksum delta rather than re-rendering.
    fn fix_checksum_survives_smp_rotation() {
        let mut c = TemplateCache::with_smp(Protocol::Fix, "FIX.4.2", "sess", "host", 7, 8);
        for seq in 0..16u64 {
            let f = c.render_new(seq, 10_000, 5, Side::Buy);
            let body_start = find_subslice(&f.bytes, b"\x0135=").expect("body start") + 1;
            let checksum_start = f.bytes.len() - 7; // "10=xxx\x01"
            let sum: u32 = f.bytes[..checksum_start]
                .iter()
                .fold(0u32, |a, b| a.wrapping_add(u32::from(*b)));
            let want = format!("{:03}", sum % 256);
            let got = String::from_utf8_lossy(&f.bytes[checksum_start + 3..checksum_start + 6])
                .to_string();
            assert_eq!(
                got,
                want,
                "seq {seq} checksum over frame {}",
                String::from_utf8_lossy(&f.bytes)
            );
            assert!(body_start > 0);
        }
    }

    #[test]
    /// smp_for is the rotation contract: count <= 1 means "no id at all".
    fn smp_for_maps_seq_to_id() {
        let none = TemplateCache::new(Protocol::Fix, "FIX.4.2", "s", "h", 1);
        assert_eq!(none.smp_for(0), SMP_ID_NONE);
        assert_eq!(none.smp_for(99), SMP_ID_NONE);

        let one = TemplateCache::with_smp(Protocol::Fix, "FIX.4.2", "s", "h", 1, 1);
        assert_eq!(
            one.smp_for(5),
            SMP_ID_NONE,
            "a single id is no constraint at all"
        );

        let eight = TemplateCache::with_smp(Protocol::Fix, "FIX.4.2", "s", "h", 1, 8);
        assert_eq!(eight.smp_for(0), 0);
        assert_eq!(eight.smp_for(7), 7);
        assert_eq!(eight.smp_for(8), 0);
        assert_eq!(eight.smp_for(9), 1);
    }
}

#[cfg(test)]
mod smp_json_tests {
    use super::*;
    use iicpc_schemas_rust::SMP_ID_NONE;

    fn body(bytes: &[u8]) -> String {
        let s = String::from_utf8_lossy(bytes).to_string();
        match s.find("\r\n\r\n") {
            Some(i) => s[i + 4..].to_string(),
            None => s,
        }
    }

    #[test]
    /// Without an SMP id the key must be ABSENT from the JSON body — not "" and not 0.
    /// Pass-2 scale scenarios rely on this to stay byte-identical to pre-SMP output.
    fn json_omits_smp_id_when_none() {
        for proto in [Protocol::Rest, Protocol::Ws] {
            let mut c = TemplateCache::new(proto, "FIX.4.2", "sess", "host", 7);
            let f = c.render_new(1, 10_000, 5, Side::Buy);
            let b = body(&f.bytes);
            assert!(
                !b.contains("smp_id"),
                "{proto:?} body must omit smp_id, got {b}"
            );
            assert_eq!(f.smp_id, SMP_ID_NONE);
        }
    }

    #[test]
    /// The id rotates and is patched in place; the body stays valid JSON throughout.
    fn json_rotates_smp_id_and_stays_valid() {
        for proto in [Protocol::Rest, Protocol::Ws] {
            let mut c = TemplateCache::with_smp(proto, "FIX.4.2", "sess", "host", 7, 8);
            for seq in 0..24u64 {
                let f = c.render_new(seq, 10_000, 5, Side::Buy);
                let b = body(&f.bytes);
                let v: serde_json::Value = serde_json::from_str(&b)
                    .unwrap_or_else(|e| panic!("{proto:?} seq {seq} invalid JSON {b}: {e}"));
                let want = (seq % 8) as u32;
                assert_eq!(f.smp_id, want, "{proto:?} seq {seq} frame smp_id");
                assert_eq!(
                    v["smp_id"].as_str(),
                    Some(format!("{want:03}").as_str()),
                    "{proto:?} seq {seq} body smp_id"
                );
            }
        }
    }

    #[test]
    /// REST Content-Length must not move as the id rotates — that is why the value is a
    /// fixed-width STRING rather than a number (JSON forbids zero-padded numbers, so a
    /// numeric id would change width at 10 and 100 and force a re-render).
    fn rest_content_length_constant_across_smp_rotation() {
        // Hold the ClOrdID digit width CONSTANT (seq 100..116, all 3 digits) so this
        // isolates the SMP rotation. A seq crossing 9->10 legitimately changes
        // Content-Length via the ClOrdID, which is pre-existing behaviour pinned by
        // rest_content_length_is_constant_while_digit_widths_are_stable — not something
        // this test is measuring.
        let mut c = TemplateCache::with_smp(Protocol::Rest, "FIX.4.2", "sess", "host", 7, 8);
        let first = c.render_new(100, 10_000, 5, Side::Buy);
        let want = String::from_utf8_lossy(&first.bytes)
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
            .map(str::to_string)
            .expect("content-length header");
        for seq in 101..117u64 {
            let f = c.render_new(seq, 10_000, 5, Side::Buy);
            let got = String::from_utf8_lossy(&f.bytes)
                .lines()
                .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
                .map(str::to_string)
                .expect("content-length header");
            assert_eq!(
                got, want,
                "seq {seq} Content-Length changed during SMP rotation"
            );
        }
    }
}
