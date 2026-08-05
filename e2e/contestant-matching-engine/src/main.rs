//! Correct limit-order-book matching-engine contestant for IICPC, speaking all three
//! platform protocols simultaneously (docs/tps-improvement-plan.md §7.3 port policy):
//! FIX on 0.0.0.0:9898 (override with BIND or PORT) and REST + WS shape-multiplexed
//! on 0.0.0.0:8080 (override with HTTP_BIND) — WS begins life as an HTTP GET Upgrade
//! handshake on that same port/connection, so one listener serves both, exactly like
//! the eBPF capture side already assumes.
//!
//! Maintains a single global price-time-priority order book shared across all
//! connections and all three protocols (the platform runs one symbol, "IICPC"). For
//! every order it emits an immediate New ack (so the eBPF latency capture sees a
//! response) and, for every fill the match produces, an ExecutionReport with the fill
//! qty/price routed back to the connection that owns that order, encoded in that
//! connection's own wire protocol (a maker hit by an aggressor on another connection
//! is filled on the maker's connection).
//!
//! The matching logic is a faithful port of the platform's reference engine
//! (correctness-validator/internal/book/book.go): limit orders match while crossing
//! and rest the remainder; market orders match without a price check and never rest;
//! cancel removes; replace keeps position on a same-price qty-decrease, else removes
//! and re-inserts (no match). Fills happen at the maker's resting price for BOTH
//! sides. Because the fills match the ground-truth engine, the run reaches QUALIFIED.
//!
//! Fill encoding (must match the eBPF parser, ebpf-latency/src/parse.rs):
//!   150=2 (ExecType=Fill), 32=<qty> (LastShares), 31=<price> (LastPx, raw integer;
//!   the eBPF scales it by 1e9, matching the reference's price*1e9). std-only.

use std::collections::{BTreeMap, HashMap, VecDeque};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

const SOH: u8 = 0x01;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Buy,
    Sell,
}

#[derive(PartialEq, Eq)]
enum Kind {
    NewLimit,
    NewMarket,
    Cancel,
    Replace,
}

struct Order {
    kind: Kind,
    side: Side,
    price: i64,
    qty: u64,
    order_id: String,
    orig_id: String,
    /// Self-match-prevention id (FIX tag 7928 / JSON `smp_id`). `None` means the order
    /// carried no id and is UNCONSTRAINED — it may match anything, including another
    /// order with no id. `Some(0)` is a real id, distinct from `None`.
    smp_id: Option<u32>,
}

/// level_all_same_smp reports whether every remaining maker shares the aggressor's
/// smp_id, meaning this price level can yield no further fills for it.
fn level_all_same_smp(level: &std::collections::VecDeque<Resting>, agg: Option<u32>) -> bool {
    let Some(a) = agg else { return false };
    !level.is_empty() && level.iter().all(|r| r.smp_id == Some(a))
}

struct Resting {
    order_id: String,
    remaining: u64,
    /// The resting order's SMP id; an aggressor carrying the same id skips it.
    smp_id: Option<u32>,
    #[allow(dead_code)]
    seq: u64,
}

#[derive(Debug)]
struct Fill {
    order_id: String,
    price: i64,
    qty: u64,
    /// FIX LastLiquidityInd (851): 2 = taker (aggressor), 1 = maker (resting).
    liquidity: u8,
}

/// Price-time-priority limit order book. bids/asks map price -> FIFO queue (front =
/// oldest = highest time priority). index maps order_id -> (side, price) for
/// cancel/replace lookups.
#[derive(Default)]
struct Engine {
    bids: BTreeMap<i64, VecDeque<Resting>>,
    asks: BTreeMap<i64, VecDeque<Resting>>,
    index: HashMap<String, (Side, i64)>,
    seq: u64,
}

impl Engine {
    /// Returns (fills, done) where `done` are order_ids that left the book (fully
    /// filled / cancelled / replaced) and can be forgotten by the router.
    fn process(&mut self, o: &Order) -> (Vec<Fill>, Vec<String>) {
        let mut fills = Vec::new();
        let mut done = Vec::new();
        match o.kind {
            Kind::NewLimit => self.match_and_rest(o, true, &mut fills, &mut done),
            Kind::NewMarket => self.match_and_rest(o, false, &mut fills, &mut done),
            Kind::Cancel => self.remove(&o.orig_id, &mut done),
            Kind::Replace => self.replace(o, &mut done),
        }
        (fills, done)
    }

    fn match_and_rest(&mut self, o: &Order, rest: bool, fills: &mut Vec<Fill>, done: &mut Vec<String>) {
        let mut remaining = o.qty;
        let buy = o.side == Side::Buy;
        // Levels temporarily removed because every maker in them shares the aggressor's
        // smp_id. Restored before this call returns — those orders are still live.
        let mut parked_levels: Vec<(i64, std::collections::VecDeque<Resting>)> = Vec::new();
        while remaining > 0 {
            // best opposite price: buy aggressor takes lowest ask; sell takes highest bid
            let level_price = if buy {
                match self.asks.keys().next().copied() {
                    Some(p) => p,
                    None => break,
                }
            } else {
                match self.bids.keys().next_back().copied() {
                    Some(p) => p,
                    None => break,
                }
            };
            if o.kind == Kind::NewLimit {
                let crosses = if buy { level_price <= o.price } else { level_price >= o.price };
                if !crosses {
                    break;
                }
            }
            let level = if buy {
                self.asks.get_mut(&level_price).unwrap()
            } else {
                self.bids.get_mut(&level_price).unwrap()
            };
            // Self-match prevention (skip-and-continue): an aggressor never matches a
            // resting order carrying the SAME smp_id. It skips that order and continues
            // to the next one at this level; nothing is cancelled, and the skipped
            // order keeps its place in the FIFO. An order with no id is unconstrained.
            let mut skipped: std::collections::VecDeque<Resting> = Default::default();
            while remaining > 0 {
                let Some(maker) = level.front_mut() else { break };
                if let (Some(a), Some(m)) = (o.smp_id, maker.smp_id) {
                    if a == m {
                        skipped.push_back(level.pop_front().unwrap());
                        continue;
                    }
                }
                let traded = remaining.min(maker.remaining);
                // o is the aggressor (taker, 851=2); the resting maker is 851=1.
                fills.push(Fill { order_id: o.order_id.clone(), price: level_price, qty: traded, liquidity: 2 });
                fills.push(Fill { order_id: maker.order_id.clone(), price: level_price, qty: traded, liquidity: 1 });
                maker.remaining -= traded;
                remaining -= traded;
                if maker.remaining == 0 {
                    let m = level.pop_front().unwrap();
                    self.index.remove(&m.order_id);
                    done.push(m.order_id);
                }
            }
            // Restore skipped makers at the FRONT, in their original order: they
            // arrived before anything still queued, so putting them back elsewhere
            // would silently reorder time priority.
            while let Some(r) = skipped.pop_back() {
                level.push_front(r);
            }
            if level.is_empty() {
                if buy {
                    self.asks.remove(&level_price);
                } else {
                    self.bids.remove(&level_price);
                }
            } else if remaining > 0 && level_all_same_smp(level, o.smp_id) {
                // Every maker left at this level shares the aggressor's id, so it can
                // yield nothing more. The aggressor must still CONTINUE to the next
                // price level — that is the "continue" in skip-and-continue — so park
                // the level out of the tree for this call and restore it afterwards.
                // Breaking here instead would stop matching entirely and leave better
                // prices deeper in the book untouched.
                let parked = if buy {
                    self.asks.remove(&level_price)
                } else {
                    self.bids.remove(&level_price)
                };
                if let Some(lv) = parked {
                    parked_levels.push((level_price, lv));
                }
            }
        }

        for (price, lv) in parked_levels {
            let tree = if buy { &mut self.asks } else { &mut self.bids };
            tree.entry(price).or_default().extend(lv);
        }

        if rest && remaining > 0 && o.kind == Kind::NewLimit {
            self.insert(o.order_id.clone(), o.side, o.price, remaining, o.smp_id);
        } else {
            // fully filled, or a market order (never rests) -> left the book
            done.push(o.order_id.clone());
        }
    }

    fn insert(&mut self, order_id: String, side: Side, price: i64, remaining: u64, smp_id: Option<u32>) {
        self.seq += 1;
        let ro = Resting { order_id: order_id.clone(), remaining, smp_id, seq: self.seq };
        self.index.insert(order_id, (side, price));
        let tree = if side == Side::Buy { &mut self.bids } else { &mut self.asks };
        tree.entry(price).or_default().push_back(ro);
    }

    fn remove(&mut self, order_id: &str, done: &mut Vec<String>) {
        let Some((side, price)) = self.index.remove(order_id) else { return };
        let tree = if side == Side::Buy { &mut self.bids } else { &mut self.asks };
        if let Some(level) = tree.get_mut(&price) {
            if let Some(pos) = level.iter().position(|r| r.order_id == order_id) {
                level.remove(pos);
            }
            if level.is_empty() {
                tree.remove(&price);
            }
        }
        done.push(order_id.to_string());
    }

    fn replace(&mut self, o: &Order, done: &mut Vec<String>) {
        let (side, price) = match self.index.get(&o.orig_id).copied() {
            Some(v) => v,
            None => return, // can't replace an order that isn't resting
        };
        let new_id = if o.order_id.is_empty() { o.orig_id.clone() } else { o.order_id.clone() };

        // same-price qty-decrease keeps time priority (in place)
        if o.price == price {
            let mut kept = false;
            {
                let tree = if side == Side::Buy { &mut self.bids } else { &mut self.asks };
                if let Some(level) = tree.get_mut(&price) {
                    if let Some(r) = level.iter_mut().find(|r| r.order_id == o.orig_id) {
                        if o.qty <= r.remaining {
                            r.remaining = o.qty;
                            if new_id != o.orig_id {
                                r.order_id = new_id.clone();
                            }
                            kept = true;
                        }
                    }
                }
            }
            if kept {
                if new_id != o.orig_id {
                    self.index.remove(&o.orig_id);
                    self.index.insert(new_id, (side, price));
                }
                return;
            }
        }
        // reprice or qty-increase: remove + re-insert at the new level (no match, per reference)
        self.remove(&o.orig_id, done);
        self.insert(new_id, o.side, o.price, o.qty, o.smp_id);
    }
}

/// Wire identifies which protocol a connection speaks, so the matcher can encode
/// responses (New ack / fill ExecutionReport) in the shape that connection expects.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Wire {
    Fix,
    Http,
    Ws,
}

/// One parsed order plus the wire protocol and channel to send THIS connection's
/// responses on.
struct Cmd {
    order: Order,
    wire: Wire,
    out: UnboundedSender<Vec<u8>>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> std::io::Result<()> {
    let fix_bind = std::env::var("BIND").unwrap_or_else(|_| {
        let port = std::env::var("PORT").unwrap_or_else(|_| "9898".to_string());
        format!("0.0.0.0:{port}")
    });
    let http_bind = std::env::var("HTTP_BIND").unwrap_or_else(|_| "0.0.0.0:8080".to_string());

    let fix_listener = TcpListener::bind(&fix_bind).await?;
    let http_listener = TcpListener::bind(&http_bind).await?;
    eprintln!("matching_engine listening on {fix_bind} (FIX) and {http_bind} (REST+WS)");

    // A SINGLE matcher task owns the book (no lock contention). Every connection,
    // on any of the three protocols, funnels its parsed orders here over one channel.
    let (mtx, mrx) = unbounded_channel::<Cmd>();
    tokio::spawn(matcher(mrx));

    let fix_mtx = mtx.clone();
    let fix_accept = tokio::spawn(async move {
        loop {
            let (stream, _) = match fix_listener.accept().await {
                Ok(x) => x,
                Err(_) => continue,
            };
            let mtx = fix_mtx.clone();
            tokio::spawn(async move {
                if let Err(e) = conn(stream, mtx).await {
                    eprintln!("FIX connection error: {e}");
                }
            });
        }
    });

    let http_accept = tokio::spawn(async move {
        loop {
            let (stream, _) = match http_listener.accept().await {
                Ok(x) => x,
                Err(_) => continue,
            };
            let mtx = mtx.clone();
            tokio::spawn(async move {
                if let Err(e) = http_conn(stream, mtx).await {
                    eprintln!("REST/WS connection error: {e}");
                }
            });
        }
    });

    let _ = tokio::join!(fix_accept, http_accept);
    Ok(())
}

/// The only CPU-bound task: serially matches every order and routes each fill to the
/// connection that owns that order_id (maker or taker). No mutex — single owner.
/// Shared across all three protocols; each route remembers the wire it was placed on
/// so the response is encoded correctly regardless of which protocol placed the order.
async fn matcher(mut mrx: tokio::sync::mpsc::UnboundedReceiver<Cmd>) {
    let mut engine = Engine::default();
    let mut routes: HashMap<String, (Wire, UnboundedSender<Vec<u8>>)> = HashMap::new();
    let mut seq: u64 = 1;
    while let Some(cmd) = mrx.recv().await {
        let id = cmd.order.order_id.clone();
        routes.insert(id.clone(), (cmd.wire, cmd.out.clone()));
        // Immediate New ack -> a response for the latency capture.
        let _ = cmd.out.send(encode_new_ack(cmd.wire, &id, seq));
        seq += 1;
        let (fills, done) = engine.process(&cmd.order);
        for f in fills {
            if let Some((wire, s)) = routes.get(&f.order_id) {
                let _ = s.send(encode_fill(*wire, &f.order_id, f.price, f.qty, seq, f.liquidity));
                seq += 1;
            }
        }
        for d in done {
            routes.remove(&d);
        }
    }
}

/// encode_new_ack renders a New ack in the wire shape the owning connection expects.
fn encode_new_ack(wire: Wire, order_id: &str, seq: u64) -> Vec<u8> {
    match wire {
        Wire::Fix => new_ack(order_id, seq),
        Wire::Http => http_response(&json_new_ack(order_id)),
        Wire::Ws => ws_text_frame(&json_new_ack(order_id)),
    }
}

/// encode_fill renders a fill ExecutionReport in the wire shape the owning
/// connection expects.
fn encode_fill(wire: Wire, order_id: &str, price: i64, qty: u64, seq: u64, liquidity: u8) -> Vec<u8> {
    match wire {
        Wire::Fix => fill_er(order_id, price, qty, seq, liquidity),
        Wire::Http => http_response(&json_fill(order_id, price, qty, liquidity)),
        Wire::Ws => ws_text_frame(&json_fill(order_id, price, qty, liquidity)),
    }
}

/// Per-connection reader: parse FIX, hand orders to the matcher. A lightweight tokio
/// task (not an OS thread), so hundreds of connections cost almost nothing.
async fn conn(stream: tokio::net::TcpStream, mtx: UnboundedSender<Cmd>) -> std::io::Result<()> {
    stream.set_nodelay(true).ok();
    let (mut rd, wr) = stream.into_split();
    let (otx, orx) = unbounded_channel::<Vec<u8>>();
    tokio::spawn(writer(wr, orx));

    let mut buf: Vec<u8> = Vec::with_capacity(16384);
    let mut chunk = [0u8; 8192];
    loop {
        let n = rd.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        let mut cursor = 0usize;
        loop {
            let Some(start_off) = find_subslice(&buf[cursor..], b"8=FIX") else { break };
            let abs_start = cursor + start_off;
            let Some(end) = find_message_end(&buf[abs_start..]) else { break };
            let msg = &buf[abs_start..abs_start + end];
            if let Some(order) = parse_order(msg) {
                if mtx.send(Cmd { order, wire: Wire::Fix, out: otx.clone() }).is_err() {
                    return Ok(());
                }
            }
            cursor = abs_start + end;
        }
        if cursor > 0 {
            buf.drain(..cursor);
        }
    }
}

/// Per-connection reader for the REST+WS listener: a connection starts as plain
/// HTTP/1.1 (REST orders, one keep-alive TCP connection, requests pipelined) and may
/// upgrade in-place to WebSocket on a GET Upgrade request — matching the eBPF
/// capture's `looks_like_http` shape-multiplexing on this same port.
async fn http_conn(stream: tokio::net::TcpStream, mtx: UnboundedSender<Cmd>) -> std::io::Result<()> {
    stream.set_nodelay(true).ok();
    let (mut rd, wr) = stream.into_split();
    let (otx, orx) = unbounded_channel::<Vec<u8>>();
    tokio::spawn(writer(wr, orx));

    let mut buf: Vec<u8> = Vec::with_capacity(16384);
    let mut chunk = [0u8; 8192];
    let mut ws_mode = false;

    loop {
        let n = rd.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);

        if !ws_mode {
            loop {
                let Some(req) = parse_http_request(&buf) else { break };
                if req.is_websocket_upgrade {
                    if let Some(key) = &req.sec_websocket_key {
                        let accept = ws_accept_key(key);
                        let resp = format!(
                            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
                        );
                        let _ = otx.send(resp.into_bytes());
                    }
                    buf.drain(..req.total_len);
                    ws_mode = true;
                    break;
                }
                if let Some(order) = parse_order_json(&req.body) {
                    if mtx.send(Cmd { order, wire: Wire::Http, out: otx.clone() }).is_err() {
                        return Ok(());
                    }
                }
                buf.drain(..req.total_len);
            }
        }
        // NOT an `else`. The upgrade handshake and the client's first order frames routinely
        // arrive in the SAME TCP segment, so by the time ws_mode flips there are already
        // complete frames sitting in `buf`. Going back to `rd.read()` here left them
        // unprocessed, and a client that waits for responses before sending more deadlocked
        // permanently: measured as 90,496 orders sent and ZERO answered over WebSocket,
        // while FIX and REST on the same engine scored 0.994 and 0.996.
        if ws_mode {
            loop {
                let Some(frame) = parse_ws_frame(&buf) else { break };
                match frame.opcode {
                    // TEXT only, deliberately. A JSON order is text (RFC 6455 opcode 0x1),
                    // and this engine exists to model what a competent contestant would
                    // write — so it must FAIL when the platform sends something else rather
                    // than quietly absorbing it. Accepting binary here too would make this
                    // engine pass while every realistically-written submission failed, which
                    // is exactly the platform fault this fixture is supposed to expose.
                    0x1 => {
                        if let Some(order) = parse_order_json(&frame.payload) {
                            if mtx.send(Cmd { order, wire: Wire::Ws, out: otx.clone() }).is_err() {
                                return Ok(());
                            }
                        }
                    }
                    0x8 => return Ok(()), // close frame
                    // Loudly, not silently. A silent `_ => {}` here cost hours: orders
                    // arrived, framed cleanly, and vanished with no counter, no log and a
                    // perfectly healthy-looking engine.
                    other => {
                        eprintln!(
                            "WS: ignoring unexpected opcode 0x{other:x} ({} bytes) — this engine \
                             accepts TEXT (0x1) JSON orders",
                            frame.payload.len()
                        );
                    }
                }
                buf.drain(..frame.total_len);
            }
        }
    }
}

/// A parsed HTTP/1.1 request: enough to route a REST order or detect a WS handshake.
struct HttpRequest {
    total_len: usize,
    body: String,
    is_websocket_upgrade: bool,
    sec_websocket_key: Option<String>,
}

/// parse_http_request parses one complete HTTP/1.1 request off the front of `buf`
/// (headers + Content-Length body), returning None if the buffer doesn't yet hold a
/// complete message. Mirrors bot-fleet's `next_http_response` framing, symmetrically,
/// for requests instead of responses.
fn parse_http_request(buf: &[u8]) -> Option<HttpRequest> {
    let header_end = find_subslice(buf, b"\r\n\r\n")? + 4;
    let header_str = std::str::from_utf8(&buf[..header_end]).ok()?;
    let mut lines = header_str.split("\r\n");
    lines.next()?; // request line: "METHOD PATH HTTP/1.1"

    let mut content_length = 0usize;
    let mut is_upgrade = false;
    let mut ws_key = None;
    for line in lines {
        let Some((k, v)) = line.split_once(':') else { continue };
        let v = v.trim();
        match k.trim().to_ascii_lowercase().as_str() {
            "content-length" => content_length = v.parse().unwrap_or(0),
            "upgrade" => is_upgrade = v.eq_ignore_ascii_case("websocket"),
            "sec-websocket-key" => ws_key = Some(v.to_string()),
            _ => {}
        }
    }

    let total_len = header_end + content_length;
    if buf.len() < total_len {
        return None;
    }
    let body = std::str::from_utf8(&buf[header_end..total_len]).ok()?.to_string();
    Some(HttpRequest {
        total_len,
        body,
        is_websocket_upgrade: is_upgrade,
        sec_websocket_key: ws_key,
    })
}

/// A parsed WebSocket frame from the client (always masked per RFC 6455).
struct WsFrame {
    total_len: usize,
    opcode: u8,
    payload: String,
}

/// parse_ws_frame parses one client->server WS frame off the front of `buf`,
/// unmasking the payload, or None if incomplete. Handles the 7-bit and 16-bit
/// extended payload-length forms (orders never approach the 64-bit form).
fn parse_ws_frame(buf: &[u8]) -> Option<WsFrame> {
    if buf.len() < 2 {
        return None;
    }
    let opcode = buf[0] & 0x0F;
    let masked = buf[1] & 0x80 != 0;
    let len7 = (buf[1] & 0x7F) as usize;
    let mut off = 2usize;
    let payload_len = if len7 < 126 {
        len7
    } else if len7 == 126 {
        if buf.len() < off + 2 {
            return None;
        }
        let l = u16::from_be_bytes([buf[off], buf[off + 1]]) as usize;
        off += 2;
        l
    } else {
        if buf.len() < off + 8 {
            return None;
        }
        let l = u64::from_be_bytes(buf[off..off + 8].try_into().ok()?) as usize;
        off += 8;
        l
    };
    let mask = if masked {
        if buf.len() < off + 4 {
            return None;
        }
        let m = [buf[off], buf[off + 1], buf[off + 2], buf[off + 3]];
        off += 4;
        Some(m)
    } else {
        None
    };
    if buf.len() < off + payload_len {
        return None;
    }
    let raw = &buf[off..off + payload_len];
    let bytes: Vec<u8> = match mask {
        Some(m) => raw.iter().enumerate().map(|(i, &b)| b ^ m[i % 4]).collect(),
        None => raw.to_vec(),
    };
    let payload = String::from_utf8(bytes).ok()?;
    Some(WsFrame { total_len: off + payload_len, opcode, payload })
}

/// ws_text_frame builds a server->client WS text frame. Server frames are never
/// masked (RFC 6455 §5.1).
fn ws_text_frame(body: &str) -> Vec<u8> {
    let payload = body.as_bytes();
    let mut frame = Vec::with_capacity(payload.len() + 10);
    frame.push(0x81); // FIN=1, opcode=text
    if payload.len() < 126 {
        frame.push(payload.len() as u8);
    } else if payload.len() <= 0xFFFF {
        frame.push(126);
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        frame.push(127);
        frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }
    frame.extend_from_slice(payload);
    frame
}

/// http_response builds a complete HTTP/1.1 200 response carrying `body` as
/// application/json, keep-alive so further orders can pipeline on the same
/// connection (matches bot-fleet's REST write path).
fn http_response(body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
        body.len(),
        body
    )
    .into_bytes()
}

/// json_new_ack/json_fill mirror this engine's own FIX ExecType convention (150=0
/// New, 150=2 Fill) so all three protocols agree on exec_type values.
fn json_new_ack(order_id: &str) -> String {
    format!("{{\"cl_ord_id\":\"{order_id}\",\"exec_type\":\"0\",\"fill_qty\":0,\"fill_price\":0}}")
}

fn json_fill(order_id: &str, price: i64, qty: u64, liquidity: u8) -> String {
    format!(
        "{{\"cl_ord_id\":\"{order_id}\",\"exec_type\":\"2\",\"fill_qty\":{qty},\"fill_price\":{price},\"liquidity\":{liquidity}}}"
    )
}

/// parse_order_json parses a bot-fleet REST/WS order payload (services/bot-fleet/src/
/// fix.rs `build_json_payload`): New/Market carry cl_ord_id/side/qty(/price or
/// ord_type=MARKET); Cancel/Replace are distinguished by an "action" field.
fn parse_order_json(body: &str) -> Option<Order> {
    let order_id = json_string(body, "cl_ord_id")?;
    // Sent as a fixed-width STRING ("007"): JSON forbids zero-padded numbers, so the
    // producer pads a string to keep its byte length constant. Absent = unconstrained.
    let smp_id = json_string(body, "smp_id").and_then(|v| v.trim().parse::<u32>().ok());
    match json_string(body, "action").as_deref() {
        Some("CANCEL") => Some(Order {
            kind: Kind::Cancel,
            side: Side::Buy,
            price: 0,
            qty: 0,
            order_id,
            orig_id: json_string(body, "orig_cl_ord_id").unwrap_or_default(),
            smp_id,
        }),
        Some("REPLACE") => Some(Order {
            kind: Kind::Replace,
            side: parse_side(&json_string(body, "side")?),
            price: json_i64(body, "price").unwrap_or(0),
            qty: json_u64(body, "qty")?,
            order_id,
            orig_id: json_string(body, "orig_cl_ord_id").unwrap_or_default(),
            smp_id,
        }),
        _ => {
            let is_market = json_string(body, "ord_type").as_deref() == Some("MARKET");
            Some(Order {
                kind: if is_market { Kind::NewMarket } else { Kind::NewLimit },
                side: parse_side(&json_string(body, "side")?),
                price: json_i64(body, "price").unwrap_or(0),
                qty: json_u64(body, "qty")?,
                order_id,
                orig_id: String::new(),
                smp_id,
            })
        }
    }
}

fn parse_side(s: &str) -> Side {
    if s.eq_ignore_ascii_case("SELL") {
        Side::Sell
    } else {
        Side::Buy
    }
}

/// json_string extracts a `"key":"value"` string field. Order ids/sides never
/// contain a `"` (bot-fleet's `validate_identifier`), so no escape handling needed.
fn json_string(body: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\":\"");
    let start = body.find(&needle)? + needle.len();
    let end = body[start..].find('"')? + start;
    Some(body[start..end].to_string())
}

fn json_u64(body: &str, key: &str) -> Option<u64> {
    json_number(body, key)?.parse().ok()
}

fn json_i64(body: &str, key: &str) -> Option<i64> {
    json_number(body, key)?.parse().ok()
}

fn json_number(body: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\":");
    let start = body.find(&needle)? + needle.len();
    let end = body[start..].find([',', '}'])? + start;
    Some(body[start..end].trim().to_string())
}

/// ws_accept_key computes the RFC 6455 `Sec-WebSocket-Accept` response value from a
/// client's `Sec-WebSocket-Key`, hand-rolled (SHA-1 + base64) to keep this reference
/// engine dependency-free (std-only, per the module's build notes).
fn ws_accept_key(client_key: &str) -> String {
    const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    let mut concat = client_key.as_bytes().to_vec();
    concat.extend_from_slice(WS_GUID.as_bytes());
    base64_encode(&sha1(&concat))
}

fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0];

    let bit_len = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for block in msg.chunks(64) {
        let mut w = [0u32; 80];
        for (i, word) in w.iter_mut().take(16).enumerate() {
            *word = u32::from_be_bytes([block[i * 4], block[i * 4 + 1], block[i * 4 + 2], block[i * 4 + 3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let (mut a, mut b, mut c, mut d, mut e) = (h[0], h[1], h[2], h[3], h[4]);
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }

    let mut out = [0u8; 20];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

fn base64_encode(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
        out.push(TABLE[(n >> 18 & 0x3F) as usize] as char);
        out.push(TABLE[(n >> 12 & 0x3F) as usize] as char);
        out.push(if chunk.len() > 1 { TABLE[(n >> 6 & 0x3F) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { TABLE[(n & 0x3F) as usize] as char } else { '=' });
    }
    out
}

/// Per-connection writer: drains its outbound channel to the socket, coalescing
/// whatever is queued into each flush.
async fn writer(
    mut wr: tokio::net::tcp::OwnedWriteHalf,
    mut orx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
) {
    while let Some(buf) = orx.recv().await {
        if wr.write_all(&buf).await.is_err() {
            return;
        }
        while let Ok(more) = orx.try_recv() {
            if wr.write_all(&more).await.is_err() {
                return;
            }
        }
        let _ = wr.flush().await;
    }
}

fn parse_order(msg: &[u8]) -> Option<Order> {
    let mt = extract_tag(msg, b"35")?;
    let order_id = std::str::from_utf8(extract_tag(msg, b"11")?).ok()?.to_string();
    let side = match extract_tag(msg, b"54") {
        Some(b"2") => Side::Sell,
        _ => Side::Buy,
    };
    let orig_id = extract_tag(msg, b"41")
        .and_then(|v| std::str::from_utf8(v).ok())
        .unwrap_or("")
        .to_string();
    let qty = extract_tag(msg, b"38").and_then(parse_u64).unwrap_or(0);
    let price = extract_tag(msg, b"44").and_then(parse_i64).unwrap_or(0);
    // FIX tag 7928 = SelfMatchPreventionID. Absent means unconstrained.
    let smp_id = extract_tag(msg, b"7928").and_then(parse_u64).map(|v| v as u32);
    let ordtype = extract_tag(msg, b"40");
    let kind = match mt {
        b"D" => {
            if ordtype == Some(b"1") {
                Kind::NewMarket
            } else {
                Kind::NewLimit
            }
        }
        b"F" => Kind::Cancel,
        b"G" => Kind::Replace,
        _ => return None, // logon/heartbeat/etc. ignored
    };
    Some(Order { kind, side, price, qty, order_id, orig_id, smp_id })
}

fn parse_u64(v: &[u8]) -> Option<u64> {
    std::str::from_utf8(v).ok()?.trim().parse().ok()
}
fn parse_i64(v: &[u8]) -> Option<i64> {
    std::str::from_utf8(v).ok()?.trim().parse().ok()
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn find_message_end(buf: &[u8]) -> Option<usize> {
    let needle = b"\x0110=";
    let i = find_subslice(buf, needle)?;
    let after = i + needle.len();
    let soh = buf[after..].iter().position(|&b| b == SOH)?;
    Some(after + soh + 1)
}

fn extract_tag<'a>(msg: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    let mut needle = Vec::with_capacity(tag.len() + 2);
    needle.push(SOH);
    needle.extend_from_slice(tag);
    needle.push(b'=');
    let pos = find_subslice(msg, &needle)?;
    let vs = pos + needle.len();
    let ve = msg[vs..].iter().position(|&b| b == SOH)?;
    Some(&msg[vs..vs + ve])
}

fn finalize(body: &str) -> Vec<u8> {
    let mut frame = format!("8=FIX.4.2\x019={}\x01{body}", body.len()).into_bytes();
    let checksum = frame.iter().fold(0u32, |s, b| s.wrapping_add(u32::from(*b))) % 256;
    frame.extend_from_slice(format!("10={checksum:03}\x01").as_bytes());
    frame
}

/// New ack (no fill) — gives the latency capture a response for every order.
fn new_ack(order_id: &str, seq: u64) -> Vec<u8> {
    finalize(&format!(
        "35=8\x0149=CONTESTANT\x0156=IICPC-BOT\x0134={seq}\x0152=19700101-00:00:00.000\x0137=EXEC_{seq}\x0111={order_id}\x0117=ACK_{seq}\x01150=0\x0139=0\x0155=IICPC\x0154=1\x0138=0\x0114=0\x016=0\x01"
    ))
}

/// Fill ExecutionReport: 150=2 (Fill), 32=qty (LastShares), 31=price (LastPx, raw;
/// eBPF scales by 1e9 to match the reference's price*1e9).
fn fill_er(order_id: &str, price: i64, qty: u64, seq: u64, liquidity: u8) -> Vec<u8> {
    finalize(&format!(
        "35=8\x0149=CONTESTANT\x0156=IICPC-BOT\x0134={seq}\x0152=19700101-00:00:00.000\x0137=EXEC_{seq}\x0111={order_id}\x0117=FILL_{seq}\x01150=2\x0139=2\x0155=IICPC\x0154=1\x0132={qty}\x0131={price}\x0114={qty}\x016={price}\x01851={liquidity}\x01"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn ord(kind: Kind, side: Side, price: i64, qty: u64, id: &str) -> Order {
        Order { kind, side, price, qty, order_id: id.into(), orig_id: String::new(), smp_id: None }
    }

    /// ord_smp is `ord` with a self-match-prevention id attached.
    pub(super) fn ord_smp(kind: Kind, side: Side, price: i64, qty: u64, id: &str, smp: u32) -> Order {
        Order { smp_id: Some(smp), ..ord(kind, side, price, qty, id) }
    }

    #[test]
    fn cross_produces_two_fills_at_maker_price() {
        let mut e = Engine::default();
        let (f, _) = e.process(&ord(Kind::NewLimit, Side::Sell, 100, 10, "S1"));
        assert!(f.is_empty(), "no cross -> rests");
        // buy @105 crosses the resting ask @100 -> fills at the MAKER price (100)
        let (f, done) = e.process(&ord(Kind::NewLimit, Side::Buy, 105, 10, "B1"));
        assert_eq!(f.len(), 2);
        assert_eq!((f[0].order_id.as_str(), f[0].price, f[0].qty), ("B1", 100, 10));
        assert_eq!((f[1].order_id.as_str(), f[1].price, f[1].qty), ("S1", 100, 10));
        // B1 is the aggressor (taker=2), S1 the resting maker (1).
        assert_eq!(f[0].liquidity, 2, "aggressor is taker");
        assert_eq!(f[1].liquidity, 1, "resting order is maker");
        assert!(done.contains(&"S1".to_string()) && done.contains(&"B1".to_string()));
    }

    #[test]
    fn time_priority_fifo() {
        let mut e = Engine::default();
        e.process(&ord(Kind::NewLimit, Side::Sell, 100, 10, "S1"));
        e.process(&ord(Kind::NewLimit, Side::Sell, 100, 10, "S2")); // later at same price
        let (f, _) = e.process(&ord(Kind::NewLimit, Side::Buy, 100, 15, "B1"));
        let s1: u64 = f.iter().filter(|x| x.order_id == "S1").map(|x| x.qty).sum();
        let s2: u64 = f.iter().filter(|x| x.order_id == "S2").map(|x| x.qty).sum();
        assert_eq!(s1, 10, "oldest filled first");
        assert_eq!(s2, 5, "newer filled with the remainder");
    }

    #[test]
    fn no_cross_rests() {
        let mut e = Engine::default();
        e.process(&ord(Kind::NewLimit, Side::Sell, 100, 10, "S1"));
        let (f, _) = e.process(&ord(Kind::NewLimit, Side::Buy, 95, 10, "B1"));
        assert!(f.is_empty());
        assert!(e.index.contains_key("B1"), "non-crossing limit rests");
    }

    #[test]
    fn market_matches_then_does_not_rest() {
        let mut e = Engine::default();
        e.process(&ord(Kind::NewLimit, Side::Sell, 100, 5, "S1"));
        let (f, done) = e.process(&ord(Kind::NewMarket, Side::Buy, 0, 10, "M1"));
        let b: u64 = f.iter().filter(|x| x.order_id == "M1").map(|x| x.qty).sum();
        assert_eq!(b, 5);
        assert!(!e.index.contains_key("M1"), "market never rests");
        assert!(done.contains(&"M1".to_string()));
    }

    #[test]
    fn cancel_removes() {
        let mut e = Engine::default();
        e.process(&ord(Kind::NewLimit, Side::Buy, 100, 10, "B1"));
        let mut c = ord(Kind::Cancel, Side::Buy, 0, 0, "C1");
        c.orig_id = "B1".into();
        e.process(&c);
        assert!(!e.index.contains_key("B1"));
    }

    #[test]
    fn replace_same_price_qty_decrease_keeps_position() {
        let mut e = Engine::default();
        e.process(&ord(Kind::NewLimit, Side::Sell, 100, 10, "S1")); // ahead
        e.process(&ord(Kind::NewLimit, Side::Sell, 100, 10, "S2"));
        let mut r = ord(Kind::Replace, Side::Sell, 100, 6, "S1b"); // S1 -> qty 6, same price
        r.orig_id = "S1".into();
        e.process(&r);
        // S1b still ahead of S2 -> a buy 6 fills S1b only
        let (f, _) = e.process(&ord(Kind::NewLimit, Side::Buy, 100, 6, "B1"));
        let got: u64 = f.iter().filter(|x| x.order_id == "S1b").map(|x| x.qty).sum();
        assert_eq!(got, 6, "qty-decrease replace keeps time priority");
        assert!(!e.index.contains_key("S1"));
    }

    #[test]
    fn parse_new_limit_fix() {
        let raw = b"8=FIX.4.2\x019=50\x0135=D\x0111=ord-1\x0155=IICPC\x0154=1\x0138=25\x0140=2\x0144=10000\x0159=0\x0110=000\x01";
        let o = parse_order(raw).expect("parse");
        assert!(matches!(o.kind, Kind::NewLimit));
        assert!(matches!(o.side, Side::Buy));
        assert_eq!(o.qty, 25);
        assert_eq!(o.price, 10000);
        assert_eq!(o.order_id, "ord-1");
    }

    #[test]
    fn sha1_matches_known_vector() {
        // SHA1("abc") = a9993e364706816aba3e25717850c26c9cd0d89
        let digest = sha1(b"abc");
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, "a9993e364706816aba3e25717850c26c9cd0d89d");
    }

    #[test]
    fn ws_accept_key_matches_rfc6455_example() {
        // RFC 6455 §1.3 worked example.
        assert_eq!(ws_accept_key("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[test]
    fn parse_order_json_new_limit() {
        let body = r#"{"cl_ord_id":"sess_1_2_O","symbol":"IICPC","side":"BUY","qty":25,"price":10000}"#;
        let o = parse_order_json(body).expect("parse");
        assert!(matches!(o.kind, Kind::NewLimit));
        assert!(matches!(o.side, Side::Buy));
        assert_eq!(o.qty, 25);
        assert_eq!(o.price, 10000);
        assert_eq!(o.order_id, "sess_1_2_O");
    }

    #[test]
    fn parse_order_json_new_market() {
        let body = r#"{"cl_ord_id":"sess_1_3_M","symbol":"IICPC","side":"SELL","qty":5,"ord_type":"MARKET"}"#;
        let o = parse_order_json(body).expect("parse");
        assert!(matches!(o.kind, Kind::NewMarket));
        assert!(matches!(o.side, Side::Sell));
        assert_eq!(o.qty, 5);
    }

    #[test]
    fn parse_order_json_cancel() {
        let body = r#"{"action":"CANCEL","cl_ord_id":"sess_1_5_C","orig_cl_ord_id":"sess_1_2_O","symbol":"IICPC"}"#;
        let o = parse_order_json(body).expect("parse");
        assert!(matches!(o.kind, Kind::Cancel));
        assert_eq!(o.order_id, "sess_1_5_C");
        assert_eq!(o.orig_id, "sess_1_2_O");
    }

    #[test]
    fn parse_order_json_replace() {
        let body = r#"{"action":"REPLACE","cl_ord_id":"sess_1_6_R","orig_cl_ord_id":"sess_1_2_O","symbol":"IICPC","side":"BUY","qty":6,"price":9900}"#;
        let o = parse_order_json(body).expect("parse");
        assert!(matches!(o.kind, Kind::Replace));
        assert_eq!(o.qty, 6);
        assert_eq!(o.price, 9900);
        assert_eq!(o.orig_id, "sess_1_2_O");
    }

    #[test]
    fn http_request_framed_by_content_length() {
        let body = r#"{"cl_ord_id":"ord-1","symbol":"IICPC","side":"BUY","qty":1,"price":100}"#;
        let raw = format!(
            "POST /orders HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let req = parse_http_request(raw.as_bytes()).expect("complete request");
        assert_eq!(req.total_len, raw.len());
        assert_eq!(req.body, body);
        assert!(!req.is_websocket_upgrade);
    }

    #[test]
    fn http_request_incomplete_returns_none() {
        let partial = "POST /orders HTTP/1.1\r\nContent-Length: 50\r\n\r\n{\"cl_ord_id\":";
        assert!(parse_http_request(partial.as_bytes()).is_none());
    }

    #[test]
    fn http_two_pipelined_requests_drain_in_order() {
        let one = "POST /orders HTTP/1.1\r\nContent-Length: 21\r\n\r\n{\"cl_ord_id\":\"ord-A\"}";
        let two = "POST /orders HTTP/1.1\r\nContent-Length: 21\r\n\r\n{\"cl_ord_id\":\"ord-B\"}";
        let mut buf = format!("{one}{two}").into_bytes();
        let a = parse_http_request(&buf).expect("first");
        assert_eq!(a.body, "{\"cl_ord_id\":\"ord-A\"}");
        buf.drain(..a.total_len);
        let b = parse_http_request(&buf).expect("second");
        assert_eq!(b.body, "{\"cl_ord_id\":\"ord-B\"}");
    }

    #[test]
    fn http_websocket_upgrade_request_detected() {
        let raw = "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
        let req = parse_http_request(raw.as_bytes()).expect("complete request");
        assert!(req.is_websocket_upgrade);
        assert_eq!(req.sec_websocket_key.as_deref(), Some("dGhlIHNhbXBsZSBub25jZQ=="));
    }

    #[test]
    fn ws_frame_masked_client_frame_unmasked_and_parsed() {
        let payload = br#"{"cl_ord_id":"ord-ws-1","symbol":"IICPC","side":"BUY","qty":3,"price":100}"#;
        let mask = [0x13u8, 0x37, 0xC0, 0xDE];
        let mut frame = vec![0x81u8, 0x80 | payload.len() as u8];
        frame.extend_from_slice(&mask);
        for (i, &b) in payload.iter().enumerate() {
            frame.push(b ^ mask[i % 4]);
        }
        let parsed = parse_ws_frame(&frame).expect("complete frame");
        assert_eq!(parsed.total_len, frame.len());
        assert_eq!(parsed.opcode, 0x1);
        let order = parse_order_json(&parsed.payload).expect("parse order");
        assert_eq!(order.order_id, "ord-ws-1");
    }

    #[test]
    fn ws_frame_incomplete_returns_none() {
        let mask = [0, 0, 0, 0];
        let mut frame = vec![0x81u8, 0x80 | 10u8];
        frame.extend_from_slice(&mask);
        frame.extend_from_slice(b"short"); // fewer than the declared 10 payload bytes
        assert!(parse_ws_frame(&frame).is_none());
    }

    #[test]
    fn ws_text_frame_round_trips_through_parse_ws_frame_shape() {
        let body = json_new_ack("ord-1");
        let encoded = ws_text_frame(&body);
        // Server frames are unmasked; verify the header + payload we sent.
        assert_eq!(encoded[0], 0x81);
        assert_eq!(encoded[1] & 0x80, 0, "server frames must not set the mask bit");
        assert_eq!(&encoded[2..], body.as_bytes());
    }

    #[test]
    fn http_response_has_matching_content_length() {
        let body = json_fill("ord-1", 10000, 5, 2);
        let resp = http_response(&body);
        let text = String::from_utf8(resp).unwrap();
        assert!(text.starts_with("HTTP/1.1 200 OK"));
        assert!(text.contains(&format!("Content-Length: {}", body.len())));
        assert!(text.ends_with(&body));
    }

    #[test]
    fn json_new_ack_and_fill_carry_engine_exec_type_convention() {
        assert!(json_new_ack("ord-1").contains("\"exec_type\":\"0\""));
        let fill = json_fill("ord-1", 10000, 5, 2);
        assert!(fill.contains("\"exec_type\":\"2\""));
        assert!(fill.contains("\"fill_qty\":5"));
        assert!(fill.contains("\"fill_price\":10000"));
        assert!(fill.contains("\"liquidity\":2"));
    }

    #[test]
    fn fill_er_encodes_fill_tags() {
        let s = String::from_utf8(fill_er("ord-9", 10000, 25, 7, 2)).unwrap();
        assert!(s.contains("\x01150=2\x01"), "ExecType=Fill");
        assert!(s.contains("\x0132=25\x01"), "LastShares=qty");
        assert!(s.contains("\x0131=10000\x01"), "LastPx=raw price");
        assert!(s.contains("\x0111=ord-9\x01"));
        assert!(s.contains("\x01851=2\x01"), "LastLiquidityInd=taker");
    }
}

#[cfg(test)]
mod smp_tests {
    use super::tests::{ord, ord_smp};
    use super::*;

    /// The core rule, and it must match services/correctness-validator/internal/book
    /// exactly: an aggressor skips a resting order sharing its smp_id and continues to
    /// the next maker at that level. The skipped order stays RESTING — skip-and-continue
    /// cancels nothing.
    #[test]
    fn skips_same_smp_id_and_leaves_it_resting() {
        let mut e = Engine::default();
        e.process(&ord_smp(Kind::NewLimit, Side::Sell, 100, 10, "mine", 3));
        e.process(&ord_smp(Kind::NewLimit, Side::Sell, 100, 10, "other", 5));

        let (fills, done) = e.process(&ord_smp(Kind::NewLimit, Side::Buy, 100, 10, "taker", 3));
        let ids: Vec<&str> = fills.iter().map(|f| f.order_id.as_str()).collect();
        assert!(!ids.contains(&"mine"), "matched an order sharing its smp_id: {ids:?}");
        assert!(ids.contains(&"other"), "should have skipped to 'other': {ids:?}");
        assert!(!done.contains(&"mine".to_string()), "skipped order must not be removed");
    }

    /// No id = unconstrained. This is all of pass-2's traffic, where SMP is not graded
    /// and the field is omitted from the wire entirely, so those runs must behave
    /// exactly as they did before SMP existed.
    #[test]
    fn no_smp_id_matches_anything() {
        let mut e = Engine::default();
        e.process(&ord(Kind::NewLimit, Side::Sell, 100, 10, "R"));
        let (fills, _) = e.process(&ord(Kind::NewLimit, Side::Buy, 100, 10, "T"));
        assert_eq!(fills.len(), 2, "id-less orders must match");
    }

    /// 0 is a VALID id, not "absent". Conflating them would make every id-less order
    /// look like participant 0 and stop the engine matching anything at all.
    #[test]
    fn zero_is_a_real_id_not_absent() {
        let mut e = Engine::default();
        e.process(&ord_smp(Kind::NewLimit, Side::Sell, 100, 10, "r0", 0));
        let (f, _) = e.process(&ord_smp(Kind::NewLimit, Side::Buy, 100, 10, "t1", 1));
        assert_eq!(f.len(), 2, "id 1 must match resting id 0");

        let mut e2 = Engine::default();
        e2.process(&ord_smp(Kind::NewLimit, Side::Sell, 100, 10, "r0b", 0));
        let (f2, _) = e2.process(&ord_smp(Kind::NewLimit, Side::Buy, 100, 10, "t0", 0));
        assert!(f2.is_empty(), "id 0 must not match resting id 0: {f2:?}");
    }

    /// Skipping must not stop the aggressor: it continues past its own order to worse
    /// prices, exactly as if the skipped order were not there.
    #[test]
    fn continues_to_next_price_level_after_skipping() {
        let mut e = Engine::default();
        e.process(&ord_smp(Kind::NewLimit, Side::Sell, 100, 10, "mine100", 2));
        e.process(&ord_smp(Kind::NewLimit, Side::Sell, 101, 10, "other101", 6));

        let (fills, _) = e.process(&ord_smp(Kind::NewLimit, Side::Buy, 101, 10, "taker", 2));
        let ids: Vec<&str> = fills.iter().map(|f| f.order_id.as_str()).collect();
        assert!(ids.contains(&"other101"), "must reach the next level: {ids:?}");
        assert!(!ids.contains(&"mine100"), "must not match own order: {ids:?}");
    }

    /// A book where every order shares one id yields NO fills and terminates. This is
    /// what pass 1 looked like before SMP ids existed: one task meant one identity, so
    /// the reference book flagged every fill as a self-trade and a correct engine
    /// scored 0 while an engine that refused to trade scored 1.0.
    #[test]
    fn fully_self_crossing_book_produces_no_fills_and_terminates() {
        let mut e = Engine::default();
        for (i, id) in ["a", "b", "c"].iter().enumerate() {
            e.process(&ord_smp(Kind::NewLimit, Side::Sell, 100 + i as i64, 10, id, 7));
        }
        let (fills, done) = e.process(&ord_smp(Kind::NewLimit, Side::Buy, 200, 30, "agg", 7));
        assert!(fills.is_empty(), "nothing may match: {fills:?}");
        assert!(!done.contains(&"a".to_string()), "resting orders must survive");
    }

    /// FIX tag 7928 and the JSON smp_id field must both parse, since the platform
    /// grades FIX, REST and WS identically.
    #[test]
    fn parses_smp_id_from_both_protocols() {
        // extract_tag matches on a LEADING SOH, so a real frame's 8=/9= header must be
        // present — a body starting at 35= leaves the first tag unreachable.
        let fix = finalize("35=D\x0149=B\x0156=C\x0134=1\x0111=o1\x0155=IICPC\x0154=1\x0138=5\x0140=2\x0144=100\x017928=007\x0159=0\x01");
        let o = parse_order(&fix).expect("fix order");
        assert_eq!(o.smp_id, Some(7), "tag 7928 must parse (zero-padded)");

        let no_tag = finalize("35=D\x0149=B\x0156=C\x0134=1\x0111=o2\x0155=IICPC\x0154=1\x0138=5\x0140=2\x0144=100\x0159=0\x01");
        assert_eq!(parse_order(&no_tag).expect("fix").smp_id, None, "absent tag = unconstrained");

        let json = r#"{"cl_ord_id":"o3","symbol":"IICPC","side":"BUY","qty":5,"price":100,"smp_id":"003"}"#;
        assert_eq!(parse_order_json(json).expect("json").smp_id, Some(3), "smp_id string must parse");

        let json_none = r#"{"cl_ord_id":"o4","symbol":"IICPC","side":"BUY","qty":5,"price":100}"#;
        assert_eq!(parse_order_json(json_none).expect("json").smp_id, None);
    }
}
