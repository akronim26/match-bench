//! Responding drain sink — a maximally-fast FIX 4.2 responder for the IICPC
//! benchmark, used to push the MEASUREMENT pipeline to its ceiling without the
//! contestant being the bottleneck.
//!
//! Listens on 0.0.0.0:9898 (override with BIND or PORT), reads NewOrderSingle
//! (35=D) off each connection, and replies to every one with an ExecutionReport
//! (35=8, OrdStatus=New) echoing the order's ClOrdID (tag 11) — the 35=8 + 11 the
//! eBPF latency capture needs to classify a response and match it by ClOrdID.
//!
//! "Responding drain": unlike the old echo it drains aggressively and does ~no
//! per-order heap work, so it never falls behind and fills the TCP window (which
//! back-pressured + collapsed the load gen at >150k). Optimizations vs the old echo:
//!   - tokio multi-thread across ALL cores (was hardcoded 2),
//!   - ALLOC-FREE response build (no `format!`; template + arithmetic length +
//!     single-pass checksum, written straight into the per-conn out buffer),
//!   - non-allocating tag extraction (stack needle, no per-call Vec),
//!   - one batched write per read.
//!
//! THROUGHPUT contestant only: acks without a real order book, so it is disqualified
//! on correctness. A qualifying contestant needs a matching engine.

use std::env;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const SOH: u8 = 0x01;

fn main() -> std::io::Result<()> {
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
        .max(2);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> std::io::Result<()> {
    let bind = env::var("BIND").unwrap_or_else(|_| {
        let port = env::var("PORT").unwrap_or_else(|_| "9898".to_string());
        format!("0.0.0.0:{port}")
    });
    let listener = TcpListener::bind(&bind).await?;
    eprintln!("responding-drain listening on {bind}");
    loop {
        // accept() errors must NEVER be fatal (2026-08-03). This was `accept().await?`,
        // which propagates out of run() -> block_on -> main and EXITS the process, taking
        // the listener with it. Observed on EKS: 500 tasks connecting at once drove ~304k
        // orders/s for 3 seconds, then the process vanished — the bot saw `connection
        // refused`, error_rate went to 1.0 and delivered tps to 0, which reads like a
        // platform ceiling but is entirely self-inflicted. ECONNABORTED (peer RSTs between
        // SYN and accept) is routine during a connect storm; EMFILE/ENFILE mean fd
        // pressure and need a brief backoff so the loop cannot spin hot. Per-connection
        // errors were already isolated in the spawned task below — only this one leaked.
        let (stream, _peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                eprintln!("accept error (continuing): {e}");
                // EMFILE=24, ENFILE=23: the fd table is full, so retrying immediately
                // just burns CPU until connections close.
                if matches!(e.raw_os_error(), Some(24) | Some(23)) {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                continue;
            }
        };
        tokio::spawn(async move {
            if let Err(e) = handle(stream).await {
                eprintln!("connection error: {e}");
            }
        });
    }
}

async fn handle(mut stream: TcpStream) -> std::io::Result<()> {
    stream.set_nodelay(true).ok();
    let mut buf: Vec<u8> = Vec::with_capacity(1 << 16);
    let mut chunk = [0u8; 1 << 16];
    let mut out: Vec<u8> = Vec::with_capacity(1 << 16);

    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(()); // peer closed
        }
        buf.extend_from_slice(&chunk[..n]);

        out.clear();
        let mut cursor = 0usize;
        loop {
            let Some(start_off) = find_subslice(&buf[cursor..], b"8=FIX") else {
                break;
            };
            let abs_start = cursor + start_off;
            let Some(end) = find_message_end(&buf[abs_start..]) else {
                break; // incomplete tail — wait for more bytes
            };
            let msg = &buf[abs_start..abs_start + end];
            if extract_tag(msg, b"35") == Some(b"D") {
                if let Some(clord) = extract_tag(msg, b"11") {
                    append_ack(&mut out, clord);
                }
            }
            cursor = abs_start + end;
        }

        if !out.is_empty() {
            stream.write_all(&out).await?;
        }
        if cursor > 0 {
            buf.drain(..cursor);
        }
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn find_message_end(buf: &[u8]) -> Option<usize> {
    let needle = b"\x0110="; // checksum field terminates a FIX message
    let i = find_subslice(buf, needle)?;
    let after = i + needle.len();
    let soh = buf[after..].iter().position(|&b| b == SOH)?;
    Some(after + soh + 1)
}

/// extract_tag finds `\x01<tag>=` and returns the value up to the next SOH.
/// Non-allocating: the needle is built on a stack array.
fn extract_tag<'a>(msg: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    let mut needle = [0u8; 8];
    needle[0] = SOH;
    needle[1..1 + tag.len()].copy_from_slice(tag);
    needle[1 + tag.len()] = b'=';
    let nl = 1 + tag.len() + 1;
    let pos = find_subslice(msg, &needle[..nl])?;
    let vs = pos + nl;
    let ve = msg[vs..].iter().position(|&b| b == SOH)?;
    Some(&msg[vs..vs + ve])
}

/// Write a u64 as ASCII into `out` without allocating.
fn write_u(out: &mut Vec<u8>, mut v: u64) {
    if v == 0 {
        out.push(b'0');
        return;
    }
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    while v > 0 {
        i -= 1;
        tmp[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    out.extend_from_slice(&tmp[i..]);
}

/// Append an ExecutionReport (35=8, OrdStatus=New) echoing `clord` (tag 11), built
/// directly into `out` with zero heap allocation. Counter tags (34/37/17) are fixed
/// constants — the eBPF matcher keys only on 35=8 + 11(ClOrdID) + 150(ExecType).
fn append_ack(out: &mut Vec<u8>, clord: &[u8]) {
    const PRE: &[u8] = b"35=8\x0149=C\x0156=B\x0134=1\x0152=19700101-00:00:00.000\x0137=E\x0111=";
    const POST: &[u8] = b"\x0117=X\x01150=0\x0139=0\x0155=IICPC\x0154=1\x0138=0\x0114=0\x016=0\x01";
    let body_len = PRE.len() + clord.len() + POST.len();

    let start = out.len();
    out.extend_from_slice(b"8=FIX.4.2\x019=");
    write_u(out, body_len as u64);
    out.push(SOH);
    out.extend_from_slice(PRE);
    out.extend_from_slice(clord);
    out.extend_from_slice(POST);

    // FIX checksum: sum of all bytes from BeginString through the byte before 10=, mod 256.
    let cksum = out[start..].iter().fold(0u32, |s, &b| s.wrapping_add(b as u32)) % 256;
    out.extend_from_slice(b"10=");
    out.push(b'0' + (cksum / 100) as u8);
    out.push(b'0' + ((cksum / 10) % 10) as u8);
    out.push(b'0' + (cksum % 10) as u8);
    out.push(SOH);
}
