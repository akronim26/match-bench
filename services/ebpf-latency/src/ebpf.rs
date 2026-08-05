//! This module implements ebpf behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

#![cfg_attr(target_arch = "bpf", no_std)]
#![cfg_attr(target_arch = "bpf", no_main)]

#[cfg(not(target_arch = "bpf"))]
/// host_placeholder performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn host_placeholder() {}

#[cfg(target_arch = "bpf")]
use aya_ebpf::{
    bindings::{__sk_buff, xdp_action::XDP_PASS, TC_ACT_PIPE},
    cty::c_void,
    helpers::{bpf_skb_load_bytes, bpf_xdp_load_bytes},
    macros::{map, xdp},
    maps::{PerCpuArray, RingBuf},
    programs::{TcContext, XdpContext},
};
#[cfg(target_arch = "bpf")]
use core::{mem, panic::PanicInfo, ptr};

#[cfg(target_arch = "bpf")]
const ETH_P_IP: u16 = 0x0800;
#[cfg(target_arch = "bpf")]
const IPPROTO_TCP: u8 = 6;
#[cfg(target_arch = "bpf")]
const FIX_PORT: u16 = 9898;
#[cfg(target_arch = "bpf")]
const HTTP_WS_PORT: u16 = 8080;

#[cfg(target_arch = "bpf")]
const CAPTURE_CAP: usize = 9029;
// Maximum payload bytes copied per packet into the capture buffer. 9029 = 9001 + 28 covers a
// full jumbo frame, so EKS keeps MTU 9001 and the platform-wide MTU-1500 clamp (and its ~6x
// packet-count tax) is no longer forced by the capture — see docs/capture-ringbuf-drops.md
// §6.2. The per-CPU scratch value is 28 + 9029 = 9057, well under the 32KB PCPU_MIN_UNIT_SIZE
// bound on per-CPU map values. The length passed to bpf_*_load_bytes must satisfy the kernel
// 6.1 BPF verifier (EKS AL2023), whose ARG_CONST_SIZE length arg requires the register to
// carry umin ≥ 1 (else a zero-size probe fails with "R3 min value is outside of the allowed
// memory range") AND umax ≤ value_size - payload_off. See capture_len for how those bounds are
// established (read_volatile + relational guards) and why oversized payloads are clamped to
// this constant rather than skipped/masked. The proof shape is structural — none of it
// references the constant's value, only COPY_CAP == CAPTURE_CAP == the payload array length.
#[cfg(target_arch = "bpf")]
const COPY_CAP: usize = CAPTURE_CAP;
// Minimum payload length we capture. Must be ≥ 2 so the `len < MIN_CAPTURE_LEN` guard in
// capture_len lowers to a relational JLT (which the verifier uses to raise umin), not a JEQ
// against 0 (which it doesn't). See capture_len for the full rationale.
#[cfg(target_arch = "bpf")]
const MIN_CAPTURE_LEN: usize = 2;
#[cfg(target_arch = "bpf")]
const CAPTURE_HEADER_LEN: usize = 28;

#[cfg(target_arch = "bpf")]
const DIR_REQUEST: u8 = 0;
#[cfg(target_arch = "bpf")]
const DIR_RESPONSE: u8 = 1;

#[cfg(target_arch = "bpf")]
#[repr(C)]
#[derive(Clone, Copy)]
/// CaptureRecord stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct CaptureRecord {
    timestamp_ns: u64,
    client_ip: u32,
    tcp_seq: u32,
    payload_len: u32,
    client_port: u16,
    server_port: u16,
    captured_len: u16,
    direction: u8,
    _pad: u8,
    payload: [u8; CAPTURE_CAP],
}

#[cfg(target_arch = "bpf")]
#[repr(C)]
#[derive(Clone, Copy)]
/// EthHdr stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct EthHdr {
    dst: [u8; 6],
    src: [u8; 6],
    eth_proto: u16,
}

#[cfg(target_arch = "bpf")]
#[repr(C)]
#[derive(Clone, Copy)]
/// Ipv4Hdr stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct Ipv4Hdr {
    version_ihl: u8,
    tos: u8,
    tot_len: u16,
    id: u16,
    frag_off: u16,
    ttl: u8,
    protocol: u8,
    check: u16,
    saddr: u32,
    daddr: u32,
}

#[cfg(target_arch = "bpf")]
#[repr(C)]
#[derive(Clone, Copy)]
/// TcpHdr stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct TcpHdr {
    source: u16,
    dest: u16,
    seq: u32,
    ack_seq: u32,
    doff_res_flags: u16,
}

#[cfg(target_arch = "bpf")]
#[derive(Clone, Copy)]
/// PacketBounds stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct PacketBounds {
    payload_offset: usize,
    payload_len: usize,
    server_port: u16,
    client_ip: u32,
    client_port: u16,
    tcp_seq: u32,
}

#[cfg(target_arch = "bpf")]
#[map]
// 256MB (was 64): survivable-starvation window. 64MB ≈ 43ms of headroom at
// 1M max-size records/s against a 5ms drain cadence — ample while userspace
// runs, but the one observed drop cause is the drain NOT running (host CPU
// starvation, docs/capture-ringbuf-drops.md §5b), and 4x the ring is 4x the
// outage the capture can absorb without loss. BPF map memory is memcg-charged
// to the pod since kernel 5.11, so this moves in lockstep with the capture
// pod's memory limit (slot.go captureResources, 2Gi).
static EVENTS: RingBuf = RingBuf::with_byte_size(256 * 1024 * 1024, 0);

#[cfg(target_arch = "bpf")]
#[map]
static SCRATCH: PerCpuArray<CaptureRecord> = PerCpuArray::with_max_entries(1, 0);

#[cfg(target_arch = "bpf")]
#[map]
static DROPPED_EVENTS: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);

#[cfg(target_arch = "bpf")]
#[map]
static TRUNCATED_CAPTURES: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);

// Funnel counters. Everything below used to fail silently: a packet that reached the
// hook and then failed to copy simply vanished, with no counter anywhere, which is how a
// ~0.2% request-side loss stayed unexplained through four rounds of diagnosis.
/// Payload-bearing packets the ingress hook accepted for capture (pre-copy).
#[cfg(target_arch = "bpf")]
#[map]
static XDP_PACKETS: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);
/// Payload-bearing packets the egress hook accepted for capture (pre-copy).
#[cfg(target_arch = "bpf")]
#[map]
static TC_PACKETS: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);
/// bpf_xdp_load_bytes returned non-zero — the packet is dropped with no record emitted.
/// Non-linear / multi-buffer skbs are the usual cause.
#[cfg(target_arch = "bpf")]
#[map]
static XDP_LOAD_FAILED: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);
/// bpf_skb_load_bytes returned non-zero — same, on the response side.
#[cfg(target_arch = "bpf")]
#[map]
static TC_LOAD_FAILED: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);
/// Payloads shorter than MIN_CAPTURE_LEN, skipped by capture_len.
#[cfg(target_arch = "bpf")]
#[map]
static SHORT_PAYLOAD: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn bump(m: &PerCpuArray<u64>) {
    if let Some(c) = m.get_ptr_mut(0) {
        unsafe { ptr::write(c, ptr::read(c).saturating_add(1)) };
    }
}

/// capture_len returns the number of payload bytes to copy into the capture buffer, or None
/// to skip this packet. It bounds the result to [MIN_CAPTURE_LEN, COPY_CAP] in a form the
/// kernel 6.1 BPF verifier accepts as the ARG_CONST_SIZE length of bpf_*_load_bytes.
///
/// Two non-obvious verifier facts shape this:
///
/// 1. The length is read through read_volatile so the optimizer treats it as opaque. Without
///    that, LLVM rewrites a compare on `payload_len` (= pkt_end - payload_off) into a compare
///    on the operands (e.g. `pkt_end == payload_off`); the verifier then refines the pointer
///    registers but NOT the length register, leaving the length unbounded at the call.
///
/// 2. ARG_CONST_SIZE requires the length register to carry umin ≥ 1, else the verifier runs a
///    zero-size probe that fails with "R3 min value is outside of the allowed memory range".
///    But JEQ/JNE against 0 (`len == 0` / `len != 0`) does NOT raise umin: the verifier tracks
///    bounds as intervals, and removing the single point 0 from [0, MAX] is unrepresentable, so
///    umin stays 0. Only RELATIONAL comparisons tighten umin. So we test `len < MIN_CAPTURE_LEN`
///    with MIN_CAPTURE_LEN = 2, which lowers to an unsigned JLT (not reducible to an equality)
///    and tightens the fall-through to umin ≥ 2. `len > COPY_CAP` (JGT) tightens umax ≤ COPY_CAP.
///    Both bounds survive inlining and the spill/reload to the call.
///
/// Empty/1-byte payloads (< MIN_CAPTURE_LEN) are skipped — never real FIX. Oversized payloads
/// are CLAMPED to COPY_CAP, not skipped: the tc egress hook runs before GSO segmentation in
/// __dev_queue_xmit, so it sees the large pre-segmentation skb whenever the server coalesces
/// responses (independent of `ethtool gso off`, which only governs on-wire framing). Skipping
/// would drop every response in a coalesced packet; clamping captures the leading COPY_CAP
/// bytes and the userspace parser recovers whatever complete FIX messages fit (the rest are a
/// sampled loss, fine for a latency distribution). Returning the CONSTANT COPY_CAP keeps the
/// length verifier-trivial on that path (umin = umax = COPY_CAP). We do NOT clamp with a
/// saturating min (folds to a bound the verifier can't see) or an AND-mask (resets umin to 0).
#[cfg(target_arch = "bpf")]
#[inline(always)]
fn capture_len(bounds: &PacketBounds) -> Option<usize> {
    let len = unsafe { ptr::read_volatile(&bounds.payload_len) };
    if len < MIN_CAPTURE_LEN {
        bump(&SHORT_PAYLOAD);
        return None;
    }
    if len > COPY_CAP {
        if let Some(c) = TRUNCATED_CAPTURES.get_ptr_mut(0) {
            unsafe { ptr::write(c, ptr::read(c).saturating_add(1)) };
        }
        return Some(COPY_CAP);
    }
    Some(len)
}

#[cfg(target_arch = "bpf")]
#[xdp]
/// iicpc_xdp_ingress performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn iicpc_xdp_ingress(ctx: XdpContext) -> u32 {
    try_xdp_ingress(&ctx);
    XDP_PASS
}

#[cfg(target_arch = "bpf")]
#[no_mangle]
#[link_section = "classifier"]
/// iicpc_tc_egress performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub extern "C" fn iicpc_tc_egress(ctx: *mut __sk_buff) -> i32 {
    try_tc_egress(TcContext::new(ctx));
    TC_ACT_PIPE
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
/// try_xdp_ingress performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn try_xdp_ingress(ctx: &XdpContext) {
    let data = ctx.data();
    let data_end = ctx.data_end();
    let Some(bounds) = xdp_payload_bounds(data, data_end) else {
        return;
    };
    bump(&XDP_PACKETS);
    let cap = match capture_len(&bounds) {
        Some(cap) => cap,
        None => return,
    };
    let Some(rec) = SCRATCH.get_ptr_mut(0) else {
        return;
    };
    let dst = unsafe { ptr::addr_of_mut!((*rec).payload) as *mut c_void };
    let ret = unsafe { bpf_xdp_load_bytes(ctx.ctx, bounds.payload_offset as u32, dst, cap as u32) };
    if ret != 0 {
        bump(&XDP_LOAD_FAILED);
        return;
    }
    emit_capture(rec, &bounds, cap, DIR_REQUEST);
}

#[cfg(target_arch = "bpf")]
#[inline(never)]
/// try_tc_egress performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn try_tc_egress(ctx: TcContext) {
    let Some(bounds) = tc_payload_bounds(&ctx) else {
        return;
    };
    bump(&TC_PACKETS);
    let cap = match capture_len(&bounds) {
        Some(cap) => cap,
        None => return,
    };
    let Some(rec) = SCRATCH.get_ptr_mut(0) else {
        return;
    };
    let dst = unsafe { ptr::addr_of_mut!((*rec).payload) as *mut c_void };
    let ret = unsafe {
        bpf_skb_load_bytes(
            ctx.skb.skb as *const c_void,
            bounds.payload_offset as u32,
            dst,
            cap as u32,
        )
    };
    if ret != 0 {
        bump(&TC_LOAD_FAILED);
        return;
    }
    emit_capture(rec, &bounds, cap, DIR_RESPONSE);
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
/// emit_capture performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn emit_capture(rec: *mut CaptureRecord, bounds: &PacketBounds, cap: usize, direction: u8) {
    let cap = if cap > CAPTURE_CAP { CAPTURE_CAP } else { cap };
    let total = CAPTURE_HEADER_LEN + cap;
    unsafe {
        ptr::addr_of_mut!((*rec).timestamp_ns).write(bpf_ktime_get_ns());
        ptr::addr_of_mut!((*rec).client_ip).write(bounds.client_ip);
        ptr::addr_of_mut!((*rec).tcp_seq).write(bounds.tcp_seq);
        ptr::addr_of_mut!((*rec).payload_len).write(bounds.payload_len as u32);
        ptr::addr_of_mut!((*rec).client_port).write(bounds.client_port);
        ptr::addr_of_mut!((*rec).server_port).write(bounds.server_port);
        ptr::addr_of_mut!((*rec).captured_len).write(cap as u16);
        ptr::addr_of_mut!((*rec).direction).write(direction);
        ptr::addr_of_mut!((*rec)._pad).write(0);

        let bytes = core::slice::from_raw_parts(rec as *const u8, total);
        if EVENTS.output(bytes, 0).is_err() {
            if let Some(c) = DROPPED_EVENTS.get_ptr_mut(0) {
                ptr::write(c, ptr::read(c).saturating_add(1));
            }
        }
    }
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
/// xdp_payload_bounds performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn xdp_payload_bounds(data: usize, data_end: usize) -> Option<PacketBounds> {
    let eth: EthHdr = load(data, data_end, 0)?;
    if u16::from_be(eth.eth_proto) != ETH_P_IP {
        return None;
    }
    let ip_offset = mem::size_of::<EthHdr>();
    let ip: Ipv4Hdr = load(data, data_end, ip_offset)?;
    if ip.version_ihl >> 4 != 4 || ip.protocol != IPPROTO_TCP {
        return None;
    }
    let ihl = usize::from(ip.version_ihl & 0x0f) * 4;
    if ihl < mem::size_of::<Ipv4Hdr>() {
        return None;
    }
    let ip_total = usize::from(u16::from_be(ip.tot_len));
    if ip_total < ihl {
        return None;
    }
    let ip_end = ip_offset.checked_add(ip_total)?;
    if ip_end > data_end.saturating_sub(data) {
        return None;
    }
    let tcp_offset = ip_offset + ihl;
    let tcp: TcpHdr = load(data, data_end, tcp_offset)?;
    let source = u16::from_be(tcp.source);
    let dest = u16::from_be(tcp.dest);
    if dest != FIX_PORT && dest != HTTP_WS_PORT {
        return None;
    }
    let data_offset = usize::from(u16::from_be(tcp.doff_res_flags) >> 12) * 4;
    if data_offset < mem::size_of::<TcpHdr>() {
        return None;
    }
    let payload_offset = tcp_offset.checked_add(data_offset)?;
    if payload_offset > ip_end {
        return None;
    }
    Some(PacketBounds {
        payload_offset,
        payload_len: ip_end - payload_offset,
        server_port: dest,
        client_ip: u32::from_be(ip.saddr),
        client_port: source,
        tcp_seq: u32::from_be(tcp.seq),
    })
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
/// tc_payload_bounds performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn tc_payload_bounds(ctx: &TcContext) -> Option<PacketBounds> {
    let packet_len = ctx.len() as usize;
    let framed = || -> Option<PacketBounds> {
        let eth: EthHdr = ctx.load(0).ok()?;
        if u16::from_be(eth.eth_proto) != ETH_P_IP {
            return None;
        }
        parse_skb_ip_tcp_at(ctx, mem::size_of::<EthHdr>(), packet_len)
    };
    framed().or_else(|| parse_skb_ip_tcp_at(ctx, 0, packet_len))
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
/// parse_skb_ip_tcp_at performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn parse_skb_ip_tcp_at(
    ctx: &TcContext,
    ip_offset: usize,
    packet_len: usize,
) -> Option<PacketBounds> {
    let ip: Ipv4Hdr = ctx.load(ip_offset).ok()?;
    if ip.version_ihl >> 4 != 4 || ip.protocol != IPPROTO_TCP {
        return None;
    }
    let ihl = usize::from(ip.version_ihl & 0x0f) * 4;
    if ihl < mem::size_of::<Ipv4Hdr>() {
        return None;
    }
    let ip_total = usize::from(u16::from_be(ip.tot_len));
    if ip_total < ihl {
        return None;
    }
    let ip_end = ip_offset.checked_add(ip_total)?;
    if ip_end > packet_len {
        return None;
    }
    let tcp_offset = ip_offset + ihl;
    let tcp: TcpHdr = ctx.load(tcp_offset).ok()?;
    let source = u16::from_be(tcp.source);
    let dest = u16::from_be(tcp.dest);
    if source != FIX_PORT && source != HTTP_WS_PORT {
        return None;
    }
    let data_offset = usize::from(u16::from_be(tcp.doff_res_flags) >> 12) * 4;
    if data_offset < mem::size_of::<TcpHdr>() {
        return None;
    }
    let payload_offset = tcp_offset.checked_add(data_offset)?;
    if payload_offset > ip_end {
        return None;
    }
    Some(PacketBounds {
        payload_offset,
        payload_len: ip_end - payload_offset,
        server_port: source,
        client_ip: u32::from_be(ip.daddr),
        client_port: dest,
        tcp_seq: u32::from_be(tcp.seq),
    })
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
/// load performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn load<T: Copy>(data: usize, data_end: usize, offset: usize) -> Option<T> {
    let len = mem::size_of::<T>();
    if data + offset + len > data_end {
        return None;
    }
    let ptr = (data + offset) as *const T;
    Some(unsafe { ptr::read_unaligned(ptr) })
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
/// bpf_ktime_get_ns performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
unsafe fn bpf_ktime_get_ns() -> u64 {
    let helper: extern "C" fn() -> u64 = mem::transmute(5usize);
    helper()
}

#[cfg(target_arch = "bpf")]
#[panic_handler]
/// panic performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn panic(_info: &PanicInfo) -> ! {
    loop {}
}

#[cfg(target_arch = "bpf")]
#[link_section = "license"]
#[no_mangle]
static LICENSE: [u8; 4] = *b"GPL\0";
