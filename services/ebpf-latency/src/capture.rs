//! This module implements capture behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.
pub const CAPTURE_HEADER_LEN: usize = 28;
/// Userspace mirror of the kernel-side CAPTURE_CAP in ebpf.rs — the two move
/// in lockstep (pinned by main.rs::default_clamp_fits_the_capture_cap).
/// decode() rejects records claiming more than this, so a stale mirror would
/// discard every complete jumbo capture as corrupt.
pub const CAPTURE_CAP: usize = 9029;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Direction enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum Direction {
    Request,
    Response,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Transport enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum Transport {
    Fix,
    HttpWs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// FlowKey stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct FlowKey {
    pub client_ip: u32,
    pub client_port: u16,
}

#[derive(Debug, Clone)]
/// Capture stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct Capture<'a> {
    pub timestamp_ns: u64,
    pub flow: FlowKey,
    #[allow(dead_code)]
    pub server_port: u16,
    pub tcp_seq: u32,
    #[allow(dead_code)]
    pub payload_len: u32,
    pub direction: Direction,
    pub transport: Transport,
    pub payload: &'a [u8],
}

#[derive(Debug, PartialEq, Eq)]
/// CaptureError enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum CaptureError {
    TooShort { got: usize },
    BadCapturedLen { captured: usize, available: usize },
    UnknownDirection(u8),
}

impl core::fmt::Display for CaptureError {
    /// fmt performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            CaptureError::TooShort { got } => {
                write!(f, "capture record shorter than header: {got} bytes")
            }
            CaptureError::BadCapturedLen {
                captured,
                available,
            } => write!(
                f,
                "captured_len {captured} exceeds available payload {available}"
            ),
            CaptureError::UnknownDirection(d) => write!(f, "unknown direction byte {d}"),
        }
    }
}

impl std::error::Error for CaptureError {}

/// read_u16 performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn read_u16(b: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([b[off], b[off + 1]])
}

/// read_u32 performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn read_u32(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}

/// read_u64 performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn read_u64(b: &[u8], off: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[off..off + 8]);
    u64::from_le_bytes(a)
}

/// decode performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn decode(bytes: &[u8]) -> Result<Capture<'_>, CaptureError> {
    if bytes.len() < CAPTURE_HEADER_LEN {
        return Err(CaptureError::TooShort { got: bytes.len() });
    }
    let timestamp_ns = read_u64(bytes, 0);
    let client_ip = read_u32(bytes, 8);
    let tcp_seq = read_u32(bytes, 12);
    let payload_len = read_u32(bytes, 16);
    let client_port = read_u16(bytes, 20);
    let server_port = read_u16(bytes, 22);
    let captured_len = read_u16(bytes, 24) as usize;
    let direction = match bytes[26] {
        0 => Direction::Request,
        1 => Direction::Response,
        other => return Err(CaptureError::UnknownDirection(other)),
    };

    let available = bytes.len() - CAPTURE_HEADER_LEN;
    if captured_len > available || captured_len > CAPTURE_CAP {
        return Err(CaptureError::BadCapturedLen {
            captured: captured_len,
            available,
        });
    }
    let payload = &bytes[CAPTURE_HEADER_LEN..CAPTURE_HEADER_LEN + captured_len];

    let transport = if server_port == 9898 {
        Transport::Fix
    } else {
        Transport::HttpWs
    };

    Ok(Capture {
        timestamp_ns,
        flow: FlowKey {
            client_ip,
            client_port,
        },
        server_port,
        tcp_seq,
        payload_len,
        direction,
        transport,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// encode performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn encode(
        ts: u64,
        client_ip: u32,
        tcp_seq: u32,
        payload_len: u32,
        client_port: u16,
        server_port: u16,
        direction: u8,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&ts.to_le_bytes());
        v.extend_from_slice(&client_ip.to_le_bytes());
        v.extend_from_slice(&tcp_seq.to_le_bytes());
        v.extend_from_slice(&payload_len.to_le_bytes());
        v.extend_from_slice(&client_port.to_le_bytes());
        v.extend_from_slice(&server_port.to_le_bytes());
        v.extend_from_slice(&(payload.len() as u16).to_le_bytes());
        v.push(direction);
        v.push(0);
        v.extend_from_slice(payload);
        v
    }

    #[test]
    /// decodes_a_fix_request_record performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn decodes_a_fix_request_record() {
        let rec = encode(42, 0x0a00_0001, 1000, 7, 51234, 9898, 0, b"8=FIX.4");
        let cap = decode(&rec).unwrap();
        assert_eq!(cap.timestamp_ns, 42);
        assert_eq!(cap.flow.client_ip, 0x0a00_0001);
        assert_eq!(cap.flow.client_port, 51234);
        assert_eq!(cap.server_port, 9898);
        assert_eq!(cap.tcp_seq, 1000);
        assert_eq!(cap.payload_len, 7);
        assert_eq!(cap.direction, Direction::Request);
        assert_eq!(cap.transport, Transport::Fix);
        assert_eq!(cap.payload, b"8=FIX.4");
    }

    #[test]
    /// decodes_an_http_response_record performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn decodes_an_http_response_record() {
        let rec = encode(99, 0x0a00_0002, 5, 4, 40000, 8080, 1, b"HTTP");
        let cap = decode(&rec).unwrap();
        assert_eq!(cap.direction, Direction::Response);
        assert_eq!(cap.transport, Transport::HttpWs);
        assert_eq!(cap.payload, b"HTTP");
    }

    #[test]
    /// rejects_short_record performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn rejects_short_record() {
        assert!(matches!(
            decode(&[0u8; 10]),
            Err(CaptureError::TooShort { got: 10 })
        ));
    }

    #[test]
    /// rejects_overlong_captured_len performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn rejects_overlong_captured_len() {
        let mut rec = encode(1, 2, 3, 4, 5, 9898, 0, b"abc");
        rec[24] = 200;
        rec[25] = 0;
        assert!(matches!(
            decode(&rec),
            Err(CaptureError::BadCapturedLen { .. })
        ));
    }
}
