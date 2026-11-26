//! Frame encoding and zero-copy decoding.

/// Largest payload a single log record may carry (64 MiB).
///
/// The bound stops a damaged length field from being trusted.
pub const MAX_PAYLOAD: usize = 64 * 1024 * 1024;

/// Largest snapshot payload: whatever the 32-bit length field can describe.
pub(crate) const MAX_SNAPSHOT: usize = u32::MAX as usize;

pub(crate) const HEADER: usize = 8;

/// Result of decoding the bytes at the start of a buffer.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Decoded<'a> {
    /// A valid frame; `size` is the number of bytes it occupies.
    Frame { payload: &'a [u8], size: usize },
    /// The bytes look like an interrupted final write.
    Torn,
    /// The bytes are damaged and are not the tail of the log.
    Corrupt(&'static str),
}

fn checksum(len: [u8; 4], payload: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&len);
    hasher.update(payload);
    hasher.finalize()
}

/// Appends one frame to `out`. The caller has already checked the size limit.
pub(crate) fn encode(payload: &[u8], out: &mut Vec<u8>) {
    debug_assert!(payload.len() <= MAX_SNAPSHOT);
    #[allow(clippy::cast_possible_truncation)] // bounded by the caller
    let len = (payload.len() as u32).to_le_bytes();
    out.extend_from_slice(&len);
    out.extend_from_slice(&checksum(len, payload).to_le_bytes());
    out.extend_from_slice(payload);
}

/// Decodes the frame at the start of `buf`, which must extend to the end of the file.
/// Lengths above `limit` are rejected.
pub(crate) fn decode(buf: &[u8], limit: usize) -> Decoded<'_> {
    let Some((len, rest)) = buf.split_first_chunk::<4>() else {
        return Decoded::Torn;
    };
    let Some((crc, body)) = rest.split_first_chunk::<4>() else {
        return Decoded::Torn;
    };
    // Some filesystems extend a file with zeros before the data reaches disk.
    if buf.iter().all(|&byte| byte == 0) {
        return Decoded::Torn;
    }
    let declared = u32::from_le_bytes(*len) as usize;
    if declared > limit {
        return Decoded::Corrupt("frame length exceeds limit");
    }
    let Some(payload) = body.get(..declared) else {
        return Decoded::Torn;
    };
    if checksum(*len, payload) != u32::from_le_bytes(*crc) {
        let tail = &body[declared..];
        return if tail.iter().all(|&byte| byte == 0) {
            Decoded::Torn
        } else {
            Decoded::Corrupt("checksum mismatch")
        };
    }
    Decoded::Frame {
        payload,
        size: HEADER + declared,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(buf: &[u8]) -> Decoded<'_> {
        super::decode(buf, MAX_PAYLOAD)
    }

    fn framed(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        encode(payload, &mut out);
        out
    }

    #[test]
    fn round_trips() {
        let buf = framed(b"hello");
        assert_eq!(
            decode(&buf),
            Decoded::Frame {
                payload: b"hello",
                size: buf.len()
            }
        );
    }

    #[test]
    fn every_strict_prefix_is_torn() {
        let buf = framed(b"hello");
        for end in 0..buf.len() {
            assert_eq!(decode(&buf[..end]), Decoded::Torn, "prefix of {end} bytes");
        }
    }

    #[test]
    fn damaged_final_frame_is_torn_but_damaged_middle_frame_is_corrupt() {
        let mut buf = framed(b"first");
        *buf.last_mut().unwrap() ^= 1;
        assert_eq!(decode(&buf), Decoded::Torn);

        buf.extend(framed(b"second"));
        assert_eq!(decode(&buf), Decoded::Corrupt("checksum mismatch"));
    }

    #[test]
    fn zero_tail_is_torn() {
        assert_eq!(decode(&[0; 64]), Decoded::Torn);
    }

    #[test]
    fn absurd_length_is_corrupt() {
        let mut buf = framed(b"x");
        buf[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(decode(&buf), Decoded::Corrupt(_)));
    }

    #[test]
    fn empty_payload_is_a_valid_frame() {
        let buf = framed(b"");
        assert!(matches!(decode(&buf), Decoded::Frame { size: HEADER, .. }));
    }
}
