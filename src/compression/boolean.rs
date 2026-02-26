/// Bitmap encoding for BOOLEAN columns.
///
/// Simply packs 8 booleans per byte (bit 0 of byte 0 = value[0], etc.).
/// This is simpler and typically as effective as RLE for boolean data.

pub fn encode(values: &[bool]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let byte_count = (values.len() + 7) / 8;
    let mut buf = vec![0u8; byte_count];

    for (i, &val) in values.iter().enumerate() {
        if val {
            buf[i / 8] |= 1 << (i % 8);
        }
    }

    buf
}

pub fn decode(data: &[u8], count: usize) -> Vec<bool> {
    let mut values = Vec::with_capacity(count);

    for i in 0..count {
        let bit = (data[i / 8] >> (i % 8)) & 1 == 1;
        values.push(bit);
    }

    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_basic() {
        let values = vec![true, false, true, true, false, false, true, false, true];
        let encoded = encode(&values);
        let decoded = decode(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_roundtrip_empty() {
        let encoded = encode(&[]);
        let decoded = decode(&encoded, 0);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_all_true() {
        let values = vec![true; 100];
        let encoded = encode(&values);
        let decoded = decode(&encoded, values.len());
        assert_eq!(decoded, values);
        assert_eq!(encoded.len(), 13); // ceil(100/8)
    }

    #[test]
    fn test_all_false() {
        let values = vec![false; 100];
        let encoded = encode(&values);
        let decoded = decode(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_compression_ratio() {
        // 10000 booleans should compress to ~1250 bytes (vs 10000 bytes raw)
        let values: Vec<bool> = (0..10000).map(|i| i % 3 == 0).collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded, values.len());
        assert_eq!(decoded, values);
        assert_eq!(encoded.len(), 1250);
    }

    #[test]
    fn test_single() {
        for v in [true, false] {
            let encoded = encode(&[v]);
            let decoded = decode(&encoded, 1);
            assert_eq!(decoded, vec![v]);
        }
    }
}
