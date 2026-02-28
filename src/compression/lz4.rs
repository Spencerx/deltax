/// LZ4 compression for high-cardinality TEXT columns.
///
/// Format:
///   For each string:
///     4 bytes — string length (u32 LE)
///     N bytes — string UTF-8 data
///   Then the entire buffer is LZ4-compressed.
pub fn encode(values: &[&str]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    // Pack all strings with length prefixes
    let total_raw: usize = values.iter().map(|s| 4 + s.len()).sum();
    let mut raw = Vec::with_capacity(total_raw);
    for &s in values {
        let bytes = s.as_bytes();
        raw.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        raw.extend_from_slice(bytes);
    }

    // LZ4 compress
    lz4_flex::compress_prepend_size(&raw)
}

pub fn decode(data: &[u8], count: usize) -> Vec<String> {
    if count == 0 {
        return Vec::new();
    }

    let raw = lz4_flex::decompress_size_prepended(data).expect("LZ4 decompression failed");

    let mut values = Vec::with_capacity(count);
    let mut offset = 0;
    for _ in 0..count {
        let str_len = u32::from_le_bytes(raw[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let s = std::str::from_utf8(&raw[offset..offset + str_len])
            .expect("invalid UTF-8 in LZ4 data")
            .to_string();
        offset += str_len;
        values.push(s);
    }

    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_basic() {
        let values = vec!["hello world", "foo bar", "test string"];
        let encoded = encode(&values);
        let decoded = decode(&encoded, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_roundtrip_empty() {
        let encoded = encode(&[]);
        let decoded = decode(&encoded, 0);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_high_cardinality() {
        let strings: Vec<String> = (0..1000)
            .map(|i| format!("unique-string-number-{}-with-some-padding", i))
            .collect();
        let values: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();

        let raw_size: usize = values.iter().map(|s| s.len()).sum();
        let encoded = encode(&values);
        let decoded = decode(&encoded, values.len());

        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);

        // LZ4 should still give some compression due to shared prefixes
        assert!(
            encoded.len() < raw_size,
            "LZ4 should compress, got {} >= {}",
            encoded.len(),
            raw_size
        );
    }

    #[test]
    fn test_empty_strings() {
        let values = vec!["", "", "hello", ""];
        let encoded = encode(&values);
        let decoded = decode(&encoded, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }
}
