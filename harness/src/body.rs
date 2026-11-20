pub fn encode(msg_id: u64, size: usize) -> String {
    let head = format!("dwb:{msg_id}:");
    let pad = size.saturating_sub(head.len());
    format!("{head}{}", ".".repeat(pad))
}

pub fn decode(s: &str) -> Option<u64> {
    let rest = s.strip_prefix("dwb:")?;
    let (id, _) = rest.split_once(':')?;
    id.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        assert_eq!(decode(&encode(42, 256)), Some(42));
        assert_eq!(encode(42, 256).len(), 256);
    }

    #[test]
    fn decode_rejects_foreign() {
        assert_eq!(decode("hello"), None);
        assert_eq!(decode("dwb:x:"), None);
    }
}
