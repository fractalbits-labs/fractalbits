use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Opaque 16-byte routing key used to map buckets to their serving NSS.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RoutingKey([u8; 16]);

impl RoutingKey {
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Parse from a UUID string (e.g. "550e8400-e29b-41d4-a716-446655440000")
    pub fn from_uuid_str(s: &str) -> Result<Self, String> {
        let hex: String = s.chars().filter(|c| *c != '-').collect();
        if hex.len() != 32 {
            return Err(format!("invalid UUID length: {}", s));
        }
        let mut bytes = [0u8; 16];
        for i in 0..16 {
            bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
                .map_err(|e| format!("invalid hex in UUID: {e}"))?;
        }
        Ok(Self(bytes))
    }

    /// Format as a UUID string
    pub fn to_uuid_string(&self) -> String {
        format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            self.0[0],
            self.0[1],
            self.0[2],
            self.0[3],
            self.0[4],
            self.0[5],
            self.0[6],
            self.0[7],
            self.0[8],
            self.0[9],
            self.0[10],
            self.0[11],
            self.0[12],
            self.0[13],
            self.0[14],
            self.0[15],
        )
    }
}

impl fmt::Debug for RoutingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RoutingKey({})", self.to_uuid_string())
    }
}

impl fmt::Display for RoutingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_uuid_string())
    }
}

impl Serialize for RoutingKey {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_uuid_string())
    }
}

impl<'de> Deserialize<'de> for RoutingKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_uuid_str(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_uuid_str() {
        let key = RoutingKey::from_uuid_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(key.to_uuid_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_roundtrip_serde() {
        let key = RoutingKey::from_uuid_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, r#""550e8400-e29b-41d4-a716-446655440000""#);
        let parsed: RoutingKey = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, key);
    }

    #[test]
    fn test_invalid_uuid() {
        assert!(RoutingKey::from_uuid_str("not-a-uuid").is_err());
    }
}
