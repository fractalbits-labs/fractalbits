mod api_command;
mod api_signature;
mod authentication;
mod bucket_name_and_key;
mod checksum_value;

pub use api_command::{ApiCommand, ApiCommandFromQuery};
pub use api_signature::{ApiSignature, ApiSignatureExtractor};
pub use authentication::{
    AuthError, AuthSource, Authentication, CanonicalRequestHasher, Scope, extract_authentication,
};
pub use bucket_name_and_key::BucketAndKeyName;
pub use checksum_value::ChecksumValueFromHeaders;

use crate::handler::common::checksum::{ChecksumAlgorithm, ChecksumValue};
use base64::{Engine, prelude::BASE64_STANDARD};
use bytes::Bytes;
use futures::StreamExt;
use ntex::http::Payload;
use ntex::web::{FromRequest, HttpRequest};
use std::collections::HashMap;

/// Custom extractor that can handle chunked streams with trailers
pub struct ChunkedBodyWithTrailers {
    pub body: Bytes,
    pub trailers: HashMap<String, String>,
}

impl<Err: ntex::web::ErrorRenderer> FromRequest<Err> for ChunkedBodyWithTrailers {
    type Error = ntex::web::Error;

    async fn from_request(_req: &HttpRequest, _payload: &mut Payload) -> Result<Self, Self::Error> {
        // For now, return empty since we're using a different approach in the handler
        // This extractor is mainly for structure but we parse manually in the handler
        Err(ntex::web::error::ErrorNotImplemented(
            "ChunkedBodyWithTrailers should be created manually in handler",
        )
        .into())
    }
}

/// Parse chunked encoding manually to extract trailers
pub async fn parse_chunked_body_with_trailers(
    mut payload: ntex::web::types::Payload,
) -> Result<ChunkedBodyWithTrailers, ntex::web::Error> {
    let mut trailers = HashMap::new();
    let mut buffer = Vec::new();

    // Collect all payload data first
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        buffer.extend_from_slice(&chunk);
    }

    // Now parse the chunked encoding
    let body_data = if let Some((parsed_body, parsed_trailers)) = parse_chunked_data(&buffer) {
        trailers = parsed_trailers;
        parsed_body
    } else {
        // If not chunked, use the raw data
        buffer
    };

    Ok(ChunkedBodyWithTrailers {
        body: Bytes::from(body_data),
        trailers,
    })
}

/// Parse HTTP chunked transfer encoding
/// Returns (body_data, trailers) if successfully parsed, None otherwise
pub fn parse_chunked_data(data: &[u8]) -> Option<(Vec<u8>, HashMap<String, String>)> {
    let mut body = Vec::new();
    let mut trailers = HashMap::new();
    let mut pos = 0;

    while pos < data.len() {
        // Find the end of the chunk size line (ending with \r\n)
        let size_line_end = find_crlf(&data[pos..])?;
        let chunk_size_str = std::str::from_utf8(&data[pos..pos + size_line_end]).ok()?;

        // Parse chunk size (hex)
        let chunk_size = usize::from_str_radix(chunk_size_str.trim(), 16).ok()?;
        pos += size_line_end + 2; // Skip \r\n

        if chunk_size == 0 {
            // This is the final chunk, now parse trailers
            // Don't skip additional \r\n here - the trailers follow immediately

            // Parse trailer headers until we hit \r\n\r\n
            while pos < data.len() {
                if pos + 1 < data.len() && &data[pos..pos + 2] == b"\r\n" {
                    // End of trailers
                    break;
                }

                let header_line_end = find_crlf(&data[pos..])?;
                let header_line = std::str::from_utf8(&data[pos..pos + header_line_end]).ok()?;

                if let Some(colon_pos) = header_line.find(':') {
                    let name = header_line[..colon_pos].trim().to_lowercase();
                    let value = header_line[colon_pos + 1..].trim().to_string();
                    trailers.insert(name, value);
                }

                pos += header_line_end + 2; // Skip \r\n
            }
            break;
        } else {
            // Read chunk data
            if pos + chunk_size + 2 > data.len() {
                return None; // Invalid chunk
            }

            body.extend_from_slice(&data[pos..pos + chunk_size]);
            pos += chunk_size + 2; // Skip chunk data and trailing \r\n
        }
    }

    Some((body, trailers))
}

/// Find position of \r\n in data
fn find_crlf(data: &[u8]) -> Option<usize> {
    (0..data.len().saturating_sub(1)).find(|&i| data[i] == b'\r' && data[i + 1] == b'\n')
}

/// Extract checksum from trailers based on algorithm
pub fn extract_checksum_from_trailers(
    trailers: &HashMap<String, String>,
    algorithm: ChecksumAlgorithm,
) -> Option<ChecksumValue> {
    let header_name = match algorithm {
        ChecksumAlgorithm::Crc32 => "x-amz-checksum-crc32",
        ChecksumAlgorithm::Crc32c => "x-amz-checksum-crc32c",
        ChecksumAlgorithm::Crc64Nvme => "x-amz-checksum-crc64nvme",
        ChecksumAlgorithm::Sha1 => "x-amz-checksum-sha1",
        ChecksumAlgorithm::Sha256 => "x-amz-checksum-sha256",
    };

    let checksum_b64 = trailers.get(header_name)?;
    let checksum_bytes = BASE64_STANDARD.decode(checksum_b64).ok()?;

    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            if checksum_bytes.len() == 4 {
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&checksum_bytes);
                Some(ChecksumValue::Crc32(bytes))
            } else {
                None
            }
        }
        ChecksumAlgorithm::Crc32c => {
            if checksum_bytes.len() == 4 {
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&checksum_bytes);
                Some(ChecksumValue::Crc32c(bytes))
            } else {
                None
            }
        }
        ChecksumAlgorithm::Crc64Nvme => {
            if checksum_bytes.len() == 8 {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&checksum_bytes);
                Some(ChecksumValue::Crc64Nvme(bytes))
            } else {
                None
            }
        }
        ChecksumAlgorithm::Sha1 => {
            if checksum_bytes.len() == 20 {
                let mut bytes = [0u8; 20];
                bytes.copy_from_slice(&checksum_bytes);
                Some(ChecksumValue::Sha1(bytes))
            } else {
                None
            }
        }
        ChecksumAlgorithm::Sha256 => {
            if checksum_bytes.len() == 32 {
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(&checksum_bytes);
                Some(ChecksumValue::Sha256(bytes))
            } else {
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_chunked_data() {
        let chunked_data = b"a\r\n0123456789\r\n0\r\nx-amz-checksum-crc32: AAAAAA==\r\n\r\n";

        let (body, trailers) = parse_chunked_data(chunked_data).unwrap();

        assert_eq!(body, b"0123456789");
        assert_eq!(
            trailers.get("x-amz-checksum-crc32"),
            Some(&"AAAAAA==".to_string())
        );
    }

    #[test]
    fn test_parse_hello_world_chunked() {
        // Test with the actual "Hello world" data from the tests
        let chunked_data = b"B\r\nHello world\r\n0\r\nx-amz-checksum-crc32:i9aeUg==\r\n\r\n";

        let (body, trailers) = parse_chunked_data(chunked_data).unwrap();

        assert_eq!(body, b"Hello world");
        assert_eq!(
            trailers.get("x-amz-checksum-crc32"),
            Some(&"i9aeUg==".to_string())
        );
    }
}
