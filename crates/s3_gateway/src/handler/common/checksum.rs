use std::hash::Hasher;

use crate::handler::common::{s3_error::S3Error, signature::SignatureError, xheader};
use actix_web::http::header::HeaderMap;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use crc32c::Crc32cHasher as Crc32c;
use crc32fast::Hasher as Crc32;
use crc64fast_nvme::Digest as Crc64Nvme;
use sha1::{Digest, Sha1};
use sha2::Sha256;

pub use data_types::object_layout::{ChecksumAlgorithm, ChecksumValue};

pub enum Checksummer {
    Crc32(Crc32),
    Crc32c(Crc32c),
    Crc64Nvme(Crc64Nvme),
    Sha1(Sha1),
    Sha256(Sha256),
}

impl Checksummer {
    pub fn new(algo: ChecksumAlgorithm) -> Self {
        match algo {
            ChecksumAlgorithm::Crc32 => Self::Crc32(Crc32::new()),
            ChecksumAlgorithm::Crc32c => Self::Crc32c(Crc32c::default()),
            ChecksumAlgorithm::Crc64Nvme => Self::Crc64Nvme(Crc64Nvme::new()),
            ChecksumAlgorithm::Sha1 => Self::Sha1(Sha1::new()),
            ChecksumAlgorithm::Sha256 => Self::Sha256(Sha256::new()),
        }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Crc32(hasher) => hasher.update(bytes),
            Self::Crc32c(hasher) => hasher.write(bytes),
            Self::Crc64Nvme(hasher) => hasher.write(bytes),
            Self::Sha1(hasher) => hasher.update(bytes),
            Self::Sha256(hasher) => hasher.update(bytes),
        }
    }

    pub fn finalize(self) -> ChecksumValue {
        match self {
            Self::Crc32(hasher) => ChecksumValue::Crc32(u32::to_be_bytes(hasher.finalize())),
            Self::Crc32c(hasher) => {
                // CRC32C should always fit in u32, but handle gracefully
                let hash_value = hasher.finish();
                let truncated = hash_value as u32;
                ChecksumValue::Crc32c(u32::to_be_bytes(truncated))
            }
            Self::Crc64Nvme(hasher) => ChecksumValue::Crc64Nvme(u64::to_be_bytes(hasher.sum64())),
            Self::Sha1(hasher) => {
                let digest = hasher.finalize();
                // SHA1 digest is always 20 bytes
                let mut result = [0u8; 20];
                result.copy_from_slice(&digest[..20]);
                ChecksumValue::Sha1(result)
            }
            Self::Sha256(hasher) => {
                let digest = hasher.finalize();
                // SHA256 digest is always 32 bytes
                let mut result = [0u8; 32];
                result.copy_from_slice(&digest[..32]);
                ChecksumValue::Sha256(result)
            }
        }
    }
}

pub fn verify_checksum(
    calculated: Option<ChecksumValue>,
    expected: Option<ChecksumValue>,
) -> Result<(), SignatureError> {
    if let Some(expected_checksum) = expected {
        match calculated {
            Some(calc) if calc == expected_checksum => Ok(()),
            _ => Err(SignatureError::InvalidDigest(format!(
                "Failed to validate checksum for algorithm {:?}",
                expected_checksum.algorithm()
            ))),
        }
    } else {
        Ok(())
    }
}

pub fn request_trailer_checksum_algorithm(
    headers: &HeaderMap,
) -> Result<Option<ChecksumAlgorithm>, SignatureError> {
    match headers
        .get(xheader::X_AMZ_TRAILER.as_str())
        .map(|x| x.to_str())
        .transpose()?
    {
        None => Ok(None),
        Some(x) if x == xheader::X_AMZ_CHECKSUM_CRC32 => Ok(Some(ChecksumAlgorithm::Crc32)),
        Some(x) if x == xheader::X_AMZ_CHECKSUM_CRC32C => Ok(Some(ChecksumAlgorithm::Crc32c)),
        Some(x) if x == xheader::X_AMZ_CHECKSUM_CRC64NVME => Ok(Some(ChecksumAlgorithm::Crc64Nvme)),
        Some(x) if x == xheader::X_AMZ_CHECKSUM_SHA1 => Ok(Some(ChecksumAlgorithm::Sha1)),
        Some(x) if x == xheader::X_AMZ_CHECKSUM_SHA256 => Ok(Some(ChecksumAlgorithm::Sha256)),
        _ => Err(SignatureError::Other("invalid checksum algorithm".into())),
    }
}

pub fn add_checksum_response_headers(
    checksum: &Option<ChecksumValue>,
    resp: &mut actix_web::HttpResponseBuilder,
) -> Result<(), S3Error> {
    match checksum {
        Some(ChecksumValue::Crc32(crc32)) => {
            resp.insert_header((
                xheader::X_AMZ_CHECKSUM_CRC32.as_str(),
                BASE64_STANDARD.encode(crc32),
            ));
        }
        Some(ChecksumValue::Crc32c(crc32c)) => {
            resp.insert_header((
                xheader::X_AMZ_CHECKSUM_CRC32C.as_str(),
                BASE64_STANDARD.encode(crc32c),
            ));
        }
        Some(ChecksumValue::Sha1(sha1)) => {
            resp.insert_header((
                xheader::X_AMZ_CHECKSUM_SHA1.as_str(),
                BASE64_STANDARD.encode(sha1),
            ));
        }
        Some(ChecksumValue::Sha256(sha256)) => {
            resp.insert_header((
                xheader::X_AMZ_CHECKSUM_SHA256.as_str(),
                BASE64_STANDARD.encode(sha256),
            ));
        }
        Some(ChecksumValue::Crc64Nvme(crc64nvme)) => {
            resp.insert_header((
                xheader::X_AMZ_CHECKSUM_CRC64NVME.as_str(),
                BASE64_STANDARD.encode(crc64nvme),
            ));
        }
        None => (),
    }
    Ok(())
}
