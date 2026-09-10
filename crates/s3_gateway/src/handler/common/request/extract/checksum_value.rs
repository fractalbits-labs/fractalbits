use actix_web::{FromRequest, HttpRequest, dev::Payload, http::header::HeaderMap};
use base64::{Engine, prelude::BASE64_STANDARD};
use futures::future::{Ready, ready};

use crate::handler::common::{
    checksum::ChecksumValue, s3_error::S3Error, signature::SignatureError, xheader,
};

pub struct ChecksumValueFromHeaders(pub Option<ChecksumValue>);

impl FromRequest for ChecksumValueFromHeaders {
    type Error = S3Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let result = extract_checksum_value_from_headers(req.headers());
        match result {
            Ok(checksum) => ready(Ok(ChecksumValueFromHeaders(checksum))),
            Err(e) => ready(Err(e.into())),
        }
    }
}

fn extract_checksum_value_from_headers(
    headers: &HeaderMap,
) -> Result<Option<ChecksumValue>, SignatureError> {
    let mut found_checksum: Option<ChecksumValue> = None;

    // Check CRC32
    if headers.contains_key(xheader::X_AMZ_CHECKSUM_CRC32.as_str()) {
        if found_checksum.is_some() {
            return Err(SignatureError::Other(
                "multiple x-amz-checksum-* headers given".into(),
            ));
        }
        let crc32 = headers
            .get(xheader::X_AMZ_CHECKSUM_CRC32.as_str())
            .and_then(|x| BASE64_STANDARD.decode(x).ok())
            .and_then(|x| x.try_into().ok())
            .ok_or_else(|| SignatureError::Other("invalid x-amz-checksum-crc32 header".into()))?;
        found_checksum = Some(ChecksumValue::Crc32(crc32));
    }

    // Check CRC32C
    if headers.contains_key(xheader::X_AMZ_CHECKSUM_CRC32C.as_str()) {
        if found_checksum.is_some() {
            return Err(SignatureError::Other(
                "multiple x-amz-checksum-* headers given".into(),
            ));
        }
        let crc32c = headers
            .get(xheader::X_AMZ_CHECKSUM_CRC32C.as_str())
            .and_then(|x| BASE64_STANDARD.decode(x).ok())
            .and_then(|x| x.try_into().ok())
            .ok_or_else(|| SignatureError::Other("invalid x-amz-checksum-crc32c header".into()))?;
        found_checksum = Some(ChecksumValue::Crc32c(crc32c));
    }

    // Check CRC64NVME
    if headers.contains_key(xheader::X_AMZ_CHECKSUM_CRC64NVME.as_str()) {
        if found_checksum.is_some() {
            return Err(SignatureError::Other(
                "multiple x-amz-checksum-* headers given".into(),
            ));
        }
        let crc64nvme = headers
            .get(xheader::X_AMZ_CHECKSUM_CRC64NVME.as_str())
            .and_then(|x| BASE64_STANDARD.decode(x).ok())
            .and_then(|x| x.try_into().ok())
            .ok_or_else(|| {
                SignatureError::Other("invalid x-amz-checksum-crc64nvme header".into())
            })?;
        found_checksum = Some(ChecksumValue::Crc64Nvme(crc64nvme));
    }

    // Check SHA1
    if headers.contains_key(xheader::X_AMZ_CHECKSUM_SHA1.as_str()) {
        if found_checksum.is_some() {
            return Err(SignatureError::Other(
                "multiple x-amz-checksum-* headers given".into(),
            ));
        }
        let sha1 = headers
            .get(xheader::X_AMZ_CHECKSUM_SHA1.as_str())
            .and_then(|x| BASE64_STANDARD.decode(x).ok())
            .and_then(|x| x.try_into().ok())
            .ok_or_else(|| SignatureError::Other("invalid x-amz-checksum-sha1 header".into()))?;
        found_checksum = Some(ChecksumValue::Sha1(sha1));
    }

    // Check SHA256
    if headers.contains_key(xheader::X_AMZ_CHECKSUM_SHA256.as_str()) {
        if found_checksum.is_some() {
            return Err(SignatureError::Other(
                "multiple x-amz-checksum-* headers given".into(),
            ));
        }
        let sha256 = headers
            .get(xheader::X_AMZ_CHECKSUM_SHA256.as_str())
            .and_then(|x| BASE64_STANDARD.decode(x).ok())
            .and_then(|x| x.try_into().ok())
            .ok_or_else(|| SignatureError::Other("invalid x-amz-checksum-sha256 header".into()))?;
        found_checksum = Some(ChecksumValue::Sha256(sha256));
    }

    Ok(found_checksum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{App, HttpResponse, test, web};
    use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};

    async fn handler(checksum: ChecksumValueFromHeaders) -> HttpResponse {
        match checksum.0 {
            Some(ChecksumValue::Crc32(crc32)) => {
                HttpResponse::Ok().body(format!("crc32:{}", BASE64_STANDARD.encode(crc32)))
            }
            Some(ChecksumValue::Crc32c(crc32c)) => {
                HttpResponse::Ok().body(format!("crc32c:{}", BASE64_STANDARD.encode(crc32c)))
            }
            Some(ChecksumValue::Sha1(sha1)) => {
                HttpResponse::Ok().body(format!("sha1:{}", BASE64_STANDARD.encode(sha1)))
            }
            Some(ChecksumValue::Sha256(sha256)) => {
                HttpResponse::Ok().body(format!("sha256:{}", BASE64_STANDARD.encode(sha256)))
            }
            Some(ChecksumValue::Crc64Nvme(crc64nvme)) => {
                HttpResponse::Ok().body(format!("crc64nvme:{}", BASE64_STANDARD.encode(crc64nvme)))
            }
            None => HttpResponse::Ok().body("no_checksum"),
        }
    }

    #[actix_web::test]
    async fn test_extract_checksum_value_none() {
        let app = test::init_service(App::new().route("/{key:.*}", web::get().to(handler))).await;

        let req = test::TestRequest::get().uri("/obj1").to_request();

        let resp = test::call_service(&app, req).await;
        let body = test::read_body(resp).await;
        let result = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(result, "no_checksum");
    }

    #[actix_web::test]
    async fn test_extract_checksum_value_crc32() {
        let app = test::init_service(App::new().route("/{key:.*}", web::get().to(handler))).await;

        let crc32_bytes = [0x12, 0x34, 0x56, 0x78];
        let crc32_b64 = BASE64_STANDARD.encode(crc32_bytes);

        let req = test::TestRequest::get()
            .uri("/obj1")
            .insert_header(("x-amz-checksum-crc32", crc32_b64.as_str()))
            .to_request();

        let resp = test::call_service(&app, req).await;
        let body = test::read_body(resp).await;
        let result = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(result, format!("crc32:{}", crc32_b64));
    }

    #[actix_web::test]
    async fn test_extract_checksum_value_multiple_headers_error() {
        let app = test::init_service(App::new().route("/{key:.*}", web::get().to(handler))).await;

        let crc32_bytes = [0x12, 0x34, 0x56, 0x78];
        let crc32c_bytes = [0x87, 0x65, 0x43, 0x21];

        let req = test::TestRequest::get()
            .uri("/obj1")
            .insert_header(("x-amz-checksum-crc32", BASE64_STANDARD.encode(crc32_bytes)))
            .insert_header((
                "x-amz-checksum-crc32c",
                BASE64_STANDARD.encode(crc32c_bytes),
            ))
            .to_request();

        let resp = test::call_service(&app, req).await;
        // Should return an error response for multiple checksum headers
        assert!(!resp.status().is_success());
    }
}
