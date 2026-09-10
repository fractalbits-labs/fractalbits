use actix_web::{FromRequest, HttpRequest, dev::Payload, web::Query};
use futures::future::{Ready, ready};
use serde::Deserialize;
use std::fmt;

#[allow(dead_code)]
#[derive(Debug, Default, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct ApiSignature {
    #[serde(rename = "uploadId")]
    pub upload_id: Option<String>,
    #[serde(rename = "partNumber")]
    pub part_number: Option<u64>,
    pub list_type: Option<String>,
    // Note this is actually from header
    pub x_amz_copy_source: Option<String>,
}

impl fmt::Display for ApiSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();
        if let Some(upload_id) = &self.upload_id {
            parts.push(format!("uploadId={upload_id}"));
        }
        if let Some(part_number) = self.part_number {
            parts.push(format!("partNumber={part_number}"));
        }
        if let Some(list_type) = &self.list_type {
            parts.push(format!("list-type={list_type}"));
        }
        if let Some(x_amz_copy_source) = &self.x_amz_copy_source {
            parts.push(format!("x-amz-copy-source={x_amz_copy_source}"));
        }
        write!(f, "{}", parts.join("&"))
    }
}

#[derive(Debug, Clone)]
pub struct ApiSignatureExtractor(pub ApiSignature);

impl std::fmt::Display for ApiSignatureExtractor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ApiSignatureExtractor(upload_id: {:?}, part_number: {:?}, list_type: {:?}, x_amz_copy_source: {:?})",
            self.0.upload_id, self.0.part_number, self.0.list_type, self.0.x_amz_copy_source
        )
    }
}

impl FromRequest for ApiSignatureExtractor {
    type Error = actix_web::Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let mut api_signature = Query::<ApiSignature>::from_query(req.query_string())
            .unwrap_or_else(|_| Query(Default::default()))
            .into_inner();
        // Extract x-amz-copy-source from headers if present
        if let Some(copy_source) = req
            .headers()
            .get("x-amz-copy-source")
            .and_then(|v| v.to_str().ok())
        {
            api_signature.x_amz_copy_source = Some(copy_source.to_string());
        }

        ready(Ok(ApiSignatureExtractor(api_signature)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{App, HttpResponse, test, web};

    async fn handler(api_signature: ApiSignatureExtractor) -> HttpResponse {
        let upload_id = api_signature.0.upload_id.unwrap_or_default();
        HttpResponse::Ok().body(upload_id)
    }

    #[actix_web::test]
    async fn test_extract_api_signature_from_query_ok() {
        let api_signature_str = "uploadId=abc123";
        assert_eq!(send_request_get_body(api_signature_str).await, "abc123");
    }

    async fn send_request_get_body(api_signature: &str) -> String {
        let app = test::init_service(App::new().route("/{key:.*}", web::get().to(handler))).await;

        let uri = if api_signature.is_empty() {
            "/obj1".to_string()
        } else {
            format!("/obj1?{}", api_signature)
        };

        let req = test::TestRequest::get().uri(&uri).to_request();

        let resp = test::call_service(&app, req).await;
        let body = test::read_body(resp).await;
        String::from_utf8(body.to_vec()).unwrap()
    }

    #[actix_web::test]
    async fn test_extract_api_signature_from_header() {
        let app = test::init_service(App::new().route(
            "/{key:.*}",
            web::get().to(|sig: ApiSignatureExtractor| async move {
                let copy_source = sig.0.x_amz_copy_source.unwrap_or_default();
                HttpResponse::Ok().body(copy_source)
            }),
        ))
        .await;

        let req = test::TestRequest::get()
            .uri("/obj1")
            .insert_header(("x-amz-copy-source", "bucket/key"))
            .to_request();

        let resp = test::call_service(&app, req).await;
        let body = test::read_body(resp).await;
        let result = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(result, "bucket/key");
    }

    #[test]
    async fn test_display_api_signature() {
        let api_signature = ApiSignature {
            upload_id: Some("abc123".to_string()),
            part_number: Some(1),
            list_type: None,
            x_amz_copy_source: None,
        };
        assert_eq!(api_signature.to_string(), "uploadId=abc123&partNumber=1");

        let api_signature = ApiSignature {
            upload_id: None,
            part_number: None,
            list_type: Some("2".to_string()),
            x_amz_copy_source: None,
        };
        assert_eq!(api_signature.to_string(), "list-type=2");

        let api_signature = ApiSignature {
            upload_id: None,
            part_number: None,
            list_type: None,
            x_amz_copy_source: Some("bucket/key".to_string()),
        };
        assert_eq!(api_signature.to_string(), "x-amz-copy-source=bucket/key");

        let api_signature = ApiSignature {
            upload_id: Some("abc123".to_string()),
            part_number: Some(1),
            list_type: Some("2".to_string()),
            x_amz_copy_source: Some("bucket/key".to_string()),
        };
        assert_eq!(
            api_signature.to_string(),
            "uploadId=abc123&partNumber=1&list-type=2&x-amz-copy-source=bucket/key"
        );

        let api_signature = ApiSignature {
            upload_id: None,
            part_number: None,
            list_type: None,
            x_amz_copy_source: None,
        };
        assert_eq!(api_signature.to_string(), "");
    }
}
