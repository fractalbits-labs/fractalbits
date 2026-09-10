#[cfg(not(test))]
use crate::AppState;
use actix_web::{FromRequest, HttpRequest, dev::Payload};
use futures::future::{Ready, ready};
#[cfg(test)]
// Minimal fake AppState for testing that only contains what we need
struct AppState {
    pub config: std::sync::Arc<crate::Config>,
}

use crate::handler::common::s3_error::S3Error;

pub struct BucketAndKeyName {
    pub bucket: String,
    pub key: String,
}

impl BucketAndKeyName {
    fn buket_name_from_host(req: &HttpRequest) -> Option<String> {
        // Get the Host header from the request
        let host_header = req.headers().get("host")?;
        let host_str = host_header.to_str().ok()?;

        // Get the app state to access config
        let app_state = req.app_data::<std::sync::Arc<AppState>>()?;
        let root_domain = &app_state.config.root_domain;

        // Check if this is a virtual-hosted-style request
        // Format: bucket-name.root_domain (e.g., my-bucket.s3.amazonaws.com)
        if host_str.ends_with(root_domain) {
            // Calculate where the bucket name ends
            let bucket_end_pos = host_str.len() - root_domain.len();
            if bucket_end_pos > 0 {
                // Extract bucket name (everything before the root domain)
                let bucket_with_dot = &host_str[..bucket_end_pos];
                // Remove trailing dot if present
                let bucket = bucket_with_dot.trim_end_matches('.');
                if !bucket.is_empty() {
                    return Some(bucket.to_owned());
                }
            }
        }

        None
    }

    pub fn get_bucket_and_key_from_path(full_key: &str) -> (String, String) {
        let mut bucket = String::new();
        let mut key = String::from("/");
        let mut bucket_part = true;
        full_key.chars().skip_while(|c| c == &'/').for_each(|c| {
            if bucket_part && c == '/' {
                bucket_part = false;
                return;
            }
            if bucket_part {
                bucket.push(c);
            } else {
                key.push(c);
            }
        });
        (bucket, key)
    }
}

impl FromRequest for BucketAndKeyName {
    type Error = S3Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let full_key = req.uri().path().to_owned();

        let result = (|| {
            let (bucket, key) = match Self::buket_name_from_host(req) {
                // Virtual-hosted-style request
                Some(bucket) => (bucket, full_key),
                // Path-style request
                None => Self::get_bucket_and_key_from_path(&full_key),
            };

            // Get the original key from the URL encoded key
            let key = percent_encoding::percent_decode_str(&key)
                .decode_utf8()
                .map_err(|_| S3Error::InvalidURI)?
                .into_owned();

            check_key_name(&key)?;
            Ok(BucketAndKeyName { bucket, key })
        })();

        ready(result)
    }
}

// Allow key names as documented:
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
fn check_key_name(n: &str) -> Result<(), S3Error> {
    if n.len() > 1024 {
        return Err(S3Error::KeyTooLongError);
    }

    // Our internal implementation reserved these characters for ending
    if n.ends_with("#") || n.ends_with("\0") {
        return Err(S3Error::KeyUnsupported);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{App, HttpResponse, test, web};
    use std::sync::Arc;

    async fn handler(bucket_key: BucketAndKeyName) -> HttpResponse {
        HttpResponse::Ok().body(bucket_key.bucket)
    }

    fn create_fake_app_state(root_domain: &str) -> Arc<AppState> {
        let config = crate::Config {
            root_domain: root_domain.to_string(),
            ..Default::default()
        };
        Arc::new(AppState {
            config: Arc::new(config),
        })
    }

    #[actix_web::test]
    async fn test_extract_bucket_name_ok() {
        let bucket_name = "my-bucket";
        assert_eq!(send_request_get_body(bucket_name).await, bucket_name);
    }

    async fn send_request_get_body(bucket_name: &str) -> String {
        let fake_app_state = create_fake_app_state("localhost");

        let app = test::init_service(
            App::new()
                .app_data(fake_app_state)
                .route("/{key:.*}", web::get().to(handler)),
        )
        .await;

        let req = test::TestRequest::get()
            .uri(&format!("/{}/obj1?query1", bucket_name))
            .to_request();

        let resp = test::call_service(&app, req).await;
        let body = test::read_body(resp).await;
        String::from_utf8(body.to_vec()).unwrap()
    }

    #[actix_web::test]
    async fn test_extract_bucket_name_virtual_hosted() {
        let bucket_name = "my-bucket";
        let fake_app_state = create_fake_app_state("localhost");

        let app = test::init_service(
            App::new()
                .app_data(fake_app_state)
                .route("/{key:.*}", web::get().to(handler)),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/obj1")
            .insert_header(("host", format!("{}.localhost", bucket_name)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        let body = test::read_body(resp).await;
        let result = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(result, bucket_name);
    }

    #[test]
    async fn test_get_bucket_and_key_from_path() {
        let (bucket, key) =
            BucketAndKeyName::get_bucket_and_key_from_path("/my-bucket/path/to/object");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "/path/to/object");

        let (bucket, key) = BucketAndKeyName::get_bucket_and_key_from_path("/bucket-name/");
        assert_eq!(bucket, "bucket-name");
        assert_eq!(key, "/");

        let (bucket, key) = BucketAndKeyName::get_bucket_and_key_from_path("///bucket///object///");
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "///object///");
    }

    #[test]
    async fn test_check_key_name() {
        // Valid keys
        assert!(check_key_name("/valid/key").is_ok());
        assert!(check_key_name("/").is_ok());
        assert!(check_key_name("/key with spaces").is_ok());

        // Key too long
        let long_key = "/".to_string() + &"a".repeat(1024);
        assert!(check_key_name(&long_key).is_err());

        // Reserved endings
        assert!(check_key_name("/key#").is_err());
        assert!(check_key_name("/key\0").is_err());
    }
}
