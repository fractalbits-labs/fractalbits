use actix_web::{FromRequest, HttpRequest, dev::Payload};
use futures::future::{Ready, ready};
use std::str::FromStr;
use strum::EnumString;

#[derive(Debug, EnumString, Copy, PartialEq, Clone, strum::Display)]
#[strum(serialize_all = "camelCase")]
pub enum ApiCommand {
    Accelerate,
    Acl,
    Attributes,
    Analytics,
    Cors,
    Delete,
    Encryption,
    IntelligentTiering,
    Inventory,
    LegalHold,
    Lifecycle,
    Location,
    Logging,
    Metrics,
    Notification,
    ObjectLock,
    OwnershipControls,
    Policy,
    PolicyStatus,
    PublicAccessBlock,
    Replication,
    RequestPayment,
    RenameFolder,
    RenameObject,
    Restore,
    Retention,
    Select,
    Session,
    Tagging,
    Torrent,
    Uploads,
    Versioning,
    Versions,
    Website,
}

#[derive(Debug, Clone)]
pub struct ApiCommandFromQuery(pub Option<ApiCommand>);

impl FromRequest for ApiCommandFromQuery {
    type Error = actix_web::Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        // Parse query string to extract API commands
        let query_string = req.query_string();

        // Split query string into key-value pairs
        let mut api_commands = Vec::new();
        for pair in query_string.split('&') {
            // API commands are query parameters with no value (e.g., "?uploads" or "?acl")
            if !pair.contains('=') && !pair.is_empty() {
                if let Ok(cmd) = ApiCommand::from_str(pair) {
                    api_commands.push(cmd);
                }
            } else if let Some((key, value)) = pair.split_once('=') {
                // Also check for commands as keys with empty values (e.g., "?uploads=")
                if value.is_empty()
                    && let Ok(cmd) = ApiCommand::from_str(key)
                {
                    api_commands.push(cmd);
                }
            }
        }

        let api_command = if api_commands.is_empty() {
            None
        } else {
            if api_commands.len() > 1 {
                tracing::warn!(
                    "Multiple api commands found: {:?}, picking the first one",
                    api_commands
                );
            }
            Some(api_commands[0])
        };

        ready(Ok(ApiCommandFromQuery(api_command)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{App, HttpResponse, test, web};

    async fn handler(ApiCommandFromQuery(api_command): ApiCommandFromQuery) -> HttpResponse {
        let result = match api_command {
            Some(api_command) => api_command.to_string(),
            None => "".to_string(),
        };
        HttpResponse::Ok().body(result)
    }

    #[actix_web::test]
    async fn test_extract_api_command_ok() {
        let api_cmd = "acl";
        assert_eq!(send_request_get_body(api_cmd).await, api_cmd);
    }

    #[actix_web::test]
    async fn test_extract_api_command_null() {
        let api_cmd = "";
        assert_eq!(send_request_get_body(api_cmd).await, api_cmd);
    }

    #[actix_web::test]
    async fn test_extract_api_command_uploads() {
        let api_cmd = "uploads";
        assert_eq!(send_request_get_body(api_cmd).await, api_cmd);
    }

    #[actix_web::test]
    async fn test_extract_api_command_with_empty_value() {
        let api_cmd = "uploads=";
        assert_eq!(send_request_get_body(api_cmd).await, "uploads");
    }

    async fn send_request_get_body(api_cmd: &str) -> String {
        let app = test::init_service(App::new().route("/{key:.*}", web::get().to(handler))).await;

        let uri = if api_cmd.is_empty() {
            "/obj1".to_string()
        } else {
            format!("/obj1?{}", api_cmd)
        };

        let req = test::TestRequest::get().uri(&uri).to_request();

        let resp = test::call_service(&app, req).await;
        let body = test::read_body(resp).await;
        String::from_utf8(body.to_vec()).unwrap()
    }

    #[test]
    async fn test_api_command_from_str() {
        assert_eq!(ApiCommand::from_str("acl").unwrap(), ApiCommand::Acl);
        assert_eq!(
            ApiCommand::from_str("uploads").unwrap(),
            ApiCommand::Uploads
        );
        assert_eq!(ApiCommand::from_str("cors").unwrap(), ApiCommand::Cors);

        // Test that invalid commands fail
        assert!(ApiCommand::from_str("invalid").is_err());
        assert!(ApiCommand::from_str("").is_err());
    }
}
