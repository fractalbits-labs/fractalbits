use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{ProvideCredentials, Region};

#[derive(Clone, Default)]
pub struct S3ClientConfig {
    pub endpoint_url: Option<String>,
    pub region: Option<String>,
}

pub async fn create_s3_client(config: S3ClientConfig) -> anyhow::Result<Client> {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest()).load().await;

    let mut s3_config = aws_sdk_s3::config::Builder::from(&sdk_config).force_path_style(true);

    if let Some(endpoint) = config.endpoint_url {
        s3_config = s3_config.endpoint_url(endpoint);
    }

    if let Some(region) = config.region {
        s3_config = s3_config.region(Region::new(region));
    }

    let client = Client::from_conf(s3_config.build());
    Ok(client)
}

pub struct CredentialsInfo {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub endpoint_url: String,
}

pub async fn get_credentials_info(config: &S3ClientConfig) -> anyhow::Result<CredentialsInfo> {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest()).load().await;

    let creds = sdk_config
        .credentials_provider()
        .ok_or_else(|| anyhow::anyhow!("No credentials provider found"))?
        .provide_credentials()
        .await?;

    let region = config
        .region
        .clone()
        .or_else(|| sdk_config.region().map(|r| r.to_string()))
        .unwrap_or_else(|| "us-east-1".to_string());

    let endpoint_url = config
        .endpoint_url
        .clone()
        .or_else(|| std::env::var("AWS_ENDPOINT_URL_S3").ok())
        .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", region));

    Ok(CredentialsInfo {
        access_key_id: creds.access_key_id().to_string(),
        secret_access_key: creds.secret_access_key().to_string(),
        region,
        endpoint_url,
    })
}
