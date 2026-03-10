use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::presigning::PresigningConfig;

use crate::s3_path::S3Path;

pub async fn execute(client: &Client, s3_uri: &str, expires_in: u64) -> anyhow::Result<()> {
    let path = S3Path::parse(s3_uri)?;

    let (bucket, key) = match &path {
        S3Path::S3 { bucket, key } => {
            if key.is_empty() {
                anyhow::bail!("Key is required for presign command");
            }
            (bucket.as_str(), key.as_str())
        }
        S3Path::Local(_) => {
            anyhow::bail!("presign requires an S3 URI (s3://bucket/key)");
        }
    };

    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(expires_in))
        .build()?;

    let presigned_request = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .presigned(presigning_config)
        .await?;

    println!("{}", presigned_request.uri());

    Ok(())
}
