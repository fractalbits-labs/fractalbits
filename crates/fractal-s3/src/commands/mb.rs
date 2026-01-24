use aws_sdk_s3::Client;

use crate::s3_path::S3Path;

pub async fn execute(client: &Client, s3_uri: &str) -> anyhow::Result<()> {
    let path = S3Path::parse(s3_uri)?;

    match path {
        S3Path::S3 { bucket, key } => {
            if !key.is_empty() {
                anyhow::bail!("mb command only accepts bucket names (s3://BUCKET), not keys");
            }
            create_bucket(client, &bucket).await
        }
        S3Path::Local(_) => {
            anyhow::bail!("mb command requires an S3 URI (s3://BUCKET)")
        }
    }
}

async fn create_bucket(client: &Client, bucket: &str) -> anyhow::Result<()> {
    client.create_bucket().bucket(bucket).send().await?;

    println!("make_bucket: s3://{}", bucket);
    Ok(())
}
