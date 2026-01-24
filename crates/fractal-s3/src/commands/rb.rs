use aws_sdk_s3::Client;

use crate::s3_path::S3Path;

pub async fn execute(client: &Client, s3_uri: &str, force: bool) -> anyhow::Result<()> {
    let path = S3Path::parse(s3_uri)?;

    match path {
        S3Path::S3 { bucket, key } => {
            if !key.is_empty() {
                anyhow::bail!("rb command only accepts bucket names (s3://BUCKET), not keys");
            }
            remove_bucket(client, &bucket, force).await
        }
        S3Path::Local(_) => {
            anyhow::bail!("rb command requires an S3 URI (s3://BUCKET)")
        }
    }
}

async fn remove_bucket(client: &Client, bucket: &str, force: bool) -> anyhow::Result<()> {
    if force {
        delete_all_objects(client, bucket).await?;
    }

    client.delete_bucket().bucket(bucket).send().await?;

    println!("remove_bucket: s3://{}", bucket);
    Ok(())
}

async fn delete_all_objects(client: &Client, bucket: &str) -> anyhow::Result<()> {
    let mut continuation_token: Option<String> = None;

    loop {
        let mut req = client.list_objects_v2().bucket(bucket);

        if let Some(token) = continuation_token.take() {
            req = req.continuation_token(token);
        }

        let resp = req.send().await?;

        for obj in resp.contents() {
            if let Some(key) = obj.key() {
                client
                    .delete_object()
                    .bucket(bucket)
                    .key(key)
                    .send()
                    .await?;
            }
        }

        if resp.is_truncated() == Some(true) {
            continuation_token = resp.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    Ok(())
}
