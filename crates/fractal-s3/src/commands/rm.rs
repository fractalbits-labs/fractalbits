use aws_sdk_s3::Client;

use crate::s3_path::S3Path;

pub async fn execute(
    client: &Client,
    s3_uri: &str,
    recursive: bool,
    quiet: bool,
) -> anyhow::Result<()> {
    let path = S3Path::parse(s3_uri)?;

    match path {
        S3Path::S3 { bucket, key } => {
            if key.is_empty() {
                if recursive {
                    // Allow recursive delete of all objects in bucket
                    delete_recursive(client, &bucket, "", quiet).await
                } else {
                    anyhow::bail!("rm command requires an object key (s3://BUCKET/KEY)");
                }
            } else if recursive {
                delete_recursive(client, &bucket, &key, quiet).await
            } else {
                delete_object(client, &bucket, &key, quiet).await
            }
        }
        S3Path::Local(_) => {
            anyhow::bail!("rm command requires an S3 URI (s3://BUCKET/KEY)")
        }
    }
}

async fn delete_object(
    client: &Client,
    bucket: &str,
    key: &str,
    quiet: bool,
) -> anyhow::Result<()> {
    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    if !quiet {
        println!("delete: s3://{}/{}", bucket, key);
    }
    Ok(())
}

async fn delete_recursive(
    client: &Client,
    bucket: &str,
    prefix: &str,
    quiet: bool,
) -> anyhow::Result<()> {
    let mut continuation_token: Option<String> = None;
    let mut deleted_count = 0;

    loop {
        let mut req = client.list_objects_v2().bucket(bucket).prefix(prefix);

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

                if !quiet {
                    println!("delete: s3://{}/{}", bucket, key);
                }
                deleted_count += 1;
            }
        }

        if resp.is_truncated() == Some(true) {
            continuation_token = resp.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    if deleted_count == 0 && !quiet {
        println!("No objects found with prefix: {}", prefix);
    }

    Ok(())
}
