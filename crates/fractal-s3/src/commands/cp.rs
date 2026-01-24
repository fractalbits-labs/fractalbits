use std::path::Path;

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::client::S3ClientConfig;
use crate::s3_path::S3Path;

pub async fn execute(
    client: &Client,
    _config: &S3ClientConfig,
    src: &str,
    dst: &str,
    recursive: bool,
    _exclude: Option<&str>,
    _include: Option<&str>,
) -> anyhow::Result<()> {
    let src_path = S3Path::parse(src)?;
    let dst_path = S3Path::parse(dst)?;

    match (&src_path, &dst_path) {
        (S3Path::Local(local_src), S3Path::S3 { bucket, key }) => {
            if recursive && local_src.is_dir() {
                upload_directory(client, local_src, bucket, key).await
            } else {
                upload_file(client, local_src, bucket, key).await
            }
        }
        (S3Path::S3 { bucket, key }, S3Path::Local(local_dst)) => {
            if recursive {
                download_prefix(client, bucket, key, local_dst).await
            } else {
                download_file(client, bucket, key, local_dst).await
            }
        }
        (
            S3Path::S3 {
                bucket: src_bucket,
                key: src_key,
            },
            S3Path::S3 {
                bucket: dst_bucket,
                key: dst_key,
            },
        ) => {
            if recursive {
                copy_prefix(client, src_bucket, src_key, dst_bucket, dst_key).await
            } else {
                copy_object(client, src_bucket, src_key, dst_bucket, dst_key).await
            }
        }
        (S3Path::Local(_), S3Path::Local(_)) => {
            anyhow::bail!("Local to local copy is not supported. Use cp command instead.")
        }
    }
}

async fn upload_file(
    client: &Client,
    local_path: &Path,
    bucket: &str,
    key: &str,
) -> anyhow::Result<()> {
    let key = if key.is_empty() || key.ends_with('/') {
        let filename = local_path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("Invalid filename"))?
            .to_string_lossy();
        format!("{}{}", key, filename)
    } else {
        key.to_string()
    };

    let body = ByteStream::from_path(local_path).await?;

    client
        .put_object()
        .bucket(bucket)
        .key(&key)
        .body(body)
        .send()
        .await?;

    println!(
        "upload: {} to s3://{}/{}",
        local_path.display(),
        bucket,
        key
    );
    Ok(())
}

async fn upload_directory(
    client: &Client,
    local_path: &Path,
    bucket: &str,
    prefix: &str,
) -> anyhow::Result<()> {
    let mut stack = vec![(local_path.to_path_buf(), prefix.to_string())];

    while let Some((current_path, current_prefix)) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&current_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let file_name = entry.file_name().to_string_lossy().to_string();

            let new_key = if current_prefix.is_empty() || current_prefix.ends_with('/') {
                format!("{}{}", current_prefix, file_name)
            } else {
                format!("{}/{}", current_prefix, file_name)
            };

            if path.is_dir() {
                let dir_key = format!("{}/", new_key);
                stack.push((path, dir_key));
            } else {
                upload_file(client, &path, bucket, &new_key).await?;
            }
        }
    }

    Ok(())
}

async fn download_file(
    client: &Client,
    bucket: &str,
    key: &str,
    local_path: &Path,
) -> anyhow::Result<()> {
    let local_path = if local_path.is_dir() {
        let filename = Path::new(key)
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("Invalid key - no filename"))?;
        local_path.join(filename)
    } else {
        local_path.to_path_buf()
    };

    if let Some(parent) = local_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let resp = client.get_object().bucket(bucket).key(key).send().await?;

    let mut file = File::create(&local_path).await?;
    let mut stream = resp.body;

    while let Some(chunk) = stream.next().await {
        let data = chunk?;
        file.write_all(&data).await?;
    }

    println!(
        "download: s3://{}/{} to {}",
        bucket,
        key,
        local_path.display()
    );
    Ok(())
}

async fn download_prefix(
    client: &Client,
    bucket: &str,
    prefix: &str,
    local_path: &Path,
) -> anyhow::Result<()> {
    let mut continuation_token: Option<String> = None;

    loop {
        let mut req = client.list_objects_v2().bucket(bucket).prefix(prefix);

        if let Some(token) = continuation_token.take() {
            req = req.continuation_token(token);
        }

        let resp = req.send().await?;

        for obj in resp.contents() {
            if let Some(key) = obj.key() {
                let relative_key = key.strip_prefix(prefix).unwrap_or(key);
                let relative_key = relative_key.trim_start_matches('/');

                let target_path = if relative_key.is_empty() {
                    let filename = Path::new(key)
                        .file_name()
                        .map(|f| f.to_string_lossy().to_string())
                        .unwrap_or_else(|| "file".to_string());
                    local_path.join(filename)
                } else {
                    local_path.join(relative_key)
                };

                download_file(client, bucket, key, &target_path).await?;
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

async fn copy_object(
    client: &Client,
    src_bucket: &str,
    src_key: &str,
    dst_bucket: &str,
    dst_key: &str,
) -> anyhow::Result<()> {
    let dst_key = if dst_key.is_empty() || dst_key.ends_with('/') {
        let filename = Path::new(src_key)
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("Invalid source key - no filename"))?
            .to_string_lossy();
        format!("{}{}", dst_key, filename)
    } else {
        dst_key.to_string()
    };

    let copy_source = format!("{}/{}", src_bucket, src_key);

    client
        .copy_object()
        .bucket(dst_bucket)
        .key(&dst_key)
        .copy_source(&copy_source)
        .send()
        .await?;

    println!(
        "copy: s3://{}/{} to s3://{}/{}",
        src_bucket, src_key, dst_bucket, dst_key
    );
    Ok(())
}

async fn copy_prefix(
    client: &Client,
    src_bucket: &str,
    src_prefix: &str,
    dst_bucket: &str,
    dst_prefix: &str,
) -> anyhow::Result<()> {
    let mut continuation_token: Option<String> = None;

    loop {
        let mut req = client
            .list_objects_v2()
            .bucket(src_bucket)
            .prefix(src_prefix);

        if let Some(token) = continuation_token.take() {
            req = req.continuation_token(token);
        }

        let resp = req.send().await?;

        for obj in resp.contents() {
            if let Some(key) = obj.key() {
                let relative_key = key.strip_prefix(src_prefix).unwrap_or(key);
                let relative_key = relative_key.trim_start_matches('/');

                let new_key = if dst_prefix.is_empty() || dst_prefix.ends_with('/') {
                    format!("{}{}", dst_prefix, relative_key)
                } else {
                    format!("{}/{}", dst_prefix, relative_key)
                };

                copy_object(client, src_bucket, key, dst_bucket, &new_key).await?;
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
