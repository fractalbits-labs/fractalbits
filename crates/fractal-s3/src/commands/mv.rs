use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_signature::{EMPTY_PAYLOAD_HASH, SigningParams, sign_request};
use chrono::Utc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::client::{S3ClientConfig, get_credentials_info};
use crate::s3_path::S3Path;

pub async fn execute(
    client: &Client,
    config: &S3ClientConfig,
    src: &str,
    dst: &str,
    recursive: bool,
) -> anyhow::Result<()> {
    let src_path = S3Path::parse(src)?;
    let dst_path = S3Path::parse(dst)?;

    match (&src_path, &dst_path) {
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
            if src_bucket == dst_bucket && src_key.ends_with('/') && dst_key.ends_with('/') {
                rename_folder(config, src_bucket, src_key, dst_key).await
            } else if recursive {
                move_prefix(client, config, src_bucket, src_key, dst_bucket, dst_key).await
            } else {
                move_object(client, config, src_bucket, src_key, dst_bucket, dst_key).await
            }
        }
        (S3Path::Local(local_src), S3Path::S3 { bucket, key }) => {
            upload_and_delete(client, local_src, bucket, key, recursive).await
        }
        (S3Path::S3 { bucket, key }, S3Path::Local(local_dst)) => {
            download_and_delete(client, bucket, key, local_dst, recursive).await
        }
        (S3Path::Local(_), S3Path::Local(_)) => {
            anyhow::bail!("Local to local move is not supported. Use mv command instead.")
        }
    }
}

async fn rename_folder(
    config: &S3ClientConfig,
    bucket: &str,
    src_path: &str,
    dst_path: &str,
) -> anyhow::Result<()> {
    let creds = get_credentials_info(config).await?;

    let http_client = reqwest::Client::new();
    let now = Utc::now();

    let host = creds
        .endpoint_url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or("localhost:8080");

    // Ensure consistent path formatting with leading /
    let src_path_normalized = if src_path.starts_with('/') {
        src_path.to_string()
    } else {
        format!("/{}", src_path)
    };
    let dst_path_normalized = if dst_path.starts_with('/') {
        dst_path.to_string()
    } else {
        format!("/{}", dst_path)
    };

    let uri = format!("/{}{}", bucket, dst_path_normalized);
    let url = format!("{}{}", creds.endpoint_url, uri);

    let mut query_params = BTreeMap::new();
    query_params.insert("renameFolder".to_string(), String::new());
    query_params.insert("src-path".to_string(), src_path_normalized.clone());

    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();

    let mut headers_map = BTreeMap::new();
    headers_map.insert("host".to_string(), host.to_string());
    headers_map.insert("x-amz-date".to_string(), amz_date.clone());
    headers_map.insert(
        "x-amz-content-sha256".to_string(),
        EMPTY_PAYLOAD_HASH.to_string(),
    );

    let signed_headers: BTreeSet<String> = headers_map.keys().cloned().collect();
    let canonical_headers: Vec<String> = headers_map
        .iter()
        .map(|(k, v)| format!("{}:{}", k, v))
        .collect();

    let signing_params = SigningParams {
        access_key_id: creds.access_key_id.clone(),
        secret_access_key: creds.secret_access_key.clone(),
        region: creds.region.clone(),
        service: "s3".to_string(),
        datetime: now,
    };

    let auth_header = sign_request(
        &signing_params,
        "PUT",
        &uri,
        &query_params,
        &canonical_headers,
        &signed_headers,
        EMPTY_PAYLOAD_HASH,
    )?;

    let query_string = format!(
        "renameFolder&src-path={}",
        urlencoding::encode(&src_path_normalized)
    );

    let response = http_client
        .put(format!("{}?{}", url, query_string))
        .header("Host", host)
        .header("X-Amz-Date", &amz_date)
        .header("X-Amz-Content-SHA256", EMPTY_PAYLOAD_HASH)
        .header("Authorization", auth_header)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("renameFolder failed: {} - {}", status, body);
    }

    println!(
        "move: s3://{}/{} to s3://{}/{} (atomic folder rename)",
        bucket, src_path, bucket, dst_path
    );
    Ok(())
}

async fn move_object(
    client: &Client,
    config: &S3ClientConfig,
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

    // Use atomic renameObject for same-bucket moves
    if src_bucket == dst_bucket {
        rename_object(config, src_bucket, src_key, &dst_key).await?;
        println!(
            "move: s3://{}/{} to s3://{}/{} (atomic rename)",
            src_bucket, src_key, dst_bucket, dst_key
        );
        return Ok(());
    }

    // Cross-bucket move: copy + delete
    let copy_source = format!("{}/{}", src_bucket, src_key);

    client
        .copy_object()
        .bucket(dst_bucket)
        .key(&dst_key)
        .copy_source(&copy_source)
        .send()
        .await?;

    client
        .delete_object()
        .bucket(src_bucket)
        .key(src_key)
        .send()
        .await?;

    println!(
        "move: s3://{}/{} to s3://{}/{}",
        src_bucket, src_key, dst_bucket, dst_key
    );
    Ok(())
}

async fn rename_object(
    config: &S3ClientConfig,
    bucket: &str,
    src_key: &str,
    dst_key: &str,
) -> anyhow::Result<()> {
    let creds = get_credentials_info(config).await?;

    let http_client = reqwest::Client::new();
    let now = Utc::now();

    let host = creds
        .endpoint_url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or("localhost:8080");

    // Ensure consistent path formatting with leading /
    let src_path = if src_key.starts_with('/') {
        src_key.to_string()
    } else {
        format!("/{}", src_key)
    };
    let dst_path = if dst_key.starts_with('/') {
        dst_key.to_string()
    } else {
        format!("/{}", dst_key)
    };

    let uri = format!("/{}{}", bucket, dst_path);
    let url = format!("{}{}", creds.endpoint_url, uri);

    let mut query_params = BTreeMap::new();
    query_params.insert("renameObject".to_string(), String::new());
    query_params.insert("src-path".to_string(), src_path.clone());

    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();

    let mut headers_map = BTreeMap::new();
    headers_map.insert("host".to_string(), host.to_string());
    headers_map.insert("x-amz-date".to_string(), amz_date.clone());
    headers_map.insert(
        "x-amz-content-sha256".to_string(),
        EMPTY_PAYLOAD_HASH.to_string(),
    );

    let signed_headers: BTreeSet<String> = headers_map.keys().cloned().collect();
    let canonical_headers: Vec<String> = headers_map
        .iter()
        .map(|(k, v)| format!("{}:{}", k, v))
        .collect();

    let signing_params = SigningParams {
        access_key_id: creds.access_key_id.clone(),
        secret_access_key: creds.secret_access_key.clone(),
        region: creds.region.clone(),
        service: "s3".to_string(),
        datetime: now,
    };

    let auth_header = sign_request(
        &signing_params,
        "PUT",
        &uri,
        &query_params,
        &canonical_headers,
        &signed_headers,
        EMPTY_PAYLOAD_HASH,
    )?;

    let query_string = format!("renameObject&src-path={}", urlencoding::encode(&src_path));

    let response = http_client
        .put(format!("{}?{}", url, query_string))
        .header("Host", host)
        .header("X-Amz-Date", &amz_date)
        .header("X-Amz-Content-SHA256", EMPTY_PAYLOAD_HASH)
        .header("Authorization", auth_header)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("renameObject failed: {} - {}", status, body);
    }

    Ok(())
}

async fn move_prefix(
    client: &Client,
    config: &S3ClientConfig,
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

                move_object(client, config, src_bucket, key, dst_bucket, &new_key).await?;
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

async fn upload_and_delete(
    client: &Client,
    local_path: &Path,
    bucket: &str,
    key: &str,
    recursive: bool,
) -> anyhow::Result<()> {
    if recursive && local_path.is_dir() {
        upload_directory_and_delete(client, local_path, bucket, key).await
    } else {
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

        tokio::fs::remove_file(local_path).await?;

        println!("move: {} to s3://{}/{}", local_path.display(), bucket, key);
        Ok(())
    }
}

async fn upload_directory_and_delete(
    client: &Client,
    local_path: &Path,
    bucket: &str,
    prefix: &str,
) -> anyhow::Result<()> {
    let mut stack = vec![(local_path.to_path_buf(), prefix.to_string())];
    let mut files_to_delete = Vec::new();
    let mut dirs_to_delete = Vec::new();

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
                stack.push((path.clone(), dir_key));
                dirs_to_delete.push(path);
            } else {
                let body = ByteStream::from_path(&path).await?;

                client
                    .put_object()
                    .bucket(bucket)
                    .key(&new_key)
                    .body(body)
                    .send()
                    .await?;

                println!("move: {} to s3://{}/{}", path.display(), bucket, new_key);
                files_to_delete.push(path);
            }
        }
    }

    for file in files_to_delete {
        tokio::fs::remove_file(file).await?;
    }

    dirs_to_delete.reverse();
    for dir in dirs_to_delete {
        tokio::fs::remove_dir(&dir).await.ok();
    }
    tokio::fs::remove_dir(local_path).await.ok();

    Ok(())
}

async fn download_and_delete(
    client: &Client,
    bucket: &str,
    key: &str,
    local_path: &Path,
    recursive: bool,
) -> anyhow::Result<()> {
    if recursive {
        download_prefix_and_delete(client, bucket, key, local_path).await
    } else {
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

        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;

        println!("move: s3://{}/{} to {}", bucket, key, local_path.display());
        Ok(())
    }
}

async fn download_prefix_and_delete(
    client: &Client,
    bucket: &str,
    prefix: &str,
    local_path: &Path,
) -> anyhow::Result<()> {
    let mut continuation_token: Option<String> = None;
    let mut keys_to_delete = Vec::new();

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

                if let Some(parent) = target_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }

                let get_resp = client.get_object().bucket(bucket).key(key).send().await?;

                let mut file = File::create(&target_path).await?;
                let mut stream = get_resp.body;

                while let Some(chunk) = stream.next().await {
                    let data = chunk?;
                    file.write_all(&data).await?;
                }

                println!("move: s3://{}/{} to {}", bucket, key, target_path.display());
                keys_to_delete.push(key.to_string());
            }
        }

        if resp.is_truncated() == Some(true) {
            continuation_token = resp.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    for key in keys_to_delete {
        client
            .delete_object()
            .bucket(bucket)
            .key(&key)
            .send()
            .await?;
    }

    Ok(())
}
