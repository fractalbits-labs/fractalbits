use aws_sdk_s3::Client;

use crate::s3_path::S3Path;

pub async fn execute(
    client: &Client,
    s3_uri: Option<&str>,
    recursive: bool,
    human_readable: bool,
) -> anyhow::Result<()> {
    match s3_uri {
        None => list_buckets(client).await,
        Some(uri) => {
            let path = S3Path::parse(uri)?;
            match path {
                S3Path::S3 { bucket, key } => {
                    list_objects(client, &bucket, &key, recursive, human_readable).await
                }
                S3Path::Local(_) => {
                    anyhow::bail!("ls command requires an S3 URI or no argument")
                }
            }
        }
    }
}

async fn list_buckets(client: &Client) -> anyhow::Result<()> {
    let resp = client.list_buckets().send().await?;

    for bucket in resp.buckets() {
        if let Some(name) = bucket.name() {
            if let Some(creation_date) = bucket.creation_date() {
                println!(
                    "{} s3://{}",
                    creation_date.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)?,
                    name
                );
            } else {
                println!("s3://{}", name);
            }
        }
    }

    Ok(())
}

async fn list_objects(
    client: &Client,
    bucket: &str,
    prefix: &str,
    recursive: bool,
    human_readable: bool,
) -> anyhow::Result<()> {
    let delimiter = if recursive { None } else { Some("/") };

    let mut continuation_token: Option<String> = None;

    loop {
        let mut req = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .set_delimiter(delimiter.map(|s| s.to_string()));

        if let Some(token) = continuation_token.take() {
            req = req.continuation_token(token);
        }

        let resp = req.send().await?;

        for prefix_entry in resp.common_prefixes() {
            if let Some(p) = prefix_entry.prefix() {
                println!("                           PRE {}", p);
            }
        }

        for obj in resp.contents() {
            let key = obj.key().unwrap_or("");
            let size = obj.size().unwrap_or(0);
            let last_modified = obj.last_modified();

            let size_str = if human_readable {
                format_human_readable_size(size)
            } else {
                format!("{:>10}", size)
            };

            if let Some(lm) = last_modified {
                println!(
                    "{} {} {}",
                    lm.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)?,
                    size_str,
                    key
                );
            } else {
                println!("{} {}", size_str, key);
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

fn format_human_readable_size(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = 1024 * KB;
    const GB: i64 = 1024 * MB;
    const TB: i64 = 1024 * GB;

    if bytes >= TB {
        format!("{:>6.1} TiB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:>6.1} GiB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:>6.1} MiB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:>6.1} KiB", bytes as f64 / KB as f64)
    } else {
        format!("{:>6} B", bytes)
    }
}
