use cmd_lib::*;

use super::DeployTarget;

/// Build cloud storage URI from bucket name and deploy target
pub fn bucket_uri(bucket: &str, target: DeployTarget) -> String {
    match target {
        DeployTarget::Gcp => format!("gs://{bucket}"),
        _ => format!("s3://{bucket}"),
    }
}

/// Build full object URI
pub fn object_uri(bucket: &str, key: &str, target: DeployTarget) -> String {
    format!("{}/{key}", bucket_uri(bucket, target))
}

/// Download a file from cloud storage (dispatches on URI prefix)
pub fn download_file(cloud_uri: &str, local_path: &str) -> CmdResult {
    if cloud_uri.starts_with("gs://") {
        run_cmd!(gcloud storage cp $cloud_uri $local_path)
    } else {
        run_cmd!(aws s3 cp --no-progress $cloud_uri $local_path)
    }
}

/// Download object as string
pub fn cat(cloud_uri: &str) -> FunResult {
    if cloud_uri.starts_with("gs://") {
        run_fun!(gcloud storage cat $cloud_uri 2>/dev/null)
    } else {
        run_fun!(aws s3 cp $cloud_uri - 2>/dev/null)
    }
}

/// Upload string content
pub fn upload_string(content: &str, cloud_uri: &str) -> CmdResult {
    if cloud_uri.starts_with("gs://") {
        run_cmd!(echo -n $content | gcloud storage cp - $cloud_uri --quiet)
    } else {
        run_cmd!(echo -n $content | aws s3 cp - $cloud_uri --quiet)
    }
}

/// Upload a local file
pub fn upload_file(local_path: &str, cloud_uri: &str) -> CmdResult {
    if cloud_uri.starts_with("gs://") {
        run_cmd!(gcloud storage cp $local_path $cloud_uri)
    } else {
        run_cmd!(aws s3 cp $local_path $cloud_uri)
    }
}

/// Sync local directory up to cloud
pub fn sync_up(local_dir: &str, cloud_uri: &str) -> CmdResult {
    if cloud_uri.starts_with("gs://") {
        run_cmd!(gcloud storage rsync -r $local_dir $cloud_uri)
    } else {
        run_cmd!(aws s3 sync $local_dir $cloud_uri)
    }
}

/// Sync local dir up with include/exclude filters (for AWS CPU-specific uploads)
pub fn sync_up_filtered(
    local_dir: &str,
    cloud_uri: &str,
    includes: &[&str],
    excludes: &[&str],
) -> CmdResult {
    // Build filter args: aws s3 sync processes filters in order
    let mut filter_args: Vec<String> = Vec::new();
    for exc in excludes {
        filter_args.push("--exclude".to_string());
        filter_args.push(exc.to_string());
    }
    for inc in includes {
        filter_args.push("--include".to_string());
        filter_args.push(inc.to_string());
    }
    run_cmd!(aws s3 sync $local_dir $cloud_uri $[filter_args])
}

/// Sync cloud directory down to local
pub fn sync_down(cloud_uri: &str, local_dir: &str) -> CmdResult {
    if cloud_uri.starts_with("gs://") {
        run_cmd!(gcloud storage rsync -r $cloud_uri $local_dir)
    } else {
        run_cmd!(aws s3 cp --no-progress $cloud_uri $local_dir --recursive)
    }
}

/// Check if object exists
pub fn head_object(bucket: &str, key: &str, target: DeployTarget) -> bool {
    match target {
        DeployTarget::Gcp => {
            let gs_path = format!("gs://{bucket}/{key}");
            run_cmd!(gcloud storage ls $gs_path 2>/dev/null).is_ok()
        }
        _ => run_cmd!(aws s3api head-object --bucket $bucket --key $key &>/dev/null).is_ok(),
    }
}

/// List objects under prefix (returns raw listing output)
pub fn list_objects(bucket: &str, prefix: &str, target: DeployTarget) -> FunResult {
    match target {
        DeployTarget::Gcp => {
            let gs_prefix = format!("gs://{bucket}/{prefix}");
            run_fun!(gcloud storage ls $gs_prefix 2>/dev/null)
        }
        _ => {
            let s3_prefix = format!("s3://{bucket}/{prefix}");
            run_fun!(aws s3 ls $s3_prefix 2>/dev/null)
        }
    }
}

/// Delete an object from cloud storage
pub fn delete_object(bucket: &str, key: &str, target: DeployTarget) -> CmdResult {
    match target {
        DeployTarget::Gcp => {
            let gs_path = format!("gs://{bucket}/{key}");
            run_cmd!(gcloud storage rm $gs_path 2>/dev/null)
        }
        _ => {
            let s3_path = format!("s3://{bucket}/{key}");
            run_cmd!(aws s3 rm $s3_path &>/dev/null)
        }
    }
}

/// Ensure bucket exists, create if not
pub fn ensure_bucket(bucket: &str, target: DeployTarget) -> CmdResult {
    match target {
        DeployTarget::Gcp => {
            let gs_bucket = format!("gs://{bucket}");
            if run_cmd!(gcloud storage ls $gs_bucket &>/dev/null).is_err() {
                run_cmd!(gcloud storage buckets create --location=us-central1 $gs_bucket)?;
            }
            Ok(())
        }
        _ => {
            let s3_bucket = format!("s3://{bucket}");
            if run_cmd!(aws s3api head-bucket --bucket $bucket &>/dev/null).is_err() {
                run_cmd!(aws s3 mb $s3_bucket)?;
            }
            Ok(())
        }
    }
}
